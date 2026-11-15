"""MT5 gateway: one MetaTrader 5 terminal, many bots.

    bots --HTTP/1.1 keep-alive, bearer token--> gateway --localhost socket | files--> EA --> MT5

Standard library only, so it costs ~15 MB next to a terminal on a 1 GB machine.

The EA holds one persistent localhost socket to us (~1 ms), and falls back to files in the
terminal's own sandbox while MT5 refuses the socket (see Link). Each bot is a
client in clients.json with its own token and magic number; the EA only ever lets a client
see or touch positions carrying that client's magic.

    python3 gateway.py --clients ~/mt5gw/clients.json --state ~/mt5gw/state --ea-dir <MQL5/Files/gw>
"""
import argparse
import itertools
import json
import os
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

EA_TIMEOUT_S = 15.0        # an order round trip to the broker is ~ms; 15 s means the terminal is stuck
EXPIRE_S = 5               # the EA refuses a command it receives later than this


def log(**kw):
    print(json.dumps({"t": round(time.time(), 3), **kw}), flush=True)


class SocketLink:
    """The EA over one persistent localhost TCP connection, NDJSON both ways: the fast path
    (~1 ms). Commands are matched to replies by id; a reconnecting EA replaces the old link."""

    def __init__(self, host, port):
        self.host, self.port = host, port
        self.conn = None
        self.wlock = threading.Lock()
        self.pending = {}
        self.ids = None                        # shared with the other link, set by Link
        self.hello, self.hb, self.hb_t = {}, {}, 0.0
        self.on_connect, self.events = [], []

    def serve(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind((self.host, self.port))
        srv.listen(2)
        log(ea_listen=f"{self.host}:{self.port}")
        while True:
            c, addr = srv.accept()
            c.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            old, self.conn = self.conn, c
            if old:
                try:
                    old.close()
                except OSError:
                    pass
            log(ea_connected=f"socket {addr}")
            threading.Thread(target=self._read, args=(c,), daemon=True).start()

    def _read(self, c):
        buf = b""
        try:
            while True:
                chunk = c.recv(65536)
                if not chunk:
                    break
                buf += chunk
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    if line.strip():
                        self._dispatch(json.loads(line))
        except (OSError, ValueError) as e:
            log(ea_read_error=str(e)[:200])
        finally:
            if self.conn is c:
                self.conn = None
                log(ea_socket_closed=True)
                for ev in list(self.pending.values()):
                    ev[1] = {"ok": False, "error": "EA disconnected", "transport": True}
                    ev[0].set()

    def _dispatch(self, m):
        if "re" in m:
            ev = self.pending.get(m["re"])
            if ev:
                ev[1] = m
                ev[0].set()
        elif m.get("ev") == "hb":
            self.hb, self.hb_t = m, time.time()
        elif m.get("ev") == "hello":
            self.hello = m
            log(ea_hello=m)
            for cb in self.on_connect:
                threading.Thread(target=cb, daemon=True).start()
        else:
            self.events = (self.events + [{**m, "rx": time.time()}])[-200:]
            log(ea_event=m)

    @property
    def connected(self):
        return self.conn is not None

    def call(self, op, timeout=EA_TIMEOUT_S, **kw):
        c = self.conn
        if c is None:
            return {"ok": False, "error": "EA not connected", "transport": True}
        i = next(self.ids)
        ev = [threading.Event(), None]
        self.pending[i] = ev
        msg = {"id": i, "op": op, "exp": int(time.time()) + EXPIRE_S, **kw}
        try:
            with self.wlock:
                c.sendall((json.dumps(msg, separators=(",", ":")) + "\n").encode())
            if not ev[0].wait(timeout):
                return {"ok": False, "error": f"EA timeout after {timeout}s ({op})", "transport": True}
            return ev[1]
        except OSError as e:
            return {"ok": False, "error": f"EA send failed: {e}", "transport": True}
        finally:
            self.pending.pop(i, None)


class FileLink:
    """The EA, through files in the terminal's sandbox (MQL5/Files/gw): requests go to in/ as
    <id>.req, replies come back in out/ as <id>.rep, unsolicited events as out/ev-*.evt, and the
    heartbeat is overwritten in status.json. Every write on both sides is .tmp + rename.

    Files rather than a socket because MT5 only lets an EA open a socket to an address on its
    WebRequest allow-list, and it forgot that list on every terminal restart (error 4014, the EA
    cut off until someone re-added it by hand in the Options dialog). The sandbox needs no
    permission at all. Cost: a few ms per round trip against ~1, on a path whose next hop is
    a 260 ms tunnel and a 2.5-5 s video delay.
    """
    FAST_S, IDLE_S = 0.0005, 0.05   # poll out/ fast only while a reply is awaited

    def __init__(self, root):
        self.root = root
        self.inbox, self.outbox = os.path.join(root, "in"), os.path.join(root, "out")
        for d in (self.inbox, self.outbox):
            os.makedirs(d, exist_ok=True)
        self.status = os.path.join(root, "status.json")
        self.pending = {}                   # id -> [Event, reply]
        self.ids = None                     # shared with the socket link, set by Link
        self.hello, self.hb, self.hb_t = {}, {}, 0.0
        self.on_connect = []                # callbacks run when the EA (re)appears
        self.events = []                    # recent unsolicited EA events (forced closes...)
        self.wake = threading.Event()
        self._was_up = False
        self.host, self.port = "files", root  # for log lines that used to name a socket

    def serve(self):
        # Leftovers from before a restart: an unanswered request is stale by definition (the
        # EA refuses it past its exp anyway), and an old reply has nobody waiting for it.
        for d in (self.inbox, self.outbox):
            for f in os.listdir(d):
                if f.endswith((".req", ".rep", ".tmp")):
                    try:
                        os.remove(os.path.join(d, f))
                    except OSError:
                        pass
        log(ea_link=f"files:{self.root}")
        while True:
            self._scan()
            self.wake.wait(self.FAST_S if self.pending else self.IDLE_S)
            self.wake.clear()

    def _scan(self):
        try:
            names = sorted(os.listdir(self.outbox))
        except OSError:
            names = []
        for f in names:
            path = os.path.join(self.outbox, f)
            if f.endswith(".rep") or f.endswith(".evt"):
                try:
                    with open(path, encoding="utf-8") as fh:
                        m = json.loads(fh.read())
                    os.remove(path)
                except (OSError, ValueError):
                    continue
                if f.endswith(".rep"):
                    ev = self.pending.get(m.get("re"))
                    if ev:
                        ev[1] = m
                        ev[0].set()
                elif m.get("ev") == "hello":
                    self.hello = m
                    log(ea_hello=m)
                    self._up()
                else:
                    self.events = (self.events + [{**m, "rx": time.time()}])[-200:]
                    log(ea_event=m)
        try:
            st = os.stat(self.status)
            if st.st_mtime > self.hb_t:
                with open(self.status, encoding="utf-8") as fh:
                    self.hb = json.loads(fh.read())
                self.hb_t = st.st_mtime
        except (OSError, ValueError):
            pass
        if self.connected and not self._was_up:
            self._up()
        elif not self.connected and self._was_up:
            self._was_up = False
            log(ea_disconnected=True)

    def _up(self):
        if not self._was_up:
            log(ea_connected="files")
        self._was_up = True
        for cb in self.on_connect:
            threading.Thread(target=cb, daemon=True).start()

    @property
    def connected(self):
        return self.hb_t > 0 and time.time() - self.hb_t < 5

    def call(self, op, timeout=EA_TIMEOUT_S, **kw):
        if not self.connected:
            return {"ok": False, "error": "EA not connected", "transport": True}
        i = next(self.ids)
        ev = [threading.Event(), None]
        self.pending[i] = ev
        msg = {"id": i, "op": op, "exp": int(time.time()) + EXPIRE_S, **kw}
        name = f"{i:012d}"
        tmp = os.path.join(self.inbox, name + ".tmp")
        try:
            with open(tmp, "w", encoding="utf-8") as fh:
                fh.write(json.dumps(msg, separators=(",", ":")))
            os.replace(tmp, os.path.join(self.inbox, name + ".req"))
            self.wake.set()
            if not ev[0].wait(timeout):
                return {"ok": False, "error": f"EA timeout after {timeout}s ({op})", "transport": True}
            return ev[1]
        except OSError as e:
            return {"ok": False, "error": f"EA request failed: {e}", "transport": True}
        finally:
            self.pending.pop(i, None)


class Link:
    """Both transports to the same EA. Requests go over the socket whenever the EA holds one
    (the fast path), otherwise over files; ids are shared, so a reply can never be mistaken
    across the two. The socket is refused while 127.0.0.1 is missing from MT5's WebRequest
    list (after every terminal restart, until deploy/executor/mt5-allowlist.sh puts it back), and
    in that window the files keep the bots trading."""

    def __init__(self, host, port, root):
        self.sock, self.files = SocketLink(host, port), FileLink(root)
        self.sock.ids = self.files.ids = itertools.count(1)
        self.on_connect = []
        self.sock.on_connect = self.files.on_connect = self.on_connect

    def serve(self):
        threading.Thread(target=self.sock.serve, daemon=True).start()
        self.files.serve()

    @property
    def connected(self):
        return self.sock.connected or self.files.connected

    @property
    def transport(self):
        return "socket" if self.sock.connected else "files" if self.files.connected else None

    @property
    def hello(self):
        return self.sock.hello if self.sock.connected else self.files.hello

    @property
    def hb(self):
        return self.sock.hb if self.sock.hb_t >= self.files.hb_t else self.files.hb

    @property
    def hb_t(self):
        return max(self.sock.hb_t, self.files.hb_t)

    @property
    def events(self):
        return sorted(self.sock.events + self.files.events, key=lambda e: e.get("rx", 0))

    def call(self, op, timeout=EA_TIMEOUT_S, **kw):
        return (self.sock if self.sock.connected else self.files).call(op, timeout, **kw)


class Idempotency:
    """client_id -> first reply, per client, persisted. A retried order returns the original
    result instead of trading twice; a duplicate arriving mid-flight waits for the first."""

    def __init__(self, path, keep=20000):
        self.path, self.keep = path, keep
        self.done, self.inflight = {}, {}
        self.lock = threading.Lock()
        if path and os.path.exists(path):
            for line in open(path):
                try:
                    r = json.loads(line)
                    self.done[(r["c"], r["id"])] = r["reply"]
                except ValueError:
                    continue

    def run(self, client, cid, fn):
        if not cid:
            return fn()
        key = (client, cid)
        with self.lock:
            if key in self.done:
                return {**self.done[key], "duplicate": True}
            ev = self.inflight.get(key)
            if ev is None:
                self.inflight[key] = threading.Event()
        if ev is not None:
            ev.wait(EA_TIMEOUT_S * 4)
            return {**self.done.get(key, {"ok": False, "error": "duplicate still in flight"}),
                    "duplicate": True}
        reply = None
        try:
            reply = fn()
            return reply
        finally:
            with self.lock:
                # Only an answer from the EA is final. "not connected"/"timeout" means we do
                # not know, and the retry must be allowed through: the EA itself refuses a
                # client_id it finds in an open position's or a recent deal's comment.
                if reply is not None and not reply.get("transport"):
                    self.done[key] = reply
                    if self.path:
                        with open(self.path, "a") as f:
                            f.write(json.dumps({"c": client, "id": cid, "reply": reply}) + "\n")
                    if len(self.done) > self.keep:
                        self._compact()
                self.inflight.pop(key).set()

    def _compact(self):
        items = list(self.done.items())[-self.keep // 2:]
        self.done = dict(items)
        if self.path:
            tmp = self.path + ".tmp"
            with open(tmp, "w") as f:
                for (c, i), r in items:
                    f.write(json.dumps({"c": c, "id": i, "reply": r}) + "\n")
            os.replace(tmp, self.path)


class Gateway:
    def __init__(self, clients_path, state_dir, ea_host, ea_port, ea_dir):
        self.clients_path = clients_path
        self.clients = self._load_clients()
        self.ea = Link(ea_host, ea_port, ea_dir)
        self.ea.on_connect.append(self.push_config)
        os.makedirs(state_dir, exist_ok=True)
        self.idem = Idempotency(os.path.join(state_dir, "idempotency.jsonl"))
        self.audit = open(os.path.join(state_dir, "audit.jsonl"), "a", buffering=1)

    def _load_clients(self):
        raw = json.load(open(self.clients_path))
        by_token = {}
        for name, c in raw.items():
            c = {"name": name, "symbols": ["*"], "max_lots": 0, "max_hold_s": 0,
                 "read_only": False, **c}
            by_token[c["token"]] = c
        magics = [c["magic"] for c in by_token.values()]
        assert len(magics) == len(set(magics)), "every client needs its own magic number"
        return by_token

    def push_config(self):
        """Tell a (re)connected EA which magics it manages and their EA-side hold limits —
        the backup time barrier works even when the bot that opened the trade is gone."""
        time.sleep(0.2)
        for c in self.clients.values():
            self.ea.call("cfg", magic=c["magic"], max_hold_s=c["max_hold_s"])

    def symbol_ok(self, client, sym):
        return "*" in client["symbols"] or sym in client["symbols"]

    # --- the API, one method per route ------------------------------------------------
    def handle(self, client, method, path, q, body):
        m, sym = client["magic"], (body.get("symbol") or q.get("symbol") or q.get("name") or "")
        if sym and not self.symbol_ok(client, sym):
            return 403, {"ok": False, "error": f"symbol {sym} not allowed for {client['name']}"}
        if method == "GET":
            if path == "/health":
                hb_age = time.time() - self.ea.hb_t if self.ea.hb_t else None
                return 200, {"ok": self.ea.connected and hb_age is not None and hb_age < 5,
                             "client": client["name"], "magic": m,
                             "ea_connected": self.ea.connected,
                             "transport": self.ea.transport,
                             "hb_age_s": round(hb_age, 2) if hb_age is not None else None,
                             "hello": self.ea.hello, "hb": self.ea.hb}
            if path == "/account":
                return 200, self.ea.call("account")
            if path == "/symbol":
                return 200, self.ea.call("symbol", symbol=sym)
            if path == "/tick":
                return 200, self.ea.call("tick", symbol=sym)
            if path == "/ticks":
                return 200, self.ea.call("ticks", symbol=sym, from_ms=int(q.get("from_ms", 0)),
                                         limit=min(int(q.get("limit", 1000)), 5000))
            if path == "/symbols":
                out = self.ea.call("symbols", offset=int(q.get("offset", 0)),
                                   limit=min(int(q.get("limit", 500)), 500))
                if out.get("ok") and "*" not in client["symbols"]:
                    out = {**out, "symbols": [s for s in out.get("symbols", [])
                                              if s.get("name") in client["symbols"]]}
                return 200, out
            if path == "/rates":
                if not sym:
                    return 400, {"ok": False, "error": "symbol required"}
                return 200, self.ea.call("rates", symbol=sym, tf=q.get("tf", "H1"),
                                         to_srv=int(q.get("to_srv", 0)),
                                         count=min(int(q.get("count", 2000)), 5000),
                                         release=int(q.get("release", 0)))
            if path == "/positions":
                return 200, self.ea.call("positions", magic=m, symbol=sym)
            if path == "/orders":
                # this client's pending orders; fills show up in /positions and /deals
                return 200, self.ea.call("orders", magic=m, symbol=sym)
            if path == "/deals":
                return 200, self.ea.call("deals", magic=m, symbol=sym,
                                         from_ms=int(q.get("from_ms", 0)),
                                         to_ms=int(q.get("to_ms", 0)))
            if path == "/balance":
                return 200, self.ea.call("balance", from_ms=int(q.get("from_ms", 0)))
            if path == "/events":
                return 200, {"ok": True, "events": [e for e in self.ea.events
                                                    if e.get("magic") in (m, None)]}
        if method == "POST" and client["read_only"]:
            # data collectors (the tick recorder) hold a token that can read prices and can
            # never place, change or close anything
            return 403, {"ok": False, "error": f"{client['name']} is read-only"}
        if method == "POST":
            cid = str(body.get("client_id") or "")[:24]
            if path == "/order":
                vol = float(body.get("volume") or 0)
                if client["max_lots"] and vol > client["max_lots"]:
                    return 400, {"ok": False, "error": f"volume {vol} > max_lots {client['max_lots']}"}
                return 200, self.idem.run(client["name"], cid, lambda: self.ea.call(
                    "order", magic=m, symbol=sym, side=body.get("side"), volume=vol,
                    sl=float(body.get("sl") or 0), tp=float(body.get("tp") or 0),
                    deviation=int(body.get("deviation") or 50), cid=cid))
            if path == "/pending":
                # a resting LIMIT order: buy below the ask, sell above the bid. The EA checks
                # side, price, stops/freeze level and the volume grid against the live tick
                vol = float(body.get("volume") or 0)
                if client["max_lots"] and vol > client["max_lots"]:
                    return 400, {"ok": False, "error": f"volume {vol} > max_lots {client['max_lots']}"}
                if not sym:
                    return 400, {"ok": False, "error": "symbol required"}
                return 200, self.idem.run(client["name"], cid, lambda: self.ea.call(
                    "pending", magic=m, symbol=sym, side=body.get("side"), volume=vol,
                    price=float(body.get("price") or 0), sl=float(body.get("sl") or 0),
                    tp=float(body.get("tp") or 0), cid=cid))
            if path == "/cancel":
                # naturally idempotent: a second cancel answers "not found"
                return 200, self.ea.call("cancel", magic=m, ticket=int(body.get("ticket") or 0))
            if path == "/close":
                return 200, self.idem.run(client["name"], cid, lambda: self.ea.call(
                    "close", magic=m, ticket=int(body["ticket"]),
                    volume=float(body.get("volume") or 0),
                    deviation=int(body.get("deviation") or 50), cid=cid))
            if path == "/modify":
                return 200, self.ea.call("modify", magic=m, ticket=int(body["ticket"]),
                                         sl=float(body.get("sl") or 0),
                                         tp=float(body.get("tp") or 0))
            if path == "/flatten":
                return 200, self.idem.run(client["name"], cid, lambda: self.ea.call(
                    "flatten", magic=m, symbol=sym, deviation=int(body.get("deviation") or 50),
                    cid=cid))
            if path == "/net":
                delta = float(body.get("delta") or 0)
                if client["max_lots"] and abs(delta) > client["max_lots"]:
                    return 400, {"ok": False, "error": f"delta {delta} > max_lots {client['max_lots']}"}
                return 200, self.idem.run(client["name"], cid, lambda: self.ea.call(
                    "net", magic=m, symbol=sym, delta=delta,
                    deviation=int(body.get("deviation") or 50), cid=cid))
        return 404, {"ok": False, "error": f"no route {method} {path}"}


def make_handler(gw):
    class H(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"          # keep-alive: one TCP/TLS setup per bot, not per call
        disable_nagle_algorithm = True         # headers and body are separate writes: +40 ms without

        def log_message(self, *a):
            pass

        def _go(self, method):
            t0 = time.time()
            u = urlparse(self.path)
            q = {k: v[0] for k, v in parse_qs(u.query).items()}
            n = int(self.headers.get("Content-Length") or 0)
            try:
                body = json.loads(self.rfile.read(n)) if n else {}
            except ValueError:
                return self._send(400, {"ok": False, "error": "bad JSON"})
            tok = (self.headers.get("Authorization") or "").removeprefix("Bearer ").strip()
            client = gw.clients.get(tok)
            if not client:
                return self._send(401, {"ok": False, "error": "bad token"})
            try:
                code, out = gw.handle(client, method, u.path, q, body)
            except Exception as e:           # a bad request must never take the gateway down
                code, out = 500, {"ok": False, "error": f"{type(e).__name__}: {e}"[:300]}
            out = {**out, "gw_ms": round((time.time() - t0) * 1000, 1)}
            if method == "POST":
                gw.audit.write(json.dumps({"t": t0, "client": client["name"], "path": u.path,
                                           "req": body, "reply": out}) + "\n")
            self._send(code, out)

        def _send(self, code, obj):
            data = json.dumps(obj, separators=(",", ":")).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_GET(self):
            self._go("GET")

        def do_POST(self):
            self._go("POST")

    return H


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--clients", default=os.getenv("MT5GW_CLIENTS", "/etc/mt5gw/clients.json"))
    p.add_argument("--state", default=os.getenv("MT5GW_STATE", "/var/lib/mt5gw"))
    p.add_argument("--listen", default=os.getenv("MT5GW_LISTEN", "127.0.0.1:8000"))
    p.add_argument("--ea", default=os.getenv("MT5GW_EA", "127.0.0.1:8765"))
    p.add_argument("--ea-dir", default=os.getenv("MT5GW_EA_DIR", os.path.join(os.getenv("MT5_DIR", ""), "MQL5/Files/gw")),
        help="the EA's sandbox folder (MQL5/Files/gw of the terminal)")
    a = p.parse_args()
    eh, ep = a.ea.rsplit(":", 1)
    gw = Gateway(a.clients, a.state, eh, int(ep), a.ea_dir)
    threading.Thread(target=gw.ea.serve, daemon=True).start()
    lh, lp = a.listen.rsplit(":", 1)
    srv = ThreadingHTTPServer((lh, int(lp)), make_handler(gw))
    srv.daemon_threads = True
    log(http_listen=a.listen, clients=[c["name"] for c in gw.clients.values()])
    srv.serve_forever()


if __name__ == "__main__":
    main()
