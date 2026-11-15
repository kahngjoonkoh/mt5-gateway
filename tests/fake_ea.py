"""A stand-in for MT5Gateway.mq5 that speaks the same file protocol in the same folder layout
(in/<id>.req -> out/<id>.rep, out/ev-*.evt, status.json; every write .tmp + rename), backed by
an in-memory hedging account. Lets the gateway, the client and any bot adapter be tested end to
end without a terminal. Semantics mirror the EA: per-magic isolation, client_id dedupe via
comments, net = close opposite oldest-first then open the remainder. Like the EA (2026-10-02),
net/flatten send every close at once and open only when all of them executed; a ticket in
`reject` never confirms, as a close the server swallowed would. Pending LIMIT orders fill when
set_price() crosses them (the deal carries the order's comment, a pending_fill event goes out)."""
import itertools
import json
import os
import socket
import threading
import time


class FakeEA:
    def __init__(self, root, hedging=True, price=100000.0, spread=10.0, port=None):
        self.root, self.hedging, self.port, self.sock = root, hedging, port, None
        self.inbox, self.outbox = os.path.join(root, "in"), os.path.join(root, "out")
        self.bid, self.ask = price - spread / 2, price + spread / 2
        self.positions, self.deals, self.orders = [], [], []
        self.tickets = itertools.count(1000)
        self.maxhold = {}
        self.reject = set()                      # tickets whose close never executes
        self.gone = {}                      # pending ticket -> (magic, what became of it)
        self.running = False
        self.evn = itertools.count()
        self.spec = {"contract_size": 1.0, "volume_min": 0.01, "volume_max": 100.0,
                     "volume_step": 0.01, "digits": 2, "point": 0.01,
                     "stops_level": 0, "freeze_level": 0}
        self.balance = 10000.0

    def start(self):
        os.makedirs(self.inbox, exist_ok=True)
        os.makedirs(self.outbox, exist_ok=True)
        self.running = True
        self._write(os.path.join(self.outbox, f"ev-{time.time():.6f}-{next(self.evn)}.evt"),
                    {"ev": "hello", "login": 123, "server": "Fake-Demo", "hedging": self.hedging,
                     "demo": True, "transport": "files"})
        threading.Thread(target=self._loop, daemon=True).start()
        if self.port:
            self.connect_socket()
        return self

    def connect_socket(self):
        """The EA's fast path: one localhost socket, NDJSON, hello on connect."""
        self.sock = socket.create_connection(("127.0.0.1", self.port))
        self._sock_send({"ev": "hello", "login": 123, "server": "Fake-Demo",
                         "hedging": self.hedging, "demo": True, "transport": "socket"})
        threading.Thread(target=self._sock_loop, args=(self.sock,), daemon=True).start()

    def drop_socket(self):
        s, self.sock = self.sock, None
        if s:
            s.close()

    def _sock_send(self, m):
        self.sock.sendall((json.dumps(m) + "\n").encode())

    def _sock_loop(self, s):
        buf = b""
        while True:
            try:
                c = s.recv(65536)
            except OSError:
                return
            if not c:
                return
            buf += c
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                m = json.loads(line)
                try:
                    s.sendall((json.dumps({"re": m["id"], **self.handle(m), "via": "socket"}) + "\n").encode())
                except OSError:
                    return

    def _event(self, m):
        """Unsolicited, like the EA's Send(): on the socket when held, else an .evt file."""
        if self.sock:
            try:
                return self._sock_send(m)
            except OSError:
                pass
        self._write(os.path.join(self.outbox, f"ev-{time.time():.6f}-{next(self.evn)}.evt"), m)

    def set_price(self, price, spread=None):
        """Move the market; any pending LIMIT order it reaches fills at its own price."""
        spread = self.ask - self.bid if spread is None else spread
        self.bid, self.ask = price - spread / 2, price + spread / 2
        for o in list(self.orders):
            if (o["side"] == "buy" and self.ask <= o["price"]) or \
                    (o["side"] == "sell" and self.bid >= o["price"]):
                self.orders.remove(o)
                f = self._open(o["symbol"], o["side"], o["volume"], o["magic"], o["comment"],
                               price=o["price"], order=o["ticket"])
                self._event({"ev": "pending_fill", "magic": o["magic"], "order": o["ticket"],
                             "deal": f["deal"], "position_id": f["position_id"],
                             "symbol": o["symbol"], "side": o["side"], "type": o["type"],
                             "volume": o["volume"], "price": o["price"], "volume_left": 0.0,
                             "comment": o["comment"], "time_msc": f["time_msc"]})
                self.gone[o["ticket"]] = (o["magic"], "filled")

    def stop(self):
        self.running = False
        self.drop_socket()

    @staticmethod
    def _write(path, obj):
        with open(path + ".tmp", "w") as f:
            f.write(json.dumps(obj))
        os.replace(path + ".tmp", path)

    def _loop(self):
        last_hb = 0.0
        while self.running:
            if time.time() - last_hb >= 0.5:
                self._write(os.path.join(self.root, "status.json"),
                            {"ev": "hb", "t": int(time.time()), "connected": True,
                             "trade_allowed": True, "equity": self.equity()})
                last_hb = time.time()
            for f in sorted(os.listdir(self.inbox)):
                if not f.endswith(".req"):
                    continue
                path = os.path.join(self.inbox, f)
                try:
                    m = json.load(open(path))
                    os.remove(path)
                except (OSError, ValueError):
                    continue
                self._write(os.path.join(self.outbox, f[:-4] + ".rep"), {"re": m["id"], **self.handle(m), "via": "files"})
            time.sleep(0.001)

    # --- account model -------------------------------------------------------------------
    def equity(self):
        return self.balance + sum(self._upl(p) for p in self.positions)

    def _upl(self, p):
        px = self.bid if p["side"] == "buy" else self.ask
        return (px - p["price_open"]) * p["volume"] * (1 if p["side"] == "buy" else -1)

    def _deal(self, sym, side, vol, magic, cid, entry, pos_id, profit=0.0, price=None, order=0):
        px = price if price is not None else self.ask if side == "buy" else self.bid
        d = {"deal": next(self.tickets), "order": order, "position_id": pos_id, "side": side,
             "entry": entry, "symbol": sym, "volume": vol, "price": px, "commission": 0.0,
             "fee": 0.0, "swap": 0.0, "profit": profit, "comment": cid, "magic": magic,
             "time_msc": int(time.time() * 1000)}
        self.deals.append(d)
        return {**d, "retcode": 10009, "bid": self.bid, "ask": self.ask}

    def _open(self, sym, side, vol, magic, cid, price=None, order=0):
        t = next(self.tickets)
        px = price if price is not None else self.ask if side == "buy" else self.bid
        self.positions.append({"ticket": t, "symbol": sym, "side": side, "volume": vol,
                               "price_open": px,
                               "magic": magic, "comment": cid, "time_msc": int(time.time() * 1000),
                               "sl": 0, "tp": 0, "profit": 0, "swap": 0})
        return self._deal(sym, side, vol, magic, cid, "in", t, price=price, order=order)

    def _close(self, p, vol, cid):
        vol = p["volume"] if not vol or vol >= p["volume"] else round(vol, 8)
        profit = self._upl(p) * vol / p["volume"]
        self.balance += profit
        p["volume"] = round(p["volume"] - vol, 8)
        if p["volume"] <= 1e-9:
            self.positions.remove(p)
        side = "sell" if p["side"] == "buy" else "buy"
        return self._deal(p["symbol"], side, vol, p["magic"], cid, "out", p["ticket"], profit)

    def _seen(self, cid):
        return cid and (any(p["comment"] == cid for p in self.positions)
                        or any(o["comment"] == cid for o in self.orders)
                        or any(d["comment"] == cid for d in self.deals))

    def handle(self, m):
        op, magic, sym, cid = m["op"], m.get("magic", 0), m.get("symbol", ""), m.get("cid", "")
        if m.get("exp") and time.time() > m["exp"]:
            return {"ok": False, "error": "expired before execution"}
        if op == "cfg":
            self.maxhold[magic] = m.get("max_hold_s", 0)
            return {"ok": True}
        if op == "account":
            return {"ok": True, "login": 123, "server": "Fake-Demo", "currency": "USD",
                    "balance": self.balance, "equity": self.equity(), "margin": 0.0,
                    "margin_free": self.equity(), "margin_level": 0.0, "leverage": 100,
                    "hedging": self.hedging, "demo": True, "connected": True,
                    "trade_allowed": True, "ping_ms": 1.0}
        if op == "balance":
            return {"ok": True, "ops": [{"deal": 1, "time_msc": 1790000000000, "type": "balance",
                                         "amount": 1000.0, "comment": "demo deposit"}]}
        if op in ("symbol", "tick"):
            return {"ok": True, "symbol": sym, "bid": self.bid, "ask": self.ask,
                    "time_msc": int(time.time() * 1000), **(self.spec if op == "symbol" else {})}
        if op == "symbols":
            names = ["BTCUSD", "ETHUSD", "EURUSD", "GOLD"]
            off, lim = m.get("offset", 0), m.get("limit", 500)
            return {"ok": True, "total": len(names), "offset": off,
                    "symbols": [{"name": n, "path": f"Fake\\{n}", **self.spec}
                                for n in names[off:off + lim]]}
        if op == "rates":
            if m.get("tf") not in ("M1", "M5", "M15", "M30", "H1", "H4", "D1", "W1", "MN1"):
                return {"ok": False, "error": f"bad tf {m.get('tf')}"}
            step = {"M1": 60, "M5": 300, "H1": 3600, "D1": 86400}.get(m["tf"], 3600)
            first = 1780000000 // step * step
            last = first + 999 * step
            to = m.get("to_srv") or last
            ts = [t for t in range(first, last + 1, step) if t <= to][-m.get("count", 2000):]
            return {"ok": True, "n": len(ts), "symbol": sym, "tf": m["tf"],
                    "time_basis": "server", "bars_total": 1000,
                    "bars": [[t, 100.0, 101.0, 99.0, 100.5, 10, 15] for t in ts]}
        if op == "positions":
            return {"ok": True, "positions": [dict(p, price_current=self.bid, profit=self._upl(p))
                                              for p in self.positions
                                              if p["magic"] == magic and (not sym or p["symbol"] == sym)]}
        if op == "deals":
            return {"ok": True, "deals": [d for d in self.deals if d["magic"] == magic
                                          and (not sym or d["symbol"] == sym)
                                          and d["time_msc"] >= m.get("from_ms", 0)]}
        if op == "orders":
            return {"ok": True, "orders": [o for o in self.orders if o["magic"] == magic
                                           and (not sym or o["symbol"] == sym)]}
        if op == "cancel":
            o = next((o for o in self.orders if o["ticket"] == m.get("ticket")
                      and o["magic"] == magic), None)
            if not o:
                g = self.gone.get(m.get("ticket"))
                return {"ok": False, "error": "not found",
                        "state": g[1] if g and g[0] == magic else "unknown"}
            self.orders.remove(o)
            self.gone[o["ticket"]] = (magic, "cancelled")
            return {"ok": True, "retcode": 10009, "error": ""}
        if op in ("order", "pending", "net", "close", "flatten") and self._seen(cid):
            return {"ok": False, "duplicate": True, "error": "client_id already executed"}
        if op == "order":
            return {"ok": True, "fill": self._open(sym, m["side"], m["volume"], magic, cid),
                    "error": ""}
        if op == "pending":
            return self._pending(m, magic, sym, cid)
        if op == "close":
            p = next((p for p in self.positions if p["ticket"] == m["ticket"]
                      and p["magic"] == magic), None)
            if not p:
                return {"ok": False, "error": "no such position for this client"}
            return {"ok": True, "fill": self._close(p, m.get("volume"), cid), "error": ""}
        if op in ("net", "flatten"):
            delta = m.get("delta", 0.0)
            buy, left = delta > 0, round(abs(delta), 8)
            fills, late = [], []
            mine = sorted((p for p in self.positions if p["magic"] == magic
                           and (not sym or p["symbol"] == sym)), key=lambda p: p["time_msc"])
            for p in mine:
                if op == "net" and ((p["side"] == "buy") == buy or not self.hedging):
                    continue
                if op == "net" and left <= 0:
                    break
                v = min(left, p["volume"]) if op == "net" else p["volume"]
                if p["ticket"] in self.reject:
                    late.append(str(p["ticket"]))
                else:
                    fills.append(self._close(p, v, cid))
                left = round(left - v, 8)
            if late:      # what executed is reported, and nothing is opened
                return {"ok": False, "fills": fills,
                        "error": f"close of {','.join(late)} not confirmed after 10 s"}
            if op == "net" and left >= self.spec["volume_min"]:
                fills.append(self._open(sym, "buy" if buy else "sell", left, magic, cid))
            return {"ok": True, "fills": fills, "error": ""}
        return {"ok": False, "error": f"unknown op {op}"}

    def _pending(self, m, magic, sym, cid):
        """The EA's PendingRequest() checks, then a resting order."""
        side, vol, price = m.get("side"), float(m.get("volume") or 0), float(m.get("price") or 0)
        sl, tp = float(m.get("sl") or 0), float(m.get("tp") or 0)
        if side not in ("buy", "sell"):
            return {"ok": False, "error": "side must be buy or sell"}
        sp = self.spec
        steps = vol / sp["volume_step"]
        if vol <= 0 or abs(steps - round(steps)) > 1e-6:
            return {"ok": False, "error": f"volume {vol} is not a positive multiple of volume_step"}
        vol = round(round(steps) * sp["volume_step"], 8)
        if vol < sp["volume_min"] - 1e-9 or vol > sp["volume_max"] + 1e-9:
            return {"ok": False, "error": f"volume {vol} outside volume_min/volume_max"}
        if price <= 0:
            return {"ok": False, "error": "price required"}
        gap = max(sp["stops_level"], sp["freeze_level"]) * sp["point"]
        if side == "buy" and not (price < self.ask and self.ask - price >= gap):
            return {"ok": False, "error": f"buy limit price {price} must be below ask {self.ask}"}
        if side == "sell" and not (price > self.bid and price - self.bid >= gap):
            return {"ok": False, "error": f"sell limit price {price} must be above bid {self.bid}"}
        if sl and ((side == "buy") != (sl < price)):
            return {"ok": False, "error": f"sl {sl} on the wrong side of {price}"}
        if tp and ((side == "buy") != (tp > price)):
            return {"ok": False, "error": f"tp {tp} on the wrong side of {price}"}
        t = next(self.tickets)
        self.orders.append({"ticket": t, "symbol": sym, "side": side, "type": f"{side}_limit",
                            "volume": vol, "volume_initial": vol, "price": price, "sl": sl,
                            "tp": tp, "comment": cid, "magic": magic,
                            "time_setup_msc": int(time.time() * 1000)})
        return {"ok": True, "order": t, "type": f"{side}_limit", "price": price, "volume": vol,
                "sl": sl, "tp": tp, "retcode": 10008, "error": ""}
