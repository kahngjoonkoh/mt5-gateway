"""End to end: client -> gateway -> fake EA. python3 tests/test_gateway.py"""
import json
import os
import socket
import sys
import tempfile
import threading
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path[:0] = [os.path.join(ROOT, "gateway"), os.path.join(ROOT, "client"),
                os.path.join(ROOT, "tests")]
from http.server import ThreadingHTTPServer  # noqa: E402

import gateway as gwmod  # noqa: E402
from fake_ea import FakeEA  # noqa: E402
from mt5gw import Gateway  # noqa: E402


def free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def boot(state=None):
    d = state or tempfile.mkdtemp()
    clients = {"ro": {"token": "tok-ro", "magic": 103, "read_only": True},
               "a": {"token": "tok-a", "magic": 101},
               "b": {"token": "tok-b", "magic": 102, "symbols": ["ETHUSD"], "max_lots": 1.0}}
    cp = os.path.join(d, "clients.json")
    json.dump(clients, open(cp, "w"))
    http_port, ea_port = free_port(), free_port()
    ea_dir = os.path.join(d, "gw")
    gw = gwmod.Gateway(cp, os.path.join(d, "state"), "127.0.0.1", ea_port, ea_dir)
    threading.Thread(target=gw.ea.serve, daemon=True).start()
    srv = ThreadingHTTPServer(("127.0.0.1", http_port), gwmod.make_handler(gw))
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    time.sleep(0.2)
    ea = FakeEA(ea_dir).start()
    for _ in range(50):
        if gw.ea.connected and gw.ea.hb_t:
            break
        time.sleep(0.05)
    url = f"http://127.0.0.1:{http_port}"
    return gw, ea, Gateway(url, "tok-a"), Gateway(url, "tok-b"), url, d, srv


def test_all():
    gw, ea, a, b, url, d, srv = boot()
    h = a.health()
    assert h["ok"] and h["client"] == "a" and h["magic"] == 101, h
    for _ in range(40):
        if len(ea.maxhold) == 3:
            break
        time.sleep(0.05)
    assert ea.maxhold == {103: 0, 101: 0, 102: 0}, "config pushed to the EA on connect"

    # bad token
    try:
        Gateway(url, "nope").health()
        raise AssertionError("bad token accepted")
    except Exception as e:
        assert "token" in str(e)

    # symbol list: paged, and filtered to a client's whitelist
    names = [s["name"] for s in a.all_symbols()]
    assert names == ["BTCUSD", "ETHUSD", "EURUSD", "GOLD"], names
    assert [s["name"] for s in b.symbols()["symbols"]] == ["ETHUSD"], "b only sees its whitelist"

    # bar history: oldest first, paged backwards by server time, whitelist enforced
    r = a.rates("EURUSD", "H1", count=300)
    assert r["ok"] and r["n"] == 300 and r["time_basis"] == "server", r
    t = [x[0] for x in r["bars"]]
    assert t == sorted(t) and len(set(t)) == 300
    older = a.rates("EURUSD", "H1", to_srv=t[0] - 1, count=300)["bars"]
    assert older[-1][0] == t[0] - 3600, "the next page ends right before this one"
    assert not a.rates("EURUSD", "H7")["ok"], "unknown timeframe refused"
    r = b.rates("EURUSD")
    assert not r["ok"] and "not allowed" in r["error"], r

    # orders are isolated per client
    r = a.order("BTCUSD", "buy", 0.05, client_id="a1")
    assert r["ok"] and r["fill"]["volume"] == 0.05, r
    assert len(a.positions("BTCUSD")["positions"]) == 1
    assert b.positions()["positions"] == [], "client b must not see client a's position"

    # idempotency: the same client_id never trades twice (gateway cache)
    r2 = a.order("BTCUSD", "buy", 0.05, client_id="a1")
    assert r2.get("duplicate") and r2["fill"]["deal"] == r["fill"]["deal"], r2
    assert len(a.positions("BTCUSD")["positions"]) == 1

    # symbol whitelist and lot cap
    assert b.order("BTCUSD", "buy", 0.1)["ok"] is False
    assert b.order("ETHUSD", "buy", 2.0)["ok"] is False
    assert b.order("ETHUSD", "buy", 0.5)["ok"]

    # net: -0.08 on +0.05 closes the long first, then opens 0.03 short — one net position
    r = a.net("BTCUSD", -0.08, client_id="n1")
    assert r["ok"] and [f["entry"] for f in r["fills"]] == ["out", "in"], r
    pos = a.positions("BTCUSD")["positions"]
    assert len(pos) == 1 and pos[0]["side"] == "sell" and abs(pos[0]["volume"] - 0.03) < 1e-9, pos
    # partial reduce
    r = a.net("BTCUSD", 0.01)
    pos = a.positions("BTCUSD")["positions"]
    assert abs(pos[0]["volume"] - 0.02) < 1e-9 and len(pos) == 1, pos

    # a reversal over many tickets (each add is its own ticket on a hedging account):
    # one deal per ticket closed, closes first in ticket age order, the opening fill LAST
    # (a bot can read the spread off fills[-1])
    a.net("BTCUSD", 0.02)                                   # back to flat
    assert a.positions("BTCUSD")["positions"] == []
    tks = [a.order("BTCUSD", "sell", 0.02)["fill"]["position_id"] for _ in range(13)]
    r = a.net("BTCUSD", 0.40, client_id="rev13")
    assert r["ok"] and [f["entry"] for f in r["fills"]] == ["out"] * 13 + ["in"], r
    assert [f["position_id"] for f in r["fills"][:13]] == tks, "oldest ticket closed first"
    assert all(f["comment"] == "rev13" for f in r["fills"]), "every deal carries the client_id"
    pos = a.positions("BTCUSD")["positions"]
    assert len(pos) == 1 and pos[0]["side"] == "buy" and abs(pos[0]["volume"] - 0.14) < 1e-9, pos
    # a close that never executes: no opening, ok=false, the error names it, fills = what did
    assert a.flatten("BTCUSD")["ok"] and a.positions("BTCUSD")["positions"] == []
    tks = [a.order("BTCUSD", "buy", 0.01)["fill"]["position_id"] for _ in range(3)]
    ea.reject = {tks[1]}
    r = a.net("BTCUSD", -0.05, client_id="rev3")
    assert r["ok"] is False and str(tks[1]) in r["error"], r
    assert [f["entry"] for f in r["fills"]] == ["out", "out"], r
    assert [p["ticket"] for p in a.positions("BTCUSD")["positions"]] == [tks[1]], "nothing opened"
    ea.reject = set()
    assert a.flatten("BTCUSD")["ok"] and a.positions("BTCUSD")["positions"] == []
    a.order("BTCUSD", "sell", 0.02)                         # where the steps below expect us

    # deals are per client, and flatten only touches the caller's positions
    assert all(dd["magic"] == 101 for dd in a.deals(symbol="BTCUSD")["deals"])
    assert a.flatten("BTCUSD")["ok"] and a.positions()["positions"] == []
    assert len(b.positions()["positions"]) == 1, "b's ETH position survived a's flatten"

    # EA down: transport error, NOT cached — the same client_id goes through once it is back
    ea.stop()
    time.sleep(0.3)
    gw.ea.files.hb_t = time.time() - 10    # the heartbeat file goes stale after 5 s; skip the wait
    r = a.order("BTCUSD", "sell", 0.01, client_id="x1")
    assert r["ok"] is False and r.get("transport"), r
    ea2 = FakeEA(gw.ea.files.root).start()
    time.sleep(0.5)
    r = a.order("BTCUSD", "sell", 0.01, client_id="x1")
    assert r["ok"], r

    # EA-side dedupe (gateway cache lost, e.g. restart): the comment check refuses it
    gw.idem.done.clear()
    ea2.positions, ea2.deals = ea.positions, ea.deals        # same account after reconnect
    ea2.deals.append({**ea2.deals[-1], "comment": "x2", "magic": 101})
    r = a.order("BTCUSD", "sell", 0.01, client_id="x2")
    assert r["ok"] is False and r["duplicate"], r

    # money movement, and a read-only token that can read but never trade
    assert a.balance_ops()["ops"][0]["amount"] == 1000.0
    ro = Gateway(url, "tok-ro")
    assert ro.tick("BTCUSD")["ok"] and ro.balance_ops()["ok"]
    r = ro.order("BTCUSD", "buy", 0.01)
    assert r["ok"] is False and "read-only" in r["error"], r
    assert ro.flatten()["ok"] is False and ro.net("BTCUSD", 0.01)["ok"] is False

    # the audit trail and the persisted idempotency file exist
    st = os.path.join(d, "state")
    assert os.path.getsize(os.path.join(st, "audit.jsonl")) > 0
    assert "a1" in open(os.path.join(st, "idempotency.jsonl")).read()
    # and a fresh gateway reloads it: the retry of a1 still returns the original answer
    idem = gwmod.Idempotency(os.path.join(st, "idempotency.jsonl"))
    assert ("a", "a1") in idem.done

    # both transports: the socket when the EA holds one, files the moment it drops
    ea3 = FakeEA(gw.ea.files.root, port=gw.ea.sock.port).start()
    ea3.positions, ea3.deals = ea2.positions, ea2.deals
    for _ in range(50):
        if gw.ea.sock.connected:
            break
        time.sleep(0.05)
    assert a.health()["transport"] == "socket"
    assert a.tick("BTCUSD")["via"] == "socket"
    t0 = time.time()
    for _ in range(50):
        a.tick("BTCUSD")
    print(f"tick round trip via socket: {(time.time() - t0) / 50 * 1000:.2f} ms")
    ea3.drop_socket()
    for _ in range(50):
        if not gw.ea.sock.connected:
            break
        time.sleep(0.05)
    assert a.health()["transport"] == "files"
    r = a.order("BTCUSD", "buy", 0.01)
    assert r["ok"] and r["via"] == "files", r
    t0 = time.time()
    for _ in range(50):
        a.tick("BTCUSD")
    print(f"tick round trip via files:  {(time.time() - t0) / 50 * 1000:.2f} ms")
    ea3.stop()

    # latency through the gateway on localhost, keep-alive
    t0 = time.time()
    for _ in range(50):
        a.tick("BTCUSD")
    per = (time.time() - t0) / 50 * 1000
    print(f"tick round trip via gateway: {per:.2f} ms")
    srv.shutdown()
    print("ok")


def test_pending():
    """LIMIT orders: placed only on the right side of the market, listed per client, cancelled,
    and filled into a deal that carries the client_id."""
    gw, ea, a, b, url, d, srv = boot()
    ask, bid = ea.ask, ea.bid                                       # 100005 / 99995

    # wrong side, off-grid volume, missing price: refused, nothing placed
    r = a.pending("BTCUSD", "buy", 0.05, ask + 1, client_id="p-bad1")
    assert r["ok"] is False and "below ask" in r["error"], r
    r = a.pending("BTCUSD", "sell", 0.05, bid - 1, client_id="p-bad2")
    assert r["ok"] is False and "above bid" in r["error"], r
    assert a.pending("BTCUSD", "buy", 0.055, 99000.0)["ok"] is False, "off the volume step"
    assert a.pending("BTCUSD", "buy", 0.05, 0)["ok"] is False, "price required"
    assert a.pending("BTCUSD", "hold", 0.05, 99000.0)["ok"] is False
    assert a.pending("BTCUSD", "buy", 0.05, 99000.0, sl=99500.0)["ok"] is False, "sl above a buy"
    assert b.pending("ETHUSD", "buy", 2.0, 99000.0)["ok"] is False, "max_lots"
    assert b.pending("BTCUSD", "buy", 0.1, 99000.0)["ok"] is False, "symbol whitelist"
    assert a.orders()["orders"] == []

    # a buy limit below the ask and a sell limit above the bid
    r = a.pending("BTCUSD", "buy", 0.05, 99000.0, client_id="p1")
    assert r["ok"] and isinstance(r["order"], int), r
    buy_tk = r["order"]
    r = a.pending("BTCUSD", "sell", 0.02, 101000.0, tp=100500.0, client_id="p2")
    assert r["ok"], r
    sell_tk = r["order"]
    o = a.orders("BTCUSD")["orders"]
    assert [x["ticket"] for x in o] == [buy_tk, sell_tk], o
    assert o[0]["side"] == "buy" and o[0]["type"] == "buy_limit" and o[0]["price"] == 99000.0
    assert o[0]["volume"] == 0.05 and o[0]["comment"] == "p1" and o[0]["time_setup_msc"] > 0
    assert o[1]["type"] == "sell_limit" and o[1]["tp"] == 100500.0 and o[1]["sl"] == 0
    assert set(o[0]) >= {"ticket", "symbol", "side", "type", "volume", "price", "sl", "tp",
                         "comment", "time_setup_msc"}
    assert a.positions()["positions"] == [], "a pending order is not a position"
    assert b.orders()["orders"] == [], "client b must not see client a's orders"
    assert a.orders("ETHUSD")["orders"] == []

    # idempotency: the gateway returns the first answer; the EA refuses a live pending cid
    r2 = a.pending("BTCUSD", "buy", 0.05, 99000.0, client_id="p1")
    assert r2.get("duplicate") and r2["order"] == buy_tk, r2
    gw.idem.done.clear()
    r2 = a.pending("BTCUSD", "buy", 0.05, 99000.0, client_id="p1")
    assert r2["ok"] is False and r2["duplicate"], r2
    assert len(a.orders()["orders"]) == 2

    # cancel: only the owner, once; the second answer is "not found"
    r = b.cancel(sell_tk)
    assert r["ok"] is False and r["error"] == "not found" and r["state"] == "unknown", r
    assert a.cancel(sell_tk)["ok"]
    r = a.cancel(sell_tk)
    assert r["ok"] is False and r["error"] == "not found" and r["state"] == "cancelled", r
    assert [x["ticket"] for x in a.orders()["orders"]] == [buy_tk]

    # the market comes down to the buy limit: it fills at its price, as a deal with the cid
    t0 = int(time.time() * 1000) - 1
    ea.set_price(98990.0)
    assert a.orders()["orders"] == []
    deals = [x for x in a.deals(from_ms=t0)["deals"] if x["comment"] == "p1"]
    assert len(deals) == 1, deals
    dl = deals[0]
    assert dl["order"] == buy_tk and dl["side"] == "buy" and dl["entry"] == "in", dl
    assert dl["price"] == 99000.0 and dl["volume"] == 0.05, dl
    pos = a.positions("BTCUSD")["positions"]
    assert len(pos) == 1 and pos[0]["comment"] == "p1" and pos[0]["ticket"] == dl["position_id"]
    for _ in range(40):
        ev = [e for e in a.events()["events"] if e.get("ev") == "pending_fill"]
        if ev:
            break
        time.sleep(0.05)
    assert ev and ev[-1]["order"] == buy_tk and ev[-1]["comment"] == "p1", ev
    assert not [e for e in b.events()["events"] if e.get("ev") == "pending_fill"], "per magic"

    # a filled order: cancel says so, and its cid can never place again
    r = a.cancel(buy_tk)
    assert r["ok"] is False and r["error"] == "not found" and r["state"] == "filled", r
    gw.idem.done.clear()
    r = a.pending("BTCUSD", "buy", 0.05, 98000.0, client_id="p1")
    assert r["ok"] is False and r["duplicate"], r

    # a read-only token can list but never place or cancel
    ro = Gateway(url, "tok-ro")
    assert ro.orders()["ok"]
    assert ro.pending("BTCUSD", "buy", 0.01, 90000.0)["ok"] is False
    assert ro.cancel(buy_tk)["ok"] is False
    srv.shutdown()
    print("pending ok")


if __name__ == "__main__":
    test_all()
    test_pending()
