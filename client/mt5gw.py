"""Client for mt5-gateway. Standard library only; copy this one file into any bot.

    from mt5gw import Gateway
    gw = Gateway("http://<executor>:8000", token)
    gw.health(); gw.symbol("ETHUSD")
    r = gw.order("ETHUSD", "buy", 0.1, sl=..., tp=...)  # r["ok"], r["fill"]["price"]
    r = gw.net("BTCUSD", -0.05)                        # move the net position by -0.05 lots
    r = gw.pending("BTCUSD", "buy", 0.05, 61000.0)     # BUY LIMIT; r["order"] is the ticket
    gw.orders("BTCUSD"); gw.cancel(r["order"])

One persistent HTTP/1.1 connection, so every call after the first is a single round trip.
Every trading call carries a client_id; a retry with the same id can never trade twice —
the gateway returns the first answer, and the EA refuses an id it finds in a deal comment.
"""
import http.client
import json
import socket
import threading
import time
import uuid
from urllib.parse import urlencode, urlparse


class GatewayError(RuntimeError):
    pass


class _Conn(http.client.HTTPConnection):
    def connect(self):
        super().connect()
        # request line+headers and the body go out as two writes; with Nagle on, the second
        # waits for the server's delayed ACK — 40 ms added to every call
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)


class Gateway:
    def __init__(self, url, token, timeout=20.0):
        u = urlparse(url)
        self.host, self.port = u.hostname, u.port or 80
        self.token, self.timeout = token, timeout
        self._conn = None
        self._lock = threading.Lock()        # one connection, one request at a time

    def _call(self, method, path, body=None, query=None, retries=2):
        if query:
            path += "?" + urlencode({k: v for k, v in query.items() if v not in (None, "")})
        data = json.dumps(body).encode() if body is not None else None
        headers = {"Authorization": f"Bearer {self.token}", "Content-Type": "application/json"}
        last = None
        with self._lock:
            for attempt in range(retries + 1):
                try:
                    if self._conn is None:
                        self._conn = _Conn(self.host, self.port, timeout=self.timeout)
                    t0 = time.time()
                    self._conn.request(method, path, body=data, headers=headers)
                    resp = self._conn.getresponse()
                    out = json.loads(resp.read() or b"{}")
                    out["rtt_ms"] = round((time.time() - t0) * 1000, 1)
                    if resp.status == 401:
                        raise GatewayError("gateway rejected the token")
                    return out
                except (OSError, http.client.HTTPException, ValueError) as e:
                    # A dropped keep-alive connection (tunnel restart, gateway restart). Safe to
                    # resend: reads are reads, and trades carry a client_id.
                    last = e
                    try:
                        self._conn.close()
                    except Exception:
                        pass
                    self._conn = None
                    time.sleep(0.2 * (attempt + 1))
        raise GatewayError(f"{method} {path}: {last}")

    @staticmethod
    def new_id():
        return uuid.uuid4().hex[:20]

    # --- reads -------------------------------------------------------------------------
    def health(self):
        return self._call("GET", "/health")

    def account(self):
        return self._call("GET", "/account")

    def symbol(self, name):
        return self._call("GET", "/symbol", query={"symbol": name})

    def tick(self, name):
        return self._call("GET", "/tick", query={"symbol": name})

    def ticks(self, name, from_ms, limit=1000):
        return self._call("GET", "/ticks", query={"symbol": name, "from_ms": from_ms,
                                                  "limit": limit})

    def symbols(self, offset=0, limit=500):
        """One page of every symbol the server offers (not only Market Watch); `total` in the
        reply says how many there are. Static specs only: use symbol() for bid/ask and swaps."""
        return self._call("GET", "/symbols", query={"offset": offset, "limit": limit})

    def all_symbols(self):
        out, off = [], 0
        while True:
            r = self.symbols(off)
            if not r.get("ok"):
                raise GatewayError(r.get("error", "symbols failed"))
            out += r["symbols"]
            off = r["offset"] + 500
            if off >= r["total"]:
                return out

    def rates(self, name, tf="H1", to_srv=0, count=2000, release=False):
        """OHLC bars, oldest first: [[t, open, high, low, close, tick_volume, spread_points]].
        `t` is the broker's SERVER time in epoch seconds (it moves with DST; convert it
        yourself). Page backwards with to_srv = oldest t - 1. A reply with `retry` means the
        terminal is still downloading that history; ask again shortly. release=True on the
        last page takes the symbol back out of Market Watch: pass it only for symbols that
        symbols() reported as not "selected" before you started, or you unsubscribe
        whoever else was watching them (a tick recorder)."""
        return self._call("GET", "/rates", query={"symbol": name, "tf": tf, "to_srv": to_srv,
                                                  "count": count, "release": int(release)})

    def positions(self, symbol=None):
        return self._call("GET", "/positions", query={"symbol": symbol})

    def deals(self, from_ms=0, to_ms=0, symbol=None):
        return self._call("GET", "/deals", query={"symbol": symbol, "from_ms": from_ms,
                                                  "to_ms": to_ms})

    def balance_ops(self, from_ms=0):
        """Deposits, withdrawals, credits, bonuses: money that moved without a trade.
        Account-wide, oldest first, times in UTC ms."""
        return self._call("GET", "/balance", query={"from_ms": from_ms})

    def orders(self, symbol=None):
        """This client's pending orders: ticket, symbol, side, type (buy_limit/sell_limit),
        volume (still unfilled), price, sl, tp, comment (the client_id), time_setup_msc."""
        return self._call("GET", "/orders", query={"symbol": symbol})

    def events(self):
        return self._call("GET", "/events")

    # --- trades ------------------------------------------------------------------------
    def order(self, symbol, side, volume, sl=0.0, tp=0.0, client_id=None, deviation=50):
        return self._call("POST", "/order", {
            "symbol": symbol, "side": side, "volume": volume, "sl": sl, "tp": tp,
            "deviation": deviation, "client_id": client_id or self.new_id()})

    def close(self, ticket, volume=0.0, client_id=None, deviation=50):
        return self._call("POST", "/close", {"ticket": ticket, "volume": volume,
                                             "deviation": deviation,
                                             "client_id": client_id or self.new_id()})

    def modify(self, ticket, sl=0.0, tp=0.0):
        return self._call("POST", "/modify", {"ticket": ticket, "sl": sl, "tp": tp})

    def pending(self, symbol, side, volume, price, sl=0.0, tp=0.0, client_id=None):
        """A resting LIMIT order, good till cancelled: side "buy" = BUY LIMIT below the ask,
        "sell" = SELL LIMIT above the bid. Refused (never turned into a market or stop order)
        when the price is on the wrong side or inside the stops/freeze level, or the volume is
        off the symbol's grid. Its fill is a deal in deals() whose comment is the client_id."""
        return self._call("POST", "/pending", {
            "symbol": symbol, "side": side, "volume": volume, "price": price, "sl": sl, "tp": tp,
            "client_id": client_id or self.new_id()})

    def cancel(self, ticket):
        """Remove a pending order. One that already filled or went answers error "not found"
        (with "state": filled / cancelled / ... when the terminal still knows)."""
        return self._call("POST", "/cancel", {"ticket": ticket})

    def flatten(self, symbol=None, client_id=None):
        return self._call("POST", "/flatten", {"symbol": symbol,
                                               "client_id": client_id or self.new_id()})

    def net(self, symbol, delta, client_id=None, deviation=50):
        """Move this client's net position in `symbol` by `delta` lots (signed). On a hedging
        account the opposite side is closed first, so there is only ever one net position."""
        return self._call("POST", "/net", {"symbol": symbol, "delta": delta,
                                           "deviation": deviation,
                                           "client_id": client_id or self.new_id()})
