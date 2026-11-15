# mt5-gateway

A MetaTrader 5 terminal on a small Linux box, shared by several trading bots.
Each box is an **executor**; there is one per broker account, and all of them run the files in
this repo unchanged.

```
bot A (remote) ──SSH tunnel──┐  token A · magic 101
                             ├──► gateway :8000 (executor) ◄─ localhost socket ─► EA in MT5 ─► broker
bot B (same LAN) ─private IP─┘  token B · magic 102
```

- **Executors**: x86 Debian/Ubuntu, 1 GB RAM + 2 GB swap is enough. The systemd units are
  written for user `ubuntu`; `install.sh` and `push.sh` rewrite them for the host's actual user.
  Each runs Xvfb, MT5 under Wine, the `MT5Gateway` EA, and `gateway.py`. Only SSH needs to be
  open publicly. Addresses are not kept in this repo; they are in your `~/.ssh/config`.

## Same code, per-host settings

Every executor runs the same `gateway.py`, EA and systemd units. What differs lives only on
the host, never in git:

| on the host | what |
|---|---|
| `~/mt5gw/host.env` | the broker's MT5 folder, installer and window title; listen address (from `deploy/executor/host.env.example`) |
| `~/.mt5/drive_c/mt5gw/startup.ini` | the broker account: `Login`, `Password`, `Server` (from `deploy/executor/startup.ini.template`) |
| `~/mt5gw/clients.json` | the bots' tokens and magic numbers (from `clients.example.json`) |

To change code on the executors: edit here, then `deploy/push.sh HOST` (dry run: what differs),
then `deploy/push.sh HOST --apply`, for each executor. It copies only the files that differ
and restarts only the services they belong to.
- **gateway** (`gateway/gateway.py`): Python standard library only, ~20 MB. HTTP/1.1
  keep-alive API for bots with a bearer token each; one TCP link to the EA.
- **EA** (`mql5/MT5Gateway.mq5`): keeps one socket to the gateway and executes commands as they
  arrive. There's no polling interval between a bot's order and `OrderSend`.
- **client** (`client/mt5gw.py`): one standard-library file. Copy it into any bot.

## Why the gateway sits next to the terminal

The usual design has each bot host its own bridge, with the EA polling it once a second.
With two bots that means one EA polling two remote bridges. It also adds up to a second of
delay to every order, and a web framework costs about 50 MB per bot. Here the gateway sits
next to the terminal instead, and the bots are clients. The contract stays the same:
`client_id` idempotency, the EA's safety rules, and reconciliation from positions and deals.

## Isolation between bots

- Each client in `clients.json` has its own token and **magic number**. Every order is stamped
  with it. `/positions`, `/deals`, `/close` and `/flatten` only ever see that magic.
- An optional `symbols` whitelist and a `max_lots` cap apply per client.
- `max_hold_s` per client is enforced **by the EA**. It keeps working when the bot, or even
  the gateway, is down (a backup time barrier).
- One terminal can be logged into one account. A second account needs a second terminal, and
  more RAM than 1 GB, so it gets its own executor.

## Safety

- **Idempotency:** every trading call has a `client_id`. The gateway returns the first reply
  for a repeated id, and the file is persisted. The EA also refuses an id it finds in an open
  position's or a live pending order's comment, or in a deal or a (partly) filled order from
  the last 3 days. So a retry after a timeout, or after a gateway restart, can't trade twice.
- **Expiry:** each command carries `exp` (5 s). The EA refuses a command that arrives later.
- **No gateway, no orders:** nothing reaches the EA.
- **Transport errors are never cached:** "EA not connected" or a timeout can be retried with
  the same `client_id`.
- **Audit log:** every POST, and its reply, is appended to `state/audit.jsonl`.

## API

All calls need the header `Authorization: Bearer <token>`. Replies are JSON with `ok`, plus
`gw_ms` (time spent in the gateway) and `rtt_ms` (added by the client).

| call | what |
|---|---|
| `GET /health` | EA link, heartbeat age, account login/server |
| `GET /account` | balance, equity, margin, leverage, hedging, demo, trade_allowed, ping |
| `GET /symbol?symbol=ETHUSD` | contract size, volume min/step/max, stops level, swaps, bid/ask |
| `GET /tick?symbol=` | bid/ask/time_msc |
| `GET /ticks?symbol=&from_ms=&limit=` | recorded ticks `[[t, bid, ask], ...]`, for measuring the spread |
| `GET /positions?symbol=` | this client's positions |
| `GET /deals?from_ms=&to_ms=&symbol=` | this client's deals (fills, commission, swap, profit) |
| `GET /orders?symbol=` | this client's pending orders (see below) |
| `GET /events` | EA events for this client (`forced_close` from max_hold, `pending_fill`) |
| `POST /order` | `{symbol, side: buy\|sell, volume, sl, tp, client_id, deviation}`. Market order with server-side SL/TP |
| `POST /close` | `{ticket, volume?, client_id}`, full or partial |
| `POST /modify` | `{ticket, sl, tp}` |
| `POST /flatten` | `{symbol?, client_id}`. Close all of this client's positions |
| `POST /net` | `{symbol, delta, client_id}`. Move the net position by `delta` lots. On a hedging account it closes the opposite side first, oldest first, and opens only the remainder |
| `POST /pending` | `{symbol, side: buy\|sell, volume, price, client_id, sl?, tp?}`. A resting LIMIT order, good till cancelled (see below) |
| `POST /cancel` | `{ticket}`. Remove a pending order |

```python
from mt5gw import Gateway
gw = Gateway("http://<executor>:8000", TOKEN)
spec = gw.symbol("ETHUSD")
r = gw.order("ETHUSD", "buy", 0.5, sl=..., tp=..., client_id="sig-20260925-0931")
if r["ok"]: print(r["fill"]["price"], r["fill"]["deal"])
```

### Pending LIMIT orders

```
POST /pending {"symbol", "side": "buy"|"sell", "volume", "price", "client_id", "sl": 0, "tp": 0}
  -> {"ok": true, "order": <ticket>, "type": "buy_limit", "price", "volume", "sl", "tp", "retcode"}
  |  {"ok": false, "error": "..."}
GET /orders?symbol=
  -> {"ok": true, "orders": [{"ticket", "symbol", "side": "buy"|"sell", "type": "buy_limit"|"sell_limit",
                              "volume", "volume_initial", "price", "sl", "tp", "comment",
                              "time_setup_msc", "magic"}]}
POST /cancel {"ticket"}
  -> {"ok": true} | {"ok": false, "error": "not found", "state": "filled"|"partial"|"cancelled"|"expired"|"rejected"|"unknown"}
                  | {"ok": false, "error": "retcode ..."}      (still pending, e.g. inside the freeze level)
```

- `buy` is a BUY LIMIT and must be below the ask; `sell` is a SELL LIMIT and must be above the
  bid, both by at least the symbol's stops level and freeze level. A price on the wrong side is
  refused, never sent: MT5 would reject it or, worse, a bot would get a different order type.
- The price is put on the symbol's tick grid, away from the market (a buy rounded down, a sell
  up); the reply's `price` is what was placed. A volume off `volume_step`, below `volume_min` or
  above `volume_max` is refused rather than rounded. `max_lots` and the symbol whitelist apply.
- Good till cancelled, filling mode per symbol (as the market path), falling back to "return"
  if the server refuses that for a resting order.
- `client_id` goes into the order comment. Like `/order`, a repeated id gets the first reply
  back from the gateway (a refusal included: a corrected order needs a new id), and the EA
  refuses an id that is on a live pending order, a position, or a fill.
- A fill is a normal deal in `/deals` (`entry: "in"`, `order` = the pending ticket, `price` =
  the fill price) whose `comment` is the client_id; when the server leaves the deal's comment
  empty, `/deals` reports the order's. The position it opens is in `/positions` with the same
  comment; on a hedging account it is a position of its own next to any others. The EA also
  posts a `pending_fill` event (`order`, `deal`, `position_id`, `side`, `volume`, `price`,
  `volume_left`, `comment`, `time_msc`) to `/events`. Placing and cancelling are not events:
  the caller already has the reply.
- `/cancel` has no client_id: a second cancel just answers `not found`. Another client's ticket
  is also `not found`. `/flatten` closes positions only; it leaves pending orders alone.

```python
r = gw.pending("BTCUSD", "buy", 0.05, 61000.0, client_id="sync-20261005-1200")
gw.orders("BTCUSD")["orders"]                     # [{"ticket": r["order"], "type": "buy_limit", ...}]
gw.cancel(r["order"])                              # {"ok": true}, or "not found" once it filled
```

## Latency

- gateway → EA over localhost: well under 1 ms, plus the EA's 1–5 ms socket check.
- bot → executor over a private network: about 1 ms.
- bot → executor through an SSH tunnel: one network round trip plus the tunnel.
- One keep-alive connection per client, so there's no per-call setup.
- `TCP_NODELAY` is set on both ends. Without it, every call cost 40 ms extra even on localhost.

## One-time terminal setting (cannot be scripted)

MT5 only accepts the EA's `127.0.0.1` socket once it is on the WebRequest allow-list **set in its own
Options dialog**. Writing `WebRequestUrl=` into `common.ini` is ignored (the EA logs error 4014).
Once, after install: open the executor's screen (e.g. x11vnc on :99 at 24-bit colour, localhost only, reached
over an SSH tunnel), Tools > Options > Expert Advisors > tick *Allow
WebRequest for listed URL* > add `http://127.0.0.1` and `127.0.0.1` > OK. The terminal keeps it.
Xvfb must run at 24-bit colour: at 16-bit, Wine draws dialogs black. No window manager: floating
panels cannot be dragged or closed by hand (`xdotool windowmove` works).

Symbol names and volume steps are the broker's: check `GET /symbol` before trading a new one.
Measured: an order is 17-38 ms gateway->EA->broker->fill on the executor.

## Operating an executor

```bash
ssh HOST
systemctl status mt5-xvfb mt5-gateway mt5-terminal
journalctl -u mt5-gateway -f                         # EA connects, hello, events
tail -f ~/mt5gw/state/audit.jsonl                    # every order and its reply
# terminal/EA logs (UTF-16):
. ~/mt5gw/host.env
iconv -f UTF-16LE -t UTF-8 "$MT5_DIR/logs/$(date +%Y%m%d).log" | tail
iconv -f UTF-16LE -t UTF-8 "$MT5_DIR/MQL5/Logs/$(date +%Y%m%d).log" | tail
```

- **Login** lives in `~/.mt5/drive_c/mt5gw/startup.ini` (chmod 600).
- **Tokens** live in `~/mt5gw/clients.json` (chmod 600). Restart `mt5-gateway` after editing.
- **Adding a bot:** add an entry with a new token and a unique magic number, then restart the gateway.
- **Rebuilding from scratch:** run `deploy/executor/install.sh` on the host.
- **Changing the EA:** `deploy/push.sh` only reports it. Copy `mql5/MT5Gateway.mq5` into
  `MQL5/Experts/`, recompile it as `install.sh` does, and restart `mt5-terminal`, one executor
  at a time, while no bot is mid-order.

## Connecting a bot

By default the gateway listens on `127.0.0.1:8000` only.

- **Over SSH (anywhere):** `ssh -N -L 127.0.0.1:18000:127.0.0.1:8000 HOST`, then
  `Gateway("http://127.0.0.1:18000", <bot token>)`.
- **Over a private network:** in `host.env` set `MT5GW_LISTEN="0.0.0.0:8000"` and
  `MT5GW_ALLOW_CIDR` to the private range (`install.sh` adds the iptables rule), allow the same
  range on TCP 8000 in the cloud firewall, then `Gateway("http://<executor-private-ip>:8000", <bot token>)`.

## Known limits

- `deploy/executor/mt5-allowlist.sh` drives MT5's Options dialog with fixed click positions for
  the 1024x768 Xvfb screen. A different MT5 build can move them; check the script if the socket
  never comes back after a restart.

## Tests

```bash
python3 tests/test_gateway.py   # client -> gateway -> fake EA (same protocol)
```

## License

Apache-2.0. See `LICENSE`. Redistributions must keep `NOTICE`.
