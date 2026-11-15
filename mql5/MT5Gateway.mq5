//+------------------------------------------------------------------+
//| MT5Gateway.mq5 — executor for the mt5-gateway service            |
//|                                                                  |
//| Two transports to the gateway, same messages on both:             |
//|  - a localhost TCP socket (~1 ms): the normal path. MT5 allows it |
//|    only while 127.0.0.1 is on the WebRequest allow-list, which it |
//|    forgets on every restart (error 4014) until deploy/executor/   |
//|    mt5-allowlist.sh puts it back                                   |
//|  - files in the terminal's own sandbox (MQL5\Files\gw), which need |
//|    no permission: the fallback while the socket is refused        |
//| Each client bot has its own magic number; a command can only see  |
//| or touch positions carrying the magic it was sent with.           |
//|                                                                  |
//| Safety that works with the bots gone:                            |
//|  - a command older than its "exp" is refused (never a stale order)|
//|  - a client_id already present in an open position's, a live      |
//|    pending order's, a recent deal's or a recent filled order's     |
//|    comment is never executed twice                                |
//|  - a pending LIMIT order is only sent on the side of the market    |
//|    where MT5 keeps it a limit: it can never become a market or     |
//|    stop order                                                     |
//|  - per-magic max hold: positions older than max_hold_s are closed |
//|  - no gateway = no new orders (nothing writes requests)           |
//+------------------------------------------------------------------+
#property strict
#property version "1.00"
#include <Trade\Trade.mqh>

input string GatewayHost = "127.0.0.1";
input int    GatewayPort = 8765;
input int    HeartbeatMs = 1000;

int    g_sock = INVALID_HANDLE;
string g_buf = "";
ulong  g_last_try = 0, g_last_inbox = 0;

#define INBOX  "gw\\in\\"
#define OUTBOX "gw\\out\\"
#define STATUS "gw\\status.json"
ulong  g_last_hb = 0, g_last_act = 0, g_evn = 0;
long   g_magic[];
int    g_maxhold[];
CTrade g_trade;
ulong  g_dev = 50;
string g_last_err = "";

//--- tiny flat-JSON reader: {"k":"v","n":1.5,"b":true} is all the gateway ever sends
string g_k[], g_v[];

bool JParse(const string s)
{
   ArrayResize(g_k, 0); ArrayResize(g_v, 0);
   int n = StringLen(s), i = 0;
   while(i < n && StringGetCharacter(s, i) != '{') i++;
   i++;
   while(i < n)
   {
      while(i < n && (StringGetCharacter(s, i) == ' ' || StringGetCharacter(s, i) == ',')) i++;
      if(i >= n || StringGetCharacter(s, i) == '}') break;
      if(StringGetCharacter(s, i) != '"') return false;
      int ks = ++i;
      while(i < n && StringGetCharacter(s, i) != '"') i++;
      string key = StringSubstr(s, ks, i - ks);
      i++;
      while(i < n && (StringGetCharacter(s, i) == ' ' || StringGetCharacter(s, i) == ':')) i++;
      string val = "";
      if(StringGetCharacter(s, i) == '"')
      {
         i++;
         while(i < n)
         {
            ushort c = StringGetCharacter(s, i);
            if(c == '\\' && i + 1 < n) { val += ShortToString(StringGetCharacter(s, i + 1)); i += 2; continue; }
            if(c == '"') break;
            val += ShortToString(c); i++;
         }
         i++;
      }
      else
      {
         int vs = i;
         while(i < n && StringGetCharacter(s, i) != ',' && StringGetCharacter(s, i) != '}') i++;
         val = StringSubstr(s, vs, i - vs);
         StringTrimLeft(val); StringTrimRight(val);
         if(val == "null") val = "";
      }
      int m = ArraySize(g_k);
      ArrayResize(g_k, m + 1); ArrayResize(g_v, m + 1);
      g_k[m] = key; g_v[m] = val;
   }
   return true;
}

string JS(const string k) { for(int i = 0; i < ArraySize(g_k); i++) if(g_k[i] == k) return g_v[i]; return ""; }
double JD(const string k) { return StringToDouble(JS(k)); }
long   JL(const string k) { return StringToInteger(JS(k)); }

//--- JSON writers
string Q(const string s)
{
   string r = s;
   StringReplace(r, "\\", "\\\\"); StringReplace(r, "\"", "\\\"");
   StringReplace(r, "\n", " "); StringReplace(r, "\r", " ");
   return "\"" + r + "\"";
}
string N(const double v, const int d = 8)
{
   if(!MathIsValidNumber(v)) return "null";
   string s = DoubleToString(v, d);
   if(StringFind(s, ".") >= 0)
   {
      while(StringLen(s) > 1 && StringGetCharacter(s, StringLen(s) - 1) == '0') s = StringSubstr(s, 0, StringLen(s) - 1);
      if(StringGetCharacter(s, StringLen(s) - 1) == '.') s = StringSubstr(s, 0, StringLen(s) - 1);
   }
   return s;
}
string B(const bool b) { return b ? "true" : "false"; }

//--- file transport. Every write is atomic: a .tmp file, then a rename, so the gateway never
//--- reads half a message (and neither do we: the gateway does the same for requests).
bool WriteAtomic(const string path, const string text)
{
   string tmp = path + ".tmp";
   int h = FileOpen(tmp, FILE_WRITE | FILE_BIN);
   if(h == INVALID_HANDLE) return false;
   uchar a[];
   int len = StringToCharArray(text, a, 0, WHOLE_ARRAY, CP_UTF8) - 1;
   FileWriteArray(h, a, 0, len);
   FileClose(h);
   return FileMove(tmp, 0, path, FILE_REWRITE);
}

string ReadAll(const string path)
{
   int h = FileOpen(path, FILE_READ | FILE_BIN);
   if(h == INVALID_HANDLE) return "";
   uchar b[];
   FileReadArray(h, b);
   FileClose(h);
   return CharArrayToString(b, 0, WHOLE_ARRAY, CP_UTF8);
}

//--- socket transport
bool SockSend(const string line)
{
   if(g_sock == INVALID_HANDLE) return false;
   uchar a[];
   int len = StringToCharArray(line + "\n", a, 0, WHOLE_ARRAY, CP_UTF8) - 1;
   if(SocketSend(g_sock, a, len) != len) { SocketClose(g_sock); g_sock = INVALID_HANDLE; return false; }
   return true;
}

void Connect()
{
   if(GetTickCount64() - g_last_try < 1000) return;
   g_last_try = GetTickCount64();
   if(g_sock != INVALID_HANDLE) SocketClose(g_sock);
   g_sock = SocketCreate();
   if(g_sock == INVALID_HANDLE) return;
   if(!SocketConnect(g_sock, GatewayHost, GatewayPort, 1000))
   {
      int e = GetLastError();
      static int fails = 0;
      if(fails++ % 60 == 0)      // once a minute while refused; the file transport carries on
         Print("socket to gateway refused, error ", e,
               e == 4014 ? " (127.0.0.1 not on the WebRequest list: using files until it is)" : "");
      SocketClose(g_sock); g_sock = INVALID_HANDLE; return;
   }
   g_buf = "";
   Print("socket to gateway up ", GatewayHost, ":", GatewayPort);
   Hello();                                    // the gateway pushes its config on every hello
}

// Unsolicited messages (hello, forced closes): over the socket when it is up, else a file
// each, named so they sort in order.
bool Send(const string line)
{
   if(g_sock != INVALID_HANDLE && SockSend(line)) return true;
   return WriteAtomic(OUTBOX + "ev-" + IntegerToString((long)TimeGMT()) + "-" +
                      IntegerToString((long)(g_evn++ % 1000000)) + ".evt", line);
}

void Hello()
{
   Send("{\"ev\":\"hello\",\"login\":" + IntegerToString(AccountInfoInteger(ACCOUNT_LOGIN)) +
        ",\"server\":" + Q(AccountInfoString(ACCOUNT_SERVER)) +
        ",\"company\":" + Q(AccountInfoString(ACCOUNT_COMPANY)) +
        ",\"build\":" + IntegerToString(TerminalInfoInteger(TERMINAL_BUILD)) +
        ",\"hedging\":" + B(AccountInfoInteger(ACCOUNT_MARGIN_MODE) == ACCOUNT_MARGIN_MODE_RETAIL_HEDGING) +
        ",\"demo\":" + B(AccountInfoInteger(ACCOUNT_TRADE_MODE) == ACCOUNT_TRADE_MODE_DEMO) +
        ",\"server_offset_s\":" + IntegerToString(ServerOffset()) +
        ",\"transport\":" + Q(g_sock != INVALID_HANDLE ? "socket" : "files") + "}");
}

// Every request waiting in gw\in, oldest first. -> how many were handled.
int ProcessInbox()
{
   string name, names[];
   long h = FileFindFirst(INBOX + "*.req", name);
   if(h == INVALID_HANDLE) return 0;
   do { int m = ArraySize(names); ArrayResize(names, m + 1); names[m] = name; } while(FileFindNext(h, name));
   FileFindClose(h);
   ArraySort(names);                               // ids are zero-padded: name order = send order
   for(int i = 0; i < ArraySize(names); i++)
   {
      string path = INBOX + names[i];
      string line = ReadAll(path);
      FileDelete(path);
      if(StringLen(line) < 2) continue;
      string reply = Handle(line);
      if(reply != "")
         WriteAtomic(OUTBOX + StringSubstr(names[i], 0, StringLen(names[i]) - 4) + ".rep", reply);
   }
   return ArraySize(names);
}

//--- helpers
// The broker's server clock runs on its own zone (GMT+2, +3 in summer): every *_TIME_MSC the terminal
// reports is server time. Subtract this to get UTC.
long ServerOffset() { return (long)(TimeTradeServer() - TimeGMT()); }

bool TF(const string s, ENUM_TIMEFRAMES &tf)
{
   if(s == "M1")  { tf = PERIOD_M1;  return true; }
   if(s == "M5")  { tf = PERIOD_M5;  return true; }
   if(s == "M15") { tf = PERIOD_M15; return true; }
   if(s == "M30") { tf = PERIOD_M30; return true; }
   if(s == "H1")  { tf = PERIOD_H1;  return true; }
   if(s == "H4")  { tf = PERIOD_H4;  return true; }
   if(s == "D1")  { tf = PERIOD_D1;  return true; }
   if(s == "W1")  { tf = PERIOD_W1;  return true; }
   if(s == "MN1") { tf = PERIOD_MN1; return true; }
   return false;
}

int MagicIdx(const long magic) { for(int i = 0; i < ArraySize(g_magic); i++) if(g_magic[i] == magic) return i; return -1; }

double NormVol(const string sym, double v)
{
   double step = SymbolInfoDouble(sym, SYMBOL_VOLUME_STEP), mx = SymbolInfoDouble(sym, SYMBOL_VOLUME_MAX);
   if(step <= 0) step = 0.01;
   v = MathRound(v / step) * step;
   if(mx > 0 && v > mx) v = mx;
   return NormalizeDouble(v, 8);
}

bool CidSeen(const string cid, string &found)
{
   if(cid == "") return false;
   for(int i = PositionsTotal() - 1; i >= 0; i--)
      if(PositionGetTicket(i) > 0 && PositionGetString(POSITION_COMMENT) == cid)
      { found = "position " + IntegerToString(PositionGetInteger(POSITION_TICKET)); return true; }
   for(int i = OrdersTotal() - 1; i >= 0; i--)                // a pending order still waiting
   {
      ulong o = OrderGetTicket(i);
      if(o > 0 && OrderGetString(ORDER_COMMENT) == cid)
      { found = "pending order " + IntegerToString((long)o); return true; }
   }
   if(HistorySelect(TimeCurrent() - 3 * 86400, TimeCurrent() + 86400))
   {
      for(int i = HistoryDealsTotal() - 1; i >= 0; i--)
      {
         ulong d = HistoryDealGetTicket(i);
         if(d > 0 && HistoryDealGetString(d, DEAL_COMMENT) == cid)
         { found = "deal " + IntegerToString((long)d); return true; }
      }
      // An order that filled (any part of it), in case the server did not copy its comment
      // onto the deal. A cancelled or rejected order filled nothing and does not count.
      for(int i = HistoryOrdersTotal() - 1; i >= 0; i--)
      {
         ulong o = HistoryOrderGetTicket(i);
         if(o > 0 && HistoryOrderGetString(o, ORDER_COMMENT) == cid &&
            HistoryOrderGetDouble(o, ORDER_VOLUME_INITIAL) - HistoryOrderGetDouble(o, ORDER_VOLUME_CURRENT) > 1e-9)
         { found = "filled order " + IntegerToString((long)o); return true; }
      }
   }
   return false;
}

string DealJson(const ulong deal, const MqlTradeResult &r)
{
   double comm = 0, fee = 0, profit = 0;
   long pos_id = 0;
   if(deal > 0 && HistoryDealSelect(deal))
   {
      comm = HistoryDealGetDouble(deal, DEAL_COMMISSION);
      fee = HistoryDealGetDouble(deal, DEAL_FEE);
      profit = HistoryDealGetDouble(deal, DEAL_PROFIT);
      pos_id = HistoryDealGetInteger(deal, DEAL_POSITION_ID);
   }
   return "{\"deal\":" + IntegerToString((long)deal) + ",\"order\":" + IntegerToString((long)r.order) +
          ",\"price\":" + N(r.price) + ",\"volume\":" + N(r.volume) + ",\"bid\":" + N(r.bid) +
          ",\"ask\":" + N(r.ask) + ",\"retcode\":" + IntegerToString(r.retcode) +
          ",\"commission\":" + N(comm) + ",\"fee\":" + N(fee) + ",\"profit\":" + N(profit) +
          ",\"position_id\":" + IntegerToString(pos_id) + "}";
}

bool Done(const uint rc) { return rc == TRADE_RETCODE_DONE || rc == TRADE_RETCODE_DONE_PARTIAL || rc == TRADE_RETCODE_PLACED; }

string PosJson()
{
   return "{\"ticket\":" + IntegerToString(PositionGetInteger(POSITION_TICKET)) +
          ",\"symbol\":" + Q(PositionGetString(POSITION_SYMBOL)) +
          ",\"side\":" + Q(PositionGetInteger(POSITION_TYPE) == POSITION_TYPE_BUY ? "buy" : "sell") +
          ",\"volume\":" + N(PositionGetDouble(POSITION_VOLUME)) +
          ",\"price_open\":" + N(PositionGetDouble(POSITION_PRICE_OPEN)) +
          ",\"price_current\":" + N(PositionGetDouble(POSITION_PRICE_CURRENT)) +
          ",\"sl\":" + N(PositionGetDouble(POSITION_SL)) + ",\"tp\":" + N(PositionGetDouble(POSITION_TP)) +
          ",\"profit\":" + N(PositionGetDouble(POSITION_PROFIT)) + ",\"swap\":" + N(PositionGetDouble(POSITION_SWAP)) +
          ",\"time_msc\":" + IntegerToString(PositionGetInteger(POSITION_TIME_MSC) - ServerOffset() * 1000) +
          ",\"magic\":" + IntegerToString(PositionGetInteger(POSITION_MAGIC)) +
          ",\"comment\":" + Q(PositionGetString(POSITION_COMMENT)) + "}";
}

//--- one trade primitive, shared by order / close / net / flatten
bool Market(const string sym, const bool buy, const double vol, const double sl, const double tp,
            const string cid, string &out)
{
   g_trade.SetTypeFillingBySymbol(sym);
   bool ok = buy ? g_trade.Buy(vol, sym, 0, sl, tp, cid) : g_trade.Sell(vol, sym, 0, sl, tp, cid);
   MqlTradeResult r; g_trade.Result(r);
   out = DealJson(r.deal, r);
   return ok && Done(r.retcode);
}

ENUM_ORDER_TYPE_FILLING Filling(const string sym)
{
   long m = SymbolInfoInteger(sym, SYMBOL_FILLING_MODE);
   if((m & SYMBOL_FILLING_FOK) != 0) return ORDER_FILLING_FOK;
   if((m & SYMBOL_FILLING_IOC) != 0) return ORDER_FILLING_IOC;
   return ORDER_FILLING_RETURN;
}

// A close sent by hand rather than through CTrade, so the deal carries the client_id in its
// comment: that is what lets CidSeen() refuse a retried close that already executed.
// The request alone, so the parallel closes in net/flatten send exactly what ClosePart sends.
bool CloseRequest(const ulong ticket, const double vol, const string cid, MqlTradeRequest &q)
{
   ZeroMemory(q);
   if(!PositionSelectByTicket(ticket)) return false;
   string sym = PositionGetString(POSITION_SYMBOL);
   bool is_buy = PositionGetInteger(POSITION_TYPE) == POSITION_TYPE_BUY;
   double pv = PositionGetDouble(POSITION_VOLUME);
   q.action = TRADE_ACTION_DEAL;
   q.position = ticket;
   q.symbol = sym;
   q.volume = (vol > 0 && vol < pv) ? vol : pv;
   q.type = is_buy ? ORDER_TYPE_SELL : ORDER_TYPE_BUY;
   q.price = is_buy ? SymbolInfoDouble(sym, SYMBOL_BID) : SymbolInfoDouble(sym, SYMBOL_ASK);
   q.deviation = g_dev;
   q.magic = PositionGetInteger(POSITION_MAGIC);
   q.comment = cid;
   q.type_filling = Filling(sym);
   return true;
}

bool ClosePart(const ulong ticket, const double vol, const string cid, string &out)
{
   MqlTradeRequest q; MqlTradeResult r;
   ZeroMemory(r);
   if(!CloseRequest(ticket, vol, cid, q)) { out = "{}"; return false; }
   bool ok = OrderSend(q, r);
   g_last_err = ok && Done(r.retcode) ? "" : ("retcode " + IntegerToString(r.retcode) + " " + r.comment);
   out = DealJson(r.deal, r);
   return ok && Done(r.retcode);
}

// 2026-10-02: a reversal over a 1.0 BTC short spread across 13 tickets closed them one
// synchronous OrderSend at a time, ~0.4 s each: 14 deals over 5.2 s while the price ran, $58/BTC
// of slippage. The closes do not depend on each other, so they now leave together
// (OrderSendAsync) and we wait for all of them. Each is still its own deal, so the owner still
// sees where every ticket went in; the opening still waits until every close has executed.
//
// The results are read back from the deal history, polled: OnTradeTransaction cannot run while
// Handle() is inside OnTimer, and Handle() must answer synchronously. A close is ours when its
// deal is an exit from that position carrying this command's client_id as comment.
//
// st[] per ticket: CL_SYNC (ClosePart's reply is in f[]), CL_ASYNC (deal found, deal[]),
// CL_SENT (sent, no deal yet = not confirmed), CL_FAIL (not executed). -> every close executed.
#define CL_FAIL  -1
#define CL_SENT   0
#define CL_ASYNC  1
#define CL_SYNC   2
#define CL_WAIT_MS 10000              // the gateway gives up on the EA after 15 s

bool CloseMany(const ulong &tk[], const double &vol[], const string cid,
               int &st[], ulong &deal[], double &bid[], double &ask[], string &f[], string &err)
{
   int n = ArraySize(tk);
   ArrayResize(st, n); ArrayResize(deal, n); ArrayResize(f, n);
   ArrayResize(bid, n); ArrayResize(ask, n); ArrayInitialize(bid, 0); ArrayInitialize(ask, 0);
   ArrayInitialize(st, CL_FAIL); ArrayInitialize(deal, 0);
   if(n == 0) return true;
   if(n == 1)                       // one ticket: the synchronous close, exactly as before
   {
      bool ok = ClosePart(tk[0], vol[0], cid, f[0]);
      st[0] = CL_SYNC;
      if(!ok) err = g_last_err;
      return ok;
   }
   ulong t0 = GetTickCount64();
   long floor_msc = ((long)TimeTradeServer() - 60) * 1000;      // deal times are server time
   int resend[]; int nre = 0;
   double pv0[]; ArrayResize(pv0, n); ArrayInitialize(pv0, 0);
   for(int a = 0; a < n; a++)
   {
      MqlTradeRequest q; MqlTradeResult r;
      ZeroMemory(r);
      if(!CloseRequest(tk[a], vol[a], cid, q))
      { if(err == "") err = "position " + IntegerToString((long)tk[a]) + " not found"; continue; }
      pv0[a] = PositionGetDouble(POSITION_VOLUME);
      bid[a] = SymbolInfoDouble(q.symbol, SYMBOL_BID); ask[a] = SymbolInfoDouble(q.symbol, SYMBOL_ASK);
      if(OrderSendAsync(q, r)) { st[a] = CL_SENT; continue; }
      // Not sent at all (false = it never reached the server, e.g. 10024 too many requests):
      // safe to send again, synchronously, once the rest are on their way.
      Print("parallel close: async close of ", tk[a], " not sent, retcode ", r.retcode, " ", r.comment,
            ": closing it synchronously");
      ArrayResize(resend, nre + 1); resend[nre++] = a;
   }
   for(int j = 0; j < nre; j++)
   {
      int a = resend[j];
      if(err != "") break;          // as before: nothing more is sent after a failed close
      bool ok = ClosePart(tk[a], vol[a], cid, f[a]);
      st[a] = CL_SYNC;                 // its reply goes in fills either way, as before
      if(!ok) err = "close of " + IntegerToString((long)tk[a]) + " failed: " + g_last_err;
   }
   // Wait for the deals. The cheap check (is each position gone or smaller?) runs every
   // millisecond; the deal history is searched once all of them have changed (and every 50 ms),
   // so the wait does not compete with the terminal, which files the replies on the same small CPU.
   int pend = 0;
   for(int a = 0; a < n; a++) if(st[a] == CL_SENT) pend++;
   ulong until = GetTickCount64() + CL_WAIT_MS, t_first = 0, t_search = 0;
   while(pend > 0 && GetTickCount64() < until)
   {
      bool moved = true;
      for(int a = 0; a < n && moved; a++)
         if(st[a] == CL_SENT && PositionSelectByTicket(tk[a]) && PositionGetDouble(POSITION_VOLUME) > pv0[a] - 1e-9)
            moved = false;
      if(GetTickCount64() - t_search >= 50) moved = true;      // and a plain search now and then
      if(moved) t_search = GetTickCount64();
      if(moved && HistorySelect((datetime)(floor_msc / 1000), TimeTradeServer() + 86400))
         for(int i = HistoryDealsTotal() - 1; i >= 0 && pend > 0; i--)
         {
            ulong d = HistoryDealGetTicket(i);
            if(d == 0 || HistoryDealGetInteger(d, DEAL_TIME_MSC) < floor_msc) continue;
            if(HistoryDealGetInteger(d, DEAL_ENTRY) != DEAL_ENTRY_OUT) continue;
            if(HistoryDealGetString(d, DEAL_COMMENT) != cid) continue;
            long pid = HistoryDealGetInteger(d, DEAL_POSITION_ID);
            for(int a = 0; a < n; a++)
               if(st[a] == CL_SENT && (long)tk[a] == pid)
               { st[a] = CL_ASYNC; deal[a] = d; pend--; if(t_first == 0) t_first = GetTickCount64(); break; }
         }
      if(pend > 0) Sleep(1);
   }
   string late = "";
   for(int a = 0; a < n; a++)
      if(st[a] == CL_SENT) late += (late == "" ? "" : ",") + IntegerToString((long)tk[a]);
   if(late != "")
      err = (err == "" ? "" : err + "; ") + "close of " + late + " not confirmed after " +
            IntegerToString(CL_WAIT_MS / 1000) + " s";
   Print("parallel close: ", n, " tickets (", nre, " resent synchronously): ",
         (err == "" ? "all done" : err), " in ", GetTickCount64() - t0, " ms, first seen after ",
         t_first > 0 ? (long)(t_first - t0) : -1, " ms");
   return err == "";
}

// The closes' fills, in ticket order. One that went out asynchronously is reported as DealJson
// reports a synchronous one: the deal's own order, price and volume, with the quote at the moment
// its request left (the deal follows within milliseconds; a tick read here cost 0.5 s cold).
string CloseFills(const int &st[], const ulong &deal[], const double &bid[], const double &ask[],
                  const string &f[])
{
   string out = "";
   for(int a = 0; a < ArraySize(st); a++)
   {
      string one = "";
      if(st[a] == CL_SYNC) one = f[a];
      else if(st[a] == CL_ASYNC)
      {
         MqlTradeResult r; ZeroMemory(r);
         r.retcode = TRADE_RETCODE_DONE;
         r.bid = bid[a]; r.ask = ask[a];
         if(HistoryDealSelect(deal[a]))
         {
            r.order = (ulong)HistoryDealGetInteger(deal[a], DEAL_ORDER);
            r.price = HistoryDealGetDouble(deal[a], DEAL_PRICE);
            r.volume = HistoryDealGetDouble(deal[a], DEAL_VOLUME);
         }
         one = DealJson(deal[a], r);
      }
      if(one != "") out += (out == "" ? "" : ",") + one;
   }
   return out;
}

//--- pending LIMIT orders
string OrderTypeName(const long t)
{
   if(t == ORDER_TYPE_BUY_LIMIT)       return "buy_limit";
   if(t == ORDER_TYPE_SELL_LIMIT)      return "sell_limit";
   if(t == ORDER_TYPE_BUY_STOP)        return "buy_stop";
   if(t == ORDER_TYPE_SELL_STOP)       return "sell_stop";
   if(t == ORDER_TYPE_BUY_STOP_LIMIT)  return "buy_stop_limit";
   if(t == ORDER_TYPE_SELL_STOP_LIMIT) return "sell_stop_limit";
   return "";                                   // market and close-by orders are not pending
}

// The order selected by OrderGetTicket()/OrderSelect(), as /orders reports it.
string OrderJson()
{
   string tn = OrderTypeName(OrderGetInteger(ORDER_TYPE));
   return "{\"ticket\":" + IntegerToString(OrderGetInteger(ORDER_TICKET)) +
          ",\"symbol\":" + Q(OrderGetString(ORDER_SYMBOL)) +
          ",\"side\":" + Q(StringFind(tn, "buy") == 0 ? "buy" : "sell") +
          ",\"type\":" + Q(tn) +
          ",\"volume\":" + N(OrderGetDouble(ORDER_VOLUME_CURRENT)) +
          ",\"volume_initial\":" + N(OrderGetDouble(ORDER_VOLUME_INITIAL)) +
          ",\"price\":" + N(OrderGetDouble(ORDER_PRICE_OPEN)) +
          ",\"sl\":" + N(OrderGetDouble(ORDER_SL)) + ",\"tp\":" + N(OrderGetDouble(ORDER_TP)) +
          ",\"comment\":" + Q(OrderGetString(ORDER_COMMENT)) +
          ",\"time_setup_msc\":" + IntegerToString(OrderGetInteger(ORDER_TIME_SETUP_MSC) - ServerOffset() * 1000) +
          ",\"magic\":" + IntegerToString(OrderGetInteger(ORDER_MAGIC)) + "}";
}

// What became of an order that is no longer pending, for this magic only: "filled",
// "partial" (part filled, rest cancelled), "cancelled", "expired", "rejected", or "unknown".
string GoneState(const ulong ticket, const long magic)
{
   if(ticket == 0 || !HistoryOrderSelect(ticket) || HistoryOrderGetInteger(ticket, ORDER_MAGIC) != magic)
      return "unknown";
   long st = HistoryOrderGetInteger(ticket, ORDER_STATE);
   double filled = HistoryOrderGetDouble(ticket, ORDER_VOLUME_INITIAL) - HistoryOrderGetDouble(ticket, ORDER_VOLUME_CURRENT);
   if(st == ORDER_STATE_FILLED) return "filled";
   if(filled > 1e-9) return "partial";
   if(st == ORDER_STATE_CANCELED) return "cancelled";
   if(st == ORDER_STATE_EXPIRED) return "expired";
   if(st == ORDER_STATE_REJECTED) return "rejected";
   return "unknown";
}

// Build a BUY LIMIT (below the ask) or SELL LIMIT (above the bid) request, or say why not.
// Everything MT5 would otherwise refuse or reinterpret is checked here against the current
// tick, so a limit can never be sent on the wrong side of the market.
bool PendingRequest(const string sym, const bool buy, double vol, double price, double sl, double tp,
                    const long magic, const string cid, MqlTradeRequest &q, string &why)
{
   string kind = buy ? "buy limit" : "sell limit";
   long tm = SymbolInfoInteger(sym, SYMBOL_TRADE_MODE);
   if(tm == SYMBOL_TRADE_MODE_DISABLED || tm == SYMBOL_TRADE_MODE_CLOSEONLY ||
      (buy && tm == SYMBOL_TRADE_MODE_SHORTONLY) || (!buy && tm == SYMBOL_TRADE_MODE_LONGONLY))
   { why = sym + " does not accept a new " + kind + " (trade_mode " + IntegerToString(tm) + ")"; return false; }
   if((SymbolInfoInteger(sym, SYMBOL_ORDER_MODE) & SYMBOL_ORDER_LIMIT) == 0)
   { why = sym + " does not accept limit orders"; return false; }
   if((SymbolInfoInteger(sym, SYMBOL_EXPIRATION_MODE) & SYMBOL_EXPIRATION_GTC) == 0)
   { why = sym + " does not accept good-till-cancelled orders"; return false; }

   // volume: refused, not rounded, when it is off the symbol's grid
   double vmin = SymbolInfoDouble(sym, SYMBOL_VOLUME_MIN), vmax = SymbolInfoDouble(sym, SYMBOL_VOLUME_MAX),
          step = SymbolInfoDouble(sym, SYMBOL_VOLUME_STEP);
   if(step <= 0) step = 0.01;
   double steps = vol / step;
   if(vol <= 0 || MathAbs(steps - MathRound(steps)) > 1e-6)
   { why = "volume " + N(vol) + " is not a positive multiple of volume_step " + N(step); return false; }
   vol = NormalizeDouble(MathRound(steps) * step, 8);
   if(vol < vmin - 1e-9) { why = "volume " + N(vol) + " below volume_min " + N(vmin); return false; }
   if(vmax > 0 && vol > vmax + 1e-9) { why = "volume " + N(vol) + " above volume_max " + N(vmax); return false; }

   // price: on the tick grid, rounded away from the market (a buy limit down, a sell limit up)
   MqlTick t;
   if(!SymbolInfoTick(sym, t) || t.bid <= 0 || t.ask <= 0) { why = "no current tick for " + sym; return false; }
   int dg = (int)SymbolInfoInteger(sym, SYMBOL_DIGITS);
   double pt = SymbolInfoDouble(sym, SYMBOL_POINT), ts = SymbolInfoDouble(sym, SYMBOL_TRADE_TICK_SIZE);
   if(ts <= 0) ts = pt;
   if(price <= 0) { why = "price required"; return false; }
   price = NormalizeDouble((buy ? MathFloor(price / ts + 1e-7) : MathCeil(price / ts - 1e-7)) * ts, dg);
   long stops = SymbolInfoInteger(sym, SYMBOL_TRADE_STOPS_LEVEL), freeze = SymbolInfoInteger(sym, SYMBOL_TRADE_FREEZE_LEVEL);
   // at least the stops level away (MT5's rule) and outside the freeze level (inside it the
   // order could not be cancelled or changed until the price moved away)
   double gap = MathMax(stops, freeze) * pt, eps = pt / 2;
   string lv = " (stops_level " + IntegerToString(stops) + ", freeze_level " + IntegerToString(freeze) + " points)";
   if(buy && !(price < t.ask - eps && t.ask - price >= gap - eps))
   { why = "buy limit price " + N(price) + " must be below ask " + N(t.ask) + (gap > 0 ? " by at least " + N(gap) + lv : ""); return false; }
   if(!buy && !(price > t.bid + eps && price - t.bid >= gap - eps))
   { why = "sell limit price " + N(price) + " must be above bid " + N(t.bid) + (gap > 0 ? " by at least " + N(gap) + lv : ""); return false; }

   // sl / tp (0 = none), the stops level away from the order's own price
   double sg = stops * pt;
   if(sl < 0 || tp < 0) { why = "sl/tp must be >= 0"; return false; }
   if(sl > 0) sl = NormalizeDouble(MathRound(sl / ts) * ts, dg);
   if(tp > 0) tp = NormalizeDouble(MathRound(tp / ts) * ts, dg);
   if(sl > 0 && (buy ? !(sl < price - eps && price - sl >= sg - eps) : !(sl > price + eps && sl - price >= sg - eps)))
   { why = "sl " + N(sl) + " must be " + (buy ? "below" : "above") + " the " + kind + " price " + N(price) + (sg > 0 ? " by at least " + N(sg) : ""); return false; }
   if(tp > 0 && (buy ? !(tp > price + eps && tp - price >= sg - eps) : !(tp < price - eps && price - tp >= sg - eps)))
   { why = "tp " + N(tp) + " must be " + (buy ? "above" : "below") + " the " + kind + " price " + N(price) + (sg > 0 ? " by at least " + N(sg) : ""); return false; }

   ZeroMemory(q);
   q.action = TRADE_ACTION_PENDING;
   q.magic = (ulong)magic;
   q.symbol = sym;
   q.volume = vol;
   q.price = price;
   q.sl = sl;
   q.tp = tp;
   q.type = buy ? ORDER_TYPE_BUY_LIMIT : ORDER_TYPE_SELL_LIMIT;
   q.type_filling = Filling(sym);
   q.type_time = ORDER_TIME_GTC;
   q.expiration = 0;
   q.comment = cid;                              // copied onto the fill's deal: the bot's marker
   return true;
}

//--- command dispatch
string Handle(const string line)
{
   if(!JParse(line)) return "";
   string id = JS("id"), op = JS("op");
   string head = "{\"re\":" + id + ",";
   long exp = JL("exp");
   if(exp > 0 && (long)TimeGMT() > exp)
      return head + "\"ok\":false,\"error\":\"expired before execution\"}";
   long magic = JL("magic");
   string sym = JS("symbol");
   g_trade.SetExpertMagicNumber((ulong)magic);
   g_dev = (ulong)MathMax(JL("deviation"), 0);
   g_trade.SetDeviationInPoints(g_dev);
   g_trade.SetAsyncMode(false);

   if(op == "ping") return head + "\"ok\":true,\"pong\":" + IntegerToString((long)TimeGMT()) + "}";

   if(op == "cfg")
   {
      int i = MagicIdx(magic);
      if(i < 0) { i = ArraySize(g_magic); ArrayResize(g_magic, i + 1); ArrayResize(g_maxhold, i + 1); }
      g_magic[i] = magic; g_maxhold[i] = (int)JL("max_hold_s");
      return head + "\"ok\":true}";
   }

   if(op == "account")
      return head + "\"ok\":true,\"login\":" + IntegerToString(AccountInfoInteger(ACCOUNT_LOGIN)) +
             ",\"server\":" + Q(AccountInfoString(ACCOUNT_SERVER)) + ",\"currency\":" + Q(AccountInfoString(ACCOUNT_CURRENCY)) +
             ",\"balance\":" + N(AccountInfoDouble(ACCOUNT_BALANCE), 2) + ",\"equity\":" + N(AccountInfoDouble(ACCOUNT_EQUITY), 2) +
             ",\"margin\":" + N(AccountInfoDouble(ACCOUNT_MARGIN), 2) + ",\"margin_free\":" + N(AccountInfoDouble(ACCOUNT_MARGIN_FREE), 2) +
             ",\"margin_level\":" + N(AccountInfoDouble(ACCOUNT_MARGIN_LEVEL), 2) +
             ",\"leverage\":" + IntegerToString(AccountInfoInteger(ACCOUNT_LEVERAGE)) +
             // where the broker calls and then closes us: margin level %, or money if so_mode says so
             ",\"margin_so_call\":" + N(AccountInfoDouble(ACCOUNT_MARGIN_SO_CALL), 2) +
             ",\"margin_so_so\":" + N(AccountInfoDouble(ACCOUNT_MARGIN_SO_SO), 2) +
             ",\"margin_so_mode\":" + Q(AccountInfoInteger(ACCOUNT_MARGIN_SO_MODE) == ACCOUNT_STOPOUT_MODE_PERCENT ? "percent" : "money") +
             ",\"hedging\":" + B(AccountInfoInteger(ACCOUNT_MARGIN_MODE) == ACCOUNT_MARGIN_MODE_RETAIL_HEDGING) +
             ",\"demo\":" + B(AccountInfoInteger(ACCOUNT_TRADE_MODE) == ACCOUNT_TRADE_MODE_DEMO) +
             ",\"connected\":" + B(TerminalInfoInteger(TERMINAL_CONNECTED) != 0) +
             ",\"trade_allowed\":" + B(TerminalInfoInteger(TERMINAL_TRADE_ALLOWED) != 0 && MQLInfoInteger(MQL_TRADE_ALLOWED) != 0 && AccountInfoInteger(ACCOUNT_TRADE_ALLOWED) != 0) +
             ",\"ping_ms\":" + N(TerminalInfoInteger(TERMINAL_PING_LAST) / 1000.0, 1) +
             ",\"server_offset_s\":" + IntegerToString(ServerOffset()) + "}";

   if(op == "symbol" || op == "tick")
   {
      if(!SymbolSelect(sym, true)) return head + "\"ok\":false,\"error\":" + Q("unknown symbol " + sym) + "}";
      MqlTick t;
      bool have = SymbolInfoTick(sym, t);
      string tick = ",\"bid\":" + N(have ? t.bid : 0) + ",\"ask\":" + N(have ? t.ask : 0) +
                    ",\"time_msc\":" + IntegerToString(have ? t.time_msc - ServerOffset() * 1000 : 0);
      if(op == "tick") return head + "\"ok\":true,\"symbol\":" + Q(sym) + tick + "}";
      return head + "\"ok\":true,\"symbol\":" + Q(sym) + tick +
             ",\"description\":" + Q(SymbolInfoString(sym, SYMBOL_DESCRIPTION)) +
             ",\"contract_size\":" + N(SymbolInfoDouble(sym, SYMBOL_TRADE_CONTRACT_SIZE)) +
             ",\"volume_min\":" + N(SymbolInfoDouble(sym, SYMBOL_VOLUME_MIN)) +
             ",\"volume_max\":" + N(SymbolInfoDouble(sym, SYMBOL_VOLUME_MAX)) +
             ",\"volume_step\":" + N(SymbolInfoDouble(sym, SYMBOL_VOLUME_STEP)) +
             ",\"digits\":" + IntegerToString(SymbolInfoInteger(sym, SYMBOL_DIGITS)) +
             ",\"point\":" + N(SymbolInfoDouble(sym, SYMBOL_POINT), 10) +
             ",\"stops_level\":" + IntegerToString(SymbolInfoInteger(sym, SYMBOL_TRADE_STOPS_LEVEL)) +
             ",\"freeze_level\":" + IntegerToString(SymbolInfoInteger(sym, SYMBOL_TRADE_FREEZE_LEVEL)) +
             ",\"trade_mode\":" + IntegerToString(SymbolInfoInteger(sym, SYMBOL_TRADE_MODE)) +
             ",\"spread_points\":" + IntegerToString(SymbolInfoInteger(sym, SYMBOL_SPREAD)) +
             ",\"swap_long\":" + N(SymbolInfoDouble(sym, SYMBOL_SWAP_LONG)) +
             ",\"swap_short\":" + N(SymbolInfoDouble(sym, SYMBOL_SWAP_SHORT)) +
             // what the two numbers above are in (points, %/year, money...) and the day it is
             // charged three times: without these a swap rate cannot be turned into dollars
             ",\"swap_mode\":" + IntegerToString(SymbolInfoInteger(sym, SYMBOL_SWAP_MODE)) +
             ",\"swap_mode_name\":" + Q(EnumToString((ENUM_SYMBOL_SWAP_MODE)SymbolInfoInteger(sym, SYMBOL_SWAP_MODE))) +
             ",\"swap_3days\":" + Q(EnumToString((ENUM_DAY_OF_WEEK)SymbolInfoInteger(sym, SYMBOL_SWAP_ROLLOVER3DAYS))) +
             ",\"tick_value\":" + N(SymbolInfoDouble(sym, SYMBOL_TRADE_TICK_VALUE)) +
             ",\"tick_size\":" + N(SymbolInfoDouble(sym, SYMBOL_TRADE_TICK_SIZE), 10) +
             ",\"margin_initial\":" + N(SymbolInfoDouble(sym, SYMBOL_MARGIN_INITIAL)) +
             ",\"currency_profit\":" + Q(SymbolInfoString(sym, SYMBOL_CURRENCY_PROFIT)) + "}";
   }

   if(op == "ticks")
   {
      MqlTick ticks[];
      long off = ServerOffset() * 1000;               // request and answer in UTC; the terminal is in server time
      int n = CopyTicksRange(sym, ticks, COPY_TICKS_INFO, (ulong)(JL("from_ms") + off), 0);
      int lim = (int)JL("limit"), start = MathMax(0, n - lim);
      string arr = "";
      for(int i = start; i < n; i++)
         arr += (i > start ? "," : "") + "[" + IntegerToString(ticks[i].time_msc - off) + "," + N(ticks[i].bid) + "," + N(ticks[i].ask) + "]";
      return head + "\"ok\":" + B(n >= 0) + ",\"n\":" + IntegerToString(n) +
             ",\"server_offset_s\":" + IntegerToString(ServerOffset()) + ",\"ticks\":[" + arr + "]}";
   }

   if(op == "symbols")
   {
      // Every symbol the server offers, not only Market Watch (brokers list 1,000+), paged.
      // Static properties only: an unselected symbol has no live tick to report.
      int total = SymbolsTotal(false), off = (int)MathMax(JL("offset"), 0), lim = (int)JL("limit");
      if(lim <= 0 || lim > 500) lim = 500;
      int end = MathMin(total, off + lim);
      string arr = "";
      for(int i = off; i < end; i++)
      {
         string s = SymbolName(i, false);
         StringAdd(arr, (i > off ? "," : "") + "{\"name\":" + Q(s) +
                   ",\"path\":" + Q(SymbolInfoString(s, SYMBOL_PATH)) +
                   ",\"description\":" + Q(SymbolInfoString(s, SYMBOL_DESCRIPTION)) +
                   ",\"trade_mode\":" + IntegerToString(SymbolInfoInteger(s, SYMBOL_TRADE_MODE)) +
                   ",\"digits\":" + IntegerToString(SymbolInfoInteger(s, SYMBOL_DIGITS)) +
                   ",\"contract_size\":" + N(SymbolInfoDouble(s, SYMBOL_TRADE_CONTRACT_SIZE)) +
                   ",\"volume_min\":" + N(SymbolInfoDouble(s, SYMBOL_VOLUME_MIN)) +
                   ",\"volume_step\":" + N(SymbolInfoDouble(s, SYMBOL_VOLUME_STEP)) +
                   ",\"currency_profit\":" + Q(SymbolInfoString(s, SYMBOL_CURRENCY_PROFIT)) +
                   ",\"selected\":" + B(SymbolInfoInteger(s, SYMBOL_SELECT) != 0) + "}");
      }
      return head + "\"ok\":true,\"total\":" + IntegerToString(total) + ",\"offset\":" +
             IntegerToString(off) + ",\"symbols\":[" + arr + "]}";
   }

   if(op == "rates")
   {
      // OHLC bars, oldest first, [time, open, high, low, close, tick_volume, spread_points].
      // Times stay in SERVER time on purpose: the broker's zone moves with DST, so subtracting today's
      // offset from a bar in the other half of the year would be an hour out. The caller
      // converts, and pages backwards with to_srv = its oldest bar's time - 1.
      ENUM_TIMEFRAMES tf;
      if(!TF(JS("tf"), tf)) return head + "\"ok\":false,\"error\":" + Q("bad tf " + JS("tf")) + "}";
      if(!SymbolSelect(sym, true)) return head + "\"ok\":false,\"error\":" + Q("unknown symbol " + sym) + "}";
      int cnt = (int)JL("count");
      if(cnt <= 0 || cnt > 5000) cnt = 2000;
      long to = JL("to_srv");
      MqlRates r[];
      ResetLastError();
      int n = to > 0 ? CopyRates(sym, tf, (datetime)to, cnt, r) : CopyRates(sym, tf, (int)0, cnt, r);
      int err = GetLastError();
      string meta = ",\"symbol\":" + Q(sym) + ",\"tf\":" + Q(JS("tf")) + ",\"time_basis\":\"server\"" +
                    ",\"server_offset_s\":" + IntegerToString(ServerOffset()) +
                    ",\"bars_total\":" + IntegerToString(Bars(sym, tf)) +
                    ",\"synced\":" + B(SeriesInfoInteger(sym, tf, SERIES_SYNCHRONIZED) != 0) +
                    ",\"max_bars\":" + IntegerToString(TerminalInfoInteger(TERMINAL_MAXBARS));
      // release: the caller's last page. Take the symbol back out of Market Watch so a scan
      // over hundreds of symbols doesn't leave the terminal streaming ticks for all of them.
      // Only the caller knows whether it was there before its scan (/symbols "selected"), so
      // it must not release a symbol someone else watches. MT5 refuses while a chart or a
      // position holds it.
      if(JL("release") != 0) SymbolSelect(sym, false);
      if(n < 0)
         return head + "\"ok\":false,\"retry\":true,\"error\":" +
                Q("CopyRates error " + IntegerToString(err) + ": history still loading, retry") + meta + "}";
      int d = (int)SymbolInfoInteger(sym, SYMBOL_DIGITS);
      string arr = "";
      for(int i = 0; i < n; i++)
         StringAdd(arr, (i > 0 ? "," : "") + "[" + IntegerToString((long)r[i].time) + "," +
                   N(r[i].open, d) + "," + N(r[i].high, d) + "," + N(r[i].low, d) + "," +
                   N(r[i].close, d) + "," + IntegerToString(r[i].tick_volume) + "," +
                   IntegerToString(r[i].spread) + "]");
      return head + "\"ok\":true,\"n\":" + IntegerToString(n) + meta + ",\"bars\":[" + arr + "]}";
   }

   if(op == "positions")
   {
      string arr = ""; int k = 0;
      for(int i = 0; i < PositionsTotal(); i++)
      {
         if(PositionGetTicket(i) == 0) continue;
         if(magic != 0 && PositionGetInteger(POSITION_MAGIC) != magic) continue;
         if(sym != "" && PositionGetString(POSITION_SYMBOL) != sym) continue;
         arr += (k++ > 0 ? "," : "") + PosJson();
      }
      return head + "\"ok\":true,\"positions\":[" + arr + "]}";
   }

   if(op == "orders")
   {
      // this client's pending orders (not positions, not orders already filled)
      string arr = ""; int k = 0;
      for(int i = 0; i < OrdersTotal(); i++)
      {
         if(OrderGetTicket(i) == 0) continue;
         if(OrderGetInteger(ORDER_MAGIC) != magic) continue;
         if(sym != "" && OrderGetString(ORDER_SYMBOL) != sym) continue;
         if(OrderTypeName(OrderGetInteger(ORDER_TYPE)) == "") continue;
         arr += (k++ > 0 ? "," : "") + OrderJson();
      }
      return head + "\"ok\":true,\"orders\":[" + arr + "]}";
   }

   if(op == "deals")
   {
      datetime from = (datetime)(JL("from_ms") / 1000 + ServerOffset()),
               to = JL("to_ms") > 0 ? (datetime)(JL("to_ms") / 1000 + ServerOffset()) : TimeCurrent() + 86400;
      if(!HistorySelect(from, to)) return head + "\"ok\":false,\"error\":\"HistorySelect failed\"}";
      string arr = ""; int k = 0, n = HistoryDealsTotal();
      for(int i = MathMax(0, n - 2000); i < n; i++)
      {
         ulong d = HistoryDealGetTicket(i);
         if(d == 0 || HistoryDealGetInteger(d, DEAL_MAGIC) != magic) continue;
         if(sym != "" && HistoryDealGetString(d, DEAL_SYMBOL) != sym) continue;
         long ty = HistoryDealGetInteger(d, DEAL_TYPE);
         if(ty != DEAL_TYPE_BUY && ty != DEAL_TYPE_SELL) continue;
         long en = HistoryDealGetInteger(d, DEAL_ENTRY);
         // the deal's comment, or the comment of the order it filled when the server left the
         // deal's empty: a pending order's client_id is how a bot recognises its own fill
         string dc = HistoryDealGetString(d, DEAL_COMMENT);
         long dord = HistoryDealGetInteger(d, DEAL_ORDER);
         if(dc == "" && dord > 0) dc = HistoryOrderGetString((ulong)dord, ORDER_COMMENT);
         arr += (k++ > 0 ? "," : "") + "{\"deal\":" + IntegerToString((long)d) +
                ",\"order\":" + IntegerToString(HistoryDealGetInteger(d, DEAL_ORDER)) +
                ",\"position_id\":" + IntegerToString(HistoryDealGetInteger(d, DEAL_POSITION_ID)) +
                ",\"time_msc\":" + IntegerToString(HistoryDealGetInteger(d, DEAL_TIME_MSC) - ServerOffset() * 1000) +
                ",\"side\":" + Q(ty == DEAL_TYPE_BUY ? "buy" : "sell") +
                ",\"entry\":" + Q(en == DEAL_ENTRY_IN ? "in" : en == DEAL_ENTRY_OUT ? "out" : en == DEAL_ENTRY_INOUT ? "inout" : "out_by") +
                ",\"symbol\":" + Q(HistoryDealGetString(d, DEAL_SYMBOL)) +
                ",\"volume\":" + N(HistoryDealGetDouble(d, DEAL_VOLUME)) + ",\"price\":" + N(HistoryDealGetDouble(d, DEAL_PRICE)) +
                ",\"commission\":" + N(HistoryDealGetDouble(d, DEAL_COMMISSION)) + ",\"fee\":" + N(HistoryDealGetDouble(d, DEAL_FEE)) +
                ",\"swap\":" + N(HistoryDealGetDouble(d, DEAL_SWAP)) + ",\"profit\":" + N(HistoryDealGetDouble(d, DEAL_PROFIT)) +
                ",\"comment\":" + Q(dc) + "}";
      }
      return head + "\"ok\":true,\"deals\":[" + arr + "]}";
   }

   if(op == "balance")
   {
      // Money in or out that was not traded for: deposits, withdrawals, credits, bonuses.
      // Account-wide (these deals carry no magic), so every client sees the same list.
      datetime from = (datetime)(JL("from_ms") / 1000 + ServerOffset());
      if(!HistorySelect(from, TimeCurrent() + 86400)) return head + "\"ok\":false,\"error\":\"HistorySelect failed\"}";
      string arr = ""; int k = 0, n = HistoryDealsTotal();
      for(int i = MathMax(0, n - 2000); i < n; i++)
      {
         ulong d = HistoryDealGetTicket(i);
         if(d == 0) continue;
         long ty = HistoryDealGetInteger(d, DEAL_TYPE);
         string tn = ty == DEAL_TYPE_BALANCE ? "balance" : ty == DEAL_TYPE_CREDIT ? "credit" :
                     ty == DEAL_TYPE_BONUS ? "bonus" : ty == DEAL_TYPE_CHARGE ? "charge" :
                     ty == DEAL_TYPE_CORRECTION ? "correction" : ty == DEAL_TYPE_COMMISSION ? "commission" : "";
         if(tn == "") continue;                    // buys and sells are /deals, not money movement
         arr += (k++ > 0 ? "," : "") + "{\"deal\":" + IntegerToString((long)d) +
                ",\"time_msc\":" + IntegerToString(HistoryDealGetInteger(d, DEAL_TIME_MSC) - ServerOffset() * 1000) +
                ",\"type\":" + Q(tn) + ",\"amount\":" + N(HistoryDealGetDouble(d, DEAL_PROFIT), 2) +
                ",\"comment\":" + Q(HistoryDealGetString(d, DEAL_COMMENT)) + "}";
      }
      return head + "\"ok\":true,\"ops\":[" + arr + "]}";
   }

   string cid = JS("cid"), seen = "";
   if((op == "order" || op == "pending" || op == "net" || op == "close" || op == "flatten") && CidSeen(cid, seen))
      return head + "\"ok\":false,\"duplicate\":true,\"error\":" + Q("client_id already executed: " + seen) + "}";

   if(op == "order")
   {
      string side = JS("side"), fill;
      double vol = NormVol(sym, JD("volume"));
      if(vol <= 0 || (side != "buy" && side != "sell")) return head + "\"ok\":false,\"error\":\"bad side/volume\"}";
      bool ok = Market(sym, side == "buy", vol, JD("sl"), JD("tp"), cid, fill);
      return head + "\"ok\":" + B(ok) + ",\"fill\":" + fill + ",\"error\":" + Q(ok ? "" : g_trade.ResultRetcodeDescription()) + "}";
   }

   if(op == "pending")
   {
      string side = JS("side"), why = "";
      if(side != "buy" && side != "sell") return head + "\"ok\":false,\"error\":\"side must be buy or sell\"}";
      if(!SymbolSelect(sym, true)) return head + "\"ok\":false,\"error\":" + Q("unknown symbol " + sym) + "}";
      MqlTradeRequest q; MqlTradeResult r;
      ZeroMemory(r);
      if(!PendingRequest(sym, side == "buy", JD("volume"), JD("price"), JD("sl"), JD("tp"), magic, cid, q, why))
         return head + "\"ok\":false,\"error\":" + Q(why) + "}";
      bool ok = OrderSend(q, r) && Done(r.retcode);
      if(!ok && r.retcode == TRADE_RETCODE_INVALID_FILL && q.type_filling != ORDER_FILLING_RETURN)
      {
         // some servers take a resting order only as "return": nothing was placed, so retry once
         q.type_filling = ORDER_FILLING_RETURN;
         ZeroMemory(r);
         ok = OrderSend(q, r) && Done(r.retcode);
      }
      if(ok && r.order == 0)                       // never seen; the client_id still guards a retry
         return head + "\"ok\":false,\"retcode\":" + IntegerToString(r.retcode) +
                ",\"error\":" + Q("placed without a ticket: look for client_id " + cid + " in /orders") + "}";
      if(!ok)
         return head + "\"ok\":false,\"retcode\":" + IntegerToString(r.retcode) +
                ",\"error\":" + Q("retcode " + IntegerToString(r.retcode) + " " + r.comment) + "}";
      return head + "\"ok\":true,\"order\":" + IntegerToString((long)r.order) +
             ",\"type\":" + Q(side == "buy" ? "buy_limit" : "sell_limit") +
             ",\"price\":" + N(q.price) + ",\"volume\":" + N(q.volume) +
             ",\"sl\":" + N(q.sl) + ",\"tp\":" + N(q.tp) +
             ",\"retcode\":" + IntegerToString(r.retcode) + ",\"error\":\"\"}";
   }

   if(op == "cancel")
   {
      // Gone already (filled, cancelled, or another client's): "not found", plus what became
      // of it when the history knows
      ulong ticket = (ulong)JL("ticket");
      if(ticket == 0 || !OrderSelect(ticket) || OrderGetInteger(ORDER_MAGIC) != magic ||
         OrderTypeName(OrderGetInteger(ORDER_TYPE)) == "")
         return head + "\"ok\":false,\"error\":\"not found\",\"state\":" + Q(GoneState(ticket, magic)) + "}";
      MqlTradeRequest q; MqlTradeResult r;
      ZeroMemory(q); ZeroMemory(r);
      q.action = TRADE_ACTION_REMOVE;
      q.order = ticket;
      bool ok = OrderSend(q, r) && Done(r.retcode);
      if(!ok && !OrderSelect(ticket))              // it filled (or went) while we asked
         return head + "\"ok\":false,\"error\":\"not found\",\"state\":" + Q(GoneState(ticket, magic)) + "}";
      return head + "\"ok\":" + B(ok) + ",\"retcode\":" + IntegerToString(r.retcode) +
             ",\"error\":" + Q(ok ? "" : "retcode " + IntegerToString(r.retcode) + " " + r.comment) + "}";
   }

   if(op == "close" || op == "modify")
   {
      ulong ticket = (ulong)JL("ticket");
      if(!PositionSelectByTicket(ticket) || PositionGetInteger(POSITION_MAGIC) != magic)
         return head + "\"ok\":false,\"error\":\"no such position for this client\"}";
      if(op == "modify")
      {
         bool ok = g_trade.PositionModify(ticket, JD("sl"), JD("tp"));
         return head + "\"ok\":" + B(ok) + ",\"error\":" + Q(ok ? "" : g_trade.ResultRetcodeDescription()) + "}";
      }
      string fill;
      bool ok = ClosePart(ticket, NormVol(PositionGetString(POSITION_SYMBOL), JD("volume")), cid, fill);
      return head + "\"ok\":" + B(ok) + ",\"fill\":" + fill + ",\"error\":" + Q(g_last_err) + "}";
   }

   if(op == "flatten" || op == "net")
   {
      // net: move this client's position in `sym` by `delta` lots. On a hedging account the
      // opposite side is closed first (oldest first) so the client holds ONE net position,
      // not a long and a short that cancel on paper and both pay spread and swap.
      bool hedging = AccountInfoInteger(ACCOUNT_MARGIN_MODE) == ACCOUNT_MARGIN_MODE_RETAIL_HEDGING;
      double delta = (op == "net") ? JD("delta") : 0;
      bool buy = delta > 0;
      double left = NormVol(sym, MathAbs(delta));
      string fills = "", err = "";
      bool allok = true;
      ulong tk[]; long tm[];
      for(int i = 0; i < PositionsTotal(); i++)
      {
         ulong t = PositionGetTicket(i);
         if(t == 0 || PositionGetInteger(POSITION_MAGIC) != magic) continue;
         if(sym != "" && PositionGetString(POSITION_SYMBOL) != sym) continue;
         bool is_buy = PositionGetInteger(POSITION_TYPE) == POSITION_TYPE_BUY;
         if(op == "net" && (is_buy == buy || !hedging)) continue;       // only the opposite side
         int m = ArraySize(tk); ArrayResize(tk, m + 1); ArrayResize(tm, m + 1);
         tk[m] = t; tm[m] = PositionGetInteger(POSITION_TIME_MSC);
      }
      for(int a = 0; a < ArraySize(tk); a++)          // oldest first
         for(int b = a + 1; b < ArraySize(tk); b++)
            if(tm[b] < tm[a]) { ulong x = tk[a]; tk[a] = tk[b]; tk[b] = x; long y = tm[a]; tm[a] = tm[b]; tm[b] = y; }
      // which tickets, and how much of each: decided up front, then closed all at once
      ulong ctk[]; double cv[];
      for(int a = 0; a < ArraySize(tk); a++)
      {
         if(op == "net" && left <= 0) break;
         if(!PositionSelectByTicket(tk[a])) continue;
         double pv = PositionGetDouble(POSITION_VOLUME);
         double v = (op == "net" && left < pv) ? left : 0;          // 0 = the whole position
         int m = ArraySize(ctk); ArrayResize(ctk, m + 1); ArrayResize(cv, m + 1);
         ctk[m] = tk[a]; cv[m] = v;
         if(op == "net") left = NormVol(sym, left - (v > 0 ? v : pv));
      }
      int st[]; ulong cdeal[]; double cbid[], cask[]; string cf[];
      allok = CloseMany(ctk, cv, cid, st, cdeal, cbid, cask, cf, err);
      string open_fill = "";
      if(op == "net" && allok && left > 0)
      {
         if(left >= SymbolInfoDouble(sym, SYMBOL_VOLUME_MIN))
         {
            bool ok = Market(sym, buy, left, 0, 0, cid, open_fill);
            if(!ok) { allok = false; err = g_trade.ResultRetcodeDescription(); }
         }
         else err = "remainder below volume_min";
      }
      // one entry per deal, closes in ticket order, the opening (if any) last
      fills = CloseFills(st, cdeal, cbid, cask, cf);
      if(open_fill != "") fills += (fills == "" ? "" : ",") + open_fill;
      return head + "\"ok\":" + B(allok) + ",\"fills\":[" + fills + "],\"error\":" + Q(err) + "}";
   }

   return head + "\"ok\":false,\"error\":" + Q("unknown op " + op) + "}";
}

//--- periodic duties: heartbeat and the EA-side time barrier
void Periodic()
{
   if(GetTickCount64() - g_last_hb < (ulong)HeartbeatMs) return;
   g_last_hb = GetTickCount64();
   int npos = 0;
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong t = PositionGetTicket(i);
      if(t == 0) continue;
      int mi = MagicIdx(PositionGetInteger(POSITION_MAGIC));
      if(mi < 0) continue;
      npos++;
      if(g_maxhold[mi] > 0 && TimeCurrent() - (datetime)PositionGetInteger(POSITION_TIME) > g_maxhold[mi])
      {
         g_trade.SetExpertMagicNumber((ulong)g_magic[mi]);
         string f;
         bool ok = ClosePart(t, 0, "maxhold", f);
         Send("{\"ev\":\"forced_close\",\"reason\":\"max_hold\",\"magic\":" + IntegerToString(g_magic[mi]) +
              ",\"ticket\":" + IntegerToString((long)t) + ",\"ok\":" + B(ok) + ",\"fill\":" + f + "}");
      }
   }
   string hb = "{\"ev\":\"hb\",\"t\":" + IntegerToString((long)TimeGMT()) +
        ",\"connected\":" + B(TerminalInfoInteger(TERMINAL_CONNECTED) != 0) +
        ",\"trade_allowed\":" + B(TerminalInfoInteger(TERMINAL_TRADE_ALLOWED) != 0 && MQLInfoInteger(MQL_TRADE_ALLOWED) != 0) +
        ",\"equity\":" + N(AccountInfoDouble(ACCOUNT_EQUITY), 2) + ",\"balance\":" + N(AccountInfoDouble(ACCOUNT_BALANCE), 2) +
        ",\"margin\":" + N(AccountInfoDouble(ACCOUNT_MARGIN), 2) + ",\"positions\":" + IntegerToString(npos) +
        ",\"ping_ms\":" + N(TerminalInfoInteger(TERMINAL_PING_LAST) / 1000.0, 1) + "}";
   WriteAtomic(STATUS, hb);                     // always: the gateway's liveness check on files
   if(g_sock != INVALID_HANDLE) SockSend(hb);
}

//--- a pending LIMIT order of a managed magic filled: tell the gateway (/events), the way a
//--- max_hold close is reported. The deal itself is in /deals with the order's client_id.
void OnTradeTransaction(const MqlTradeTransaction &trans, const MqlTradeRequest &request, const MqlTradeResult &result)
{
   if(trans.type != TRADE_TRANSACTION_DEAL_ADD || trans.deal == 0) return;
   ulong d = trans.deal;
   if(!HistoryDealSelect(d)) return;
   long magic = HistoryDealGetInteger(d, DEAL_MAGIC);
   if(MagicIdx(magic) < 0) return;
   ulong ord = (ulong)HistoryDealGetInteger(d, DEAL_ORDER);
   if(ord == 0 || !HistoryOrderSelect(ord)) return;
   long ty = HistoryOrderGetInteger(ord, ORDER_TYPE);
   if(ty != ORDER_TYPE_BUY_LIMIT && ty != ORDER_TYPE_SELL_LIMIT) return;
   string dc = HistoryDealGetString(d, DEAL_COMMENT);
   if(dc == "") dc = HistoryOrderGetString(ord, ORDER_COMMENT);
   Send("{\"ev\":\"pending_fill\",\"magic\":" + IntegerToString(magic) +
        ",\"order\":" + IntegerToString((long)ord) + ",\"deal\":" + IntegerToString((long)d) +
        ",\"position_id\":" + IntegerToString(HistoryDealGetInteger(d, DEAL_POSITION_ID)) +
        ",\"symbol\":" + Q(HistoryDealGetString(d, DEAL_SYMBOL)) +
        ",\"side\":" + Q(ty == ORDER_TYPE_BUY_LIMIT ? "buy" : "sell") +
        ",\"type\":" + Q(OrderTypeName(ty)) +
        ",\"volume\":" + N(HistoryDealGetDouble(d, DEAL_VOLUME)) + ",\"price\":" + N(HistoryDealGetDouble(d, DEAL_PRICE)) +
        ",\"volume_left\":" + N(HistoryOrderGetDouble(ord, ORDER_VOLUME_CURRENT)) +
        ",\"comment\":" + Q(dc) +
        ",\"time_msc\":" + IntegerToString(HistoryDealGetInteger(d, DEAL_TIME_MSC) - ServerOffset() * 1000) + "}");
}

//--- event loop. The timer handler stays inside for ~0.5 s, checking gw\in every 1 ms while
//--- busy and every 5 ms while idle: a request starts within milliseconds of being written,
//--- without spinning a burstable 1/8-core machine flat out.
int OnInit()
{
   FolderCreate("gw");
   FolderCreate("gw\\in");
   FolderCreate("gw\\out");
   EventSetMillisecondTimer(10);
   Connect();
   if(g_sock == INVALID_HANDLE) Hello();       // no socket yet: announce ourselves on files
   Print("MT5Gateway: socket ", (g_sock != INVALID_HANDLE ? "up" : "refused, files meanwhile"));
   return INIT_SUCCEEDED;
}

void OnDeinit(const int reason)
{
   EventKillTimer();
   if(g_sock != INVALID_HANDLE) SocketClose(g_sock);
}

// Requests read off the socket, answered on the socket.
int ProcessSocket()
{
   uint n = SocketIsReadable(g_sock);
   if(n == 0) return 0;
   uchar a[];
   int got = SocketRead(g_sock, a, n, 100);
   if(got > 0) g_buf += CharArrayToString(a, 0, got, CP_UTF8);
   int p, k = 0;
   while((p = StringFind(g_buf, "\n")) >= 0)
   {
      string line = StringSubstr(g_buf, 0, p);
      g_buf = StringSubstr(g_buf, p + 1);
      if(StringLen(line) > 1) { string reply = Handle(line); if(reply != "") SockSend(reply); k++; }
   }
   return k;
}

void OnTimer()
{
   ulong until = GetTickCount64() + 500;
   while(!IsStopped() && GetTickCount64() < until)
   {
      if(g_sock != INVALID_HANDLE && !SocketIsConnected(g_sock)) { SocketClose(g_sock); g_sock = INVALID_HANDLE; }
      if(g_sock == INVALID_HANDLE) Connect();          // once a second while refused
      bool act = false;
      if(g_sock != INVALID_HANDLE && ProcessSocket() > 0) act = true;
      // The file inbox: polled fast only while the socket is down (it is the live transport
      // then); with the socket up the gateway sends nothing there, so a slow sweep is enough.
      if(g_sock == INVALID_HANDLE || GetTickCount64() - g_last_inbox >= 50)
      {
         if(ProcessInbox() > 0) act = true;
         g_last_inbox = GetTickCount64();
      }
      if(act) g_last_act = GetTickCount64();
      Periodic();
      Sleep(GetTickCount64() - g_last_act < 200 ? 1 : 5);
   }
}
