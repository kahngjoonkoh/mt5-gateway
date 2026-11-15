#!/bin/bash
# Keeps 127.0.0.1 on MT5's WebRequest allow-list, so the EA's socket (the ~1 ms path) is allowed.
#
# MT5 forgets that list on every terminal restart (it is not in common.ini, the start config or
# the Wine registry), and the EA then falls back to the file transport. This watchdog notices
# (the gateway reports transport=files for over a minute while the EA is alive) and re-adds the
# two entries the only way MT5 accepts them: its own Options dialog, driven with xdotool on the
# virtual display. Coordinates are for the 1024x768x24 Xvfb in mt5-xvfb.service.
#
# Runs as mt5-allowlist.service. Log: journalctl -u mt5-allowlist
set -u
export DISPLAY=:99
L=${MT5GW_LISTEN:-127.0.0.1:8000}
GW=http://${L/#0.0.0.0/127.0.0.1}
TOKEN=$(python3 -c 'import json,os;print(json.load(open(os.path.expanduser("~/mt5gw/clients.json")))["recorder"]["token"])')
files_since=0

transport() {   # socket | files | none
  curl -s -m 5 -H "Authorization: Bearer $TOKEN" "$GW/health" |
    python3 -c 'import json,sys
try: print(json.load(sys.stdin).get("transport") or "none")
except Exception: print("none")'
}

restore() {
  local main opt
  main=$(xdotool search --name "${MT5_WINDOW:?set MT5_WINDOW in ~/mt5gw/host.env}" | head -1)
  [ -z "$main" ] && { echo "no terminal window yet"; return 1; }
  for w in $(xdotool search --name "^Options$"); do xdotool key --window "$w" Escape; done
  nav=$(xdotool search --name "^Navigator$" | head -1)           # a floating Navigator covers the menu
  [ -n "$nav" ] && xdotool windowmove "$nav" 700 440
  xdotool windowactivate "$main" 2>/dev/null
  xdotool mousemove 216 41 click 1; sleep 2                      # Tools
  xdotool key Up; sleep 0.5; xdotool key Return; sleep 8         # ... Options (last item)
  opt=$(xdotool search --name "^Options$" | head -1)
  [ -z "$opt" ] && { echo "Options dialog did not open"; return 1; }
  xdotool mousemove 393 282 click 1; sleep 1                     # the Expert Advisors tab
  xdotool mousemove 380 503 click --repeat 2 --delay 120 1; sleep 1.5
  xdotool type --delay 60 "http://127.0.0.1"; xdotool key Return; sleep 1.5
  xdotool mousemove 380 523 click --repeat 2 --delay 120 1; sleep 1.5
  xdotool type --delay 60 "127.0.0.1"; xdotool key Return; sleep 1.5
  xdotool mousemove 632 636 click 1                              # OK
  return 0
}

while true; do
  t=$(transport)
  if [ "$t" = "files" ]; then
    [ "$files_since" = 0 ] && files_since=$(date +%s)
    if [ $(( $(date +%s) - files_since )) -ge 60 ]; then
      echo "EA on files for 60 s+ (allow-list lost?): restoring"
      if restore; then
        for i in $(seq 1 15); do sleep 2; [ "$(transport)" = "socket" ] && break; done
        if [ "$(transport)" = "socket" ]; then echo "restored: socket up"; files_since=0
        else echo "socket still refused after restore; retrying in 5 min"; files_since=$(( $(date +%s) + 240 )); fi
      else
        files_since=$(( $(date +%s) - 30 ))                      # try again in ~30 s
      fi
    fi
  else
    files_since=0
  fi
  sleep 30
done
