#!/bin/bash
# The MT5 terminal for mt5-terminal.service, with room for a LiveUpdate to finish.
#
# When a new build has been downloaded, MT5 starts its updater
# (AppData\...\liveupdate\terminal64.exe /update) on the next launch and exits 0. If that exit
# ends the service, systemd and `wineserver -k` kill the updater mid-install, the next start
# finds the same staged build, and the terminal loops start -> update -> exit every ~2 min
# without ever loading the EA (seen on a host cloned from another that had downloaded a new
# build and not restarted since).
#
# So this script stays the service's main process until the updater is done. The updater
# relaunches the terminal on its own, without our /config; that copy is stopped here, and
# systemd's restart starts the new build with our startup.ini.
set -u
cd "${MT5_DIR:?set MT5_DIR in ~/mt5gw/host.env}"
# [StartUp] in startup.ini opens the one EA chart on every launch; MT5 would ALSO restore the
# charts it saved at exit, giving two EAs fighting over the one gateway link. Start clean.
rm -f MQL5/Profiles/Charts/Default/chart*.chr
wine terminal64.exe /portable '/config:C:\mt5gw\startup.ini'
rc=$?
for i in $(seq 1 150); do                      # up to 5 minutes
  pgrep -f 'liveupdate' >/dev/null || break
  [ "$i" = 1 ] && echo "LiveUpdate running: letting it finish"
  sleep 2
done
[ "${i:-1}" -gt 1 ] && echo "LiveUpdate done after $(( (i - 1) * 2 ))s"
wineserver -k 2>/dev/null
exit $rc
