#!/bin/bash
# Rebuild the MT5 host (x86 Debian/Ubuntu, 1 GB RAM is enough) from scratch.
# Run ON the executor host as the user the services will run as, from a checkout of this repo, after
#     mkdir -p ~/mt5gw && cp deploy/executor/host.env.example ~/mt5gw/host.env   # and fill it in
#     bash deploy/executor/install.sh
# then fill ~/.mt5/drive_c/mt5gw/startup.ini (login) and ~/mt5gw/clients.json (tokens) and
#     sudo systemctl enable --now mt5-xvfb mt5-gateway mt5-terminal mt5-allowlist
set -euxo pipefail
REPO=$(cd "$(dirname "$0")/../.." && pwd)
export WINEPREFIX=$HOME/.mt5 WINEARCH=win64 WINEDEBUG=-all WINEDLLOVERRIDES="mscoree,mshtml="
. ~/mt5gw/host.env
MT5=${MT5_DIR:?set MT5_DIR in ~/mt5gw/host.env}

# 1 GB of RAM: MT5 under Wine needs the swap
if ! swapon --show | grep -q /swapfile; then
  sudo fallocate -l 2G /swapfile && sudo chmod 600 /swapfile && sudo mkswap /swapfile && sudo swapon /swapfile
  grep -q "^/swapfile" /etc/fstab || echo "/swapfile none swap sw 0 0" | sudo tee -a /etc/fstab
  echo "vm.swappiness=10" | sudo tee /etc/sysctl.d/99-swap.conf && sudo sysctl -p /etc/sysctl.d/99-swap.conf
fi

export DEBIAN_FRONTEND=noninteractive
sudo dpkg --add-architecture i386
sudo mkdir -pm755 /etc/apt/keyrings
sudo wget -qO /etc/apt/keyrings/winehq-archive.key https://dl.winehq.org/wine-builds/winehq.key
sudo wget -qNP /etc/apt/sources.list.d/ https://dl.winehq.org/wine-builds/$(. /etc/os-release; echo "$ID/dists/$VERSION_CODENAME/winehq-$VERSION_CODENAME").sources
sudo apt-get update -q
sudo apt-get install -y -q --install-recommends winehq-stable
sudo apt-get install -y -q xvfb xauth cabextract curl wget python3 x11-apps netpbm

# Wine prefix. Slow on a burstable 1/8-core machine (minutes); do not interrupt it — a
# half-built prefix fails later with "could not load kernel32.dll".
[ -f "$WINEPREFIX/drive_c/windows/system32/kernel32.dll" ] || xvfb-run -a wineboot -u
wine reg add "HKCU\\Software\\Wine" /v Version /d win10 /f

# the broker's own MT5 build (knows its servers). /auto = silent install.
if [ ! -f "$MT5/terminal64.exe" ]; then
  curl -sLo /tmp/mt5setup.exe "${MT5_SETUP_URL:?set MT5_SETUP_URL in ~/mt5gw/host.env}"
  timeout 1500 xvfb-run -a wine /tmp/mt5setup.exe /auto || true
fi
# first launch unpacks MQL5/ (Include, Experts...); give it time, then stop it
if [ ! -f "$MT5/MQL5/Include/Trade/Trade.mqh" ]; then
  (cd "$MT5" && timeout 180 xvfb-run -a wine terminal64.exe /portable || true)
fi

# the EA
cp "$REPO/mql5/MT5Gateway.mq5" "$MT5/MQL5/Experts/"
(cd "$MT5" && timeout 300 xvfb-run -a wine MetaEditor64.exe /portable \
   /compile:"MQL5\\Experts\\MT5Gateway.mq5" /log:"C:\\compile.log" || true)
iconv -f UTF-16LE -t UTF-8 "$WINEPREFIX/drive_c/compile.log" | tail -1
test -f "$MT5/MQL5/Experts/MT5Gateway.ex5"

# allow the EA's localhost socket and algo trading (Tools > Options > Expert Advisors)
python3 - "$MT5/Config/common.ini" <<'EOF'
import sys
p = sys.argv[1]
s = open(p, encoding="utf-16").read()
if "[Experts]" not in s:
    s = s.rstrip("\r\n") + ("\r\n[Experts]\r\nAllowLiveTrading=1\r\nAllowDllImport=0\r\nEnabled=1\r\n"
                            "Account=0\r\nProfile=0\r\nChart=0\r\nApi=0\r\nWebRequest=1\r\n"
                            "WebRequestUrl=http://127.0.0.1;127.0.0.1\r\n")
    open(p, "w", encoding="utf-16").write(s)
EOF

# startup config (login filled in by hand: it holds the password)
mkdir -p "$WINEPREFIX/drive_c/mt5gw"
[ -f "$WINEPREFIX/drive_c/mt5gw/startup.ini" ] || cp "$REPO/deploy/executor/startup.ini.template" "$WINEPREFIX/drive_c/mt5gw/startup.ini"
chmod 600 "$WINEPREFIX/drive_c/mt5gw/startup.ini"

# gateway
mkdir -p ~/mt5gw/state && chmod 700 ~/mt5gw
cp "$REPO/gateway/gateway.py" "$REPO/deploy/executor/mt5-allowlist.sh" "$REPO/deploy/executor/mt5-terminal.sh" ~/mt5gw/
chmod 755 ~/mt5gw/mt5-allowlist.sh ~/mt5gw/mt5-terminal.sh
sudo apt-get install -y -q xdotool curl
[ -f ~/mt5gw/clients.json ] || cp "$REPO/clients.example.json" ~/mt5gw/clients.json
chmod 600 ~/mt5gw/clients.json

# the units say user ubuntu and /home/ubuntu: write them for this user
for u in "$REPO"/deploy/executor/mt5-*.service; do
  sed "s|/home/ubuntu|$HOME|g; s|^User=ubuntu$|User=$(id -un)|" "$u" | sudo tee /etc/systemd/system/"$(basename "$u")" >/dev/null
done
sudo systemctl daemon-reload

# bots on a private network: open 8000 to that range only (also allow it in the cloud
# firewall). Leave MT5GW_ALLOW_CIDR empty to keep 8000 closed and use an SSH tunnel.
if [ -n "${MT5GW_ALLOW_CIDR:-}" ] && ! sudo iptables -C INPUT -s "$MT5GW_ALLOW_CIDR" -p tcp --dport 8000 -j ACCEPT 2>/dev/null; then
  n=$(sudo iptables -L INPUT --line-numbers | awk '/REJECT/{print $1; exit}')
  sudo iptables -I INPUT "${n:-1}" -s "$MT5GW_ALLOW_CIDR" -p tcp --dport 8000 -j ACCEPT
  sudo sh -c 'iptables-save > /etc/iptables/rules.v4'
fi
echo "installed. Fill startup.ini + clients.json, then: sudo systemctl enable --now mt5-xvfb mt5-gateway mt5-terminal mt5-allowlist"
