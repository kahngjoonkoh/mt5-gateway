#!/bin/bash
# Bring one executor host in line with this checkout.
#
#     deploy/push.sh HOST          # dry run: list what differs on HOST
#     deploy/push.sh HOST --apply    # copy what differs, restart only what it belongs to
#
# Every executor runs the same files; what differs per host stays ON the host and is never
# touched here: ~/.mt5/drive_c/mt5gw/startup.ini (the broker login), ~/mt5gw/clients.json (the
# bots' tokens) and ~/mt5gw/host.env (broker paths, listen address). The EA is only reported: recompiling it swaps the running EA inside a live
# terminal, so that is done by hand (see README, "Changing the EA").
set -euo pipefail
HOST=${1:?usage: deploy/push.sh HOST [--apply]}
APPLY=${2:-}
REPO=$(cd "$(dirname "$0")/.." && pwd)
RH=$(ssh "$HOST" 'echo "$HOME"') RU=$(ssh "$HOST" 'id -un')
render() { [[ $1 == *.service ]] && sed "s|/home/ubuntu|$RH|g; s|^User=ubuntu$|User=$RU|" "$REPO/$1" || cat "$REPO/$1"; }

# the units and scripts read host.env; refuse to push them to a host that lacks it
MT5=$(ssh "$HOST" '. ~/mt5gw/host.env 2>/dev/null && echo "$MT5_DIR"' || true)
[ -n "$MT5" ] || { echo "$HOST has no ~/mt5gw/host.env with MT5_DIR (see deploy/executor/host.env.example)"; exit 1; }

# local file | remote path | service to restart when it changes
FILES=(
  "gateway/gateway.py|$RH/mt5gw/gateway.py|mt5-gateway"
  "deploy/executor/mt5-allowlist.sh|$RH/mt5gw/mt5-allowlist.sh|mt5-allowlist"
  "deploy/executor/mt5-terminal.sh|$RH/mt5gw/mt5-terminal.sh|mt5-terminal"
)
for u in "$REPO"/deploy/executor/mt5-*.service; do
  n=$(basename "$u" .service)
  FILES+=("deploy/executor/$n.service|/etc/systemd/system/$n.service|$n")
done

changed=() restart=() units=0
for f in "${FILES[@]}"; do
  IFS='|' read -r src dst svc <<< "$f"
  if ssh "$HOST" "cat '$dst' 2>/dev/null" | cmp -s - <(render "$src"); then
    echo "same     $src"
  else
    echo "DIFFERS  $src -> $HOST:$dst"
    changed+=("$f"); restart+=("$svc")
    [[ $dst == /etc/systemd/* ]] && units=1
  fi
done
if ssh "$HOST" "cat '$MT5/MQL5/Experts/MT5Gateway.mq5' 2>/dev/null" | cmp -s - "$REPO/mql5/MT5Gateway.mq5"; then
  echo "same     mql5/MT5Gateway.mq5"
else
  echo "DIFFERS  mql5/MT5Gateway.mq5 (not pushed: recompile by hand, see README)"
fi

[ ${#changed[@]} -eq 0 ] && { echo "$HOST is up to date"; exit 0; }
[ "$APPLY" = "--apply" ] || { echo "dry run; add --apply to push"; exit 0; }

for f in "${changed[@]}"; do
  IFS='|' read -r src dst svc <<< "$f"
  if [[ $dst == /etc/* ]]; then
    render "$src" | ssh "$HOST" "sudo tee '$dst' >/dev/null"
  else
    ssh "$HOST" "cat > '$dst.new' && chmod --reference='$dst' '$dst.new' 2>/dev/null || chmod 755 '$dst.new'; mv '$dst.new' '$dst'" < "$REPO/$src"
  fi
  echo "pushed   $src"
done
[ $units = 1 ] && ssh "$HOST" "sudo systemctl daemon-reload"
svcs=$(printf '%s\n' "${restart[@]}" | sort -u | tr '\n' ' ')
echo "restarting on $HOST: $svcs(bots see the gateway drop for a few seconds and retry)"
ssh "$HOST" "sudo systemctl restart $svcs && systemctl is-active $svcs"
