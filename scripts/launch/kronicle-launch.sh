#!/usr/bin/env bash

APP_DIR="${KRONICLE_APP_DIR:-$HOME/kronicle}"
APP_PORT="${KRONICLE_APP_PORT:-8008}"

cd "$APP_DIR" || exit 1

STATE_DIR="${XDG_STATE_HOME:-$HOME/.local/state}/kronicle"
STATE_FILE="$STATE_DIR/launch-fails"
MAX_FAILS=6          # ~30 min of consecutive failures before escalating
mkdir -p "$STATE_DIR"

if curl -sfm 5 http://localhost:$APP_PORT/health/ready | grep -q '"status" *: *"ready"'; then
    rm -f "$STATE_FILE"          # healthy: reset the counter
    exit 0
fi

fails=0
[[ -f "$STATE_FILE" ]] && fails=$(cat "$STATE_FILE")
fails=$((fails + 1))

podman-compose down 2>/dev/null || true
podman-compose up -d

if (( fails >= MAX_FAILS )); then
    systemctl --user stop kronicle-launch.timer
    logger -t kronicle-launch "DB down for too long ($fails consecutive checks); supervised stack halted"
    exit 1                       # service shows failed -> visible to you/sysadmin
fi

echo "$fails" > "$STATE_FILE"
exit 0
