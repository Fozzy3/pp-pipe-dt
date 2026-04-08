#!/usr/bin/env bash
# Install and enable the GTFS-RT collector systemd timer (user-level)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
SYSTEMD_DIR="$HOME/.config/systemd/user"

mkdir -p "$SYSTEMD_DIR"

cp "$PROJECT_DIR/systemd/gtfs-collector.service" "$SYSTEMD_DIR/"
cp "$PROJECT_DIR/systemd/gtfs-collector.timer" "$SYSTEMD_DIR/"

systemctl --user daemon-reload
systemctl --user enable gtfs-collector.timer
systemctl --user start gtfs-collector.timer

echo "Timer installed and started. Check status with:"
echo "  systemctl --user status gtfs-collector.timer"
echo "  journalctl --user -u gtfs-collector -f"
