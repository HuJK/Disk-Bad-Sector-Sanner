#!/usr/bin/env bash
set -euo pipefail

# Detach loop devices created for disk_scan testing and remove backing files.
# Usage: ./scripts/cleanup_loops.sh [base_dir]
# base_dir defaults to /tmp/disk-scan-loops

BASE_DIR=${1:-/tmp/disk-scan-loops}

if [[ ! -d "$BASE_DIR" ]]; then
  echo "No loop directory found at $BASE_DIR"
  exit 0
fi

# Detach loop devices bound to files in BASE_DIR
while read -r line; do
  loopdev=$(echo "$line" | cut -d: -f1)
  losetup -d "$loopdev" || true
done < <(losetup -a | grep "$BASE_DIR" || true)

rm -f "$BASE_DIR"/fake*.img
rmdir "$BASE_DIR" 2>/dev/null || true

echo "Cleaned loop devices and removed $BASE_DIR"
