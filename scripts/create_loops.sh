#!/usr/bin/env bash
set -euo pipefail

# Create loop devices backed by sparse files for testing disk_scan.
# Sizes are fixed: 50M, 100M, 200M, 400M.
# Usage: ./scripts/create_loops.sh

BASE_DIR=${BASE_DIR:-/tmp/disk-scan-loops}
mkdir -p "$BASE_DIR"

sizes=(50M 100M 200M 400M)
created=()

for idx in "${!sizes[@]}"; do
  size=${sizes[$idx]}
  file="$BASE_DIR/fake${idx}.img"
  if [[ ! -f "$file" ]]; then
    truncate -s "$size" "$file"
  fi

  loopdev=$(losetup -f)
  losetup -P "$loopdev" "$file"
  created+=("$loopdev")
done

echo "Created loop devices: ${created[*]} (backing files in $BASE_DIR)"
echo
echo "Run the scanner (destructive!) with:"
echo "  cargo run -- --skip-warning $(printf ' -d %s' "${created[@]}")"
