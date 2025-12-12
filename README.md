# disk_scan

Destructive bad-sector checker for block devices. It writes a deterministic xoroshiro128++ stream (seed 42) across each target, then reads it back to verify every byte without buffering the data. A real-time TUI shows per-disk progress, speed, ETA, and error markers.

## ⚠️ Destructive
Running this tool **erases data** on every device passed via `-d/--device`. The binary prompts unless `--skip-warning` is provided.

## Usage

```bash
cargo run -- \
  -d /dev/sdX \
  -d /dev/sdY \
  --timeout-ms 30000 \
  --chunk-size 1048576 \
  --mode rich
```

Flags:
- `-d, --device <PATH>`: block device path (repeatable)
- `--timeout-ms <ms>`: per-chunk read/write timeout
- `--chunk-size <bytes>`: chunk size for IO (default 1 MiB)
- `--mode [rich|basic]`: Unicode or ASCII progress bars
- `--skip-warning`: skip the destructive confirmation prompt
- `--skip-fs-check`: skip pre-flight detection of partition tables/filesystems (dangerous)

The UI auto-sizes to the terminal width. Write progress uses solid blocks (`█`/`#`), read uses shaded blocks (`░`/`=`). Errors render as `X` (write), `E` (read), and `V` (value mismatch). ETA is per-stage (write then read).

## Local testing with fake devices

You can simulate block devices with sparse files:

```bash
mkdir -p /tmp/disk-scan-test
truncate -s 32M /tmp/disk-scan-test/fake0.img
truncate -s 16M /tmp/disk-scan-test/fake1.img

cargo run -- \
  --skip-warning \
  -d /tmp/disk-scan-test/fake0.img \
  -d /tmp/disk-scan-test/fake1.img \
  --mode basic
```

To simulate errors, remove write permissions or flip bytes manually between the write and read stages.

## FreeBSD

FreeBSD is supported for the core scanner and TUI (size probing uses `DIOCGMEDIASIZE`). Loop-device helpers are Linux-only; on FreeBSD, create md(4) devices backed by sparse files for local testing.

## Reporting

When the scan completes, a summary is printed listing total errors and per-chunk failure details for each disk.
