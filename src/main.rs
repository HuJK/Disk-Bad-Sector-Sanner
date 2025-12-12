use anyhow::{anyhow, Context, Result};
use clap::{ArgAction, Parser, ValueEnum};
use crossbeam_channel::{unbounded, Receiver, Sender};
use crossterm::{cursor, event};
use crossterm::terminal::{self, ClearType};
use rand::{RngCore, SeedableRng};
use rand_xoshiro::Xoroshiro128PlusPlus;
use signal_hook::consts::SIGINT;
use signal_hook::flag as signal_flag;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const DEFAULT_CHUNK_SIZE: usize = 1024 * 1024; // 1 MiB
#[cfg(target_os = "linux")]
const BLKGETSIZE64: libc::c_ulong = 0x8008_1272; // fallback value if libc missing symbol
#[cfg(target_os = "freebsd")]
const DIOCGMEDIASIZE: libc::c_ulong = libc::DIOCGMEDIASIZE;
const ERROR_MARKS_LIMIT: usize = 1024;
const DESTRUCTIVE_WARNING: &str = "WARNING: This operation overwrites data on the specified block devices. All data will be destroyed.";
const MAX_SCAN_BYTES: usize = 1024 * 1024; // 1 MiB
const MIN_GPT_SIZE: u64 = 1024;
const MIN_MBR_SIZE: u64 = 512;
const MIN_NTFS_SIZE: u64 = 1024 * 1024;
const MIN_EXFAT_SIZE: u64 = 1024 * 1024;
const MIN_FAT32_SIZE: u64 = 32 * 1024 * 1024;
const MIN_EXT_SIZE: u64 = 2048;
const MIN_APFS_SIZE: u64 = 1024 * 1024;
const MIN_ZFS_SIZE: u64 = 64 * 1024 * 1024;

#[derive(Parser, Debug)]
#[command(author, version, about = "Disk bad sector checker (destructive)")]
struct Args {
    /// Block device path, e.g. -d /dev/sda (can repeat)
    #[arg(short = 'd', long = "device", action = ArgAction::Append)]
    devices: Vec<PathBuf>,

    /// Timeout for individual read/write operations in milliseconds
    #[arg(long, default_value = "30000")]
    timeout_ms: u64,

    /// Size of each chunk read/written in bytes
    #[arg(long, default_value_t = DEFAULT_CHUNK_SIZE)]
    chunk_size: usize,

    /// Render mode: rich (unicode) or basic (ASCII only)
    #[arg(long, default_value = "rich")]
    mode: UiMode,

    /// Skip the destructive warning prompt
    #[arg(long)]
    skip_warning: bool,

    /// Skip filesystem/partition table detection before destructive scan
    #[arg(long)]
    skip_fs_check: bool,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum UiMode {
    Rich,
    Basic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage {
    Writing,
    Reading,
    Done,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ErrorKind {
    Write,
    Read,
    Value,
}

#[derive(Debug, Clone)]
struct ErrorRecord {
    chunk_idx: u64,
    offset: u64,
    kind: ErrorKind,
    message: String,
}

#[derive(Debug, Clone)]
struct InitEvent {
    device: String,
    total_bytes: u64,
    total_chunks: u64,
}

#[derive(Debug, Clone)]
struct ProgressEvent {
    device: String,
    stage: Stage,
    chunk_idx: u64,
    bytes: u64,
    error: Option<ErrorRecord>,
}

#[derive(Debug, Clone)]
struct FinishEvent {
    device: String,
}

#[derive(Debug, Clone)]
enum WorkerEvent {
    Init(InitEvent),
    Progress(ProgressEvent),
    Finish(FinishEvent),
    Fatal { device: String, message: String },
}

#[derive(Debug)]
struct UiDeviceState {
    total_bytes: u64,
    total_chunks: u64,
    written_bytes: u64,
    read_bytes: u64,
    write_errors: u64,
    read_errors: u64,
    value_errors: u64,
    stage: Stage,
    write_start: Instant,
    read_start: Option<Instant>,
    last_stage_bytes: u64,
    last_stage_instant: Instant,
    speed_mbps: f64,
    errors: Vec<ErrorRecord>,
    error_marks: VecDeque<ErrorRecord>,
    write_end: Option<Instant>,
    read_end: Option<Instant>,
}

fn main() -> Result<()> {
    let args = Args::parse();

    if args.devices.is_empty() {
        return Err(anyhow!("At least one -d/--device must be provided"));
    }

    if !args.skip_fs_check {
        enforce_fs_checks(&args.devices)?;
    }

    if !args.skip_warning {
        print_destructive_warning()?;
    }

    let timeout = Duration::from_millis(args.timeout_ms);
    let (tx, rx) = unbounded();
    let cancel = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let cancel_signal = cancel.clone();
    signal_flag::register(SIGINT, cancel_signal)?;

    for device in &args.devices {
        let device_path = device.clone();
        let tx_for_thread = tx.clone();
        let mode = args.mode;
        let chunk_size = args.chunk_size;
        let cancel_clone = cancel.clone();
        thread::spawn(move || {
            let display_name = device_path.display().to_string();
            if let Err(err) = scan_device(device_path, chunk_size, timeout, tx_for_thread.clone(), mode, cancel_clone) {
                let _ = tx_for_thread.send(WorkerEvent::Fatal {
                    device: display_name,
                    message: format!("{:#}", err),
                });
            }
        });
    }

    drop(tx); // close extra sender handles when workers exit

    let mut ui_guard = TerminalGuard::enter()?;
    render_loop(&mut ui_guard, rx, args.mode, cancel.clone())?;
    Ok(())
}

fn scan_device(
    path: PathBuf,
    chunk_size: usize,
    timeout: Duration,
    tx: Sender<WorkerEvent>,
    _mode: UiMode,
    cancel: Arc<std::sync::atomic::AtomicBool>,
) -> Result<()> {
    let size = detect_device_size(&path)?;
    let total_chunks = ((size + chunk_size as u64 - 1) / chunk_size as u64).max(1);

    tx.send(WorkerEvent::Init(InitEvent {
        device: path.display().to_string(),
        total_bytes: size,
        total_chunks,
    }))
    .ok();

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .with_context(|| format!("failed to open {}", path.display()))?;

    let file = Arc::new(Mutex::new(file));

    let mut rng = Xoroshiro128PlusPlus::seed_from_u64(42);
    let mut write_failed: HashSet<u64> = HashSet::new();

    // Write phase
    for chunk_idx in 0..total_chunks {
        if cancel.load(std::sync::atomic::Ordering::Relaxed) {
            break;
        }
        let offset = chunk_idx * chunk_size as u64;
        let current_chunk = std::cmp::min(chunk_size as u64, size.saturating_sub(offset)) as usize;
        let mut buffer = vec![0u8; current_chunk];
        fill_buffer(&mut rng, &mut buffer);

        let file_clone = file.clone();
        let write_result = run_with_timeout(timeout, move || {
            let mut file = file_clone
                .lock()
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "poisoned lock"))?;
            file.seek(SeekFrom::Start(offset))?;
            file.write_all(&buffer)?;
            file.flush()?;
            Ok(())
        });

        let mut error_record = None;
        if let Err(err) = write_result {
            write_failed.insert(chunk_idx);
            error_record = Some(ErrorRecord {
                chunk_idx,
                offset,
                kind: ErrorKind::Write,
                message: err.to_string(),
            });
        }

        tx.send(WorkerEvent::Progress(ProgressEvent {
            device: path.display().to_string(),
            stage: Stage::Writing,
            chunk_idx,
            bytes: current_chunk as u64,
            error: error_record,
        }))
        .ok();
    }

    let _ = file
        .lock()
        .map(|f| f.sync_all())
        .unwrap_or(Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "failed to lock for sync",
        )));

    // Read + verify phase
    let mut rng = Xoroshiro128PlusPlus::seed_from_u64(42);
    for chunk_idx in 0..total_chunks {
        if cancel.load(std::sync::atomic::Ordering::Relaxed) {
            break;
        }
        let offset = chunk_idx * chunk_size as u64;
        let current_chunk = std::cmp::min(chunk_size as u64, size.saturating_sub(offset)) as usize;

        if write_failed.contains(&chunk_idx) {
            tx.send(WorkerEvent::Progress(ProgressEvent {
                device: path.display().to_string(),
                stage: Stage::Reading,
                chunk_idx,
                bytes: current_chunk as u64,
                error: None,
            }))
            .ok();
            // advance RNG for skipped segment to stay in sync
            let mut sink = vec![0u8; current_chunk];
            fill_buffer(&mut rng, &mut sink);
            continue;
        }

        let mut expected = vec![0u8; current_chunk];
        fill_buffer(&mut rng, &mut expected);

        let file_clone = file.clone();
        let read_result: std::io::Result<Vec<u8>> = run_with_timeout(timeout, move || {
            let mut buf = vec![0u8; current_chunk];
            let mut file = file_clone
                .lock()
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::Other, "poisoned lock"))?;
            file.seek(SeekFrom::Start(offset))?;
            file.read_exact(&mut buf)?;
            Ok(buf)
        });

        let mut error_record = None;
        match read_result {
            Err(err) => {
                error_record = Some(ErrorRecord {
                    chunk_idx,
                    offset,
                    kind: ErrorKind::Read,
                    message: err.to_string(),
                });
            }
            Ok(data) => {
                if data != expected {
                    error_record = Some(ErrorRecord {
                        chunk_idx,
                        offset,
                        kind: ErrorKind::Value,
                        message: "mismatched bytes".to_string(),
                    });
                }
            }
        }

        tx.send(WorkerEvent::Progress(ProgressEvent {
            device: path.display().to_string(),
            stage: Stage::Reading,
            chunk_idx,
            bytes: current_chunk as u64,
            error: error_record,
        }))
        .ok();
    }

    tx.send(WorkerEvent::Finish(FinishEvent {
        device: path.display().to_string(),
    }))
    .ok();

    Ok(())
}

fn detect_device_size(path: &Path) -> Result<u64> {
    let metadata = std::fs::metadata(path)
        .with_context(|| format!("failed to read metadata for {}", path.display()))?;
    let len = metadata.len();
    if len > 0 {
        return Ok(len);
    }

    #[cfg(target_os = "linux")]
    {
        let file = File::open(path).with_context(|| format!("failed to open {} for size", path.display()))?;
        let fd = file.as_raw_fd();
        let mut size: libc::c_ulonglong = 0;
        let rc = unsafe { libc::ioctl(fd, BLKGETSIZE64, &mut size) };
        if rc < 0 {
            return Err(anyhow!(
                "unable to determine size for {} (ioctl BLKGETSIZE64 failed)",
                path.display()
            ));
        }
        return Ok(size as u64);
    }

    #[cfg(target_os = "freebsd")]
    {
        use std::mem::MaybeUninit;
        let file = File::open(path).with_context(|| format!("failed to open {} for size", path.display()))?;
        let fd = file.as_raw_fd();
        let mut size = MaybeUninit::<libc::off_t>::uninit();
        let rc = unsafe { libc::ioctl(fd, DIOCGMEDIASIZE, size.as_mut_ptr()) };
        if rc < 0 {
            return Err(anyhow!(
                "unable to determine size for {} (ioctl DIOCGMEDIASIZE failed)",
                path.display()
            ));
        }
        let size = unsafe { size.assume_init() } as u64;
        return Ok(size);
    }

    #[cfg(not(any(target_os = "linux", target_os = "freebsd")))]
    {
        Err(anyhow!(
            "unable to determine size for {} (unsupported platform)",
            path.display()
        ))
    }
}

fn enforce_fs_checks(devices: &[PathBuf]) -> Result<()> {
    let mut found = Vec::new();
    for path in devices {
        if let Some(kind) = detect_existing_fs(path)? {
            found.push((path.display().to_string(), kind));
        }
    }

    if found.is_empty() {
        return Ok(());
    }

    let mut message = String::from("Existing partition table or filesystem detected:\n");
    for (device, kind) in &found {
        message.push_str(&format!("- {}: {}\n", device, kind));
    }
    message.push_str(
        "Refusing to continue to protect data. Back up important data, then wipe the partition table/filesystem (e.g. `wipefs -a <device>` or `dd if=/dev/zero of=<device> bs=1M count=10`), or rerun with --skip_fs_check to force continue.",
    );

    Err(anyhow!(message))
}

fn detect_existing_fs(path: &Path) -> Result<Option<String>> {
    let size = detect_device_size(path)?;
    if size == 0 {
        return Ok(None);
    }

    let mut file = File::open(path)
        .with_context(|| format!("failed to open {} for filesystem check", path.display()))?;
    let mut buf = vec![0u8; std::cmp::min(size as usize, MAX_SCAN_BYTES)];
    let mut read = 0;
    while read < buf.len() {
        let n = file.read(&mut buf[read..])?;
        if n == 0 {
            break;
        }
        read += n;
    }
    buf.truncate(read);

    Ok(detect_signature(&buf, size))
}

fn detect_signature(buf: &[u8], size: u64) -> Option<String> {
    if size >= MIN_GPT_SIZE && has_gpt_with_partitions(buf) {
        return Some("GPT partition table".to_string());
    }
    if size >= MIN_MBR_SIZE && has_mbr_with_partitions(buf) {
        return Some("MBR partition table".to_string());
    }
    if size >= MIN_NTFS_SIZE && has_ntfs(buf) {
        return Some("NTFS filesystem".to_string());
    }
    if size >= MIN_EXFAT_SIZE && has_exfat(buf) {
        return Some("exFAT filesystem".to_string());
    }
    if size >= MIN_FAT32_SIZE && has_fat32(buf) {
        return Some("FAT32 filesystem".to_string());
    }
    if size >= MIN_EXT_SIZE && has_ext(buf) {
        return Some("ext2/3/4 filesystem".to_string());
    }
    if size >= MIN_APFS_SIZE && has_apfs(buf) {
        return Some("APFS filesystem".to_string());
    }
    if size >= MIN_ZFS_SIZE && has_zfs(buf) {
        return Some("ZFS pool".to_string());
    }
    None
}

fn has_gpt_with_partitions(buf: &[u8]) -> bool {
    if buf.len() < 520 || &buf[512..520] != b"EFI PART" {
        return false;
    }

    // GPT header fields (little endian)
    if buf.len() < 92 {
        return true; // conservative: header present but truncated
    }
    let entries_lba = u64::from_le_bytes(buf[72..80].try_into().unwrap());
    let entries_count = u32::from_le_bytes(buf[80..84].try_into().unwrap());
    let entry_size = u32::from_le_bytes(buf[84..88].try_into().unwrap()).max(1);

    if entries_count == 0 {
        return false;
    }

    let sector_size = 512u64;
    let entries_offset = entries_lba.saturating_mul(sector_size) as usize;
    let bytes_needed = entries_count.saturating_mul(entry_size) as usize;

    if entries_offset >= buf.len() {
        return true; // conservative: header exists, entries outside scanned window
    }

    let available = buf.len().saturating_sub(entries_offset);
    let slice_len = bytes_needed.min(available).min(4 * entry_size as usize); // sample a few entries
    let entries_slice = &buf[entries_offset..entries_offset + slice_len];
    entries_slice.iter().any(|b| *b != 0)
}

fn has_mbr_with_partitions(buf: &[u8]) -> bool {
    if buf.len() < 512 {
        return false;
    }
    if buf[510] != 0x55 || buf[511] != 0xAA {
        return false;
    }
    let entries = &buf[446..510];
    entries.chunks(16).any(|entry| entry.iter().any(|b| *b != 0))
}

fn has_ntfs(buf: &[u8]) -> bool {
    buf.len() >= 11 && &buf[3..11] == b"NTFS    "
}

fn has_exfat(buf: &[u8]) -> bool {
    buf.len() >= 11 && &buf[3..11] == b"EXFAT   "
}

fn has_fat32(buf: &[u8]) -> bool {
    buf.len() >= 90 && &buf[82..90] == b"FAT32   "
}

fn has_ext(buf: &[u8]) -> bool {
    buf.len() >= 1082 && buf[1080] == 0x53 && buf[1081] == 0xEF
}

fn has_apfs(buf: &[u8]) -> bool {
    buf.windows(4).any(|w| w == b"NXSB")
}

fn has_zfs(buf: &[u8]) -> bool {
    buf.windows(9).any(|w| w == b"ZFS LABEL")
}

fn fill_buffer(rng: &mut Xoroshiro128PlusPlus, buf: &mut [u8]) {
    let mut i = 0;
    while i < buf.len() {
        let val = rng.next_u64();
        let bytes = val.to_le_bytes();
        let remaining = buf.len() - i;
        let take = remaining.min(8);
        buf[i..i + take].copy_from_slice(&bytes[..take]);
        i += take;
    }
}

fn run_with_timeout<F, T>(timeout: Duration, op: F) -> std::io::Result<T>
where
    F: FnOnce() -> std::io::Result<T> + Send + 'static,
    T: Send + 'static,
{
    let (tx, rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let res = op();
        let _ = tx.send(res);
    });

    match rx.recv_timeout(timeout) {
        Ok(res) => res,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
            Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "operation timed out"))
        }
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "io worker thread died unexpectedly",
            ))
        }
    }
}

struct TerminalGuard;

impl TerminalGuard {
    fn enter() -> Result<Self> {
        let mut stdout = std::io::stdout();
        crossterm::execute!(stdout, terminal::EnterAlternateScreen, cursor::Hide)?;
        Ok(Self)
    }

    fn restore(&mut self) {
        let mut stdout = std::io::stdout();
        let _ = crossterm::execute!(stdout, cursor::Show, terminal::LeaveAlternateScreen);
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        self.restore();
    }
}

fn render_loop(guard: &mut TerminalGuard, rx: Receiver<WorkerEvent>, mode: UiMode, cancel: Arc<std::sync::atomic::AtomicBool>) -> Result<()> {
    let mut states: HashMap<String, UiDeviceState> = HashMap::new();
    let mut last_render = Instant::now();
    let render_interval = Duration::from_millis(100);

    loop {
        if cancel.load(std::sync::atomic::Ordering::Relaxed) {
            break;
        }

        match rx.recv_timeout(Duration::from_millis(50)) {
            Ok(event) => match event {
                WorkerEvent::Init(init) => {
                    states.insert(
                        init.device.clone(),
                        UiDeviceState {
                            total_bytes: init.total_bytes,
                            total_chunks: init.total_chunks,
                            written_bytes: 0,
                            read_bytes: 0,
                            write_errors: 0,
                            read_errors: 0,
                            value_errors: 0,
                            stage: Stage::Writing,
                            write_start: Instant::now(),
                            read_start: None,
                            last_stage_bytes: 0,
                            last_stage_instant: Instant::now(),
                            speed_mbps: 0.0,
                            errors: Vec::new(),
                            error_marks: VecDeque::new(),
                            write_end: None,
                            read_end: None,
                        },
                    );
                }
                WorkerEvent::Progress(update) => {
                    if let Some(state) = states.get_mut(&update.device) {
                        let _ = update.chunk_idx; // currently unused but kept for future per-chunk UI mapping
                        match update.stage {
                        Stage::Writing => {
                            state.stage = Stage::Writing;
                            state.written_bytes = state.written_bytes.saturating_add(update.bytes);
                        }
                        Stage::Reading => {
                            if state.read_start.is_none() {
                                state.read_start = Some(Instant::now());
                                state.write_end = Some(Instant::now());
                                state.last_stage_instant = Instant::now();
                                state.last_stage_bytes = 0;
                            }
                            state.stage = Stage::Reading;
                            state.read_bytes = state.read_bytes.saturating_add(update.bytes);
                            }
                            Stage::Done => {}
                        }

                        if let Some(err) = update.error.clone() {
                            match err.kind {
                                ErrorKind::Write => state.write_errors += 1,
                                ErrorKind::Read => state.read_errors += 1,
                                ErrorKind::Value => state.value_errors += 1,
                            }
                            state.errors.push(err.clone());
                            state.error_marks.push_back(err);
                            while state.error_marks.len() > ERROR_MARKS_LIMIT {
                                state.error_marks.pop_front();
                            }
                        }

                        // Update speed using stage deltas
                        let now = Instant::now();
                        let stage_bytes = match state.stage {
                            Stage::Writing => state.written_bytes,
                            Stage::Reading => state.read_bytes,
                            Stage::Done => state.read_bytes,
                        };
                        let elapsed = now.duration_since(state.last_stage_instant).as_secs_f64();
                        if elapsed >= 0.2 {
                            let delta = stage_bytes.saturating_sub(state.last_stage_bytes);
                            state.speed_mbps = if elapsed > 0.0 {
                                (delta as f64 * 8.0) / (elapsed * 1_000_000.0)
                            } else {
                                0.0
                            };
                            state.last_stage_bytes = stage_bytes;
                            state.last_stage_instant = now;
                        }
                    }
                }
            WorkerEvent::Finish(done) => {
                if let Some(state) = states.get_mut(&done.device) {
                    state.stage = Stage::Done;
                    state.read_end = Some(Instant::now());
                }
            }
                WorkerEvent::Fatal { device, message } => {
                    eprintln!("fatal error for {}: {}", device, message);
                }
            },
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                // keep looping to allow periodic redraws
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                break;
            }
        }

        if last_render.elapsed() >= render_interval {
            draw_ui(&states, mode)?;
            last_render = Instant::now();
        }
    }

    // Final render after channel closes
    draw_ui(&states, mode)?;
    prompt_any_key(&states)?;
    guard.restore();
    print_report(&states);
    Ok(())
}

fn draw_ui(states: &HashMap<String, UiDeviceState>, mode: UiMode) -> Result<()> {
    let (width, _) = terminal::size()?;
    let width = width as usize;
    let mut stdout = std::io::stdout();
    crossterm::execute!(stdout, cursor::MoveTo(0, 0), terminal::Clear(ClearType::All), cursor::MoveTo(0, 0))?;

    let disk_col_width = states
        .keys()
        .map(|k| k.len())
        .max()
        .unwrap_or(4)
        .max("Disk".len())
        .min(30);

    let fixed_cols = disk_col_width + 1 /*space*/ + 2 /*| */ + 1 /* | */ + 6 /* Mbps */
        + 3 /* spaces */ + 10 /* ETA */ + 4 /* Errs */ + 8;
    let bar_width = width.saturating_sub(fixed_cols).max(10);

    let header = format!(
        "{:<width$} | {:<bar$} | Mbps |        Eta | Errs ",
        "Disk",
        "Progress",
        width = disk_col_width,
        bar = bar_width
    );
    let separator = format!(
        "{:-<width$}-+-{:-<bar$}-+------+-{:->10}-+------",
        "",
        "",
        "",
        width = disk_col_width,
        bar = bar_width
    );
    writeln!(stdout, "{}", header)?;
    writeln!(stdout, "{}", separator)?;

    let mut devices: Vec<_> = states.keys().cloned().collect();
    devices.sort();
    for name in devices {
        if let Some(state) = states.get(&name) {
            let bar = build_progress_bar(state, bar_width, mode);
            let eta = compute_eta(state);
            let speed = state.speed_mbps;
            let errs = state.write_errors + state.read_errors + state.value_errors;
            writeln!(
                stdout,
                "{:<width$} | {} | {:>4.0} | {:>10} | {:>5}",
                name,
                bar,
                speed,
                eta,
                errs,
                width = disk_col_width
            )?;
        }
    }
    stdout.flush()?;
    Ok(())
}

fn build_progress_bar(state: &UiDeviceState, width: usize, mode: UiMode) -> String {
    let (write_char, read_char, empty_char) = match mode {
        UiMode::Rich => ('█', '░', ' '),
        UiMode::Basic => ('#', '=', ' '),
    };

    let mut bar = vec![empty_char; width];
    let write_fill = ((state.written_bytes as f64 / state.total_bytes.max(1) as f64)
        * width as f64)
        .clamp(0.0, width as f64) as usize;
    let read_fill = ((state.read_bytes as f64 / state.total_bytes.max(1) as f64) * width as f64)
        .clamp(0.0, width as f64) as usize;

    for i in 0..write_fill.min(width) {
        bar[i] = write_char;
    }
    for i in 0..read_fill.min(width) {
        bar[i] = read_char;
    }

    for err in &state.error_marks {
        let pos = ((err.chunk_idx as f64 / state.total_chunks.max(1) as f64) * width as f64)
            .clamp(0.0, (width.saturating_sub(1)) as f64) as usize;
        let symbol = match err.kind {
            ErrorKind::Write => 'X',
            ErrorKind::Read => 'E',
            ErrorKind::Value => 'V',
        };
        bar[pos] = symbol;
    }

    if mode == UiMode::Rich {
        // try smoother tails
        let partials = ['▏', '▎', '▍', '▌', '▋', '▊', '▉'];
        let write_exact = (state.written_bytes as f64 / state.total_bytes.max(1) as f64) * width as f64;
        let tail = write_exact.fract();
        if tail > 0.0 {
            let idx = (tail * partials.len() as f64).floor() as usize;
            if idx < partials.len() {
                let pos = write_fill.min(width.saturating_sub(1));
                if bar[pos] == write_char || bar[pos] == empty_char {
                    bar[pos] = partials[idx];
                }
            }
        }
    }

    bar.into_iter().collect()
}

fn compute_eta(state: &UiDeviceState) -> String {
    match state.stage {
        Stage::Writing => format_eta(state.written_bytes, state.total_bytes, state.write_start, state.speed_mbps),
        Stage::Reading => {
            let start = state.read_start.unwrap_or(state.write_start);
            format_eta(state.read_bytes, state.total_bytes, start, state.speed_mbps)
        }
        Stage::Done => format_duration(0),
    }
}

fn format_eta(done: u64, total: u64, start: Instant, speed_mbps: f64) -> String {
    let elapsed = start.elapsed().as_secs_f64();
    let rate_bytes = if elapsed > 0.0 {
        done as f64 / elapsed
    } else {
        0.0
    };
    let rate = if rate_bytes > 0.0 {
        rate_bytes
    } else if speed_mbps > 0.0 {
        // fallback on instantaneous speed
        speed_mbps * 1_000_000.0 / 8.0
    } else {
        0.0
    };

    if rate <= 0.0 {
        return "--:--:--".to_string();
    }

    let remaining = total.saturating_sub(done) as f64;
    let seconds = (remaining / rate).round() as u64;
    format_duration(seconds)
}

fn format_duration(secs: u64) -> String {
    let hours = secs / 3600;
    let mins = (secs % 3600) / 60;
    let secs = secs % 60;
    // Avoid left-padding hours so "0:00:00" is shown instead of "00:00:00".
    format!("{}:{:02}:{:02}", hours, mins, secs)
}

fn print_destructive_warning() -> Result<()> {
    println!("{}", DESTRUCTIVE_WARNING);
    println!("Type YES to continue: ");
    use std::io::{stdin, stdout};
    let _ = stdout().flush();
    let mut input = String::new();
    stdin().read_line(&mut input)?;
    if input.trim() != "YES" {
        Err(anyhow!("aborted by user"))
    } else {
        Ok(())
    }
}

fn print_report(states: &HashMap<String, UiDeviceState>) {
    println!("\nScan complete.");
    for (device, state) in states {
        let total_errs = state.write_errors + state.read_errors + state.value_errors;
        let write_speed = avg_speed_mbps(state.total_bytes, state.write_start, state.write_end);
        let read_speed = avg_speed_mbps(state.read_bytes, state.read_start.unwrap_or(state.write_start), state.read_end);
        println!(
            "{}: {} bytes, write {:.1} Mbps, read {:.1} Mbps, {} total errors (write {}, read {}, value {})",
            device,
            state.total_bytes,
            write_speed,
            read_speed,
            total_errs,
            state.write_errors,
            state.read_errors,
            state.value_errors
        );
        if !state.errors.is_empty() {
            for err in &state.errors {
                println!(
                    "  - chunk {} @ {} (sector {}): {:?} ({})",
                    err.chunk_idx,
                    err.offset,
                    err.offset / 512,
                    err.kind,
                    err.message
                );
            }
        }
    }
}

fn avg_speed_mbps(bytes: u64, start: Instant, end: Option<Instant>) -> f64 {
    if let Some(end) = end {
        let secs = end.saturating_duration_since(start).as_secs_f64();
        if secs > 0.0 {
            return (bytes as f64 * 8.0) / (secs * 1_000_000.0);
        }
    }
    0.0
}

fn prompt_any_key(states: &HashMap<String, UiDeviceState>) -> Result<()> {
    // Place prompt beneath the table to avoid overwriting bars.
    let rows = (states.len() + 3) as u16; // header + separator + devices
    let mut stdout = std::io::stdout();
    crossterm::execute!(stdout, cursor::MoveTo(0, rows))?;
    print!("Press any key to exit...");
    stdout.flush()?;

    loop {
        if event::poll(Duration::from_millis(200))? {
            let ev = event::read()?;
            if matches!(ev, event::Event::Key(_)) {
                break;
            }
        }
    }
    Ok(())
}
