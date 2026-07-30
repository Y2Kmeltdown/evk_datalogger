//! EVK4 ingestion pipeline with HTTP API control.
//!
//! Streams decoded DVS / trigger events to Unix sockets and records raw data
//! to timestamped files. Recording is ALWAYS ON by default and cannot be
//! disabled; passing --record-toggle instead starts the pipeline with
//! recording off and enables start/stop control via the HTTP API.
//! A circular buffer keeps recent raw history in memory, so a recording that
//! starts later still contains the moments *before* the record command
//! (pre-roll). The buffer is bounded by two caps applied together: chunks
//! older than `max_age_secs` are dropped, and the oldest chunks are dropped
//! while the total size exceeds `max_bytes`. Setting either cap to 0
//! effectively disables the buffer.
//!
//! HTTP API (default bind: 0.0.0.0:8081)
//! ─────────────────────────────────────
//!   GET  /api/status       Full state: recording, file, biases, rate limit, buffer
//!   GET  /api/recording    {"recording": bool, "recording_control": bool, "current_file": path|null}
//!   PUT  /api/recording    body: {"recording": true|false}  (only with --record-toggle)
//!   GET  /api/biases       Current camera biases (full driver struct)
//!   PUT  /api/biases       body: any subset of {"diff","diff_on","diff_off",
//!                          "hpf","refr","pr","fo","inv"} — applied at runtime
//!   GET  /api/rate-limit   {"events_per_second": n}  (0 = unlimited)
//!   PUT  /api/rate-limit   body: {"events_per_second": n}
//!   GET  /api/buffer       Circular buffer caps + live fill stats
//!   PUT  /api/buffer       body: any subset of {"max_age_secs","max_bytes"}

use std::collections::VecDeque;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
#[cfg(unix)]
#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};
#[cfg(windows)]
use uds_windows::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use neuromorphic_drivers::prophesee_evk4;
use neuromorphic_drivers::UsbDevice;

use chrono::Utc;
use clap::Parser;

// ── CLI ───────────────────────────────────────────────────────────────────────

/// Base directory for default socket paths and recordings.
/// `/tmp` on Unix; the per-user temp directory on Windows.
#[cfg(unix)]
fn default_tmp_dir() -> PathBuf {
    PathBuf::from("/tmp")
}

/// Base directory for default socket paths and recordings.
/// `/tmp` on Unix; the per-user temp directory on Windows.
#[cfg(windows)]
fn default_tmp_dir() -> PathBuf {
    std::env::temp_dir()
}

#[derive(Parser, Debug)]
#[command(
    name = "evk4-pipeline",
    about = "Ingest EVK4 events and stream them to Unix sockets; recording and camera \
             settings are controlled at runtime via an HTTP API"
)]
struct Args {
    /// Unix socket path for decoded DVS events
    #[arg(long, default_value_os_t = default_tmp_dir().join("evk4_events.sock"))]
    events_socket: PathBuf,

    /// Unix socket path for decoded trigger events
    #[arg(long, default_value_os_t = default_tmp_dir().join("evk4_triggers.sock"))]
    triggers_socket: PathBuf,

    /// Directory to write raw recording files into (when recording is enabled)
    #[arg(long, default_value_os_t = default_tmp_dir().join("evk4_raw"))]
    output_dir: PathBuf,

    /// How often (in seconds) to roll over to a new raw file while recording
    #[arg(long, default_value_t = 60)]
    file_length: u64,

    /// Initial hardware event-rate limit (events per second). 0 = unlimited.
    #[arg(long, default_value_t = 0)]
    rate_limit: u64,

    /// Initial camera bias: diff_on (ON-event contrast threshold)
    #[arg(long, default_value_t = 102)]
    diff_on: u8,

    /// Initial camera bias: diff_off (OFF-event contrast threshold)
    #[arg(long, default_value_t = 102)]
    diff_off: u8,

    /// Start with recording OFF and allow it to be toggled via the HTTP API.
    /// Without this flag, recording is always on and cannot be disabled.
    #[arg(long, default_value = "false")]
    record_toggle: String,

    /// HTTP API bind address
    #[arg(long, default_value = "0.0.0.0:8081")]
    api_bind: String,

    /// Circular buffer cap: maximum age (seconds) of retained data. 0 = buffer off.
    #[arg(long, default_value_t = 10)]
    buffer_max_age: u64,

    /// Circular buffer cap: maximum total size in bytes. 0 = buffer off.
    #[arg(long, default_value_t = 100 * 1024 * 1024)]
    buffer_max_bytes: u64,
}

// ── Binary event structs ──────────────────────────────────────────────────────
//
// Each struct is serialised as a tightly-packed, little-endian byte sequence.
// The layout is fixed and documented here so the Python client can unpack it
// with a single `struct.unpack` call.
//
// DvsEvent  — 13 bytes
//   t      : u64  (8 bytes, little-endian) — sensor timestamp in microseconds
//   x      : u16  (2 bytes, little-endian) — pixel column
//   y      : u16  (2 bytes, little-endian) — pixel row
//   on     : u8   (1 byte)                 — 1 = ON polarity, 0 = OFF
//
// TriggerEvent — 26 bytes
//   system_time      : u64  (8 bytes, little-endian)
//   system_timestamp : u64  (8 bytes, little-endian)
//   t                : u64  (8 bytes, little-endian) — sensor timestamp
//   id               : u8   (1 byte)                 — trigger channel ID
//   rising           : u8   (1 byte)                 — 1 = rising, 0 = falling

const DVS_EVENT_SIZE: usize = 13;
const TRIGGER_EVENT_SIZE: usize = 26;

struct DvsEvent {
    t: u64,
    x: u16,
    y: u16,
    on: u8,
}

impl DvsEvent {
    /// Serialise to a fixed-size byte array — no heap allocation.
    fn to_bytes(&self) -> [u8; DVS_EVENT_SIZE] {
        let mut buf = [0u8; DVS_EVENT_SIZE];
        buf[0..8].copy_from_slice(&self.t.to_le_bytes());
        buf[8..10].copy_from_slice(&self.x.to_le_bytes());
        buf[10..12].copy_from_slice(&self.y.to_le_bytes());
        buf[12] = self.on;
        buf
    }
}

struct TriggerEvent {
    system_time: u64,
    system_timestamp: u64,
    t: u64,
    id: u8,
    rising: u8,
}

impl TriggerEvent {
    /// Serialise to a fixed-size byte array — no heap allocation.
    fn to_bytes(&self) -> [u8; TRIGGER_EVENT_SIZE] {
        let mut buf = [0u8; TRIGGER_EVENT_SIZE];
        buf[0..8].copy_from_slice(&self.system_time.to_le_bytes());
        buf[8..16].copy_from_slice(&self.system_timestamp.to_le_bytes());
        buf[16..24].copy_from_slice(&self.t.to_le_bytes());
        buf[24] = self.id;
        buf[25] = self.rising;
        buf
    }
}

// ── Owned pipeline packet ─────────────────────────────────────────────────────

/// Raw bytes copied out of the BufferView plus index data captured at ingestion.
/// No borrows — safe to send across threads.
struct OwnedPacket {
    raw_bytes: Vec<u8>,
    index_data: [u8; 16],
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Build a timestamped filename inside `dir`, e.g. `20240315T123456Z_evk.raw`.
fn timestamped_path(dir: &Path, ext: String) -> PathBuf {
    let ts = Utc::now().format("%Y%m%dT%H%M%SZ");
    dir.join(format!("{ts}_evk{ext}"))
}

/// Open a new raw output file, creating the directory if needed.
/// Returns the path and the opened file.
fn open_raw_file(dir: &Path) -> (PathBuf, std::fs::File) {
    std::fs::create_dir_all(dir).expect("Failed to create output directory");
    let path = timestamped_path(dir, String::from(".raw"));
    println!("[processor] New raw file: {}", path.display());
    let file = std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&path)
        .unwrap_or_else(|e| panic!("Failed to open raw file {}: {e}", path.display()));
    (path, file)
}

/// Try to write a length-prefixed binary message to a stream.
/// On write failure (client disconnected) the stream slot is set to None
/// so the next packet iteration will poll for a fresh connection.
fn try_send(stream: &mut Option<UnixStream>, data: &[u8]) {
    let Some(s) = stream.as_mut() else { return };
    let len = data.len() as u32;
    let ok = s.write_all(&len.to_le_bytes()).and_then(|_| s.write_all(data));
    if let Err(e) = ok {
        eprintln!("[processor] Socket write error (client disconnected?): {e}");
        *stream = None;
    }
}

/// Poll a non-blocking listener for a new client connection.
fn accept_next(listener: &UnixListener) -> Option<UnixStream> {
    match listener.accept() {
        Ok((stream, _)) => {
            println!("[processor] New socket client connected.");
            Some(stream)
        }
        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => None,
        Err(e) => {
            eprintln!("[processor] accept() error: {e}");
            None
        }
    }
}

/// Build the hardware rate limiter for a user-facing events-per-second limit.
/// 0 = unlimited (None).
fn make_rate_limiter(events_per_second: u64) -> Option<prophesee_evk4::RateLimiter> {
    if events_per_second == 0 {
        return None;
    }
    // Choose a 1 ms reference period (1000 µs) for smooth hardware limiting.
    // maximum_events_per_period = rate * (1000 / 1_000_000)
    let reference_period_us: u16 = 1000;
    let maximum_events_per_period =
        ((events_per_second * reference_period_us as u64) / 1_000_000).max(1) as u32;
    Some(prophesee_evk4::RateLimiter {
        reference_period_us,
        maximum_events_per_period,
    })
}

// ── Shared control state ──────────────────────────────────────────────────────

/// State shared between the HTTP API thread and the pipeline threads.
///
/// The atomics are written by the API and read by the processor thread.
/// `configuration` travels the other way: the API mutates it under the mutex
/// and forwards clones to the ingester thread, which owns the device and is
/// therefore the only place `update_configuration` is called.
struct SharedState {
    /// Recording on/off — set by the API (only when `recording_toggle` is
    /// enabled), consumed by the processor thread.
    recording: AtomicBool,
    /// When false, recording is always on and PUT /api/recording is rejected.
    /// Set once at startup from --record-toggle; read-only afterwards.
    recording_toggle: bool,
    /// Circular buffer caps — set by the API.
    buffer_max_age_secs: AtomicU64,
    buffer_max_bytes: AtomicU64,
    /// Live buffer fill — written by the processor, read by the API.
    buffer_bytes: AtomicU64,
    buffer_chunks: AtomicU64,
    /// File currently being recorded — written by the processor, read by the API.
    current_file: Mutex<Option<PathBuf>>,
    /// Full device configuration holding the current biases and rate limiter.
    configuration: Mutex<prophesee_evk4::Configuration>,
    /// User-facing rate limit (events/s, 0 = unlimited) — mirrors the configuration.
    rate_limit_eps: AtomicU64,
}

// ── Circular buffer ───────────────────────────────────────────────────────────

/// Bounded history of raw packet bytes.
///
/// Every packet is appended; eviction enforces both caps at once — chunks
/// older than `max_age` are dropped, and the oldest chunks are dropped while
/// the total size exceeds `max_bytes`. The contents are written as pre-roll
/// when recording starts.
struct CircularBuffer {
    chunks: VecDeque<(Instant, Vec<u8>)>,
    total_bytes: usize,
}

impl CircularBuffer {
    fn new() -> Self {
        Self {
            chunks: VecDeque::new(),
            total_bytes: 0,
        }
    }

    fn push(&mut self, data: Vec<u8>) {
        self.total_bytes += data.len();
        self.chunks.push_back((Instant::now(), data));
    }

    fn evict(&mut self, max_age: Duration, max_bytes: usize) {
        // checked_sub: a huge max_age must not underflow — it just disables
        // the age cap (cutoff lands before the beginning of time).
        let cutoff = Instant::now().checked_sub(max_age);
        while let Some((arrived, data)) = self.chunks.front() {
            let too_old = cutoff.is_some_and(|c| *arrived < c);
            if too_old || self.total_bytes > max_bytes {
                self.total_bytes -= data.len();
                self.chunks.pop_front();
            } else {
                break;
            }
        }
    }
}

// ── HTTP API ──────────────────────────────────────────────────────────────────

/// Minimal parsed HTTP request — enough to route and read a JSON body.
struct HttpRequest {
    method: String,
    path: String,
    body: Vec<u8>,
}

/// Parse a request from a `BufReader`. Returns `None` on connection errors.
fn parse_request(reader: &mut BufReader<TcpStream>) -> Option<HttpRequest> {
    // Request line — query strings are stripped, we don't use them.
    let mut request_line = String::new();
    reader.read_line(&mut request_line).ok()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_string();
    let raw_path = parts.next()?.to_string();
    let path = raw_path.split('?').next().unwrap_or(&raw_path).to_string();

    // Headers — only Content-Length matters.
    let mut content_length = 0usize;
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).ok()?;
        if line.trim_end_matches(['\r', '\n']).is_empty() {
            break;
        }
        let lower = line.to_lowercase();
        if lower.starts_with("content-length:") {
            content_length = lower.split(':').nth(1)?.trim().parse().ok()?;
        }
    }

    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        reader.read_exact(&mut body).ok()?;
    }
    Some(HttpRequest { method, path, body })
}

/// Write a complete JSON response; the connection is closed by the caller.
fn write_json(stream: &mut TcpStream, status: u16, status_text: &str, json: &str) {
    let header = format!(
        "HTTP/1.1 {status} {status_text}\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Cache-Control: no-cache\r\n\
         Access-Control-Allow-Origin: *\r\n\
         \r\n",
        json.len()
    );
    let _ = stream.write_all(header.as_bytes());
    let _ = stream.write_all(json.as_bytes());
}

fn json_err(stream: &mut TcpStream, status: u16, status_text: &str, reason: &str) {
    let body = format!(r#"{{"error":{}}}"#, serde_json::to_string(reason).unwrap());
    write_json(stream, status, status_text, &body);
}

/// Payload for PUT /api/recording.
#[derive(serde::Deserialize)]
struct RecordingPatch {
    recording: bool,
}

/// Payload for PUT /api/biases — every field optional; only supplied biases change.
#[derive(serde::Deserialize)]
struct BiasesPatch {
    diff: Option<u8>,
    diff_on: Option<u8>,
    diff_off: Option<u8>,
    hpf: Option<u8>,
    refr: Option<u8>,
    pr: Option<u8>,
    fo: Option<u8>,
    inv: Option<u8>,
}

/// Payload for PUT /api/rate-limit.
#[derive(serde::Deserialize)]
struct RateLimitPatch {
    events_per_second: u64,
}

/// Payload for PUT /api/buffer — either cap may be updated alone.
#[derive(serde::Deserialize)]
struct BufferPatch {
    max_age_secs: Option<u64>,
    max_bytes: Option<u64>,
}

/// Current recording file as a JSON value (string or null).
fn current_file_json(shared: &SharedState) -> String {
    match shared.current_file.lock().unwrap().as_ref() {
        Some(p) => serde_json::to_string(&p.display().to_string()).unwrap(),
        None => String::from("null"),
    }
}

/// Buffer caps plus live fill stats as a JSON object.
fn buffer_json(shared: &SharedState) -> String {
    format!(
        "{{\"max_age_secs\":{},\"max_bytes\":{},\"bytes\":{},\"chunks\":{}}}",
        shared.buffer_max_age_secs.load(Ordering::Relaxed),
        shared.buffer_max_bytes.load(Ordering::Relaxed),
        shared.buffer_bytes.load(Ordering::Relaxed),
        shared.buffer_chunks.load(Ordering::Relaxed),
    )
}

/// Forward the current shared configuration to the ingester thread, which
/// applies it to the hardware via `update_configuration`.
fn push_configuration(
    shared: &SharedState,
    config_tx: &mpsc::Sender<prophesee_evk4::Configuration>,
) {
    let cfg = shared.configuration.lock().unwrap().clone();
    if config_tx.send(cfg).is_err() {
        eprintln!("[api] Ingester thread gone — configuration update dropped.");
    }
}

fn handle_api(
    stream: &mut TcpStream,
    req: &HttpRequest,
    shared: &Arc<SharedState>,
    config_tx: &mpsc::Sender<prophesee_evk4::Configuration>,
) {
    match (req.method.as_str(), req.path.as_str()) {
        ("GET", "/api/status") => {
            let biases = {
                let cfg = shared.configuration.lock().unwrap();
                serde_json::to_string(&cfg.biases).unwrap()
            };
            let body = format!(
                "{{\"recording\":{},\"recording_control\":{},\"current_file\":{},\
                 \"rate_limit_events_per_second\":{},\"biases\":{},\"buffer\":{}}}",
                shared.recording.load(Ordering::Relaxed),
                shared.recording_toggle,
                current_file_json(shared),
                shared.rate_limit_eps.load(Ordering::Relaxed),
                biases,
                buffer_json(shared),
            );
            write_json(stream, 200, "OK", &body);
        }

        ("GET", "/api/recording") => {
            let body = format!(
                "{{\"recording\":{},\"recording_control\":{},\"current_file\":{}}}",
                shared.recording.load(Ordering::Relaxed),
                shared.recording_toggle,
                current_file_json(shared),
            );
            write_json(stream, 200, "OK", &body);
        }

        ("PUT", "/api/recording") => {
            if !shared.recording_toggle {
                json_err(
                    stream,
                    403,
                    "Forbidden",
                    "Recording control is disabled — restart with --record-toggle to enable it.",
                );
                return;
            }
            match serde_json::from_slice::<RecordingPatch>(&req.body) {
                Ok(patch) => {
                    shared.recording.store(patch.recording, Ordering::Relaxed);
                    eprintln!(
                        "[api] recording {}",
                        if patch.recording { "enabled" } else { "disabled" }
                    );
                    let body = format!("{{\"recording\":{}}}", patch.recording);
                    write_json(stream, 200, "OK", &body);
                }
                Err(e) => json_err(stream, 400, "Bad Request", &format!("Invalid JSON: {e}")),
            }
        }

        ("GET", "/api/biases") => {
            let cfg = shared.configuration.lock().unwrap();
            write_json(stream, 200, "OK", &serde_json::to_string(&cfg.biases).unwrap());
        }

        ("PUT", "/api/biases") => match serde_json::from_slice::<BiasesPatch>(&req.body) {
            Ok(patch) => {
                {
                    let mut cfg = shared.configuration.lock().unwrap();
                    let b = &mut cfg.biases;
                    if let Some(v) = patch.diff { b.diff = v; }
                    if let Some(v) = patch.diff_on { b.diff_on = v; }
                    if let Some(v) = patch.diff_off { b.diff_off = v; }
                    if let Some(v) = patch.hpf { b.hpf = v; }
                    if let Some(v) = patch.refr { b.refr = v; }
                    if let Some(v) = patch.pr { b.pr = v; }
                    if let Some(v) = patch.fo { b.fo = v; }
                    if let Some(v) = patch.inv { b.inv = v; }
                }
                push_configuration(shared, config_tx);
                eprintln!("[api] biases updated");
                let cfg = shared.configuration.lock().unwrap();
                write_json(stream, 200, "OK", &serde_json::to_string(&cfg.biases).unwrap());
            }
            Err(e) => json_err(stream, 400, "Bad Request", &format!("Invalid JSON: {e}")),
        },

        ("GET", "/api/rate-limit") => {
            let body = format!(
                "{{\"events_per_second\":{}}}",
                shared.rate_limit_eps.load(Ordering::Relaxed)
            );
            write_json(stream, 200, "OK", &body);
        }

        ("PUT", "/api/rate-limit") => match serde_json::from_slice::<RateLimitPatch>(&req.body) {
            Ok(patch) => {
                {
                    let mut cfg = shared.configuration.lock().unwrap();
                    cfg.rate_limiter = make_rate_limiter(patch.events_per_second);
                }
                shared
                    .rate_limit_eps
                    .store(patch.events_per_second, Ordering::Relaxed);
                push_configuration(shared, config_tx);
                eprintln!("[api] rate limit: {} events/s", patch.events_per_second);
                let body = format!("{{\"events_per_second\":{}}}", patch.events_per_second);
                write_json(stream, 200, "OK", &body);
            }
            Err(e) => json_err(stream, 400, "Bad Request", &format!("Invalid JSON: {e}")),
        },

        ("GET", "/api/buffer") => {
            write_json(stream, 200, "OK", &buffer_json(shared));
        }

        ("PUT", "/api/buffer") => match serde_json::from_slice::<BufferPatch>(&req.body) {
            Ok(patch) => {
                if let Some(v) = patch.max_age_secs {
                    shared.buffer_max_age_secs.store(v, Ordering::Relaxed);
                }
                if let Some(v) = patch.max_bytes {
                    shared.buffer_max_bytes.store(v, Ordering::Relaxed);
                }
                eprintln!(
                    "[api] buffer caps: {}s / {} bytes",
                    shared.buffer_max_age_secs.load(Ordering::Relaxed),
                    shared.buffer_max_bytes.load(Ordering::Relaxed),
                );
                write_json(stream, 200, "OK", &buffer_json(shared));
            }
            Err(e) => json_err(stream, 400, "Bad Request", &format!("Invalid JSON: {e}")),
        },

        // CORS preflight
        ("OPTIONS", _) => {
            let header = "HTTP/1.1 204 No Content\r\n\
                          Access-Control-Allow-Origin: *\r\n\
                          Access-Control-Allow-Methods: GET, PUT, OPTIONS\r\n\
                          Access-Control-Allow-Headers: Content-Type\r\n\
                          \r\n";
            let _ = stream.write_all(header.as_bytes());
        }

        _ => json_err(stream, 404, "Not Found", "Unknown endpoint."),
    }
}

/// Blocking HTTP server — one request per connection, connections handled
/// sequentially. API calls are infrequent and cheap, so no per-client threads.
fn run_http_server(
    bind: &str,
    shared: Arc<SharedState>,
    config_tx: mpsc::Sender<prophesee_evk4::Configuration>,
) {
    let listener = TcpListener::bind(bind)
        .unwrap_or_else(|e| panic!("Failed to bind HTTP API on {bind}: {e}"));
    println!("[api] Listening on http://{bind}");

    for stream in listener.incoming() {
        let mut stream = match stream {
            Ok(s) => s,
            Err(_) => continue,
        };
        // Clone the stream so the BufReader owns a read handle while `stream`
        // stays writable for the response.
        let mut reader = match stream.try_clone() {
            Ok(s) => BufReader::new(s),
            Err(_) => continue,
        };
        let req = match parse_request(&mut reader) {
            Some(r) => r,
            None => continue,
        };
        handle_api(&mut stream, &req, &shared, &config_tx);
    }
}

// ── Main ──────────────────────────────────────────────────────────────────────

fn main() -> Result<(), neuromorphic_drivers::Error> {
    let args = Args::parse();

    // ── Unix socket setup ─────────────────────────────────────────────────────
    let _ = std::fs::remove_file(&args.events_socket);
    let _ = std::fs::remove_file(&args.triggers_socket);

    let events_listener = UnixListener::bind(&args.events_socket)
        .expect("Failed to bind events socket");
    events_listener
        .set_nonblocking(true)
        .expect("Failed to set events listener non-blocking");

    let triggers_listener = UnixListener::bind(&args.triggers_socket)
        .expect("Failed to bind triggers socket");
    triggers_listener
        .set_nonblocking(true)
        .expect("Failed to set triggers listener non-blocking");

    println!("[main] Events socket:    {}", args.events_socket.display());
    println!("[main] Triggers socket:  {}", args.triggers_socket.display());
    println!("[main] Output directory: {}", args.output_dir.display());
    println!("[main] File interval:    {}s", args.file_length);
    println!(
        "[main] Circular buffer:  {}s / {} bytes cap",
        args.buffer_max_age, args.buffer_max_bytes
    );
    let record_toggle = args.record_toggle.to_lowercase() == "true";

    if record_toggle {
        println!("[main] Recording:        OFF (toggle via the HTTP API)");
    } else {
        println!("[main] Recording:        ALWAYS ON (no API control — see --record-toggle)");
    }
    println!("[main] Device starting — socket clients can connect at any time.");

    // ── Device setup ──────────────────────────────────────────────────────────
    let (flag, event_loop) = neuromorphic_drivers::flag_and_event_loop()?;

    let mut evk_configuration = prophesee_evk4::DEFAULT_CONFIGURATION;
    evk_configuration.biases.diff_on = args.diff_on;
    evk_configuration.biases.diff_off = args.diff_off;
    println!(
        "[main] Biases: diff_on={}, diff_off={}",
        args.diff_on, args.diff_off
    );

    evk_configuration.rate_limiter = make_rate_limiter(args.rate_limit);
    match &evk_configuration.rate_limiter {
        Some(rl) => println!(
            "[main] Hardware rate limiter enabled: {} events/s ({} events per {} µs)",
            args.rate_limit, rl.maximum_events_per_period, rl.reference_period_us
        ),
        None => println!("[main] Hardware rate limiter disabled."),
    }

    let device = prophesee_evk4::open(
        neuromorphic_drivers::SerialOrBusNumberAndAddress::None,
        evk_configuration.clone(),
        &prophesee_evk4::DEFAULT_USB_CONFIGURATION,
        event_loop,
        flag.clone(),
    )?;

    let mut adapter = device.create_adapter();

    // ── Shared state ──────────────────────────────────────────────────────────
    let shared = Arc::new(SharedState {
        // Always-on unless --record-toggle was passed — then it starts off
        // and the HTTP API controls it.
        recording: AtomicBool::new(!record_toggle),
        recording_toggle: record_toggle,
        buffer_max_age_secs: AtomicU64::new(args.buffer_max_age),
        buffer_max_bytes: AtomicU64::new(args.buffer_max_bytes),
        buffer_bytes: AtomicU64::new(0),
        buffer_chunks: AtomicU64::new(0),
        current_file: Mutex::new(None),
        configuration: Mutex::new(evk_configuration),
        rate_limit_eps: AtomicU64::new(args.rate_limit),
    });

    let output_dir = args.output_dir.clone();
    let file_interval = Duration::from_secs(args.file_length);

    // ── Channels ──────────────────────────────────────────────────────────────
    let (tx, rx) = mpsc::channel::<OwnedPacket>();
    // API → ingester: full device configurations to apply at runtime.
    let (config_tx, config_rx) = mpsc::channel::<prophesee_evk4::Configuration>();

    // ── HTTP API thread ───────────────────────────────────────────────────────
    {
        let api_bind = args.api_bind.clone();
        let shared = Arc::clone(&shared);
        thread::spawn(move || run_http_server(&api_bind, shared, config_tx));
    }

    // ── Processing thread ─────────────────────────────────────────────────────
    let shared_processor = Arc::clone(&shared);
    let processor = thread::spawn(move || {
        let shared = shared_processor;

        // Reusable buffers — cleared each packet, grown as needed, never reallocated
        // once they reach steady-state capacity.
        let mut events_buf: Vec<u8> = Vec::new();
        let mut triggers_buf: Vec<u8> = Vec::new();

        // Recording is off until the API enables it — no file is open.
        let mut raw_file: Option<std::fs::File> = None;
        let mut last_rollover = Instant::now();

        let mut circular = CircularBuffer::new();

        let mut events_stream: Option<UnixStream> = None;
        let mut triggers_stream: Option<UnixStream> = None;

        let mut last_metrics = Instant::now();
        let mut total_packets: u64 = 0;
        let mut total_dvs_events: u64 = 0;
        let mut total_trigger_events: u64 = 0;
        let mut total_raw_bytes: u64 = 0;
        let mut total_decode_us: u64 = 0;
        let mut total_file_write_us: u64 = 0;
        let mut total_socket_send_us: u64 = 0;

        while let Ok(packet) = rx.recv() {
            // ── Accept new clients if none connected ──────────────────────────
            if events_stream.is_none() {
                events_stream = accept_next(&events_listener);
            }
            if triggers_stream.is_none() {
                triggers_stream = accept_next(&triggers_listener);
            }

            // ── Recording state transitions ───────────────────────────────────
            let recording = shared.recording.load(Ordering::Relaxed);
            if recording && raw_file.is_none() {
                // Rising edge — open a file and write the circular buffer as
                // pre-roll, so the recording includes the moments *before*
                // the record command. The current packet is not in the buffer
                // yet, so nothing is written twice.
                let (path, mut file) = open_raw_file(&output_dir);
                let mut preroll_bytes = 0usize;
                for (_, data) in &circular.chunks {
                    if let Err(e) = file.write_all(data) {
                        eprintln!("[processor] Pre-roll write error: {e}");
                        break;
                    }
                    preroll_bytes += data.len();
                }
                if preroll_bytes > 0 {
                    println!(
                        "[processor] Recording started — wrote {preroll_bytes} bytes of pre-roll."
                    );
                } else {
                    println!("[processor] Recording started (buffer empty — no pre-roll).");
                }
                *shared.current_file.lock().unwrap() = Some(path);
                raw_file = Some(file);
                last_rollover = Instant::now();
            } else if !recording && raw_file.is_some() {
                // Falling edge — close the file.
                raw_file = None;
                *shared.current_file.lock().unwrap() = None;
                println!("[processor] Recording stopped — file closed.");
            }

            // ── File rollover (only while recording) ──────────────────────────
            if raw_file.is_some() && last_rollover.elapsed() >= file_interval {
                let (path, file) = open_raw_file(&output_dir);
                *shared.current_file.lock().unwrap() = Some(path);
                raw_file = Some(file);
                last_rollover = Instant::now();
            }

            let mut dvs_count = 0u64;
            let mut trigger_count = 0u64;

            let system_time = u64::from_le_bytes(
                packet.index_data[0..8].try_into().expect("8 bytes"),
            );
            let system_timestamp = u64::from_le_bytes(
                packet.index_data[8..16].try_into().expect("8 bytes"),
            );

            events_buf.clear();
            triggers_buf.clear();

            let t_decode = Instant::now();
            adapter.convert(
                &packet.raw_bytes,
                |dvs_event| {
                    let event = DvsEvent {
                        t: dvs_event.t,
                        x: dvs_event.x,
                        y: dvs_event.y,
                        on: dvs_event.polarity as u8,
                    };
                    events_buf.extend_from_slice(&event.to_bytes());
                    dvs_count += 1;
                },
                |trigger_event| {
                    let event = TriggerEvent {
                        system_time,
                        system_timestamp,
                        t: trigger_event.t,
                        id: trigger_event.id,
                        rising: trigger_event.polarity as u8,
                    };
                    triggers_buf.extend_from_slice(&event.to_bytes());
                    trigger_count += 1;
                },
            );
            let decode_us = t_decode.elapsed().as_micros() as u64;

            // ── Write raw bytes to file (recording only) ──────────────────────
            let t_file = Instant::now();
            if let Some(file) = raw_file.as_mut() {
                if let Err(e) = file.write_all(&packet.raw_bytes) {
                    eprintln!("[processor] Raw file write error: {e}");
                }
            }
            let file_write_us = t_file.elapsed().as_micros() as u64;

            // ── Send binary events over unix sockets ──────────────────────────
            let t_socket = Instant::now();
            if !events_buf.is_empty() {
                try_send(&mut events_stream, &events_buf);
            }
            if !triggers_buf.is_empty() {
                try_send(&mut triggers_stream, &triggers_buf);
            }
            let socket_send_us = t_socket.elapsed().as_micros() as u64;

            // ── Metrics ───────────────────────────────────────────────────────
            total_packets += 1;
            total_dvs_events += dvs_count;
            total_trigger_events += trigger_count;
            total_raw_bytes += packet.raw_bytes.len() as u64;
            total_decode_us += decode_us;
            total_file_write_us += file_write_us;
            total_socket_send_us += socket_send_us;

            // ── Circular buffer ───────────────────────────────────────────────
            // Last use of the packet — move its bytes into the buffer (no copy).
            circular.push(packet.raw_bytes);
            circular.evict(
                Duration::from_secs(shared.buffer_max_age_secs.load(Ordering::Relaxed)),
                shared.buffer_max_bytes.load(Ordering::Relaxed) as usize,
            );
            shared
                .buffer_bytes
                .store(circular.total_bytes as u64, Ordering::Relaxed);
            shared
                .buffer_chunks
                .store(circular.chunks.len() as u64, Ordering::Relaxed);

            if last_metrics.elapsed() >= Duration::from_secs(1) {
                let secs = last_metrics.elapsed().as_secs_f64();
                eprintln!(
                    "[processor] {:.0} pkts/s | {:.0} DVS/s | {:.0} trig/s | {:.2} MB/s raw | \
                     decode={}µs avg | file={}µs avg | socket={}µs avg",
                    total_packets as f64 / secs,
                    total_dvs_events as f64 / secs,
                    total_trigger_events as f64 / secs,
                    (total_raw_bytes as f64 / secs) / 1_048_576.0,
                    total_decode_us / total_packets.max(1),
                    total_file_write_us / total_packets.max(1),
                    total_socket_send_us / total_packets.max(1),
                );
                total_packets = 0;
                total_dvs_events = 0;
                total_trigger_events = 0;
                total_raw_bytes = 0;
                total_decode_us = 0;
                total_file_write_us = 0;
                total_socket_send_us = 0;
                last_metrics = Instant::now();
            }
        }

        println!("[processor] Sender dropped — all packets processed. Shutting down.");
    });

    // ── Ingestion thread ──────────────────────────────────────────────────────
    let ingester = thread::spawn(move || {
        loop {
            // Apply pending device configuration updates from the HTTP API.
            // Runs before each read, so updates land within one timeout period.
            while let Ok(configuration) = config_rx.try_recv() {
                device.update_configuration(configuration);
                println!("[ingester] Applied configuration update from API.");
            }

            let buffer_view = device.next_with_timeout(&std::time::Duration::from_millis(100));
            if let Some(buffer_view) = buffer_view {

                let now_us = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_micros() as u64;
                let delay_us = buffer_view.delay().as_micros() as u64;
                let system_timestamp_us = now_us.saturating_sub(delay_us);

                let mut index_data = [0u8; 16];
                index_data[0..8].copy_from_slice(&now_us.to_le_bytes());
                index_data[8..16].copy_from_slice(&system_timestamp_us.to_le_bytes());

                let packet = OwnedPacket {
                    raw_bytes: buffer_view.slice.to_vec(),
                    index_data,
                };

                if tx.send(packet).is_err() {
                    eprintln!("[ingester] Receiver gone — stopping ingestion.");
                    break;
                }
            }

            let _ = flag.load_error();

            if flag.load_warning().is_some() {
                println!("[ingester] USB circular buffer overflow");
            }
        }
    });

    ingester.join().expect("Ingestion thread panicked");
    processor.join().expect("Processing thread panicked");

    println!("[main] Pipeline complete.");
    Ok(())
}
