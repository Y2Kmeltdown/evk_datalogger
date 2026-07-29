//! Offline replay tool — reads a raw `.bin` file recorded by the pipeline,
//! runs it through `adapter.convert()`, and publishes the decoded events
//! to Unix sockets in the same binary format as the live pipeline.
//!
//! Real-time pacing: the tool tracks the latest sensor timestamp (µs) seen
//! per packet and sleeps between packets so that wall-clock time advances
//! at the same rate as sensor time. A `--speed` multiplier allows faster or
//! slower than real-time replay.
//!
//! Usage:
//!     cargo run --bin replay -- <path/to/recording.bin>
//!     cargo run --bin replay -- <path/to/recording.bin> --speed 0.5   # half speed
//!     cargo run --bin replay -- <path/to/recording.bin> --speed 2.0   # double speed

use std::io::{Read, Write};
#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};
#[cfg(windows)]
use uds_windows::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use clap::Parser;

// ── CLI ───────────────────────────────────────────────────────────────────────

/// Base directory for the default socket path.
/// `/tmp` on Unix; the per-user temp directory on Windows.
#[cfg(unix)]
fn default_tmp_dir() -> PathBuf {
    PathBuf::from("/tmp")
}

/// Base directory for the default socket path.
/// `/tmp` on Unix; the per-user temp directory on Windows.
#[cfg(windows)]
fn default_tmp_dir() -> PathBuf {
    std::env::temp_dir()
}

#[derive(Parser, Debug)]
#[command(name = "replay", about = "Replay a raw EVK4 recording over Unix sockets")]
struct Args {
    /// Path to the raw .raw recording file
    recording: PathBuf,

    /// Unix socket path for decoded DVS events
    #[arg(long, default_value_os_t = default_tmp_dir().join("evk4_events.sock"))]
    events_socket: PathBuf,

    /// Unix socket path for decoded trigger events (omit to disable trigger publishing)
    #[arg(long)]
    triggers_socket: Option<PathBuf>,

    /// Playback speed multiplier (1.0 = real-time, 0.5 = half speed, 2.0 = double speed)
    #[arg(long, default_value_t = 1.0)]
    speed: f64,
}

// ── Binary event structs (must match main pipeline) ───────────────────────────

const DVS_EVENT_SIZE: usize = 13;
const TRIGGER_EVENT_SIZE: usize = 26;

fn dvs_to_bytes(t: u64, x: u16, y: u16, on: u8) -> [u8; DVS_EVENT_SIZE] {
    let mut buf = [0u8; DVS_EVENT_SIZE];
    buf[0..8].copy_from_slice(&t.to_le_bytes());
    buf[8..10].copy_from_slice(&x.to_le_bytes());
    buf[10..12].copy_from_slice(&y.to_le_bytes());
    buf[12] = on;
    buf
}

fn trigger_to_bytes(
    system_time: u64,
    system_timestamp: u64,
    t: u64,
    id: u8,
    rising: u8,
) -> [u8; TRIGGER_EVENT_SIZE] {
    let mut buf = [0u8; TRIGGER_EVENT_SIZE];
    buf[0..8].copy_from_slice(&system_time.to_le_bytes());
    buf[8..16].copy_from_slice(&system_timestamp.to_le_bytes());
    buf[16..24].copy_from_slice(&t.to_le_bytes());
    buf[24] = id;
    buf[25] = rising;
    buf
}

// ── Socket send helper ────────────────────────────────────────────────────────

/// Write a length-prefixed binary payload to a stream.
/// Returns false if the client has disconnected.
fn try_send(stream: &mut UnixStream, data: &[u8]) -> bool {
    let len = data.len() as u32;
    stream.write_all(&len.to_le_bytes()).is_ok() && stream.write_all(data).is_ok()
}

// ── Real-time pacer ───────────────────────────────────────────────────────────

/// Tracks the mapping between sensor timestamps (µs) and wall-clock time so
/// that packets can be released at the correct real-time rate.
struct Pacer {
    /// Wall-clock instant corresponding to `sensor_origin_us`
    wall_origin: Instant,
    /// Sensor timestamp (µs) of the first event seen
    sensor_origin_us: u64,
    /// Playback speed multiplier
    speed: f64,
}

impl Pacer {
    fn new(first_sensor_us: u64, speed: f64) -> Self {
        Self {
            wall_origin: Instant::now(),
            sensor_origin_us: first_sensor_us,
            speed,
        }
    }

    /// Sleep until the wall clock catches up to `sensor_ts_us` in sensor time.
    /// Returns immediately if we're already behind (no catch-up accumulation).
    fn wait_until(&self, sensor_ts_us: u64) {
        if sensor_ts_us <= self.sensor_origin_us {
            return;
        }

        // How far into the recording this timestamp is, in sensor µs.
        let sensor_delta_us = sensor_ts_us - self.sensor_origin_us;

        // Scale by speed: at speed=2.0, 1s of sensor time should pass in 0.5s wall.
        let wall_target_us = (sensor_delta_us as f64 / self.speed) as u64;
        let wall_target = self.wall_origin + Duration::from_micros(wall_target_us);

        let now = Instant::now();
        if wall_target > now {
            std::thread::sleep(wall_target - now);
        }
        // If wall_target <= now we're running behind — emit immediately, no sleep.
    }
}

// ── Main ──────────────────────────────────────────────────────────────────────

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    if args.speed <= 0.0 {
        eprintln!("--speed must be greater than 0");
        std::process::exit(1);
    }

    // ── Bind sockets ──────────────────────────────────────────────────────────
    let _ = std::fs::remove_file(&args.events_socket);
    if let Some(ref path) = args.triggers_socket {
        let _ = std::fs::remove_file(path);
    }

    let events_listener = UnixListener::bind(&args.events_socket)?;
    println!("[replay] Events socket:   {}", args.events_socket.display());

    let triggers_listener = args.triggers_socket.as_ref().map(|path| {
        let _ = std::fs::remove_file(path);
        let listener = UnixListener::bind(path)
            .unwrap_or_else(|e| panic!("Failed to bind triggers socket {}: {e}", path.display()));
        println!("[replay] Triggers socket: {}", path.display());
        listener
    });

    println!("[replay] Recording:       {}", args.recording.display());
    println!("[replay] Speed:           {}x", args.speed);
    println!("[replay] Waiting for socket clients to connect...");

    let (mut events_stream, _) = events_listener.accept()?;
    println!("[replay] Events client connected.");

    let mut triggers_stream = match triggers_listener {
        Some(ref listener) => {
            let (stream, _) = listener.accept()?;
            println!("[replay] Triggers client connected.");
            Some(stream)
        }
        None => {
            println!("[replay] Triggers socket disabled — trigger events will be discarded.");
            None
        }
    };

    println!("[replay] Starting replay...");

    // ── Open recording ────────────────────────────────────────────────────────
    let mut file = std::fs::File::open(&args.recording)
        .unwrap_or_else(|e| panic!("Failed to open {}: {e}", args.recording.display()));

    // ── Create adapter ────────────────────────────────────────────────────────
    let mut adapter = neuromorphic_drivers::adapters::evt3::Adapter::from_dimensions(1280, 720);

    // ── Counters ──────────────────────────────────────────────────────────────
    let mut chunk_count: u64 = 0;
    let mut total_dvs: u64 = 0;
    let mut total_triggers: u64 = 0;

    // Reusable encode buffers — cleared each chunk.
    let mut events_buf: Vec<u8> = Vec::new();
    let mut triggers_buf: Vec<u8> = Vec::new();

    let system_time: u64 = 0;
    let system_timestamp: u64 = 0;

    // ── Real-time pacer — initialised on the first event seen ─────────────────
    let mut pacer: Option<Pacer> = None;

    // ── Read loop ─────────────────────────────────────────────────────────────
    // Read the file in fixed-size chunks. The EVT3 adapter is stateful and
    // handles events that span chunk boundaries correctly, so chunk size only
    // affects how often we pace and publish — not correctness.
    let mut chunk = vec![0u8; 131072];

    loop {
        let n = match file.read(&mut chunk) {
            Ok(0) => break,  // clean EOF
            Ok(n) => n,
            Err(e) => return Err(e.into()),
        };

        chunk_count += 1;
        let mut dvs_count = 0u64;
        let mut trigger_count = 0u64;

        // Separate timestamp trackers per closure — two closures cannot both
        // mutably borrow the same variable simultaneously in Rust.
        let mut dvs_last_t: Option<u64>     = None;
        let mut trigger_last_t: Option<u64> = None;

        events_buf.clear();
        triggers_buf.clear();

        adapter.convert(
            &chunk[..n],
            |dvs_event| {
                let bytes = dvs_to_bytes(
                    dvs_event.t,
                    dvs_event.x,
                    dvs_event.y,
                    dvs_event.polarity as u8,
                );
                events_buf.extend_from_slice(&bytes);
                dvs_last_t = Some(dvs_event.t);
                dvs_count += 1;
            },
            |trigger_event| {
                let bytes = trigger_to_bytes(
                    system_time,
                    system_timestamp,
                    trigger_event.t,
                    trigger_event.id,
                    trigger_event.polarity as u8,
                );
                triggers_buf.extend_from_slice(&bytes);
                trigger_last_t = Some(trigger_event.t);
                trigger_count += 1;
            },
        );

        // Take the latest timestamp across both event types as the pace target.
        let chunk_last_t = std::cmp::max(dvs_last_t, trigger_last_t);

        // ── Real-time pacing ──────────────────────────────────────────────────
        if let Some(last_t) = chunk_last_t {
            let p = pacer.get_or_insert_with(|| {
                println!("[replay] First event timestamp: {last_t} µs — starting pacer.");
                Pacer::new(last_t, args.speed)
            });
            p.wait_until(last_t);
        }

        // ── Publish to sockets ────────────────────────────────────────────────
        if !events_buf.is_empty() && !try_send(&mut events_stream, &events_buf) {
            eprintln!("[replay] Events client disconnected — stopping.");
            break;
        }
        if !triggers_buf.is_empty() {
            if let Some(ref mut stream) = triggers_stream {
                if !try_send(stream, &triggers_buf) {
                    eprintln!("[replay] Triggers client disconnected — stopping.");
                    break;
                }
            }
        }

        total_dvs += dvs_count;
        total_triggers += trigger_count;

        eprintln!(
            "[replay] chunk={chunk_count:>6} | \
             dvs={dvs_count:>6} (total={total_dvs}) | \
             triggers={trigger_count} (total={total_triggers}) | \
             bytes_read={n}"
        );
    }

    println!(
        "[replay] Complete — {chunk_count} chunks, {total_dvs} DVS events, \
         {total_triggers} trigger events."
    );
    Ok(())
}