//! Offline replay tool — reads a raw `.bin` file recorded by the pipeline,
//! runs it through `adapter.convert()`, and publishes the decoded events
//! to Unix sockets in the same binary format as the live pipeline.
//!
//! Usage:
//!     cargo run --bin replay -- <path/to/recording.bin>
//!
//! The viewfinder and Python client can connect to the sockets exactly as
//! they would with the live pipeline.

use std::io::{Read, Write};
use std::os::unix::net::UnixListener;
use std::path::PathBuf;

use clap::Parser;

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(name = "replay", about = "Replay a raw EVK4 recording over Unix sockets")]
struct Args {
    /// Path to the raw .bin recording file
    recording: PathBuf,

    /// Unix socket path for decoded DVS events
    #[arg(long, default_value = "/tmp/evk4_events.sock")]
    events_socket: PathBuf,

    /// Unix socket path for decoded trigger events (omit to disable trigger publishing)
    #[arg(long)]
    triggers_socket: Option<PathBuf>,

    /// Wait for socket clients to connect before starting replay
    #[arg(long, default_value_t = true)]
    wait_for_clients: bool,
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

fn trigger_to_bytes(system_time: u64, system_timestamp: u64, t: u64, id: u8, rising: u8) -> [u8; TRIGGER_EVENT_SIZE] {
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
fn try_send(stream: &mut std::os::unix::net::UnixStream, data: &[u8]) -> bool {
    let len = data.len() as u32;
    stream.write_all(&len.to_le_bytes()).is_ok()
        && stream.write_all(data).is_ok()
}

// ── Main ──────────────────────────────────────────────────────────────────────

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    // ── Bind sockets ──────────────────────────────────────────────────────────
    let _ = std::fs::remove_file(&args.events_socket);
    if let Some(ref path) = args.triggers_socket {
        let _ = std::fs::remove_file(path);
    }

    let events_listener = UnixListener::bind(&args.events_socket)?;
    println!("[replay] Events socket:   {}", args.events_socket.display());

    // Triggers socket is optional — if not specified, trigger events are decoded
    // but silently discarded rather than blocking startup on a client connecting.
    let triggers_listener = args.triggers_socket.as_ref().map(|path| {
        let _ = std::fs::remove_file(path);
        let listener = UnixListener::bind(path)
            .unwrap_or_else(|e| panic!("Failed to bind triggers socket {}: {e}", path.display()));
        println!("[replay] Triggers socket: {}", path.display());
        listener
    });

    println!("[replay] Recording:       {}", args.recording.display());

    // ── Wait for clients ──────────────────────────────────────────────────────
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
    let mut packet_count: u64 = 0;
    let mut total_dvs: u64 = 0;
    let mut total_triggers: u64 = 0;

    // Reusable encode buffers — grown to steady-state capacity, never reallocated.
    let mut events_buf: Vec<u8> = Vec::new();
    let mut triggers_buf: Vec<u8> = Vec::new();

    // index_data is zeroed for replay since we have no live system timestamps.
    let index_data = [0u8; 16];
    let system_time = u64::from_le_bytes(index_data[0..8].try_into().unwrap());
    let system_timestamp = u64::from_le_bytes(index_data[8..16].try_into().unwrap());

    // ── Read loop ─────────────────────────────────────────────────────────────
    let mut len_buf = [0u8; 4];

    loop {
        match file.read_exact(&mut len_buf) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
            Err(e) => return Err(e.into()),
        }

        let payload_len = u32::from_le_bytes(len_buf) as usize;
        let mut raw = vec![0u8; payload_len];
        file.read_exact(&mut raw)?;

        packet_count += 1;
        let mut dvs_count = 0u64;
        let mut trigger_count = 0u64;

        events_buf.clear();
        triggers_buf.clear();

        adapter.convert(
            &raw,
            |dvs_event| {
                let bytes = dvs_to_bytes(
                    dvs_event.t,
                    dvs_event.x,
                    dvs_event.y,
                    dvs_event.polarity as u8,
                );
                events_buf.extend_from_slice(&bytes);
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
                trigger_count += 1;
            },
        );

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
            "[replay] packet={packet_count:>6} | \
             dvs={dvs_count:>6} (total={total_dvs}) | \
             triggers={trigger_count} (total={total_triggers}) | \
             raw_bytes={payload_len}"
        );
    }

    println!(
        "[replay] Complete — {packet_count} packets, {total_dvs} DVS events, \
         {total_triggers} trigger events."
    );

    Ok(())
}