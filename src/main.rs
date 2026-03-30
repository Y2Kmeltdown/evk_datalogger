use std::io::Write;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};
use neuromorphic_drivers::{UsbDevice};

use chrono::Utc;
use clap::Parser;

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(
    name = "evk4-pipeline",
    about = "Ingest EVK4 events and stream them to Unix sockets while recording raw data"
)]
struct Args {
    /// Unix socket path for decoded DVS events
    #[arg(long, default_value = "/tmp/evk4_events.sock")]
    events_socket: PathBuf,

    /// Unix socket path for decoded trigger events
    #[arg(long, default_value = "/tmp/evk4_triggers.sock")]
    triggers_socket: PathBuf,

    /// Directory to write raw recording files into
    #[arg(long, default_value = "/tmp/evk4_raw")]
    output_dir: PathBuf,

    /// How often (in seconds) to roll over to a new raw output file
    #[arg(long, default_value_t = 30)]
    file_interval_secs: u64,
}

// ── Data ──────────────────────────────────────────────────────────────────────

/// Owned packet: raw bytes copied out of the BufferView, plus the index data
/// captured at the moment of ingestion. No borrows — safe to send across threads.
struct OwnedPacket {
    raw_bytes: Vec<u8>,
    index_data: [u8; 16],
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Build a timestamped filename inside `dir`, e.g. `20240315T123456Z.bin`.
fn timestamped_path(dir: &Path) -> PathBuf {
    let ts = Utc::now().format("%Y%m%dT%H%M%SZ");
    dir.join(format!("{ts}.raw"))
}

/// Open a new raw output file, creating the directory if needed.
fn open_raw_file(dir: &Path) -> std::fs::File {
    std::fs::create_dir_all(dir).expect("Failed to create output directory");
    let path = timestamped_path(dir);
    println!("[processor] New raw file: {}", path.display());
    std::fs::OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&path)
        .unwrap_or_else(|e| panic!("Failed to open raw file {}: {e}", path.display()))
}

/// Try to write a length-prefixed message to a stream.
/// If the write fails (client disconnected), returns the stream slot back as None
/// so we stop trying to write and wait for a new connection.
fn try_send(stream: &mut Option<UnixStream>, data: &[u8]) {
    let Some(s) = stream.as_mut() else { return };
    let len = data.len() as u32;
    let ok = s.write_all(&len.to_le_bytes()).and_then(|_| s.write_all(data));
    if let Err(e) = ok {
        eprintln!("[processor] Socket write error (client disconnected?): {e}");
        *stream = None; // drop the broken stream; accept_next() will reconnect
    }
}

/// Poll the listener (non-blocking) for a new client connection.
/// Returns Some(stream) if one connected this call, None otherwise.
fn accept_next(listener: &UnixListener) -> Option<UnixStream> {
    match listener.accept() {
        Ok((stream, _)) => {
            // Put the accepted stream into non-blocking mode so that writes
            // to a slow client don't stall the processing thread.
            stream
                .set_nonblocking(false) // writes should block briefly; reads never happen
                .expect("set_nonblocking failed");
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

// ── Main ──────────────────────────────────────────────────────────────────────

fn main() -> Result<(), neuromorphic_drivers::Error> {
    let args = Args::parse();

    // ── Unix socket setup ─────────────────────────────────────────────────────
    let _ = std::fs::remove_file(&args.events_socket);
    let _ = std::fs::remove_file(&args.triggers_socket);

    // Set both listeners to non-blocking so accept() returns immediately
    // when no client is waiting rather than blocking the processing thread.
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
    println!("[main] File interval:    {}s", args.file_interval_secs);
    println!("[main] Device starting — socket clients can connect at any time.");

    // ── Device setup ──────────────────────────────────────────────────────────
    let (flag, event_loop) = neuromorphic_drivers::flag_and_event_loop()?;

    let mut evk_configuration = neuromorphic_drivers::prophesee_evk4::DEFAULT_CONFIGURATION;
    evk_configuration.biases.diff_on = 73;
    evk_configuration.biases.diff_on = 102;

    let device = neuromorphic_drivers::prophesee_evk4::open(
        neuromorphic_drivers::SerialOrBusNumberAndAddress::None,
        evk_configuration,
        &neuromorphic_drivers::prophesee_evk4::DEFAULT_USB_CONFIGURATION,
        event_loop,
        flag.clone(),
    )?;

    let mut adapter = device.create_adapter();

    let output_dir = args.output_dir.clone();
    let file_interval = Duration::from_secs(args.file_interval_secs);

    // ── Channel ───────────────────────────────────────────────────────────────
    let (tx, rx) = mpsc::channel::<OwnedPacket>();

    // ── Processing thread ─────────────────────────────────────────────────────
    let processor = thread::spawn(move || {
        let mut events_bytes: Vec<u8> = Vec::new();
        let mut triggers_bytes: Vec<u8> = Vec::new();

        let mut raw_file = open_raw_file(&output_dir);
        let mut last_rollover = Instant::now();

        // Streams start as None — populated when a client connects.
        let mut events_stream: Option<UnixStream> = None;
        let mut triggers_stream: Option<UnixStream> = None;

        #[cfg(debug_assertions)]
        let mut packet_count: u64 = 0;
        #[cfg(debug_assertions)]
        let mut total_dvs_events: u64 = 0;
        #[cfg(debug_assertions)]
        let mut total_trigger_events: u64 = 0;

        while let Ok(packet) = rx.recv() {
            // ── Accept new clients if none are connected ───────────────────────
            // These are non-blocking polls — they return immediately if nobody
            // is waiting, so they never stall the processing loop.
            if events_stream.is_none() {
                events_stream = accept_next(&events_listener);
            }
            if triggers_stream.is_none() {
                triggers_stream = accept_next(&triggers_listener);
            }

            // ── File rollover check ───────────────────────────────────────────
            if last_rollover.elapsed() >= file_interval {
                raw_file = open_raw_file(&output_dir);
                last_rollover = Instant::now();
            }

            #[cfg(debug_assertions)]
            { packet_count += 1; }

            let mut dvs_count = 0u64;
            let mut trigger_count = 0u64;

            let system_time = u64::from_le_bytes(
                packet.index_data[0..8].try_into().expect("8 bytes"),
            );
            let system_timestamp = u64::from_le_bytes(
                packet.index_data[8..16].try_into().expect("8 bytes"),
            );

            events_bytes.clear();
            triggers_bytes.clear();

            adapter.convert(
                &packet.raw_bytes,
                |dvs_event| {
                    let t = dvs_event.t;
                    let x = dvs_event.x;
                    let y = dvs_event.y;
                    let on = dvs_event.polarity as u8;
                    events_bytes.extend(format!("{t},{x},{y},{on}\n").as_bytes());
                    dvs_count += 1;
                },
                |trigger_event| {
                    let t = trigger_event.t;
                    let id = trigger_event.id;
                    let rising = trigger_event.polarity as u8;
                    triggers_bytes.extend(
                        format!("{system_time},{system_timestamp},{t},{id},{rising}\n")
                            .as_bytes(),
                    );
                    trigger_count += 1;
                },
            );

            // ── Write raw bytes to file ───────────────────────────────────────
            let len = packet.raw_bytes.len() as u32;
            if let Err(e) = raw_file
                .write_all(&len.to_le_bytes())
                .and_then(|_| raw_file.write_all(&packet.raw_bytes))
            {
                eprintln!("[processor] Raw file write error: {e}");
            }

            // ── Send decoded events over unix sockets ─────────────────────────
            // try_send is a no-op when the stream is None (no client connected)
            // and sets it back to None if the client has disconnected.
            if !events_bytes.is_empty() {
                try_send(&mut events_stream, &events_bytes);
            }
            if !triggers_bytes.is_empty() {
                try_send(&mut triggers_stream, &triggers_bytes);
            }

            #[cfg(debug_assertions)]
            {
                total_dvs_events += dvs_count;
                total_trigger_events += trigger_count;
                eprintln!(
                    "[processor] packet={packet_count:>6} | \
                     dvs={dvs_count:>6} (total={total_dvs_events}) | \
                     triggers={trigger_count} (total={total_trigger_events}) | \
                     raw_bytes={}",
                    packet.raw_bytes.len(),
                );
            }
        }

        println!("[processor] Sender dropped — all packets processed. Shutting down.");
    });

    // ── Ingestion thread ──────────────────────────────────────────────────────
    let ingester = thread::spawn(move || {
        let index_data: [u8; 16] = [0u8; 16];

        loop {
            if let Some(buffer_view) =
                device.next_with_timeout(&Duration::from_millis(100))
            {
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
                eprintln!("[ingester] USB circular buffer overflow");
            }
        }
    });

    ingester.join().expect("Ingestion thread panicked");
    processor.join().expect("Processing thread panicked");

    println!("[main] Pipeline complete.");
    Ok(())
}