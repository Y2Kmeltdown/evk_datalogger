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
    #[arg(long, default_value_t = 60)]
    file_length: u64,
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
// TriggerEvent — 33 bytes
//   system_time      : u64  (8 bytes, little-endian)
//   system_timestamp : u64  (8 bytes, little-endian)
//   t                : u64  (8 bytes, little-endian) — sensor timestamp
//   id               : u8   (1 byte)                 — trigger channel ID
//   rising           : u8   (1 byte)                 — 1 = rising, 0 = falling
//   _pad             : u8   (6 bytes)                — reserved, always 0

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
    println!("[main] Device starting — socket clients can connect at any time.");

    // ── Device setup ──────────────────────────────────────────────────────────
    let (flag, event_loop) = neuromorphic_drivers::flag_and_event_loop()?;

    let mut evk_configuration = neuromorphic_drivers::prophesee_evk4::DEFAULT_CONFIGURATION;
    evk_configuration.biases.diff_on = 73;
    evk_configuration.biases.diff_off = 102;

    let device = neuromorphic_drivers::prophesee_evk4::open(
        neuromorphic_drivers::SerialOrBusNumberAndAddress::None,
        evk_configuration,
        &neuromorphic_drivers::prophesee_evk4::DEFAULT_USB_CONFIGURATION,
        event_loop,
        flag.clone(),
    )?;

    // Method of updating configuration
    // let mut test_configuration = neuromorphic_drivers::prophesee_evk4::DEFAULT_CONFIGURATION;
    // test_configuration.biases.diff_on = 73;
    // test_configuration.biases.diff_on = 102;
    // device.update_configuration(test_configuration);

    let mut adapter = device.create_adapter();

    let output_dir = args.output_dir.clone();
    let file_interval = Duration::from_secs(args.file_length);

    // ── Channel ───────────────────────────────────────────────────────────────
    let (tx, rx) = mpsc::channel::<OwnedPacket>();

    // ── Processing thread ─────────────────────────────────────────────────────
    let processor = thread::spawn(move || {
        // Reusable buffers — cleared each packet, grown as needed, never reallocated
        // once they reach steady-state capacity.
        let mut events_buf: Vec<u8> = Vec::new();
        let mut triggers_buf: Vec<u8> = Vec::new();

        let mut raw_file = open_raw_file(&output_dir);
        let mut last_rollover = Instant::now();

        let mut events_stream: Option<UnixStream> = None;
        let mut triggers_stream: Option<UnixStream> = None;

        #[cfg(debug_assertions)]
        let mut packet_count: u64 = 0;
        #[cfg(debug_assertions)]
        let mut total_dvs_events: u64 = 0;
        #[cfg(debug_assertions)]
        let mut total_trigger_events: u64 = 0;

        while let Ok(packet) = rx.recv() {
            // ── Accept new clients if none connected ──────────────────────────
            if events_stream.is_none() {
                events_stream = accept_next(&events_listener);
            }
            if triggers_stream.is_none() {
                triggers_stream = accept_next(&triggers_listener);
            }

            // ── File rollover ─────────────────────────────────────────────────
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

            events_buf.clear();
            triggers_buf.clear();

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

            //let state = adapter.state();
            //eprintln!("{state:?}");
            

            // ── Write raw bytes to file ───────────────────────────────────────
            let len = packet.raw_bytes.len() as u32;
            if let Err(e) = raw_file
                .write_all(&len.to_le_bytes())
                .and_then(|_| raw_file.write_all(&packet.raw_bytes))
            {
                eprintln!("[processor] Raw file write error: {e}");
            }

            // ── Send binary events over unix sockets ──────────────────────────
            if !events_buf.is_empty() {
                try_send(&mut events_stream, &events_buf);
            }
            if !triggers_buf.is_empty() {
                try_send(&mut triggers_stream, &triggers_buf);
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
        loop {
            if let Some(buffer_view) =
                device.next_with_timeout(&Duration::from_millis(100))
            {

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