//! EVK4 MJPEG server
//!
//! Connects to the events Unix socket published by the Rust pipeline,
//! accumulates DVS events into a grayscale frame, and serves the result
//! as an MJPEG stream over HTTP.
//!
//! Usage:
//!     cargo run --release --bin viewfinder -- --width 1280 --height 720
//!
//! Then open http://localhost:8080 in a browser.

use std::io::{Cursor, Read, Write};
use std::net::TcpListener;
use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use clap::Parser;
use image::codecs::jpeg::JpegEncoder;
use image::{GrayImage};

// ── CLI ───────────────────────────────────────────────────────────────────────

#[derive(Parser, Debug, Clone)]
#[command(name = "evk4-mjpeg", about = "EVK4 MJPEG stream server")]
struct Args {
    /// Width of the sensor in pixels
    #[arg(long, default_value_t = 1280)]
    width: u32,

    /// Height of the sensor in pixels
    #[arg(long, default_value_t = 720)]
    height: u32,

    /// Target frame rate for the MJPEG stream
    #[arg(long, default_value_t = 50)]
    fps: u64,

    /// JPEG quality (1–100)
    #[arg(long, default_value_t = 80)]
    quality: u8,

    /// HTTP bind address
    #[arg(long, default_value = "0.0.0.0:8080")]
    bind: String,

    /// Unix socket path for the decoded DVS events
    #[arg(long, default_value = "/tmp/evk4_events.sock")]
    events_socket: String,

}

// ── Binary event layout (must match the Rust pipeline) ───────────────────────
//   t      : u64 LE  (8 bytes)  — not needed for rendering, skipped
//   x      : u16 LE  (2 bytes)
//   y      : u16 LE  (2 bytes)
//   on     : u8      (1 byte)
//   total  : 13 bytes

const DVS_EVENT_SIZE: usize = 13;

// ── Socket helpers ────────────────────────────────────────────────────────────

fn recv_exact(stream: &mut UnixStream, buf: &mut [u8]) -> std::io::Result<()> {
    let mut total = 0;
    while total < buf.len() {
        let n = stream.read(&mut buf[total..])?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "socket closed",
            ));
        }
        total += n;
    }
    Ok(())
}

// ── JPEG encoding ─────────────────────────────────────────────────────────────

fn encode_jpeg(pixels: &[u8], width: u32, height: u32, quality: u8) -> Vec<u8> {
    let img = GrayImage::from_raw(width, height, pixels.to_vec())
        .expect("pixel buffer size mismatch");
    let mut buf = Cursor::new(Vec::new());
    JpegEncoder::new_with_quality(&mut buf, quality)
        .encode_image(&img)
        .expect("JPEG encoding failed");
    buf.into_inner()
}

// ── MJPEG boundary ────────────────────────────────────────────────────────────

const BOUNDARY: &str = "evk4frame";

fn mjpeg_part(jpeg: &[u8]) -> Vec<u8> {
    let mut part = format!(
        "--{BOUNDARY}\r\nContent-Type: image/jpeg\r\nContent-Length: {}\r\n\r\n",
        jpeg.len()
    )
    .into_bytes();
    part.extend_from_slice(jpeg);
    part.extend_from_slice(b"\r\n");
    part
}

// ── Main ──────────────────────────────────────────────────────────────────────

fn main() {
    let args = Args::parse();

    let width = args.width;
    let height = args.height;
    let n_pixels = (width * height) as usize;
    let frame_interval = Duration::from_nanos(1_000_000_000 / args.fps);

    // ── Shared state ──────────────────────────────────────────────────────────
    // `latest_frame` — the most recently JPEG-encoded frame for HTTP clients.
    let latest_frame: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));

    // `shared_pixels` — the live pixel buffer written by the socket reader and
    // snapshotted by the encoder. Using a Mutex here means the encoder only
    // needs to hold the lock for the duration of a memcpy, not for the whole
    // JPEG encode, keeping the socket reader unblocked.
    let shared_pixels: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(vec![127u8; n_pixels]));

    // ── Encoder thread ────────────────────────────────────────────────────────
    // Wakes at `fps` intervals, snapshots the pixel buffer, JPEG-encodes it,
    // resets the buffer to mid-grey, and publishes the frame. Decoupled from
    // the socket reader so encoding latency never slows down event ingestion.
    {
        let latest_frame = Arc::clone(&latest_frame);
        let shared_pixels = Arc::clone(&shared_pixels);
        let args = args.clone();

        thread::spawn(move || {
            // Local snapshot buffer — avoids holding the lock during encode.
            let mut snapshot = vec![0u8; n_pixels];

            loop {
                thread::sleep(frame_interval);

                #[cfg(debug_assertions)]
                let encode_start = Instant::now();

                // Snapshot and reset inside a brief lock window.
                {
                    let mut px = shared_pixels.lock().unwrap();
                    snapshot.copy_from_slice(&px);
                    px.fill(127);
                }
                // Lock released — socket reader can paint again immediately.

                let jpeg = encode_jpeg(&snapshot, width, height, args.quality);

                #[cfg(debug_assertions)]
                let encode_us = encode_start.elapsed().as_micros();

                *latest_frame.lock().unwrap() = Some(jpeg);

                #[cfg(debug_assertions)]
                eprintln!("[encoder] encode={encode_us:>5}µs");
            }
        });
    }

    // ── Socket reader thread ──────────────────────────────────────────────────
    // Reads entire payloads in one syscall, slices events out of the bulk
    // buffer in-memory, and paints directly into `shared_pixels`.
    // Never blocks on encoding — if the encoder is busy the pixel buffer just
    // keeps accumulating events until the next snapshot.
    {
        let shared_pixels = Arc::clone(&shared_pixels);
        let args = args.clone();

        thread::spawn(move || {
            // Reusable heap buffer for bulk payload reads — grows to the
            // largest payload seen and never shrinks, avoiding repeated allocation.
            let mut payload_buf: Vec<u8> = Vec::new();
            let mut len_buf = [0u8; 4];

            loop {
                // (Re)connect — retry on failure so the server survives restarts.
                println!("[reader] Connecting to {} ...", args.events_socket);
                let mut stream = loop {
                    match UnixStream::connect(&args.events_socket) {
                        Ok(s) => break s,
                        Err(e) => {
                            eprintln!("[reader] Connect failed: {e} — retrying in 1s");
                            thread::sleep(Duration::from_secs(1));
                        }
                    }
                };
                println!("[reader] Connected.");

                loop {
                    // ── Read entire payload in one syscall ────────────────────
                    if recv_exact(&mut stream, &mut len_buf).is_err() {
                        eprintln!("[reader] Socket closed — reconnecting.");
                        break;
                    }
                    let payload_len = u32::from_le_bytes(len_buf) as usize;
                    let n_events = payload_len / DVS_EVENT_SIZE;

                    // Grow the buffer if this payload is larger than any seen before.
                    if payload_buf.len() < payload_len {
                        payload_buf.resize(payload_len, 0);
                    }
                    if recv_exact(&mut stream, &mut payload_buf[..payload_len]).is_err() {
                        eprintln!("[reader] Socket closed mid-payload — reconnecting.");
                        break;
                    }

                    #[cfg(debug_assertions)]
                    let paint_start = Instant::now();
                    #[cfg(debug_assertions)]
                    let mut painted = 0usize;

                    // ── Slice events out of the bulk buffer in-memory ─────────
                    // No per-event syscall — all events are already in `payload_buf`.
                    // Lock once for the whole payload so the encoder doesn't
                    // interleave a snapshot mid-paint.
                    {
                        let mut px = shared_pixels.lock().unwrap();
                        for i in 0..n_events {
                            let offset = i * DVS_EVENT_SIZE;
                            let chunk = &payload_buf[offset..offset + DVS_EVENT_SIZE];

                            // Parse x, y, on directly from the bulk buffer slice.
                            // t is at [0..8], skipped.
                            let x = u16::from_le_bytes([chunk[8], chunk[9]]) as u32;
                            let y = u16::from_le_bytes([chunk[10], chunk[11]]) as u32;
                            let on = chunk[12];

                            if x < width && y < height {
                                px[y as usize * width as usize + x as usize] =
                                    if on != 0 { 255 } else { 0 };

                                #[cfg(debug_assertions)]
                                { painted += 1; }
                            }
                        }
                    }

                    #[cfg(debug_assertions)]
                    eprintln!(
                        "[reader]  payload={payload_len:>7} B | events={n_events:>6} | \
                         painted={painted:>6} | paint={paint_us:>5}µs",
                        paint_us = paint_start.elapsed().as_micros(),
                    );
                }
            }
        });
    }

    // ── HTTP server ───────────────────────────────────────────────────────────
    let listener = TcpListener::bind(&args.bind)
        .unwrap_or_else(|e| panic!("Failed to bind {}: {e}", args.bind));

    println!("[http] Listening on http://{}", args.bind);
    println!(
        "[http] Open in browser: http://{}",
        if args.bind.starts_with("0.0.0.0") {
            format!("localhost:{}", args.bind.split(':').last().unwrap())
        } else {
            args.bind.clone()
        }
    );

    for tcp_stream in listener.incoming() {
        let tcp_stream = match tcp_stream {
            Ok(s) => s,
            Err(_) => continue,
        };

        let latest_frame = Arc::clone(&latest_frame);

        thread::spawn(move || {
            handle_client(tcp_stream, latest_frame, frame_interval);
        });
    }
}

// ── HTTP client handler ───────────────────────────────────────────────────────

fn handle_client(
    mut stream: std::net::TcpStream,
    latest_frame: Arc<Mutex<Option<Vec<u8>>>>,
    frame_interval: Duration,
) {
    let mut request_buf = [0u8; 1024];
    if stream.read(&mut request_buf).is_err() {
        return;
    }

    let headers = format!(
        "HTTP/1.1 200 OK\r\n\
         Content-Type: multipart/x-mixed-replace; boundary={BOUNDARY}\r\n\
         Cache-Control: no-cache\r\n\
         Connection: close\r\n\
         \r\n"
    );
    if stream.write_all(headers.as_bytes()).is_err() {
        return;
    }

    let mut last_sent = Instant::now();

    loop {
        let elapsed = last_sent.elapsed();
        if elapsed < frame_interval {
            thread::sleep(frame_interval - elapsed);
        }

        let frame = latest_frame.lock().unwrap().clone();

        if let Some(jpeg) = frame {
            let part = mjpeg_part(&jpeg);
            if stream.write_all(&part).is_err() {
                break;
            }
            last_sent = Instant::now();
        }
    }
}