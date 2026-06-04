//! EVK4 MJPEG server
//!
//! Connects to the events Unix socket published by the Rust pipeline,
//! accumulates DVS events into a grayscale frame, and serves the result
//! as an MJPEG stream over HTTP.
//!
//! Frame generation (socket reader + encoder thread) is unchanged.
//! Scaling is applied *after* the full-resolution frame is produced.
//!
//! REST API
//! ────────
//!   GET  /                   Browser viewer page
//!   GET  /stream             MJPEG multipart stream
//!   GET  /snapshot           Latest frame as a single JPEG
//!   GET  /api/settings       {"src_width":…,"src_height":…,"out_width":…,"out_height":…,"quality":…}
//!   PUT  /api/settings       body: {"out_width":…,"out_height":…,"quality":…}  (any subset)
//!   GET  /api/streaming      {"streaming": true|false}
//!   PUT  /api/streaming      body: {"streaming": true|false}
//!
//! Usage:
//!     cargo run --release --bin viewfinder -- --width 1280 --height 720
//!
//! Then open http://localhost:8080 in a browser.
//!
//! Required Cargo.toml additions:
//!   serde       = { version = "1", features = ["derive"] }
//!   serde_json  = "1"

use std::io::{BufRead, BufReader, Cursor, Read, Write};
use std::net::TcpListener;
use std::os::unix::net::UnixStream;
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use clap::Parser;
use image::codecs::jpeg::JpegEncoder;
use image::imageops::FilterType;
use image::GrayImage;
use serde::{Deserialize, Serialize};

use array2d::Array2D;

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

// ── Stream settings (runtime-tunable via REST API) ────────────────────────────

/// All parameters that affect how a finished full-resolution JPEG is processed
/// before being published to HTTP clients.
///
/// Guarded by an `RwLock` — the encoder thread is the only writer, HTTP
/// handlers are concurrent readers, so `RwLock` is cheaper than `Mutex` here.
///
/// `src_width` / `src_height` are immutable and stored here only so they can
/// be returned in GET /api/settings for client reference. The encoder always
/// produces frames at full sensor resolution regardless of output dimensions.
#[derive(Debug, Clone, Serialize)]
struct StreamSettings {
    /// Sensor/source resolution — read-only, set once at startup
    src_width:  u32,
    src_height: u32,

    /// MJPEG output resolution — can be freely scaled down from source
    out_width:  u32,
    out_height: u32,

    /// JPEG compression quality 1–100
    quality: u8,

    /// When false, frames are still generated but not pushed to stream clients.
    /// /snapshot continues to return the latest frame regardless.
    streaming: bool,
}

impl StreamSettings {
    fn new(src_width: u32, src_height: u32, quality: u8) -> Self {
        Self {
            src_width,
            src_height,
            out_width: src_width,
            out_height: src_height,
            quality,
            streaming: true,
        }
    }
}

/// Partial-update payload for PUT /api/settings.
/// All fields are optional — only supplied fields are applied.
#[derive(Debug, Deserialize)]
struct SettingsPatch {
    out_width:  Option<u32>,
    out_height: Option<u32>,
    quality:    Option<u8>,
}

/// Payload for PUT /api/streaming.
#[derive(Debug, Deserialize)]
struct StreamingPatch {
    streaming: bool,
}

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

// ── JPEG encoding + optional downscale ───────────────────────────────────────

/// Encode a full-resolution grayscale pixel buffer to JPEG at `quality`.
/// This is unchanged from the original — it always works on the full sensor
/// frame and knows nothing about output dimensions.
fn encode_jpeg(pixels: &[u8], width: u32, height: u32, quality: u8) -> Vec<u8> {
    let img = GrayImage::from_raw(width, height, pixels.to_vec())
        .expect("pixel buffer size mismatch");
    let mut buf = Cursor::new(Vec::new());
    JpegEncoder::new_with_quality(&mut buf, quality)
        .encode_image(&img)
        .expect("JPEG encoding failed");
    buf.into_inner()
}

/// Decode a JPEG, resize to (out_width × out_height), re-encode at `quality`.
///
/// Called only when output dimensions differ from source dimensions, so the
/// common case (scale = 1.0 / no resize) hits encode_jpeg directly.
///
/// Uses `image::imageops::resize` with Lanczos3 — good quality for
/// downscaling; swap to `FilterType::Nearest` if CPU budget is tight.
fn scale_jpeg(jpeg: &[u8], out_width: u32, out_height: u32, quality: u8) -> Vec<u8> {
    let img = image::load_from_memory(jpeg).expect("failed to decode JPEG for scaling");
    let scaled = image::imageops::resize(&img, out_width, out_height, FilterType::Lanczos3);
    let mut buf = Cursor::new(Vec::new());
    JpegEncoder::new_with_quality(&mut buf, quality)
        .encode_image(&scaled)
        .expect("JPEG re-encode after scale failed");
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

// ── HTTP helpers ──────────────────────────────────────────────────────────────

/// Minimal parsed HTTP request — enough to route and read a body.
struct HttpRequest {
    method:       String,
    path:         String,
    content_length: usize,
    body:         Vec<u8>,
}

/// Parse a request from a `BufReader`. Returns `None` on connection errors.
fn parse_request(reader: &mut BufReader<std::net::TcpStream>) -> Option<HttpRequest> {
    // Read the request line
    let mut request_line = String::new();
    reader.read_line(&mut request_line).ok()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_string();
    // Strip query string — e.g. "/stream?t=1234" → "/stream".
    // The dashboard appends ?t= for cache-busting; we don't need it server-side.
    let raw_path = parts.next()?.to_string();
    let path = raw_path.split('?').next().unwrap_or(&raw_path).to_string();

    // Read headers until blank line, collect Content-Length
    let mut content_length = 0usize;
    loop {
        let mut line = String::new();
        reader.read_line(&mut line).ok()?;
        let line = line.trim_end_matches(['\r', '\n']);
        if line.is_empty() {
            break;
        }
        let lower = line.to_lowercase();
        if lower.starts_with("content-length:") {
            content_length = lower
                .split(':')
                .nth(1)
                .and_then(|v| v.trim().parse().ok())
                .unwrap_or(0);
        }
    }

    // Read body if present
    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        reader.read_exact(&mut body).ok()?;
    }

    Some(HttpRequest { method, path, content_length, body })
}

/// Write a complete HTTP response.
fn write_response(
    stream: &mut std::net::TcpStream,
    status: u16,
    status_text: &str,
    content_type: &str,
    body: &[u8],
) {
    let header = format!(
        "HTTP/1.1 {status} {status_text}\r\n\
         Content-Type: {content_type}\r\n\
         Content-Length: {}\r\n\
         Cache-Control: no-cache\r\n\
         Access-Control-Allow-Origin: *\r\n\
         \r\n",
        body.len()
    );
    let _ = stream.write_all(header.as_bytes());
    let _ = stream.write_all(body);
}

fn json_response(stream: &mut std::net::TcpStream, status: u16, status_text: &str, json: &str) {
    write_response(stream, status, status_text, "application/json", json.as_bytes());
}

fn json_ok(stream: &mut std::net::TcpStream, json: &str) {
    json_response(stream, 200, "OK", json);
}

fn json_err(stream: &mut std::net::TcpStream, status: u16, reason: &str) {
    let body = format!(r#"{{"error":{}}}"#, serde_json::to_string(reason).unwrap());
    json_response(stream, status, "Bad Request", &body);
}

// ── Viewer HTML page ──────────────────────────────────────────────────────────
//
// The HTML lives in src/viewer.html next to this file.
// include_str! embeds it at compile time — no runtime file I/O, no extra
// dependencies, and the HTML file can be edited freely without escaping rules.

fn viewer_html() -> &'static str {
    include_str!("viewer.html")
}


// ── Main ──────────────────────────────────────────────────────────────────────

fn main() {
    let args = Args::parse();

    let width    = args.width;
    let height   = args.height;
    let n_pixels = (width * height) as usize;
    let frame_interval = Duration::from_nanos(1_000_000_000 / args.fps);

    // ── Shared state ──────────────────────────────────────────────────────────

    // `latest_frame` — the most recently published JPEG (after any scaling).
    // This is what HTTP clients receive.
    let latest_frame: Arc<Mutex<Option<Vec<u8>>>> = Arc::new(Mutex::new(None));

    // `shared_pixels` — live pixel buffer written by the socket reader and
    // snapshotted by the encoder. Unchanged from original.
    let shared_pixels: Arc<Mutex<Vec<u8>>> = Arc::new(Mutex::new(vec![127u8; n_pixels]));

    // `settings` — runtime-tunable stream parameters behind an RwLock.
    // Multiple HTTP handler threads read concurrently; the encoder writes
    // occasionally (only when a PUT /api/settings arrives).
    let settings: Arc<RwLock<StreamSettings>> = Arc::new(RwLock::new(
        StreamSettings::new(width, height, args.quality),
    ));

    // ── Encoder thread ────────────────────────────────────────────────────────
    // UNCHANGED: still snapshots shared_pixels, encodes at full sensor
    // resolution. The only additions are:
    //   1. quality is read from `settings` each cycle instead of args.
    //   2. if out dimensions differ from src, scale_jpeg() is called after encode.
    //   3. if streaming is disabled, latest_frame is still updated (for /snapshot)
    //      but the frame_ready condvar is not signalled (handled by clients).
    {
        let latest_frame  = Arc::clone(&latest_frame);
        let shared_pixels = Arc::clone(&shared_pixels);
        let settings      = Arc::clone(&settings);

        thread::spawn(move || {
            let mut snapshot = vec![0u8; n_pixels];

            loop {
                thread::sleep(frame_interval);

                #[cfg(debug_assertions)]
                let t0 = Instant::now();

                // ── Snapshot pixel buffer (unchanged) ─────────────────────────
                {
                    let px = shared_pixels.lock().unwrap();
                    snapshot.copy_from_slice(&px);
                    //px.fill(127);
                }

                // ── Read current settings atomically ──────────────────────────
                let (quality, out_width, out_height) = {
                    let s = settings.read().unwrap();
                    (s.quality, s.out_width, s.out_height)
                };

                // ── Encode at full sensor resolution (unchanged) ──────────────
                let mut jpeg = encode_jpeg(&snapshot, width, height, quality);

                // ── Scale down if output dimensions differ from source ─────────
                // This step is entirely post-generation — the pixel accumulation
                // and encoding above are untouched.
                if out_width != width || out_height != height {
                    jpeg = scale_jpeg(&jpeg, out_width, out_height, quality);
                }

                // ── Publish (always, so /snapshot stays fresh) ────────────────
                *latest_frame.lock().unwrap() = Some(jpeg);

                #[cfg(debug_assertions)]
                eprintln!("[encoder] total={:>5}µs  out={}×{}", t0.elapsed().as_micros(), out_width, out_height);
            }
        });
    }

    // ── Socket reader thread (UNCHANGED) ─────────────────────────────────────
    {
        let shared_pixels = Arc::clone(&shared_pixels);
        let args = args.clone();

        thread::spawn(move || {
            let mut payload_buf: Vec<u8> = Vec::new();
            let mut len_buf = [0u8; 4];

            let mut next_frame_time:u64 = 0;
            let increment:u64 = 33333;

            let mut grid: Array2D<u8> = Array2D::filled_with(0, height as usize, width as usize);

            loop {
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
                    if recv_exact(&mut stream, &mut len_buf).is_err() {
                        eprintln!("[reader] Socket closed — reconnecting.");
                        break;
                    }
                    let payload_len = u32::from_le_bytes(len_buf) as usize;
                    let n_events = payload_len / DVS_EVENT_SIZE;

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

                    {
                        let mut px = shared_pixels.lock().unwrap();
                        for i in 0..n_events {
                            let offset = i * DVS_EVENT_SIZE;
                            let chunk = &payload_buf[offset..offset + DVS_EVENT_SIZE];
                            let ts = u64::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7]]);
                            let x  = u16::from_le_bytes([chunk[8],  chunk[9]])  as u32;
                            let y  = u16::from_le_bytes([chunk[10], chunk[11]]) as u32;
                            let on = chunk[12];

                            
                            if x < width && y < height {
                                grid[(y as usize, x as usize)] = if on != 0 { 255 } else { 0 };
                            }

                            if ts >= next_frame_time {
                                *px = grid.as_row_major();
                                grid = Array2D::filled_with(127, height as usize, width as usize);
                                #[cfg(debug_assertions)]
                                { painted += 1; }


                                next_frame_time = next_frame_time + increment;
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
    println!("[http] API: GET/PUT /api/settings  |  GET/PUT /api/streaming");

    for tcp_stream in listener.incoming() {
        let tcp_stream = match tcp_stream {
            Ok(s) => s,
            Err(_) => continue,
        };

        let latest_frame = Arc::clone(&latest_frame);
        let settings     = Arc::clone(&settings);

        thread::spawn(move || {
            handle_client(tcp_stream, latest_frame, settings, frame_interval);
        });
    }
}

// ── HTTP client handler ───────────────────────────────────────────────────────

fn handle_client(
    stream: std::net::TcpStream,
    latest_frame: Arc<Mutex<Option<Vec<u8>>>>,
    settings:     Arc<RwLock<StreamSettings>>,
    frame_interval: Duration,
) {
    // Clone the stream so we can hand a owned copy to the BufReader while
    // still keeping a writable reference for responses.
    let write_stream = match stream.try_clone() {
        Ok(s) => s,
        Err(_) => return,
    };
    let mut write_stream = write_stream;
    let mut reader = BufReader::new(stream);

    let req = match parse_request(&mut reader) {
        Some(r) => r,
        None    => return,
    };

    match (req.method.as_str(), req.path.as_str()) {

        // ── GET / ─────────────────────────────────────────────────────────────
        ("GET", "/") | ("GET", "/index.html") => {
            let html = viewer_html();
            write_response(&mut write_stream, 200, "OK", "text/html; charset=utf-8", html.as_bytes());
        }

        // ── GET /stream ───────────────────────────────────────────────────────
        ("GET", "/stream") => {
            // Check gate before committing to the streaming response
            let is_streaming = settings.read().unwrap().streaming;
            if !is_streaming {
                json_err(&mut write_stream, 503, "Streaming is currently disabled.");
                return;
            }

            let headers = format!(
                "HTTP/1.1 200 OK\r\n\
                 Content-Type: multipart/x-mixed-replace; boundary={BOUNDARY}\r\n\
                 Cache-Control: no-cache\r\n\
                 Access-Control-Allow-Origin: *\r\n\
                 Connection: close\r\n\
                 \r\n"
            );
            if write_stream.write_all(headers.as_bytes()).is_err() {
                return;
            }

            let mut last_sent = Instant::now();

            loop {
                // Honour the streaming gate — pause without dropping the connection
                if !settings.read().unwrap().streaming {
                    thread::sleep(Duration::from_millis(100));
                    continue;
                }

                let elapsed = last_sent.elapsed();
                if elapsed < frame_interval {
                    thread::sleep(frame_interval - elapsed);
                }

                let frame = latest_frame.lock().unwrap().clone();
                if let Some(jpeg) = frame {
                    let part = mjpeg_part(&jpeg);
                    if write_stream.write_all(&part).is_err() {
                        break;
                    }
                    last_sent = Instant::now();
                }
            }
        }

        // ── GET /snapshot ─────────────────────────────────────────────────────
        ("GET", "/snapshot") => {
            let frame = latest_frame.lock().unwrap().clone();
            match frame {
                Some(jpeg) => write_response(&mut write_stream, 200, "OK", "image/jpeg", &jpeg),
                None       => json_err(&mut write_stream, 503, "No frame available yet."),
            }
        }

        // ── GET /api/settings ─────────────────────────────────────────────────
        ("GET", "/api/settings") => {
            let s    = settings.read().unwrap();
            let json = serde_json::to_string(&*s).unwrap();
            json_ok(&mut write_stream, &json);
        }

        // ── PUT /api/settings ─────────────────────────────────────────────────
        ("PUT", "/api/settings") => {
            let patch: SettingsPatch = match serde_json::from_slice(&req.body) {
                Ok(p)  => p,
                Err(e) => {
                    json_err(&mut write_stream, 400, &format!("Invalid JSON: {e}"));
                    return;
                }
            };

            let mut errors: Vec<String> = Vec::new();
            let mut s = settings.write().unwrap();

            if let Some(w) = patch.out_width {
                if w < 1 || w > s.src_width {
                    errors.push(format!("out_width must be 1–{}.", s.src_width));
                } else {
                    s.out_width = w;
                }
            }
            if let Some(h) = patch.out_height {
                if h < 1 || h > s.src_height {
                    errors.push(format!("out_height must be 1–{}.", s.src_height));
                } else {
                    s.out_height = h;
                }
            }
            if let Some(q) = patch.quality {
                if q < 1 || q > 100 {
                    errors.push("quality must be 1–100.".to_string());
                } else {
                    s.quality = q;
                }
            }

            if !errors.is_empty() {
                let msg = errors.join("; ");
                drop(s); // release write lock before responding
                json_err(&mut write_stream, 400, &msg);
                return;
            }

            let json = serde_json::to_string(&*s).unwrap();
            drop(s);
            eprintln!("[api] settings updated: {json}");
            json_ok(&mut write_stream, &json);
        }

        // ── GET /api/streaming ────────────────────────────────────────────────
        ("GET", "/api/streaming") => {
            let on = settings.read().unwrap().streaming;
            json_ok(&mut write_stream, &format!(r#"{{"streaming":{on}}}"#));
        }

        // ── PUT /api/streaming ────────────────────────────────────────────────
        ("PUT", "/api/streaming") => {
            let patch: StreamingPatch = match serde_json::from_slice(&req.body) {
                Ok(p)  => p,
                Err(e) => {
                    json_err(&mut write_stream, 400, &format!("Invalid JSON: {e}"));
                    return;
                }
            };

            settings.write().unwrap().streaming = patch.streaming;
            let on = patch.streaming;
            eprintln!("[api] streaming {}", if on { "enabled" } else { "disabled" });
            json_ok(&mut write_stream, &format!(r#"{{"streaming":{on}}}"#));
        }

        // ── OPTIONS (CORS preflight) ──────────────────────────────────────────
        ("OPTIONS", _) => {
            let header = "HTTP/1.1 204 No Content\r\n\
                          Access-Control-Allow-Origin: *\r\n\
                          Access-Control-Allow-Methods: GET, PUT, OPTIONS\r\n\
                          Access-Control-Allow-Headers: Content-Type\r\n\
                          \r\n";
            let _ = write_stream.write_all(header.as_bytes());
        }

        // ── 404 ───────────────────────────────────────────────────────────────
        _ => {
            json_err(&mut write_stream, 404, "Not found.");
        }
    }
}