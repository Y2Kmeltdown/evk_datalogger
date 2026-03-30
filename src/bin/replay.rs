//! Offline replay tool — reads a raw `.bin` file recorded by the pipeline
//! and runs it through `adapter.convert()` to reproduce decoded events.
//!
//! Usage:
//!     cargo run --bin replay -- <path/to/recording.bin>

use std::io::Read;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ── CLI ───────────────────────────────────────────────────────────────────
    let path = std::env::args().nth(1).unwrap_or_else(|| {
        eprintln!("Usage: replay <path/to/recording.bin>");
        std::process::exit(1);
    });

    // ── Open file ─────────────────────────────────────────────────────────────
    let mut file = std::fs::File::open(&path)
        .unwrap_or_else(|e| panic!("Failed to open {path}: {e}"));

    println!("[replay] Opened: {path}");

    // ── Create adapter ────────────────────────────────────────────────────────
    // The adapter is stateless with respect to the file — it only needs to know
    // the device type. No USB connection is required for offline use.
    let mut adapter = neuromorphic_drivers::adapters::evt3::Adapter::from_dimensions(1280, 720);
    

    // ── Counters ──────────────────────────────────────────────────────────────
    let mut packet_count: u64 = 0;
    let mut total_dvs: u64 = 0;
    let mut total_triggers: u64 = 0;

    // ── Read loop ─────────────────────────────────────────────────────────────
    // Each packet in the file is stored as:
    //   [4 bytes: u32 LE length][N bytes: raw payload]
    let mut len_buf = [0u8; 4];

    loop {
        // Read the 4-byte length prefix.
        match file.read_exact(&mut len_buf) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break, // clean end of file
            Err(e) => return Err(e.into()),
        }

        let payload_len = u32::from_le_bytes(len_buf) as usize;

        // Read the raw payload.
        let mut raw = vec![0u8; payload_len];
        file.read_exact(&mut raw)?;

        packet_count += 1;
        let mut dvs_count = 0u64;
        let mut trigger_count = 0u64;

        // Decode — identical call to what the processor thread uses live.
        adapter.convert(
            &raw,
            |dvs_event| {
                let t  = dvs_event.t;
                let x  = dvs_event.x;
                let y  = dvs_event.y;
                let on = dvs_event.polarity as u8;

                // Replace this with your actual processing logic.
                println!("[dvs]     t={t:>12}  x={x:>4}  y={y:>4}  on={on}");
                dvs_count += 1;
            },
            |trigger_event| {
                let t      = trigger_event.t;
                let id     = trigger_event.id;
                let rising = trigger_event.polarity as u8;

                // Replace this with your actual processing logic.
                println!("[trigger] t={t:>12}  id={id}  rising={rising}");
                trigger_count += 1;
            },
        );

        total_dvs      += dvs_count;
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