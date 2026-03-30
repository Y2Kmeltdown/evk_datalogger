use std::sync::mpsc;
use std::thread;
use neuromorphic_drivers::{UsbDevice};

// Owned packet: raw bytes copied out of the BufferView, plus the index data
// captured at the moment of ingestion. No borrows — safe to send across threads.
struct OwnedPacket {
    raw_bytes: Vec<u8>,
    index_data: [u8; 16],
}

fn main() -> Result<(), neuromorphic_drivers::Error> {
    // Channel carries fully-owned data, so there is no lifetime tie to `device`.
    let (tx, rx) = mpsc::channel::<OwnedPacket>();

    // Create Flag and Event Loop
    let (flag, event_loop) = neuromorphic_drivers::flag_and_event_loop()?;

    // Create EVK Configuration
    let mut evk_configuration = neuromorphic_drivers::prophesee_evk4::DEFAULT_CONFIGURATION;
    evk_configuration.biases.diff_on = 73;
    evk_configuration.biases.diff_on = 102;

    // Create Device with default configuration
    let device = neuromorphic_drivers::prophesee_evk4::open(
        neuromorphic_drivers::SerialOrBusNumberAndAddress::None,
        evk_configuration,
        &neuromorphic_drivers::prophesee_evk4::DEFAULT_USB_CONFIGURATION,
        event_loop,
        flag.clone(),
    )?;

    // `adapter` is only needed in the processor thread — move it there directly.
    let mut adapter = device.create_adapter();

// ── Processing thread ────────────────────────────────────────────────────
    let processor = thread::spawn(move || {
        let mut triggers_bytes: Vec<u8> = Vec::new();
        let mut events_bytes: Vec<u8> = Vec::new();
 
        // let mut packet_count: u64 = 0;
        // let mut total_dvs_events: u64 = 0;
        // let mut total_trigger_events: u64 = 0;
 
        while let Ok(packet) = rx.recv() {
            // packet_count += 1;
            let mut dvs_count = 0u64;
            let mut trigger_count = 0u64;
 
            // Decode the index data that was captured alongside the raw bytes.
            let system_time = u64::from_le_bytes(
                packet.index_data[0..8].try_into().expect("8 bytes"),
            );
            let system_timestamp = u64::from_le_bytes(
                packet.index_data[8..16].try_into().expect("8 bytes"),
            );
 
            // `convert` now operates on our owned slice — no borrow of `device`.
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
 
            // total_dvs_events += dvs_count;
            // total_trigger_events += trigger_count;
 
            // eprintln!(
            //     "[processor] packet={packet_count:>6} | \
            //      dvs={dvs_count:>6} (total={total_dvs_events}) | \
            //      triggers={trigger_count} (total={total_trigger_events}) | \
            //      raw_bytes={}",
            //     packet.raw_bytes.len(),
            // );
        }
 
        println!("[processor] Sender dropped — all packets processed. Shutting down.");
    });

    // ── Ingestion thread ─────────────────────────────────────────────────────
    // `device` is moved here. Its lifetime is now entirely within this thread,
    // so there is no cross-thread borrow conflict.
    let ingester = thread::spawn(move || {
        //let mut previous = std::time::Instant::now();
        let index_data: [u8; 16] = [0u8; 16];

        loop {
            if let Some(buffer_view) =
                device.next_with_timeout(&std::time::Duration::from_millis(100))
            {
                // Copy the borrowed slice into an owned Vec before the
                // BufferView is released. This is the key fix: we no longer
                // send a reference — we send owned data.
                let packet = OwnedPacket {
                    raw_bytes: buffer_view.slice.to_vec(),
                    index_data,
                };

                if tx.send(packet).is_err() {
                    eprintln!("[ingester]  Receiver gone — stopping ingestion.");
                    break;
                }

                // let now = std::time::Instant::now();
                // // Print out packet information
                // eprintln!(
                //     "{} B (backlog: {} packets, delay: {} µs, data rate: {:.3} MB/s)",
                //     buffer_view.slice.len(),
                //     buffer_view.backlog(),
                //     buffer_view.delay().as_micros(),
                //     (buffer_view.slice.len() as f64 / 1e6) / now.duration_since(previous).as_secs_f64()
                // );
                // previous = now;
            }
            // Check libusb flag for errors and return if an error is detected
            let _ =flag.load_error();

            
            // Check for warnings
            if flag.load_warning().is_some() {

                eprintln!("USB circular buffer overflow");

            }
        }
    });

    ingester.join().expect("Ingestion thread panicked");
    processor.join().expect("Processing thread panicked");

    println!("[main]      Pipeline complete.");
    Ok(())
}
