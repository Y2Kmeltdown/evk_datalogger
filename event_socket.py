"""
EVK4 binary socket client
=========================
Connects to the EVK4 Unix domain sockets and decodes the binary event stream.

Wire format (per socket message):
  - 4 bytes  : u32 little-endian payload length
  - N bytes  : tightly-packed array of fixed-size event structs (see below)

DvsEvent — 13 bytes each:
  t      : u64 LE  — sensor timestamp (µs)
  x      : u16 LE  — pixel column
  y      : u16 LE  — pixel row
  on     : u8      — 1 = ON polarity, 0 = OFF

TriggerEvent — 26 bytes each:
  system_time      : u64 LE
  system_timestamp : u64 LE
  t                : u64 LE  — sensor timestamp (µs)
  id               : u8      — trigger channel ID
  rising           : u8      — 1 = rising edge, 0 = falling

Usage:
    python evk4_client.py

Stop with Ctrl+C.
"""

import socket
import struct
import threading
import sys
from collections import deque
import time

EVENTS_SOCKET_PATH   = "/tmp/evk4_events.sock"
TRIGGERS_SOCKET_PATH = "/tmp/evk4_triggers.sock"

# struct format strings (little-endian, no padding)
DVS_FORMAT     = "<QHHb"       # t(u64) x(u16) y(u16) on(u8)
DVS_SIZE       = struct.calcsize(DVS_FORMAT)   # 13 bytes

TRIGGER_FORMAT = "<QQQbb"      # system_time(u64) system_ts(u64) t(u64) id(u8) rising(u8)
TRIGGER_SIZE   = struct.calcsize(TRIGGER_FORMAT)  # 26 bytes


def recv_exact(sock: socket.socket, n: int) -> bytes | None:
    """Read exactly n bytes, returning None if the connection closes."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf.extend(chunk)
    return bytes(buf)


def read_framed_messages(sock: socket.socket):
    """Yield raw binary payloads from a 4-byte length-prefixed stream."""
    while True:
        header = recv_exact(sock, 4)
        if header is None:
            break
        (length,) = struct.unpack("<I", header)
        payload = recv_exact(sock, length)
        if payload is None:
            break
        yield payload


def events_worker(stop_event: threading.Event) -> None:
    samples = deque()
    total_bytes = 0
    window_seconds = 2.0

    print(f"[events]   Connecting to {EVENTS_SOCKET_PATH} ...")
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.connect(EVENTS_SOCKET_PATH)
        print("[events]   Connected.")

        for payload in read_framed_messages(sock):
            if stop_event.is_set():
                break

            now = time.monotonic()
            chunk_size = len(payload)
            total_bytes += chunk_size
            samples.append((now, chunk_size))

            # Drop samples outside the rolling window
            cutoff = now - window_seconds
            while samples and samples[0][0] < cutoff:
                samples.popleft()

            # Calculate speed over the window
            window_bytes = sum(s[1] for s in samples)
            actual_window = now - samples[0][0] if len(samples) > 1 else 1
            speed_mbps = (window_bytes * 8) / (actual_window * 1_000_000)

            print(f"\rCurrent: {speed_mbps:.2f} Mbps", end="", flush=True)

            # Each payload is a packed array of DVS_SIZE-byte event structs.
            # n_events = len(payload) // DVS_SIZE
            # for i in range(n_events):
            #     chunk = payload[i * DVS_SIZE : (i + 1) * DVS_SIZE]
            #     t, x, y, on = struct.unpack(DVS_FORMAT, chunk)
            #     print(f"[event]    t={t:>12}  x={x:>4}  y={y:>4}  on={on}")

    except ConnectionRefusedError:
        print(f"[events]   ERROR: could not connect to {EVENTS_SOCKET_PATH}. "
              "Is the Rust pipeline running?", file=sys.stderr)
    except Exception as e:
        print(f"[events]   ERROR: {e}", file=sys.stderr)
    finally:
        sock.close()
        stop_event.set()
        print("[events]   Disconnected.")


def triggers_worker(stop_event: threading.Event) -> None:
    print(f"[triggers] Connecting to {TRIGGERS_SOCKET_PATH} ...")
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.connect(TRIGGERS_SOCKET_PATH)
        print("[triggers] Connected.")

        for payload in read_framed_messages(sock):
            if stop_event.is_set():
                break

            n_events = len(payload) // TRIGGER_SIZE
            for i in range(n_events):
                chunk = payload[i * TRIGGER_SIZE : (i + 1) * TRIGGER_SIZE]
                system_time, system_ts, t, id_, rising = struct.unpack(TRIGGER_FORMAT, chunk)
                print(f"[trigger]  system_time={system_time}  "
                      f"system_ts={system_ts}  "
                      f"t={t:>12}  id={id_}  rising={rising}")

    except ConnectionRefusedError:
        print(f"[triggers] ERROR: could not connect to {TRIGGERS_SOCKET_PATH}. "
              "Is the Rust pipeline running?", file=sys.stderr)
    except Exception as e:
        print(f"[triggers] ERROR: {e}", file=sys.stderr)
    finally:
        sock.close()
        stop_event.set()
        print("[triggers] Disconnected.")


def main() -> None:
    stop_event = threading.Event()

    events_thread = threading.Thread(
        target=events_worker, args=(stop_event,), daemon=True, name="events"
    )
    triggers_thread = threading.Thread(
        target=triggers_worker, args=(stop_event,), daemon=True, name="triggers"
    )

    events_thread.start()
    triggers_thread.start()

    try:
        events_thread.join()
        triggers_thread.join()
    except KeyboardInterrupt:
        print("\n[main]     Shutting down...")
        stop_event.set()
        events_thread.join(timeout=2)
        triggers_thread.join(timeout=2)

    print("[main]     Done.")


if __name__ == "__main__":
    main()