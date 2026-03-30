"""
EVK4 socket client
==================
Connects to both the events and triggers Unix domain sockets published by the
Rust pipeline and prints decoded data to stdout.

Usage:
    python evk4_client.py

Stop with Ctrl+C.
"""

import socket
import struct
import threading
import sys

EVENTS_SOCKET_PATH = "/tmp/evk4_events.sock"
TRIGGERS_SOCKET_PATH = "/tmp/evk4_triggers.sock"


def recv_exact(sock: socket.socket, n: int) -> bytes | None:
    """Read exactly n bytes from a socket, returning None if the connection closes."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf.extend(chunk)
    return bytes(buf)


def read_framed_messages(sock: socket.socket):
    """
    Yield decoded string messages from a length-prefixed stream.
    Each message is preceded by a 4-byte little-endian uint32 length.
    """
    while True:
        header = recv_exact(sock, 4)
        if header is None:
            break
        (length,) = struct.unpack("<I", header)
        payload = recv_exact(sock, length)
        if payload is None:
            break
        yield payload.decode("utf-8")


def events_worker(stop_event: threading.Event) -> None:
    """Connect to the events socket and print each DVS event."""
    print(f"[events]   Connecting to {EVENTS_SOCKET_PATH} ...")
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.connect(EVENTS_SOCKET_PATH)
        print("[events]   Connected.")
        for message in read_framed_messages(sock):
            if stop_event.is_set():
                break
            for line in message.splitlines():
                if not line:
                    continue
                t, x, y, on = line.split(",")
                #print(f"[event]    t={t:>10}  x={x:>4}  y={y:>4}  on={on}")
    except ConnectionRefusedError:
        print(f"[events]   ERROR: Could not connect to {EVENTS_SOCKET_PATH}. "
              "Is the Rust pipeline running?", file=sys.stderr)
    except Exception as e:
        print(f"[events]   ERROR: {e}", file=sys.stderr)
    finally:
        sock.close()
        stop_event.set()
        print("[events]   Disconnected.")


def triggers_worker(stop_event: threading.Event) -> None:
    """Connect to the triggers socket and print each trigger event."""
    print(f"[triggers] Connecting to {TRIGGERS_SOCKET_PATH} ...")
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        sock.connect(TRIGGERS_SOCKET_PATH)
        print("[triggers] Connected.")
        for message in read_framed_messages(sock):
            if stop_event.is_set():
                break
            for line in message.splitlines():
                if not line:
                    continue
                system_time, system_timestamp, t, id_, rising = line.split(",")
                # print(f"[trigger]  system_time={system_time}  "
                #       f"system_ts={system_timestamp}  "
                #       f"t={t:>10}  id={id_}  rising={rising}")
    except ConnectionRefusedError:
        print(f"[triggers] ERROR: Could not connect to {TRIGGERS_SOCKET_PATH}. "
              "Is the Rust pipeline running?", file=sys.stderr)
    except Exception as e:
        print(f"[triggers] ERROR: {e}", file=sys.stderr)
    finally:
        sock.close()
        stop_event.set()
        print("[triggers] Disconnected.")


def main() -> None:
    # Shared flag — either thread can signal the other to stop.
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
        # Block main thread until both workers finish or Ctrl+C is pressed.
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