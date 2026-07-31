#!/usr/bin/env bash
#
# usb_replug.sh — software "unplug + replug" of a USB device via the sysfs
# `authorized` attribute. Deconfiguring and reconfiguring the device resets
# its endpoint state in the kernel, which revives a silently-wedged endpoint
# (e.g. the RK3588 kernel-6.1 xHCI bug where a SuperSpeed bulk endpoint dies
# after a transfer error) without physically touching the cable.
#
# Recovery order: try this first; if the device still does not stream, try
# usb_reset.sh (USBDEVFS_RESET); only then resort to a physical re-plug.
#
# Usage:
#   ./usb_replug.sh [VID:PID] [DELAY]
#
#   VID:PID  Device to re-plug, e.g. 1409:8e00. When omitted, the first
#            device found from the built-in event-camera list is used:
#            1409:8e00 (IDS uEye XCP-E), 04b4:00f4/00f5 (Prophesee EVK4),
#            31f7:0003/0004 (CenturyArks SilkyEvCam HD).
#   DELAY    Seconds between deauthorize and reauthorize (default: 3).
#
# Exit status: 0 if the device is present and authorized afterwards, else 1.
#
# Requires root for the sysfs write; re-executes itself through sudo when
# started as a regular user. NOTE: any application holding the device open
# must re-open it after the re-plug.

set -euo pipefail

KNOWN_IDS=("1409:8e00" "04b4:00f4" "04b4:00f5" "31f7:0003" "31f7:0004")

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    sed -n '2,26p' "$0"
    exit 0
fi

DELAY="${2:-3}"

# sysfs `authorized` is root-only — re-exec via sudo when needed.
if [[ $EUID -ne 0 ]]; then
    exec sudo -- "$0" "$@"
fi

# find_device <vid> <pid> → prints device id (e.g. "8-1"); rc=1 when absent.
find_device() {
    local vid="$1" pid="$2" d
    for d in /sys/bus/usb/devices/*; do
        [[ -r "$d/idVendor" && -r "$d/idProduct" ]] || continue
        if [[ "$(<"$d/idVendor")" == "$vid" && "$(<"$d/idProduct")" == "$pid" ]]; then
            basename "$d"
            return 0
        fi
    done
    return 1
}

VID=""; PID=""
if [[ -n "${1:-}" ]]; then
    VID="${1%%:*}"; PID="${1##*:}"
else
    for id in "${KNOWN_IDS[@]}"; do
        if find_device "${id%%:*}" "${id##*:}" >/dev/null; then
            VID="${id%%:*}"; PID="${id##*:}"
            break
        fi
    done
    [[ -n "$VID" ]] || { echo "usb_replug: no known event camera found — pass VID:PID explicitly" >&2; exit 1; }
fi

if ! DEV="$(find_device "$VID" "$PID")"; then
    echo "usb_replug: device $VID:$PID not found on the bus" >&2
    exit 1
fi
NODE="/sys/bus/usb/devices/$DEV"

echo "usb_replug: re-plugging $VID:$PID ($NODE)"
echo 0 > "$NODE/authorized"
sleep "$DELAY"
echo 1 > "$NODE/authorized"
sleep 2

if find_device "$VID" "$PID" >/dev/null && [[ "$(<"$NODE/authorized")" == "1" ]]; then
    echo "usb_replug: OK — $VID:$PID present and re-authorized"
    echo "usb_replug: restart (or re-open the device in) your application now"
    exit 0
fi

echo "usb_replug: device did not come back — try usb_reset.sh or a physical re-plug" >&2
exit 1
