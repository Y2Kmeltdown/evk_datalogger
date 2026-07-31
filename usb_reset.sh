#!/usr/bin/env bash
#
# usb_reset.sh — send a USBDEVFS_RESET ioctl to a USB device: a device-level
# reset over the wire without kernel re-enumeration. Use this when
# usb_replug.sh (sysfs `authorized` toggle) fails to revive a wedged device,
# and before resorting to physically unplugging the cable.
#
# Usage:
#   ./usb_reset.sh [VID:PID]
#
#   VID:PID  Device to reset, e.g. 1409:8e00. When omitted, the first device
#            found from the built-in event-camera list is used:
#            1409:8e00 (IDS uEye XCP-E), 04b4:00f4/00f5 (Prophesee EVK4),
#            31f7:0003/0004 (CenturyArks SilkyEvCam HD).
#
# Exit status: 0 if the reset ioctl succeeded and the device is still present.
#
# Root is usually NOT required: the neuromorphic-drivers udev rules install
# MODE 0666 for the supported event cameras' /dev/bus/usb nodes. Requires
# python3 (used for the ioctl; no third-party packages).
# NOTE: any application holding the device open must re-open it afterwards.

set -euo pipefail

KNOWN_IDS=("1409:8e00" "04b4:00f4" "04b4:00f5" "31f7:0003" "31f7:0004")

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    sed -n '2,24p' "$0"
    exit 0
fi

# find_devnode <vid> <pid> → prints /dev/bus/usb/BBB/DDD; rc=1 when absent.
find_devnode() {
    local vid="$1" pid="$2" d
    for d in /sys/bus/usb/devices/*; do
        [[ -r "$d/idVendor" && -r "$d/idProduct" ]] || continue
        if [[ "$(<"$d/idVendor")" == "$vid" && "$(<"$d/idProduct")" == "$pid" ]]; then
            printf '/dev/bus/usb/%03d/%03d\n' "$(<"$d/busnum")" "$(<"$d/devnum")"
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
        if find_devnode "${id%%:*}" "${id##*:}" >/dev/null; then
            VID="${id%%:*}"; PID="${id##*:}"
            break
        fi
    done
    [[ -n "$VID" ]] || { echo "usb_reset: no known event camera found — pass VID:PID explicitly" >&2; exit 1; }
fi

if ! NODE="$(find_devnode "$VID" "$PID")"; then
    echo "usb_reset: device $VID:$PID not found on the bus" >&2
    exit 1
fi

command -v python3 >/dev/null || { echo "usb_reset: python3 is required" >&2; exit 1; }

echo "usb_reset: resetting $VID:$PID ($NODE)"
python3 - "$NODE" <<'PY'
import fcntl, sys
USBDEVFS_RESET = 0x5514  # from <linux/usbdevice_fs.h>
with open(sys.argv[1], "wb") as dev:
    fcntl.ioctl(dev, USBDEVFS_RESET, 0)
PY

sleep 2
if find_devnode "$VID" "$PID" >/dev/null; then
    echo "usb_reset: OK — $VID:$PID present after reset"
    echo "usb_reset: restart (or re-open the device in) your application now"
    exit 0
fi

echo "usb_reset: device did not come back — physical re-plug required" >&2
exit 1
