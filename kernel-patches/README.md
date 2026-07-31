# RK3588 vendor-kernel 6.1 USB3 (xHCI/DWC3) stability patch set

Patch set for the Armbian **vendor** kernel (`rk35xx`, BRANCH=`vendor`,
6.1.115, Rockchip BSP branch `rk-6.1-rkr5.1`) that fixes the xHCI endpoint
recovery misbehavior behind the "event camera freezes at high data rates,
must unplug/replug" failure on the Orange Pi 5 Max (and other RK3588 boards).

Everything is confined to `drivers/usb/` — rknpu, MPP (hardware encode),
GPU, and the device-tree overlays are untouched.

## The bug (evidence)

With a SuperSpeed bulk-IN device flooding at 40–120 MB/s (Prophesee
EVK4-class event camera, VID:PID `1409:8e00`, 32 concurrent 128 KiB URBs,
infinite timeout), a link-level transfer error sends the stock 6.1.115 xHCI
driver into an endless recovery loop:

```
Transfer error for slot 1 ep 2 on endpoint
Hard-reset ep 2, slot 1
Set TR Deq ptr 0x…, cycle 0
Transfer error for slot 1 ep 2 on endpoint   ← repeats forever, silently
```

The endpoint never recovers; the infinite-timeout URBs never complete, so
userspace freezes with **no error anywhere**. Re-enumeration was the only
way out. Reliable reproducer: start the stream, then `kill -9` the process
mid-flood — the mass URB cancellation + endpoint reset wedged the endpoint
every time on the stock kernel.

Root cause fit: the 6.1.y stable line received a family of Stop-Endpoint /
halt-recovery fixes in **6.1.116–6.1.124** (after the Armbian vendor branch
froze at 6.1.115), and several related xHCI fixes were never backported to
6.1.y at all. This set picks up exactly those.

## Patch contents (`rk35xx-vendor-6.1/`)

Applied by the Armbian build in lexical order. `001`–`015` are verbatim
upstream commits fetched from git.kernel.org; `900` is a hand-backport of
the four that didn't apply (see `README.txt` in the patch dir for the exact
deviations — all mechanical, logic preserved).

| File | Upstream commit | In 6.1.y | What it fixes |
|---|---|---|---|
| `001-xhci-fix-link-trb-cmd-ring.patch` | `075919f6df5d` | 6.1.116 | Command-ring wedge on stopped completion at a Link TRB |
| `002-xhci-td-invalidation-set-deq.patch` | `484c3bab2d5d` | 6.1.120 | Cancelled TDs not given back under pending Set TR Dequeue |
| `004-xhci-retry-stop-endpoint.patch` | `fd9d55d190c0` | 6.1.124 | Retry Stop Endpoint on Context State Error (busy endpoints) |
| `008-dwc3-halt-state-timeout.patch` | `d3a8c28426fc` | 6.1.129 | dwc3 controller halt enter/exit timeout with LPM enabled |
| `010-dwc3-suspendenable-after-phy-init.patch` | `cc5bfc4e16fc` | 6.1.131 | dwc3 PHY suspend bit set too late |
| `011-xhci-fix-td-matching.patch` | `91edf5a0c2fb` | never (v6.9) | Completion events misattributed to wrong TD → silently dead endpoint |
| `014-xhci-ehb-clear-at-end.patch` | `15f3ef070933` | never (v6.6) | Event Handler Busy cleared mid-processing → lost events |
| `015-xhci-iman-flush.patch` | `f5bce30ad25e` | never (v6.16) | Lost interrupt from unflushed IMAN posted write |
| `900-manual-backports-usb.patch` | `42b758137601`, `474538b8dd1c`, `e21ebe51af68` (6.1.124), `6328bdc988d2` (v6.15, never) | mixed | Stop-Endpoint retry limiting + redundant-command avoidance + generic error handling; don't trust EP-context cycle bit after halt |

Deliberately **dropped** (see `README.txt` for justifications):
`fbcbffbac994` + `3126ea9be66b` (naneng-combphy reset — already in
Rockchip's vendor PHY driver in substance), `e30e9ad9ed66` (ERDP update —
requires a prerequisite event-ring rework series that doesn't exist in 6.1).

## Build (replicate)

Native on the board (tools present: gcc/make/git; ~25 GB disk, ~40 min) or
any machine that can run the Armbian build framework:

```bash
git clone --depth 1 https://github.com/armbian/build
mkdir -p build/userpatches/kernel/rk35xx-vendor-6.1
cp kernel-patches/rk35xx-vendor-6.1/*.patch build/userpatches/kernel/rk35xx-vendor-6.1/

cd build
./compile.sh kernel BOARD=orangepi5-max BRANCH=vendor RELEASE=resolute \
    EXPERT=yes BATCH_MODE=yes KERNEL_CONFIGURE=no
# debs land in output/debs/
```

Notes:

- `EXPERT=yes` is required because `orangepi5-max` is a community (`.csc`)
  board. For other RK3588 boards just change `BOARD=` — the
  `rk35xx-vendor-6.1` userpatches dir is shared by the whole family.
- The build relaunches itself via sudo and installs host packages.
- The framework applies userpatches with `git am`/`git apply`, so they do
  **not** appear as `* applying …` lines in the build log. To confirm they
  went in, diff a patched file against the pristine branch, e.g.
  `drivers/usb/host/xhci-ring.c`, or check that the ORAS artifact hash in
  the log (`…-P<patchhash>-…`) differs from the stock build.

## Install + verify

```bash
sudo dpkg -i output/debs/linux-image-vendor-rk35xx_*.deb \
            output/debs/linux-dtb-vendor-rk35xx_*.deb
sudo reboot
```

The version string stays `6.1.115-vendor-rk35xx` (same base), so `uname -r`
can't tell old from patched. Verify instead by behavior: with xHCI dynamic
debug on (`echo 'module xhci_hcd +p' | sudo tee /sys/kernel/debug/dynamic_debug/control`),
a mid-stream `kill -9` of the datalogger produces clean teardown lines —
`Not queuing Stop Endpoint on slot 1 ep 2 in state 0x4` and
`All TDs cleared, ring doorbell` — instead of the endless
`Hard-reset ep … / Transfer error` loop.

## Validation results (2026-07-31, Orange Pi 5 Max)

**Fixed / improved**

- No more Stop-Endpoint retry storms or hard-reset loops in dmesg; URB
  cancellation completes cleanly (`All TDs cleared, ring doorbell`).
- Camera recovers from mild wedges with a software re-plug —
  `usb_reset.sh` (USBDEVFS_RESET) or `usb_replug.sh` (sysfs `authorized`
  toggle), both in this repo — no physical access needed.
- In standard use with `--rate-limit 50000000` the stream runs
  consistently; previously it froze at far lower sustained rates.

**Known remaining issue**

Under the extreme synthetic trigger (SIGKILL mid-flood), a link-level
`Transfer error` can still occur and the **camera firmware** can wedge hard
enough that only USBDEVFS_RESET or a power cycle revives it — the kernel
side now recovers cleanly, but the endpoint error itself still happens on
rare occasions. On 6.18/7.1.5 the error does not occur at all; the likely
reason is the mainline `phy-rockchip-usbdp` driver (merged in v6.10) with
better signal-integrity defaults, which cannot be backported to the 6.1
vendor stack. If the residual wedge matters in your deployment, the
mitigations below cover it.

## Runtime mitigations (recommended in production)

- `--rate-limit 50000000` (hardware rate limiter) — keeps the link away
  from the error-prone regime.
- `--watchdog-secs 10` (this repo, `src/main.rs`) — detects total data
  silence, drops the device, runs the two recovery scripts next to the
  binary, and re-opens automatically. Retry loops until the camera returns.
- `usbcore.quirks=1409:8e00:k` (NO_LPM) in `extraargs` in
  `/boot/armbianEnv.txt` if link errors persist.
- The userspace OOM fix (bounded ingest channel, commit `954eec8`) —
  without it, high event rates OOM-kill the process, and the resulting
  SIGKILL mid-stream is itself a wedge trigger on this platform.

## Files

- `kernel-patches/rk35xx-vendor-6.1/` — the 9 patch files + `README.txt`
  (per-patch dispositions and port deviations)
- `usb_replug.sh` — software re-plug via sysfs `authorized` (needs root)
- `usb_reset.sh` — device reset via USBDEVFS_RESET (no root needed)
- `src/main.rs` — `--watchdog-secs` recovery + bounded ingest channel
