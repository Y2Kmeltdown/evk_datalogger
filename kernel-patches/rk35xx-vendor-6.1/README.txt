Final userpatches set for Armbian vendor kernel 6.1.115 (RK3588, rk-6.1-rkr5.1)
Target tree: ~/kernel-build/linux-rockchip
Apply in lexical order: 001 002 004 008 010 011 014 015, then 900.

Per-patch disposition
=====================

KEPT CLEAN (copied unchanged from ~/kernel-build/patches/):
  001-xhci-fix-link-trb-cmd-ring.patch      applies w/ offset -22
  002-xhci-td-invalidation-set-deq.patch    applies w/ offset; hunk #2 fuzz 2
  004-xhci-retry-stop-endpoint.patch        applies w/ offset -11
  008-dwc3-halt-state-timeout.patch         applies w/ offset 25
  010-dwc3-suspendenable-after-phy-init.patch applies w/ offsets; hunks #1/#5/#8 fuzz 1-2
  011-xhci-fix-td-matching.patch            applies w/ offset 47
  014-xhci-ehb-clear-at-end.patch           applies w/ offsets 94-99
  015-xhci-iman-flush.patch                 applies w/ offsets; fuzz 2
  (fuzz is caused by vendor tree context differing slightly from the mainline
  context in the patch files; these files were left unmodified per instructions)

HAND-PORTED INTO 900-manual-backports-usb.patch:
  005-xhci-limit-stop-endpoint-retries.patch  (upstream 42b758137601)
    Original reject: xhci-ring.c hunk on the Stop Endpoint completion handler
    only matched after 004 (fd9d55d190c0) was applied first - 004 adds the
    EP_STATE_STOPPED/NEC case that 005 rewrites. No content change needed.
  006-xhci-avoid-redundant-stop-endpoint.patch (upstream 474538b8dd1c)
    Original reject: xhci.h hunk - upstream anchors the new
    xhci_process_cancelled_tds() declaration after xhci_stop_endpoint_sync(),
    which does not exist in the 6.1 vendor tree. Declaration added after
    count_trbs() instead. ring.c/xhci.c parts applied unchanged.
  007-xhci-stop-endpoint-error-generic.patch  (upstream e21ebe51af68)
    Original reject: its context is the code added by 005, so it only applies
    after 005. Applied verbatim in that order.
  012-xhci-ep-context-cycle-bit.patch         (upstream 6328bdc988d2)
    Original reject: ring.c hunk #2 - vendor struct xhci_td uses ->last_trb
    (upstream renamed it ->end_trb) so context mismatched. Hand-ported with
    last_trb: new_cycle now initialized from td->last_trb cycle bit, loop
    toggle condition changed from cycle_found to td_last_trb_found, variable
    renamed to hw_dequeue_found. Logic identical to upstream.

DROPPED - ALREADY PRESENT IN SUBSTANCE:
  003-phy-naneng-combphy-reset.patch (upstream fbcbffbac994)
    The vendor combphy driver already obtains resets individually by name
    (devm_reset_control_get_optional(dev, "combphy") for the phy reset and
    "combphy-apb" for the apb reset) and only asserts/deasserts phy_rst in
    rockchip_combphy_init()/exit(). The upstream commit message states its
    fix is "what the vendor kernel does" - this vendor driver is the
    reference implementation of the fix. Nothing to port.
  009-phy-naneng-combphy-old-dt.patch (upstream 3126ea9be66b)
    Follow-up to 003 adding a fallback for mainline DTs without reset-names.
    The vendor driver never used devm_reset_control_array_get_exclusive()
    and the vendor DTS (rk3588s.dtsi) always specifies
    reset-names = "combphy-apb", "combphy". Not applicable; dropped.

DROPPED - NOT APPLICABLE (vendor code diverged; NOT already applied):
  013-xhci-erdp-update.patch (upstream e30e9ad9ed66)
    Contrary to the initial triage ("reversed/previously applied"), a forced
    dry-run shows both hunks FAILED. The vendor tree carries the pre-rework
    6.1 event handling: xhci_update_erst_dequeue() still takes the cached
    event_ring_deq pointer argument and inc_deq() is called conditionally at
    the end of xhci_handle_event(). Commit 013 fixes a bug introduced by
    dc0ffbea5729 ("xhci: update event ring dequeue pointer on purpose") and
    depends on the event-ring rework commits 3321f84bfae0 and d1830364e963,
    none of which exist in this tree. Faithfully preserving its intent would
    require backporting the whole prerequisite rework series, which is out of
    scope and risky; the pre-rework ERDP update path does not have the bug
    shape 013 addresses. Dropped rather than force-ported.

Verification
============
On a pristine checkout, applying 001 002 004 008 010 011 014 015 900 with
patch -p1 yields zero rejects (offsets and the pre-existing fuzz noted above
only). 900 was generated with git diff from exactly this cumulative state.
