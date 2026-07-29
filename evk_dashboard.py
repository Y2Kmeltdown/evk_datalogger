#!/usr/bin/env python3
"""Web dashboard for the EVK4 datalogger and viewfinder HTTP APIs.

Serves a single-page GUI and proxies both backend services, so the browser
only ever talks to this app (no CORS issues, works from any machine that can
reach this process):

    datalogger  (default http://localhost:8081) — recording, biases, rate
                                                limit, circular buffer
    viewfinder  (default http://localhost:8080) — stream settings, streaming
                                                toggle, MJPEG stream, snapshot

Usage:
    python evk_dashboard.py [--host 0.0.0.0] [--port 5000]
                            [--dlog http://localhost:8081] [--vf http://localhost:8080]

Then open http://localhost:5000/ in a browser.
"""

import argparse

import requests
from flask import Flask, Response, jsonify, request, stream_with_context

app = Flask(__name__)

DLOG_URL = "http://localhost:8081"
VF_URL = "http://localhost:8080"

# ── Single-page GUI ───────────────────────────────────────────────────────────

PAGE = """<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>EVK4 Control</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
  body{font-family:system-ui,sans-serif;background:#15181d;color:#dde;margin:0;padding:1rem 2rem}
  h1{font-size:1.3rem;margin:.5rem 0 1rem}
  h2{font-size:1rem;margin:0 0 .4rem;color:#9ab}
  .layout{display:grid;grid-template-columns:minmax(230px,300px) minmax(0,1fr) minmax(230px,300px);
          gap:1rem;align-items:start}
  .col{display:flex;flex-direction:column;gap:1rem}
  .card{background:#1e232b;border:1px solid #2c333d;border-radius:8px;padding:1rem}
  .statusbar{display:flex;gap:1.5rem;flex-wrap:wrap;align-items:center;
             font-family:monospace;font-size:.8rem;margin-bottom:1rem}
  @media (max-width:1100px){.layout{grid-template-columns:1fr}}
  label{display:block;font-size:.78rem;color:#9ab;margin-top:.5rem}
  input{width:100%;box-sizing:border-box;background:#12151a;border:1px solid #333b47;
        color:#dde;border-radius:4px;padding:.35rem}
  button{margin:.7rem .4rem 0 0;padding:.45rem .9rem;border:0;border-radius:5px;
         background:#3a6ea5;color:#fff;cursor:pointer}
  button.stop{background:#a53a3a}
  button:hover{filter:brightness(1.15)}
  button:disabled{opacity:.4;cursor:default}
  .dot{display:inline-block;width:.7em;height:.7em;border-radius:50%;background:#777;margin-right:.4em}
  .dot.on{background:#e44}.dot.off{background:#4a4}
  img.stream{width:100%;border-radius:6px;background:#000;margin-top:.4rem}
  a{color:#6aa5d8}
  #msg{font-size:.8rem;color:#8c8}
</style>
</head>
<body>
<h1>EVK4 Datalogger Control</h1>

<div class="card statusbar">
  <span><span id="recdot" class="dot"></span><span id="rectext">&hellip;</span></span>
  <span id="statfile">file: &mdash;</span>
  <span id="statrate"></span>
  <span id="statbuf"></span>
</div>

<div class="layout">
  <div class="col">

    <div class="card"><h2>Recording</h2>
      <div id="rechint" style="font-size:.8rem;color:#9ab">New recordings start with the circular buffer as pre-roll.</div>
      <button id="recstart" onclick="record(true)">Start recording</button>
      <button id="recstop" class="stop" onclick="record(false)">Stop</button>
    </div>

    <div class="card"><h2>Biases</h2>
      <div id="biases">loading&hellip;</div>
      <button onclick="applyBiases()">Apply biases</button>
    </div>

  </div>
  <div class="col">

    <div class="card"><h2>Live view</h2>
      <img class="stream" src="/vf/stream" alt="MJPEG stream (enable streaming at right)">
      <p style="margin:.4rem 0 0"><a href="/vf/snapshot" target="_blank">Open snapshot</a> &middot; <span id="msg"></span></p>
    </div>

  </div>
  <div class="col">

    <div class="card"><h2>Rate limit</h2>
      <label>events/second (0 = unlimited)</label>
      <input id="rl" type="number" min="0">
      <button onclick="applyRate()">Apply</button>
    </div>

    <div class="card"><h2>Circular buffer</h2>
      <label>max age (seconds, 0 = off)</label>
      <input id="bage" type="number" min="0">
      <label>max bytes (0 = off)</label>
      <input id="bbytes" type="number" min="0">
      <button onclick="applyBuffer()">Apply</button>
    </div>

    <div class="card"><h2>Viewfinder</h2>
      <label>output width</label><input id="vfw" type="number" min="1">
      <label>output height</label><input id="vfh" type="number" min="1">
      <label>JPEG quality (1&ndash;100)</label><input id="vfq" type="number" min="1" max="100">
      <button onclick="applyVf()">Apply</button>
      <button onclick="vfStreaming(true)">Stream on</button>
      <button class="stop" onclick="vfStreaming(false)">Stream off</button>
    </div>

  </div>
</div>
<script>
const BIAS_FIELDS = ["diff", "diff_on", "diff_off", "hpf", "refr", "pr", "fo", "inv"];

function msg(t, ok = true) {
  const el = document.getElementById("msg");
  el.textContent = t;
  el.style.color = ok ? "#8c8" : "#e88";
  setTimeout(() => { if (el.textContent === t) el.textContent = ""; }, 5000);
}

async function api(path, method, body) {
  const opt = { method: method || "GET" };
  if (body !== undefined) {
    opt.headers = { "Content-Type": "application/json" };
    opt.body = JSON.stringify(body);
  }
  const r = await fetch(path, opt);
  let data = null;
  try { data = await r.json(); } catch (e) { /* non-JSON body */ }
  if (!r.ok) throw new Error((data && data.error) || r.statusText);
  return data;
}

async function refresh() {
  try {
    const s = await api("/api/dlog/status");
    document.getElementById("recdot").className = "dot " + (s.recording ? "on" : "off");
    document.getElementById("rectext").textContent = s.recording ? "RECORDING" : "idle";
    const canToggle = s.recording_control !== false;
    document.getElementById("recstart").disabled = !canToggle;
    document.getElementById("recstop").disabled = !canToggle;
    if (!canToggle) {
      document.getElementById("rechint").textContent =
        "Always recording \u2014 API control disabled (restart with --record-toggle).";
    }
    document.getElementById("statfile").textContent = "file: " + (s.current_file || "\u2014");
    document.getElementById("statrate").textContent = "rate limit: " + s.rate_limit_events_per_second + " ev/s";
    document.getElementById("statbuf").textContent =
      "buffer: " + s.buffer.bytes + " B / " + s.buffer.chunks + " chunks" +
      " (caps " + s.buffer.max_age_secs + "s / " + s.buffer.max_bytes + " B)";
  } catch (e) {
    document.getElementById("statfile").textContent = "datalogger unreachable: " + e.message;
  }
}

async function record(on) {
  try {
    await api("/api/dlog/recording", "PUT", { recording: on });
    msg(on ? "recording started" : "recording stopped");
    refresh();
  } catch (e) { msg(e.message, false); }
}

async function loadBiases() {
  const box = document.getElementById("biases");
  try {
    const b = await api("/api/dlog/biases");
    box.innerHTML = "";
    for (const f of BIAS_FIELDS) {
      const l = document.createElement("label");
      l.textContent = f;
      const i = document.createElement("input");
      i.id = "bias_" + f; i.type = "number"; i.min = 0; i.max = 255;
      i.value = (b[f] !== undefined) ? b[f] : 0;
      l.appendChild(i);
      box.appendChild(l);
    }
  } catch (e) { box.textContent = "datalogger unreachable"; }
}

async function applyBiases() {
  const body = {};
  for (const f of BIAS_FIELDS) {
    const v = document.getElementById("bias_" + f).value;
    if (v !== "") body[f] = Number(v);
  }
  try { await api("/api/dlog/biases", "PUT", body); msg("biases applied"); }
  catch (e) { msg(e.message, false); }
}

async function loadRate() {
  try {
    const r = await api("/api/dlog/rate-limit");
    document.getElementById("rl").value = r.events_per_second;
  } catch (e) { /* shown in status card */ }
}

async function applyRate() {
  try {
    await api("/api/dlog/rate-limit", "PUT",
              { events_per_second: Number(document.getElementById("rl").value) });
    msg("rate limit applied");
  } catch (e) { msg(e.message, false); }
}

async function loadBuffer() {
  try {
    const b = await api("/api/dlog/buffer");
    document.getElementById("bage").value = b.max_age_secs;
    document.getElementById("bbytes").value = b.max_bytes;
  } catch (e) { /* shown in status card */ }
}

async function applyBuffer() {
  try {
    await api("/api/dlog/buffer", "PUT", {
      max_age_secs: Number(document.getElementById("bage").value),
      max_bytes: Number(document.getElementById("bbytes").value)
    });
    msg("buffer caps applied");
  } catch (e) { msg(e.message, false); }
}

async function loadVf() {
  try {
    const s = await api("/api/vf/settings");
    document.getElementById("vfw").value = s.out_width;
    document.getElementById("vfh").value = s.out_height;
    document.getElementById("vfq").value = s.quality;
  } catch (e) { /* viewfinder down */ }
}

async function applyVf() {
  try {
    await api("/api/vf/settings", "PUT", {
      out_width: Number(document.getElementById("vfw").value),
      out_height: Number(document.getElementById("vfh").value),
      quality: Number(document.getElementById("vfq").value)
    });
    msg("viewfinder settings applied");
  } catch (e) { msg(e.message, false); }
}

async function vfStreaming(on) {
  try {
    await api("/api/vf/streaming", "PUT", { streaming: on });
    msg("streaming " + (on ? "enabled" : "disabled"));
  } catch (e) { msg(e.message, false); }
}

refresh(); loadBiases(); loadRate(); loadBuffer(); loadVf();
setInterval(refresh, 2000);
</script>
</body>
</html>
"""

# ── Proxy endpoints ───────────────────────────────────────────────────────────


def proxy(base: str, path: str) -> Response:
    """Forward a JSON API request to one of the backend services."""
    try:
        r = requests.request(
            request.method,
            f"{base}/api/{path}",
            data=request.get_data(),
            headers={"Content-Type": "application/json"},
            timeout=5,
        )
    except requests.RequestException as e:
        return jsonify(error=f"{base} unreachable: {e}"), 502
    return Response(
        r.content,
        status=r.status_code,
        content_type=r.headers.get("Content-Type", "application/json"),
    )


@app.route("/")
def index() -> Response:
    return Response(PAGE, mimetype="text/html")


@app.route("/api/dlog/<path:p>", methods=["GET", "PUT"])
def dlog(p: str) -> Response:
    return proxy(DLOG_URL, p)


@app.route("/api/vf/<path:p>", methods=["GET", "PUT"])
def vf(p: str) -> Response:
    return proxy(VF_URL, p)


@app.route("/vf/snapshot")
def snapshot() -> Response:
    try:
        r = requests.get(f"{VF_URL}/snapshot", timeout=10)
    except requests.RequestException as e:
        return jsonify(error=f"viewfinder unreachable: {e}"), 502
    return Response(
        r.content,
        status=r.status_code,
        content_type=r.headers.get("Content-Type", "image/jpeg"),
    )


@app.route("/vf/stream")
def stream() -> Response:
    """Pass the viewfinder's MJPEG multipart stream through untouched."""
    try:
        r = requests.get(f"{VF_URL}/stream", stream=True, timeout=None)
    except requests.RequestException as e:
        return jsonify(error=f"viewfinder unreachable: {e}"), 502

    def passthrough():
        try:
            for chunk in r.iter_content(chunk_size=4096):
                yield chunk
        finally:
            r.close()

    return Response(
        stream_with_context(passthrough()),
        status=r.status_code,
        content_type=r.headers.get(
            "Content-Type", "multipart/x-mixed-replace; boundary=evk4frame"
        ),
    )


# ── Entry point ───────────────────────────────────────────────────────────────


def main() -> None:
    global DLOG_URL, VF_URL
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="0.0.0.0", help="bind address for this GUI")
    parser.add_argument("--port", type=int, default=5000, help="port for this GUI")
    parser.add_argument("--dlog", default=DLOG_URL, help="datalogger API base URL")
    parser.add_argument("--vf", default=VF_URL, help="viewfinder API base URL")
    args = parser.parse_args()

    DLOG_URL = args.dlog.rstrip("/")
    VF_URL = args.vf.rstrip("/")

    print(f"Dashboard:  http://{args.host}:{args.port}/")
    print(f"Datalogger: {DLOG_URL}")
    print(f"Viewfinder: {VF_URL}")
    app.run(host=args.host, port=args.port, threaded=True)


if __name__ == "__main__":
    main()
