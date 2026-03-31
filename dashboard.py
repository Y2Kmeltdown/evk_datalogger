"""
EVK4 Dashboard
==============
Controls the live and replay viewfinder processes, lists recordings,
and serves the MJPEG viewer.

Usage:
    pip install flask
    python dashboard.py \
        --recordings-dir /tmp/evk4_raw \
        --viewfinder-bin ./target/release/viewfinder \
        --replay-bin     ./target/release/replay

Then open http://localhost:5000
"""

import argparse
import os
import subprocess
import threading
from pathlib import Path

from flask import (
    Flask,
    abort,
    jsonify,
    render_template_string,
    request,
    send_file,
)

app = Flask(__name__)

# ── Global process state ──────────────────────────────────────────────────────

proc_lock = threading.Lock()

# Two managed viewfinder processes
viewfinders: dict[str, subprocess.Popen | None] = {"live": None, "replay": None}

# One managed replay process
replay_proc: subprocess.Popen | None = None

# Current viewfinder configs (persisted so the UI can reflect them)
vf_configs: dict[str, dict] = {
    "live":   {"fps": 50, "quality": 80, "width": 1280, "height": 720},
    "replay": {"fps": 50, "quality": 80, "width": 1280, "height": 720},
}

# ── Config (populated in main) ────────────────────────────────────────────────
cfg: dict = {}

# ── Process helpers ───────────────────────────────────────────────────────────

def kill_proc(proc: subprocess.Popen | None) -> None:
    if proc and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()


def start_viewfinder(mode: str, params: dict) -> tuple[bool, str]:
    """Start or restart a viewfinder process for `mode` ('live' or 'replay')."""
    bind_port   = cfg["live_port"]   if mode == "live" else cfg["replay_port"]
    events_sock = cfg["live_events_socket"] if mode == "live" else cfg["replay_events_socket"]

    cmd = [
        cfg["viewfinder_bin"],
        "--bind",          f"0.0.0.0:{bind_port}",
        "--events-socket", events_sock,
        "--fps",           str(params["fps"]),
        "--quality",       str(params["quality"]),
        "--width",         str(params["width"]),
        "--height",        str(params["height"]),
    ]

    try:
        proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return proc, None
    except FileNotFoundError:
        return None, f"viewfinder binary not found: {cfg['viewfinder_bin']}"


# ── API ───────────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template_string(HTML, cfg=cfg, vf_configs=vf_configs)


@app.route("/api/viewfinder/<mode>/start", methods=["POST"])
def api_vf_start(mode):
    if mode not in ("live", "replay"):
        abort(400)

    data = request.get_json() or {}
    params = {
        "fps":     int(data.get("fps",     vf_configs[mode]["fps"])),
        "quality": int(data.get("quality", vf_configs[mode]["quality"])),
        "width":   int(data.get("width",   vf_configs[mode]["width"])),
        "height":  int(data.get("height",  vf_configs[mode]["height"])),
    }

    with proc_lock:
        kill_proc(viewfinders[mode])
        proc, err = start_viewfinder(mode, params)
        if err:
            return jsonify({"error": err}), 500
        viewfinders[mode] = proc
        vf_configs[mode] = params

    return jsonify({"ok": True, "params": params})


@app.route("/api/viewfinder/<mode>/stop", methods=["POST"])
def api_vf_stop(mode):
    if mode not in ("live", "replay"):
        abort(400)
    with proc_lock:
        kill_proc(viewfinders[mode])
        viewfinders[mode] = None
    return jsonify({"ok": True})


@app.route("/api/viewfinder/<mode>/status")
def api_vf_status(mode):
    if mode not in ("live", "replay"):
        abort(400)
    with proc_lock:
        proc = viewfinders[mode]
        running = proc is not None and proc.poll() is None
    return jsonify({"running": running, "params": vf_configs[mode]})


@app.route("/api/recordings")
def list_recordings():
    recordings_dir = Path(cfg["recordings_dir"])
    if not recordings_dir.exists():
        return jsonify({"files": []})
    files = sorted(
        [{"name": f.name, "size": f.stat().st_size}
         for f in recordings_dir.glob("*.raw") if f.is_file()],
        key=lambda x: x["name"],
        reverse=True,
    )
    return jsonify({"files": files})


@app.route("/api/recordings/<filename>/download")
def download_recording(filename):
    recordings_dir = Path(cfg["recordings_dir"])
    filepath = (recordings_dir / filename).resolve()
    if filepath.parent != recordings_dir.resolve():
        abort(400)
    if not filepath.exists():
        abort(404)
    return send_file(filepath, as_attachment=True, download_name=filename)


@app.route("/api/replay/start", methods=["POST"])
def api_replay_start():
    global replay_proc
    data = request.get_json() or {}
    filename = data.get("filename", "")
    speed    = float(data.get("speed", 1.0))

    if not filename:
        return jsonify({"error": "No filename provided"}), 400

    filepath = (Path(cfg["recordings_dir"]) / filename).resolve()
    if filepath.parent != Path(cfg["recordings_dir"]).resolve():
        return jsonify({"error": "Invalid filename"}), 400
    if not filepath.exists():
        return jsonify({"error": "File not found"}), 404

    with proc_lock:
        kill_proc(replay_proc)
        cmd = [
            cfg["replay_bin"],
            str(filepath),
            "--events-socket", cfg["replay_events_socket"],
            "--speed",         str(speed),
        ]
        try:
            replay_proc = subprocess.Popen(
                cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
        except FileNotFoundError:
            return jsonify({"error": f"replay binary not found: {cfg['replay_bin']}"}), 500

    return jsonify({"ok": True, "filename": filename, "speed": speed})


@app.route("/api/replay/stop", methods=["POST"])
def api_replay_stop():
    global replay_proc
    with proc_lock:
        kill_proc(replay_proc)
        replay_proc = None
    return jsonify({"ok": True})


@app.route("/api/replay/status")
def api_replay_status():
    with proc_lock:
        running = replay_proc is not None and replay_proc.poll() is None
    return jsonify({"running": running})


# ── HTML ──────────────────────────────────────────────────────────────────────

HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width,initial-scale=1.0"/>
<title>EVK4 Dashboard</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Mono:ital,wght@0,300;0,400;0,500;1,300&family=Bebas+Neue&display=swap" rel="stylesheet">
<style>
:root {
  --bg:        #080808;
  --surface:   #0f0f0f;
  --surface2:  #141414;
  --border:    #1f1f1f;
  --border2:   #2a2a2a;
  --accent:    #00e676;
  --accent-dim:#00e67622;
  --warn:      #ff4757;
  --warn-dim:  #ff475722;
  --amber:     #ffb300;
  --text:      #e0e0e0;
  --text-dim:  #555;
  --text-mid:  #888;
  --mono:      'DM Mono', monospace;
  --display:   'Bebas Neue', sans-serif;
  --r:         2px;
}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
html,body{height:100%;overflow:hidden}
body{
  background:var(--bg);color:var(--text);
  font-family:var(--mono);font-size:12px;
  display:grid;grid-template-rows:48px 1fr;
}
header{
  display:flex;align-items:center;justify-content:space-between;
  padding:0 20px;border-bottom:1px solid var(--border);
  background:var(--surface);flex-shrink:0;
}
.logo{font-family:var(--display);font-size:20px;letter-spacing:.1em;color:var(--accent)}
.logo em{color:var(--text-dim);font-style:normal}
.workspace{display:grid;grid-template-columns:300px 1fr;overflow:hidden;min-height:0}
.sidebar{
  border-right:1px solid var(--border);display:flex;flex-direction:column;
  overflow:hidden;background:var(--surface);
}
.sidebar-scroll{flex:1;overflow-y:auto;min-height:0}
.sidebar-scroll::-webkit-scrollbar{width:3px}
.sidebar-scroll::-webkit-scrollbar-thumb{background:var(--border2)}
.panel{border-bottom:1px solid var(--border);padding:14px}
.panel-title{
  font-size:9px;letter-spacing:.2em;text-transform:uppercase;
  color:var(--text-dim);margin-bottom:12px;
}

/* Segment control */
.seg{display:flex;gap:0;border:1px solid var(--border2);border-radius:var(--r);overflow:hidden;margin-bottom:14px}
.seg-btn{
  flex:1;padding:6px 0;text-align:center;font-family:var(--mono);
  font-size:10px;letter-spacing:.1em;background:none;border:none;
  color:var(--text-dim);cursor:pointer;transition:all .15s;
}
.seg-btn.active-live  {background:var(--accent-dim);color:var(--accent)}
.seg-btn.active-replay{background:var(--warn-dim);color:var(--warn)}
.seg-btn:hover:not(.active-live):not(.active-replay){background:var(--surface2);color:var(--text)}

/* Viewfinder config panel */
.vf-panel{display:none}
.vf-panel.visible{display:block}

.vf-status-row{
  display:flex;align-items:center;justify-content:space-between;
  margin-bottom:12px;
}
.vf-status{display:flex;align-items:center;gap:6px;font-size:10px;letter-spacing:.1em}
.dot{width:6px;height:6px;border-radius:50%;background:var(--text-dim);flex-shrink:0}
.dot.on{background:var(--accent);box-shadow:0 0 6px var(--accent);animation:pulse 2s infinite}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.3}}

.form-row{display:flex;gap:8px;margin-bottom:8px}
.form-col{flex:1;min-width:0}
.form-label{
  font-size:9px;letter-spacing:.12em;text-transform:uppercase;
  color:var(--text-dim);margin-bottom:4px;display:block;
}
.form-input{
  width:100%;background:var(--bg);border:1px solid var(--border2);
  color:var(--text);font-family:var(--mono);font-size:11px;
  padding:5px 7px;border-radius:var(--r);outline:none;transition:border-color .15s;
}
.form-input:focus{border-color:var(--accent)}
.form-input[type=range]{padding:0;height:20px;accent-color:var(--accent);cursor:pointer}

.apply-btn{
  width:100%;margin-top:4px;padding:7px 0;
  font-family:var(--mono);font-size:10px;letter-spacing:.12em;
  border-radius:var(--r);cursor:pointer;
  border:1px solid var(--accent);color:var(--accent);
  background:var(--accent-dim);transition:all .15s;
}
.apply-btn:hover{background:#00e67633}
.apply-btn:active{background:#00e67644}

/* Recordings */
.rec-header{
  display:flex;align-items:center;justify-content:space-between;
  padding:12px 14px 8px;position:sticky;top:0;
  background:var(--surface);z-index:1;border-bottom:1px solid var(--border);
}
.rec-title{font-size:9px;letter-spacing:.2em;text-transform:uppercase;color:var(--text-dim)}
.btn-sm{
  background:none;border:1px solid var(--border2);color:var(--text-dim);
  padding:2px 7px;font-family:var(--mono);font-size:9px;letter-spacing:.1em;
  cursor:pointer;border-radius:var(--r);transition:all .15s;
}
.btn-sm:hover{border-color:var(--accent);color:var(--accent)}
.speed-row{
  display:flex;align-items:center;gap:6px;
  padding:8px 14px;border-bottom:1px solid var(--border);
}
.speed-row label{font-size:9px;letter-spacing:.12em;text-transform:uppercase;color:var(--text-dim);white-space:nowrap}
.speed-row input{flex:1;accent-color:var(--amber)}
.speed-val{font-size:10px;color:var(--amber);white-space:nowrap;min-width:36px;text-align:right}
.rec-list{padding:4px 0}
.rec-item{
  display:flex;align-items:center;gap:8px;
  padding:8px 14px;border-bottom:1px solid #111;transition:background .1s;
}
.rec-item:hover{background:var(--surface2)}
.rec-item.playing{background:#0d1a10;border-left:2px solid var(--accent)}
.rec-icon{color:var(--text-dim);font-size:14px;flex-shrink:0}
.rec-info{flex:1;min-width:0}
.rec-name{font-size:11px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.rec-meta{font-size:10px;color:var(--text-dim);margin-top:1px}
.rec-btns{display:flex;gap:4px;flex-shrink:0}
.icon-btn{
  width:24px;height:24px;display:flex;align-items:center;justify-content:center;
  background:none;border:1px solid var(--border2);color:var(--text-dim);
  cursor:pointer;border-radius:var(--r);font-size:11px;transition:all .15s;
}
.icon-btn:hover{border-color:var(--accent);color:var(--accent);background:var(--accent-dim)}
.icon-btn.play-btn:hover{border-color:var(--warn);color:var(--warn);background:var(--warn-dim)}
.icon-btn.playing-btn{border-color:var(--warn);color:var(--warn);background:var(--warn-dim)}
.empty{padding:24px 14px;text-align:center;color:var(--text-dim);line-height:1.8;font-size:11px}

/* Viewer */
.viewer{position:relative;background:#030303;display:flex;flex-direction:column;overflow:hidden}
.viewer iframe{flex:1;border:none;width:100%;display:block;min-height:0}
.viewer-bar{
  display:flex;align-items:center;gap:10px;
  padding:6px 12px;background:var(--surface);
  border-top:1px solid var(--border);flex-shrink:0;
}
.pill{
  font-size:9px;letter-spacing:.15em;text-transform:uppercase;
  padding:2px 8px;border-radius:10px;border:1px solid var(--border2);color:var(--text-dim);
}
.pill.live  {border-color:#00e67644;color:var(--accent)}
.pill.replay{border-color:#ff475744;color:var(--warn)}

/* Toast */
#toast{
  position:fixed;bottom:16px;right:16px;
  background:var(--surface2);border:1px solid var(--border2);
  padding:8px 14px;font-size:11px;border-radius:var(--r);
  opacity:0;transform:translateY(6px);transition:all .2s;
  pointer-events:none;z-index:999;max-width:260px;
}
#toast.show{opacity:1;transform:translateY(0)}
#toast.err{border-color:var(--warn);color:var(--warn)}
#toast.ok {border-color:var(--accent);color:var(--accent)}
</style>
</head>
<body>

<header>
  <div class="logo">EVK4 <em>/</em> DASHBOARD</div>
  <div style="font-size:10px;color:var(--text-dim);letter-spacing:.1em">
    VIEWING: <span id="hdr-mode-val" style="color:var(--accent)">LIVE</span>
  </div>
</header>

<div class="workspace">
<aside class="sidebar">
<div class="sidebar-scroll">

  <div class="panel">
    <div class="panel-title">Viewfinder Source</div>

    <!-- Segment toggle -->
    <div class="seg">
      <button class="seg-btn active-live"  id="seg-live"   onclick="setView('live')">LIVE</button>
      <button class="seg-btn"              id="seg-replay" onclick="setView('replay')">REPLAY</button>
    </div>

    <!-- Live settings — visible when live is selected -->
    <div class="vf-panel visible" id="panel-live">
      <div class="vf-status-row">
        <span style="font-size:9px;letter-spacing:.15em;color:var(--text-dim)">LIVE VIEWFINDER</span>
        <div class="vf-status">
          <div class="dot" id="dot-live"></div>
          <span id="status-live" style="color:var(--text-dim)">STOPPED</span>
        </div>
      </div>
      <div class="form-row">
        <div class="form-col">
          <label class="form-label">FPS</label>
          <input class="form-input" type="number" id="live-fps" value="50" min="1" max="200">
        </div>
        <div class="form-col">
          <label class="form-label">Width</label>
          <input class="form-input" type="number" id="live-width" value="1280" min="1">
        </div>
        <div class="form-col">
          <label class="form-label">Height</label>
          <input class="form-input" type="number" id="live-height" value="720" min="1">
        </div>
      </div>
      <div class="form-row">
        <div class="form-col">
          <label class="form-label">JPEG Quality — <span id="live-q-val">80</span></label>
          <input class="form-input" type="range" id="live-quality" min="1" max="100" value="80"
                 oninput="document.getElementById('live-q-val').textContent=this.value">
        </div>
      </div>
      <div style="display:flex;gap:6px;margin-top:4px">
        <button class="apply-btn" style="flex:2" onclick="applyVF('live')">↺ APPLY &amp; RESTART</button>
        <button class="apply-btn" style="flex:1;border-color:var(--accent);color:var(--accent)" onclick="startVF('live')">▶</button>
        <button class="apply-btn" style="flex:1;border-color:var(--warn);color:var(--warn);background:var(--warn-dim)" onclick="stopVF('live')">■</button>
      </div>
    </div>

    <!-- Replay settings — hidden until replay is selected -->
    <div class="vf-panel" id="panel-replay">
      <div class="vf-status-row">
        <span style="font-size:9px;letter-spacing:.15em;color:var(--text-dim)">REPLAY VIEWFINDER</span>
        <div class="vf-status">
          <div class="dot" id="dot-replay"></div>
          <span id="status-replay" style="color:var(--text-dim)">STOPPED</span>
        </div>
      </div>
      <div class="form-row">
        <div class="form-col">
          <label class="form-label">FPS</label>
          <input class="form-input" type="number" id="replay-fps" value="50" min="1" max="200">
        </div>
        <div class="form-col">
          <label class="form-label">Width</label>
          <input class="form-input" type="number" id="replay-width" value="1280" min="1">
        </div>
        <div class="form-col">
          <label class="form-label">Height</label>
          <input class="form-input" type="number" id="replay-height" value="720" min="1">
        </div>
      </div>
      <div class="form-row">
        <div class="form-col">
          <label class="form-label">JPEG Quality — <span id="replay-q-val">80</span></label>
          <input class="form-input" type="range" id="replay-quality" min="1" max="100" value="80"
                 oninput="document.getElementById('replay-q-val').textContent=this.value">
        </div>
      </div>

    </div>
  </div>

  <!-- Recordings -->
  <div class="rec-header">
    <span class="rec-title">Recordings</span>
    <button class="btn-sm" onclick="loadRecordings()">↻ REFRESH</button>
  </div>
  <div class="speed-row">
    <label>REPLAY SPEED</label>
    <input type="range" id="speed-slider" min="0.1" max="4" step="0.1" value="1.0"
           oninput="document.getElementById('speed-val').textContent=parseFloat(this.value).toFixed(1)+'×'">
    <span class="speed-val" id="speed-val">1.0×</span>
  </div>
  <div id="recordings-list">
    <div class="empty">Loading recordings…</div>
  </div>

</div>
</aside>

<main class="viewer">
  <iframe id="vf-frame" src="about:blank" title="EVK4 Stream"></iframe>
  <div class="viewer-bar">
    <span class="pill live" id="view-pill">LIVE</span>
    <span style="color:var(--text-dim);font-size:10px" id="view-url"></span>
  </div>
</main>
</div>

<div id="toast"></div>

<script>
const LIVE_URL   = 'http://{{ cfg.live_host }}:{{ cfg.live_port }}';
const REPLAY_URL = 'http://{{ cfg.live_host }}:{{ cfg.replay_port }}';

let currentView = 'live';
let playingFile = null;

// ── View toggle ───────────────────────────────────────────────────────────────
function setView(mode) {
  currentView = mode;
  const isLive = mode === 'live';

  // Swap iframe src
  document.getElementById('vf-frame').src = isLive ? LIVE_URL : REPLAY_URL;

  // Pill + header
  const pill = document.getElementById('view-pill');
  pill.className   = 'pill ' + mode;
  pill.textContent = mode.toUpperCase();
  document.getElementById('hdr-mode-val').textContent  = mode.toUpperCase();
  document.getElementById('hdr-mode-val').style.color  = isLive ? 'var(--accent)' : 'var(--warn)';
  document.getElementById('view-url').textContent      = isLive ? LIVE_URL : REPLAY_URL;

  // Segment buttons
  document.getElementById('seg-live').className   = 'seg-btn' + (isLive  ? ' active-live'   : '');
  document.getElementById('seg-replay').className = 'seg-btn' + (!isLive ? ' active-replay' : '');

  // Show only the relevant settings panel
  document.getElementById('panel-live').classList.toggle('visible',  isLive);
  document.getElementById('panel-replay').classList.toggle('visible', !isLive);
}

// ── Start / stop viewfinder ───────────────────────────────────────────────────
async function startVF(mode) {
  const params = {
    fps:     parseInt(document.getElementById(`${mode}-fps`).value),
    quality: parseInt(document.getElementById(`${mode}-quality`).value),
    width:   parseInt(document.getElementById(`${mode}-width`).value),
    height:  parseInt(document.getElementById(`${mode}-height`).value),
  };
  const res  = await fetch(`/api/viewfinder/${mode}/start`, {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify(params),
  });
  const data = await res.json();
  if (!res.ok) { toast(data.error || 'Failed to start viewfinder', 'err'); return; }
  toast(`${mode.toUpperCase()} viewfinder started`, 'ok');
  updateVFStatus(mode, true);
  if (mode === currentView) { const f = document.getElementById('vf-frame'); f.src = f.src; }
}

async function stopVF(mode) {
  await fetch(`/api/viewfinder/${mode}/stop`, { method: 'POST' });
  toast(`${mode.toUpperCase()} viewfinder stopped`);
  updateVFStatus(mode, false);
}

// ── Apply & restart viewfinder ────────────────────────────────────────────────
async function applyVF(mode) {
  const params = {
    fps:     parseInt(document.getElementById(`${mode}-fps`).value),
    quality: parseInt(document.getElementById(`${mode}-quality`).value),
    width:   parseInt(document.getElementById(`${mode}-width`).value),
    height:  parseInt(document.getElementById(`${mode}-height`).value),
  };
  const res  = await fetch(`/api/viewfinder/${mode}/start`, {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify(params),
  });
  const data = await res.json();
  if (!res.ok) { toast(data.error || 'Failed to restart viewfinder', 'err'); return; }

  toast(`${mode.toUpperCase()} viewfinder restarted`, 'ok');
  updateVFStatus(mode, true);

  // Reload iframe if this is the active view
  if (mode === currentView) {
    const frame = document.getElementById('vf-frame');
    frame.src = frame.src; // force reload
  }
}

function updateVFStatus(mode, running) {
  const dot    = document.getElementById(`dot-${mode}`);
  const status = document.getElementById(`status-${mode}`);
  dot.className      = 'dot' + (running ? ' on' : '');
  status.textContent = running ? 'RUNNING' : 'STOPPED';
  status.style.color = running ? 'var(--accent)' : 'var(--text-dim)';
}

async function pollVFStatus() {
  for (const mode of ['live', 'replay']) {
    try {
      const res  = await fetch(`/api/viewfinder/${mode}/status`);
      const data = await res.json();
      updateVFStatus(mode, data.running);
    } catch {}
  }
}

// ── Recordings ────────────────────────────────────────────────────────────────
function fmtSize(b) {
  if (b < 1024)    return b + ' B';
  if (b < 1048576) return (b/1024).toFixed(1) + ' KB';
  return (b/1048576).toFixed(1) + ' MB';
}
function fmtName(name) {
  const m = name.match(/^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z\.raw$/);
  if (!m) return name;
  return `${m[1]}-${m[2]}-${m[3]} ${m[4]}:${m[5]}:${m[6]} UTC`;
}

async function loadRecordings() {
  const list = document.getElementById('recordings-list');
  try {
    const res  = await fetch('/api/recordings');
    const data = await res.json();
    if (!data.files?.length) {
      list.innerHTML = '<div class="empty">No recordings found.</div>';
      return;
    }
    list.innerHTML = '<div class="rec-list">' + data.files.map(f => `
      <div class="rec-item ${playingFile===f.name?'playing':''}" id="ri-${CSS.escape(f.name)}">
        <span class="rec-icon">◈</span>
        <div class="rec-info">
          <div class="rec-name" title="${f.name}">${fmtName(f.name)}</div>
          <div class="rec-meta">${fmtSize(f.size)}</div>
        </div>
        <div class="rec-btns">
          <button class="icon-btn play-btn ${playingFile===f.name?'playing-btn':''}"
                  title="${playingFile===f.name?'Stop':'Play'}"
                  onclick="startReplay('${f.name}')">
            ${playingFile===f.name ? '■' : '▶'}
          </button>
          <button class="icon-btn" title="Download"
                  onclick="window.location.href='/api/recordings/${encodeURIComponent(f.name)}/download'">↓</button>
        </div>
      </div>
    `).join('') + '</div>';
  } catch(e) {
    list.innerHTML = `<div class="empty">Error: ${e}</div>`;
  }
}

async function startReplay(filename) {
  const speed = parseFloat(document.getElementById('speed-slider').value);

  if (playingFile === filename) {
    await fetch('/api/replay/stop', { method: 'POST' });
    await fetch('/api/viewfinder/replay/stop', { method: 'POST' });
    playingFile = null;
    updateVFStatus('replay', false);
    loadRecordings();
    toast('Replay stopped.');
    return;
  }

  // Start the replay viewfinder with current settings before launching the file
  const vfParams = {
    fps:     parseInt(document.getElementById('replay-fps').value),
    quality: parseInt(document.getElementById('replay-quality').value),
    width:   parseInt(document.getElementById('replay-width').value),
    height:  parseInt(document.getElementById('replay-height').value),
  };
  const vfRes = await fetch('/api/viewfinder/replay/start', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify(vfParams),
  });
  if (!vfRes.ok) {
    const vfData = await vfRes.json();
    toast(vfData.error || 'Failed to start replay viewfinder', 'err');
    return;
  }
  updateVFStatus('replay', true);

  // Then start the replay file
  const res  = await fetch('/api/replay/start', {
    method: 'POST',
    headers: {'Content-Type':'application/json'},
    body: JSON.stringify({ filename, speed }),
  });
  const data = await res.json();
  if (!res.ok) { toast(data.error || 'Failed to start replay', 'err'); return; }

  playingFile = filename;
  loadRecordings();
  if (currentView !== 'replay') setView('replay');
  toast(`Playing at ${speed}×: ${fmtName(filename)}`, 'ok');
}

// ── Toast ─────────────────────────────────────────────────────────────────────
let toastTimer;
function toast(msg, type='') {
  const el = document.getElementById('toast');
  el.textContent = msg;
  el.className = 'show' + (type ? ' '+type : '');
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => el.className = '', 3000);
}

// ── Init ──────────────────────────────────────────────────────────────────────
setView('live');
loadRecordings();
pollVFStatus();
setInterval(loadRecordings, 10000);
setInterval(pollVFStatus, 3000);
</script>
</body>
</html>
"""

# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="EVK4 Dashboard")
    parser.add_argument("--recordings-dir",        default="/tmp/evk4_raw")
    parser.add_argument("--viewfinder-bin",        default="./target/release/viewfinder")
    parser.add_argument("--replay-bin",            default="./target/release/replay")
    parser.add_argument("--live-events-socket",    default="/tmp/evk4_events.sock")
    parser.add_argument("--replay-events-socket",  default="/tmp/evk4_replay_events.sock")
    parser.add_argument("--live-port",             type=int, default=8080)
    parser.add_argument("--replay-port",           type=int, default=8081)
    parser.add_argument("--live-host",             default="localhost")
    parser.add_argument("--host",                  default="0.0.0.0")
    parser.add_argument("--port",                  type=int, default=5000)
    args = parser.parse_args()

    cfg.update(vars(args))

    print(f"[dashboard] Recordings dir:       {args.recordings_dir}")
    print(f"[dashboard] Viewfinder binary:    {args.viewfinder_bin}")
    print(f"[dashboard] Replay binary:        {args.replay_bin}")
    print(f"[dashboard] Live stream:          http://{args.live_host}:{args.live_port}")
    print(f"[dashboard] Replay stream:        http://{args.live_host}:{args.replay_port}")
    print(f"[dashboard] Serving at:           http://{args.host}:{args.port}")

    # Auto-start both viewfinders with default config
    for mode in ("live",):
        proc, err = start_viewfinder(mode, vf_configs[mode])
        if err:
            print(f"[dashboard] WARNING: could not auto-start {mode} viewfinder: {err}")
        else:
            viewfinders[mode] = proc
            print(f"[dashboard] Auto-started {mode} viewfinder.")

    app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()