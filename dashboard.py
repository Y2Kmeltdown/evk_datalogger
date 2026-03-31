"""
EVK4 Dashboard
==============
A minimal web frontend for the EVK4 pipeline.

  - Toggle viewfinder between the live datalogger socket and a replay socket
  - Browse recordings directory with download and playback per file
  - Embedded MJPEG viewer

Usage:
    pip install flask
    python dashboard.py --recordings-dir /tmp/evk4_raw \
                        --mjpeg-url http://localhost:8080 \
                        --replay-bin ./target/release/replay

Then open http://localhost:5000
"""

import argparse
import os
import subprocess
import threading
from pathlib import Path

from flask import (
    Flask,
    Response,
    abort,
    jsonify,
    render_template_string,
    request,
    send_file,
)

app = Flask(__name__)

# ── State ─────────────────────────────────────────────────────────────────────

# Tracks the currently running replay subprocess so we can kill it before
# starting a new one.
replay_lock = threading.Lock()
replay_proc: subprocess.Popen | None = None

# ── Config (populated in main) ────────────────────────────────────────────────
cfg: dict = {}

# ── HTML ──────────────────────────────────────────────────────────────────────

HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>EVK4 Dashboard</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Mono:wght@300;400;500&family=Bebas+Neue&display=swap" rel="stylesheet">
<style>
  :root {
    --bg:       #0a0a0a;
    --surface:  #111111;
    --border:   #222222;
    --accent:   #00ff88;
    --accent2:  #ff3c5f;
    --dim:      #444444;
    --text:     #e8e8e8;
    --text-dim: #666666;
    --mono:     'DM Mono', monospace;
    --display:  'Bebas Neue', sans-serif;
    --radius:   2px;
  }

  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

  body {
    background: var(--bg);
    color: var(--text);
    font-family: var(--mono);
    font-size: 13px;
    min-height: 100vh;
    display: grid;
    grid-template-rows: auto 1fr;
  }

  /* ── Header ── */
  header {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0 24px;
    height: 52px;
    border-bottom: 1px solid var(--border);
    background: var(--surface);
  }

  .logo {
    font-family: var(--display);
    font-size: 22px;
    letter-spacing: 0.08em;
    color: var(--accent);
  }

  .logo span { color: var(--text-dim); }

  .status-dot {
    width: 7px; height: 7px;
    border-radius: 50%;
    background: var(--accent);
    box-shadow: 0 0 8px var(--accent);
    animation: pulse 2s ease-in-out infinite;
    display: inline-block;
    margin-right: 8px;
  }

  @keyframes pulse {
    0%, 100% { opacity: 1; }
    50%       { opacity: 0.3; }
  }

  /* ── Layout ── */
  .workspace {
    display: grid;
    grid-template-columns: 340px 1fr;
    height: calc(100vh - 52px);
    overflow: hidden;
  }

  /* ── Sidebar ── */
  .sidebar {
    border-right: 1px solid var(--border);
    display: flex;
    flex-direction: column;
    overflow: hidden;
    background: var(--surface);
  }

  .sidebar-section {
    border-bottom: 1px solid var(--border);
    padding: 16px;
  }

  .section-label {
    font-size: 9px;
    letter-spacing: 0.2em;
    text-transform: uppercase;
    color: var(--text-dim);
    margin-bottom: 12px;
  }

  /* ── Source toggle ── */
  .toggle-row {
    display: flex;
    align-items: center;
    gap: 10px;
  }

  .toggle-label {
    font-size: 11px;
    color: var(--text-dim);
    width: 60px;
    text-align: right;
  }

  .toggle-label.active { color: var(--accent); }
  .toggle-label-right { text-align: left; }

  .toggle-track {
    position: relative;
    width: 44px; height: 22px;
    background: var(--border);
    border-radius: 11px;
    cursor: pointer;
    transition: background 0.2s;
    border: 1px solid var(--dim);
    flex-shrink: 0;
  }

  .toggle-track.replay { background: #1a1a2e; border-color: #ff3c5f44; }

  .toggle-thumb {
    position: absolute;
    top: 3px; left: 3px;
    width: 14px; height: 14px;
    border-radius: 50%;
    background: var(--accent);
    transition: transform 0.2s, background 0.2s;
    box-shadow: 0 0 6px var(--accent);
  }

  .toggle-track.replay .toggle-thumb {
    transform: translateX(22px);
    background: var(--accent2);
    box-shadow: 0 0 6px var(--accent2);
  }

  .source-badge {
    display: inline-block;
    font-size: 9px;
    letter-spacing: 0.15em;
    padding: 3px 7px;
    border-radius: var(--radius);
    background: #00ff8822;
    color: var(--accent);
    border: 1px solid #00ff8833;
    margin-top: 10px;
    transition: all 0.3s;
  }

  .source-badge.replay {
    background: #ff3c5f22;
    color: var(--accent2);
    border-color: #ff3c5f33;
  }

  /* ── Recordings list ── */
  .recordings-wrap {
    flex: 1;
    overflow-y: auto;
    padding: 0;
  }

  .recordings-wrap::-webkit-scrollbar { width: 4px; }
  .recordings-wrap::-webkit-scrollbar-track { background: transparent; }
  .recordings-wrap::-webkit-scrollbar-thumb { background: var(--border); border-radius: 2px; }

  .recordings-header {
    padding: 16px 16px 8px;
    display: flex;
    align-items: center;
    justify-content: space-between;
    position: sticky;
    top: 0;
    background: var(--surface);
    border-bottom: 1px solid var(--border);
    z-index: 1;
  }

  .refresh-btn {
    background: none;
    border: 1px solid var(--border);
    color: var(--text-dim);
    padding: 3px 8px;
    font-family: var(--mono);
    font-size: 10px;
    cursor: pointer;
    border-radius: var(--radius);
    transition: all 0.15s;
    letter-spacing: 0.1em;
  }

  .refresh-btn:hover { border-color: var(--accent); color: var(--accent); }

  .recording-item {
    display: flex;
    align-items: center;
    padding: 10px 16px;
    border-bottom: 1px solid #1a1a1a;
    gap: 8px;
    transition: background 0.1s;
  }

  .recording-item:hover { background: #151515; }
  .recording-item.playing { background: #0d1f15; border-left: 2px solid var(--accent); }

  .rec-icon {
    font-size: 16px;
    flex-shrink: 0;
    opacity: 0.6;
  }

  .rec-info { flex: 1; min-width: 0; }

  .rec-name {
    font-size: 11px;
    color: var(--text);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    letter-spacing: 0.03em;
  }

  .rec-meta {
    font-size: 10px;
    color: var(--text-dim);
    margin-top: 2px;
  }

  .rec-actions { display: flex; gap: 6px; flex-shrink: 0; }

  .btn-icon {
    background: none;
    border: 1px solid var(--border);
    color: var(--text-dim);
    width: 26px; height: 26px;
    display: flex; align-items: center; justify-content: center;
    cursor: pointer;
    border-radius: var(--radius);
    font-size: 12px;
    transition: all 0.15s;
    flex-shrink: 0;
  }

  .btn-icon:hover { border-color: var(--accent); color: var(--accent); background: #00ff8811; }
  .btn-icon.play:hover { border-color: var(--accent2); color: var(--accent2); background: #ff3c5f11; }
  .btn-icon.active-play { border-color: var(--accent2); color: var(--accent2); background: #ff3c5f18; }

  .empty-state {
    padding: 32px 16px;
    text-align: center;
    color: var(--text-dim);
    font-size: 11px;
    line-height: 1.8;
  }

  /* ── Main viewer ── */
  .viewer {
    background: #050505;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    position: relative;
    overflow: hidden;
  }

  .viewer-frame {
    width: 100%;
    height: 100%;
    border: none;
    display: block;
  }

  .viewer-overlay {
    position: absolute;
    inset: 0;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    pointer-events: none;
  }

  .viewer-info {
    position: absolute;
    top: 12px; left: 12px;
    display: flex; gap: 8px;
    pointer-events: none;
  }

  .pill {
    font-size: 9px;
    letter-spacing: 0.15em;
    text-transform: uppercase;
    padding: 3px 8px;
    border-radius: 10px;
    background: #000000aa;
    border: 1px solid var(--border);
    color: var(--text-dim);
    backdrop-filter: blur(4px);
  }

  .pill.live { border-color: #00ff8844; color: var(--accent); }
  .pill.replay-mode { border-color: #ff3c5f44; color: var(--accent2); }

  /* ── Toast ── */
  #toast {
    position: fixed;
    bottom: 20px; right: 20px;
    background: var(--surface);
    border: 1px solid var(--border);
    padding: 10px 16px;
    font-size: 11px;
    border-radius: var(--radius);
    color: var(--text);
    opacity: 0;
    transform: translateY(8px);
    transition: all 0.2s;
    pointer-events: none;
    z-index: 999;
    max-width: 280px;
  }

  #toast.show { opacity: 1; transform: translateY(0); }
  #toast.err  { border-color: var(--accent2); color: var(--accent2); }

  /* ── Scrollbar ── */
  * { scrollbar-width: thin; scrollbar-color: var(--border) transparent; }
</style>
</head>
<body>

<header>
  <div class="logo">EVK4 <span>/</span> DASHBOARD</div>
  <div style="display:flex;align-items:center;gap:16px;font-size:11px;color:var(--text-dim)">
    <span><span class="status-dot"></span>PIPELINE ACTIVE</span>
    <span id="hdr-source" style="color:var(--accent)">LIVE</span>
  </div>
</header>

<div class="workspace">

  <!-- ── Sidebar ── -->
  <aside class="sidebar">

    <!-- Source toggle -->
    <div class="sidebar-section">
      <div class="section-label">Viewfinder Source</div>
      <div class="toggle-row">
        <span class="toggle-label active" id="lbl-live">LIVE</span>
        <div class="toggle-track" id="source-toggle" onclick="toggleSource()">
          <div class="toggle-thumb"></div>
        </div>
        <span class="toggle-label toggle-label-right" id="lbl-replay">REPLAY</span>
      </div>
      <div class="source-badge" id="source-badge">● DATALOGGER SOCKET</div>
    </div>

    <!-- Recordings -->
    <div class="recordings-header">
      <span class="section-label" style="margin:0">Recordings</span>
      <button class="refresh-btn" onclick="loadRecordings()">↻ REFRESH</button>
    </div>
    <div class="recordings-wrap" id="recordings-list">
      <div class="empty-state">Loading recordings…</div>
    </div>

  </aside>

  <!-- ── Viewer ── -->
  <main class="viewer">
    <iframe
      id="mjpeg-frame"
      class="viewer-frame"
      src="{{ mjpeg_url }}"
      title="EVK4 MJPEG Stream"
      allowfullscreen
    ></iframe>

    <div class="viewer-info">
      <span class="pill live" id="mode-pill">LIVE</span>
    </div>
  </main>

</div>

<div id="toast"></div>

<script>
const MJPEG_LIVE   = {{ mjpeg_url|tojson }};
const MJPEG_REPLAY = {{ mjpeg_replay_url|tojson }};

let isReplay     = false;
let playingFile  = null;

// ── Source toggle ─────────────────────────────────────────────────────────────
function toggleSource() {
  isReplay = !isReplay;

  const track  = document.getElementById('source-toggle');
  const badge  = document.getElementById('source-badge');
  const pill   = document.getElementById('mode-pill');
  const hdr    = document.getElementById('hdr-source');
  const frame  = document.getElementById('mjpeg-frame');
  const lblL   = document.getElementById('lbl-live');
  const lblR   = document.getElementById('lbl-replay');

  if (isReplay) {
    track.classList.add('replay');
    badge.classList.add('replay');
    badge.textContent  = '● REPLAY SOCKET';
    pill.className     = 'pill replay-mode';
    pill.textContent   = 'REPLAY';
    hdr.style.color    = 'var(--accent2)';
    hdr.textContent    = 'REPLAY';
    lblL.classList.remove('active');
    lblR.classList.add('active');
    lblR.style.color   = 'var(--accent2)';
    frame.src          = MJPEG_REPLAY;
  } else {
    track.classList.remove('replay');
    badge.classList.remove('replay');
    badge.textContent  = '● DATALOGGER SOCKET';
    pill.className     = 'pill live';
    pill.textContent   = 'LIVE';
    hdr.style.color    = 'var(--accent)';
    hdr.textContent    = 'LIVE';
    lblL.classList.add('active');
    lblL.style.color   = '';
    lblR.classList.remove('active');
    lblR.style.color   = '';
    frame.src          = MJPEG_LIVE;
  }
}

// ── Recordings ────────────────────────────────────────────────────────────────
function fmtSize(bytes) {
  if (bytes < 1024)       return bytes + ' B';
  if (bytes < 1048576)    return (bytes / 1024).toFixed(1) + ' KB';
  return (bytes / 1048576).toFixed(1) + ' MB';
}

function fmtName(name) {
  // 20240315T123456Z.bin → 2024-03-15 12:34:56
  const m = name.match(/^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z\.bin$/);
  if (!m) return name;
  return `${m[1]}-${m[2]}-${m[3]}  ${m[4]}:${m[5]}:${m[6]} UTC`;
}

async function loadRecordings() {
  const list = document.getElementById('recordings-list');
  list.innerHTML = '<div class="empty-state">Loading…</div>';
  try {
    const res  = await fetch('/api/recordings');
    const data = await res.json();
    if (!data.files || data.files.length === 0) {
      list.innerHTML = '<div class="empty-state">No recordings found.<br>Start the datalogger to begin recording.</div>';
      return;
    }
    list.innerHTML = data.files.map(f => `
      <div class="recording-item ${playingFile === f.name ? 'playing' : ''}" id="rec-${CSS.escape(f.name)}">
        <span class="rec-icon">◈</span>
        <div class="rec-info">
          <div class="rec-name" title="${f.name}">${fmtName(f.name)}</div>
          <div class="rec-meta">${fmtSize(f.size)}</div>
        </div>
        <div class="rec-actions">
          <button class="btn-icon play ${playingFile === f.name ? 'active-play' : ''}"
                  title="Play back"
                  onclick="startReplay('${f.name}', this)">▶</button>
          <button class="btn-icon"
                  title="Download"
                  onclick="downloadFile('${f.name}')">↓</button>
        </div>
      </div>
    `).join('');
  } catch(e) {
    list.innerHTML = `<div class="empty-state">Error loading recordings.<br>${e}</div>`;
  }
}

async function startReplay(filename, btn) {
  // If this file is already playing, stop it
  if (playingFile === filename) {
    await fetch('/api/replay/stop', { method: 'POST' });
    playingFile = null;
    loadRecordings();
    toast('Replay stopped.');
    return;
  }

  try {
    const res  = await fetch('/api/replay/start', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ filename })
    });
    const data = await res.json();
    if (!res.ok) { toast(data.error || 'Failed to start replay', true); return; }
    playingFile = filename;
    loadRecordings();

    // Auto-switch viewfinder to replay socket
    if (!isReplay) toggleSource();
    toast(`Playing: ${fmtName(filename)}`);
  } catch(e) {
    toast('Error starting replay: ' + e, true);
  }
}

function downloadFile(filename) {
  window.location.href = `/api/recordings/${encodeURIComponent(filename)}/download`;
}

// ── Toast ─────────────────────────────────────────────────────────────────────
let toastTimer = null;
function toast(msg, isErr = false) {
  const el = document.getElementById('toast');
  el.textContent = msg;
  el.className   = 'show' + (isErr ? ' err' : '');
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => el.className = '', 3000);
}

// ── Init ──────────────────────────────────────────────────────────────────────
loadRecordings();
setInterval(loadRecordings, 10000); // auto-refresh every 10s
</script>
</body>
</html>
"""

# ── API routes ────────────────────────────────────────────────────────────────

@app.route("/")
def index():
    return render_template_string(
        HTML,
        mjpeg_url=cfg["mjpeg_url"],
        mjpeg_replay_url=cfg["mjpeg_replay_url"],
    )


@app.route("/api/recordings")
def list_recordings():
    recordings_dir = Path(cfg["recordings_dir"])
    if not recordings_dir.exists():
        return jsonify({"files": []})

    files = sorted(
        [
            {"name": f.name, "size": f.stat().st_size}
            for f in recordings_dir.glob("*.raw")
            if f.is_file()
        ],
        key=lambda x: x["name"]
        #reverse=True,  # newest first
    )
    return jsonify({"files": files})


@app.route("/api/recordings/<filename>/download")
def download_recording(filename):
    recordings_dir = Path(cfg["recordings_dir"])
    filepath = recordings_dir / filename
    # Prevent path traversal
    if not filepath.resolve().parent == recordings_dir.resolve():
        abort(400)
    if not filepath.exists():
        abort(404)
    return send_file(filepath, as_attachment=True, download_name=filename)


@app.route("/api/replay/start", methods=["POST"])
def start_replay():
    global replay_proc

    data = request.get_json()
    filename = data.get("filename", "")
    if not filename:
        return jsonify({"error": "No filename provided"}), 400

    filepath = Path(cfg["recordings_dir"]) / filename
    if not filepath.resolve().parent == Path(cfg["recordings_dir"]).resolve():
        return jsonify({"error": "Invalid filename"}), 400
    if not filepath.exists():
        return jsonify({"error": "File not found"}), 404

    with replay_lock:
        # Kill any existing replay process
        if replay_proc and replay_proc.poll() is None:
            replay_proc.terminate()
            try:
                replay_proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                replay_proc.kill()

        cmd = [
            cfg["replay_bin"],
            str(filepath),
            "--events-socket", cfg["replay_events_socket"],
        ]
        try:
            replay_proc = subprocess.Popen(
                cmd,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        except FileNotFoundError:
            return jsonify({"error": f"replay binary not found: {cfg['replay_bin']}"}), 500

    return jsonify({"ok": True, "filename": filename})


@app.route("/api/replay/stop", methods=["POST"])
def stop_replay():
    global replay_proc
    with replay_lock:
        if replay_proc and replay_proc.poll() is None:
            replay_proc.terminate()
            try:
                replay_proc.wait(timeout=3)
            except subprocess.TimeoutExpired:
                replay_proc.kill()
            replay_proc = None
    return jsonify({"ok": True})


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="EVK4 Dashboard")
    parser.add_argument("--recordings-dir",     default="/tmp/evk4_raw",              help="Directory where .bin recordings are stored")
    parser.add_argument("--mjpeg-url",          default="http://localhost:8080",       help="URL of the live MJPEG stream")
    parser.add_argument("--mjpeg-replay-url",   default="http://localhost:8081",       help="URL of the replay MJPEG stream")
    parser.add_argument("--replay-bin",         default="./target/release/replay",     help="Path to the compiled replay binary")
    parser.add_argument("--replay-events-socket", default="/tmp/evk4_replay_events.sock", help="Unix socket path the replay binary publishes events to")
    parser.add_argument("--host",               default="0.0.0.0")
    parser.add_argument("--port",               type=int, default=5000)
    args = parser.parse_args()

    cfg.update(vars(args))
    cfg["recordings_dir"] = args.recordings_dir

    print(f"[dashboard] Recordings:    {args.recordings_dir}")
    print(f"[dashboard] MJPEG live:    {args.mjpeg_url}")
    print(f"[dashboard] MJPEG replay:  {args.mjpeg_replay_url}")
    print(f"[dashboard] Replay bin:    {args.replay_bin}")
    print(f"[dashboard] Serving at:    http://{args.host}:{args.port}")

    app.run(host=args.host, port=args.port, debug=False)


if __name__ == "__main__":
    main()