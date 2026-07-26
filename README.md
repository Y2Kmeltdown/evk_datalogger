## Eventide module

This repository is an installable [Eventide](https://github.com/Y2Kmeltdown/eventide)
module — `eventide-module.json` at the repo root advertises **two** supervisor
services that install together from the dashboard's MODULES tab:

| Service | What it does |
| ------- | ------------ |
| `event_based_camera` | Runs `evk_datalogger` — records segmented raw event data into `<recordings_dir>/evk/` and publishes decoded events/triggers to Unix sockets. |
| `evk_mjpeg_server` | Runs `viewfinder` — consumes the events socket and serves an MJPEG live stream on port `8081` (nginx proxies it at `/stream/evk/`). |

Install: open the dashboard → **MODULES** → paste this repo's URL → INSTALL.
The installer installs the apt build dependencies, creates a module venv and
installs `requirements.txt` into it, installs the neuromorphic-drivers udev
rules, builds with `cargo build --release`, copies the binaries to
`/usr/local/eventide/code/`, and registers both services with supervisord.

## Installation (standalone)
First you need to install rust follow instructions on [rust website](https://rust-lang.org/tools/install/).

Then you will need to install neuromorphic drivers python package follow instructions on the [neuromorphic-drivers github](https://github.com/neuromorphicsystems/neuromorphic-drivers)

Next install flask
```
pip install flask
```

Finally you can build the project by running this command in the root directory of the project
```
cargo run --release
```


## Usage

Run the following command from project root directory to start the datalogger

```
target/release/evk_datalogger --output-dir /path/to/data/recordings/
```

Then you can run the dashboard through this command from project root directory
```
python dashboard.py --recordings-dir /path/to/data/recordings/ \
--viewfinder-bin target/release/viewfinder \
--replay-bin target/release/replay
```


