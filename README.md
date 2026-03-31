## Installation
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


