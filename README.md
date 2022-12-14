# Crash Pipeline PoC

Caution: This is a work in progress. Take anything you see here with a hearty grain of salt.

## Setup

Install with `venv`:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## Running

Execute the pipeline with `python crash_pipeline.py`.
By itself, that won't actually _do_ anything, this uses
flags to indicate stages to execute:

- `--refresh` to refresh the crash data with new pings
- `--download` to download the symbol files locally
- `--insert` to insert the symbols data into BQ
- `--symbolicate` to symbolicate the crash pings w/ symbols data

e.g. `python crash_pipeline.py --download --insert`

## TODO:
- Symbolication should return a format that can be used by signature generation
- Try running sig generation in micropython UDF (or convert to js)
