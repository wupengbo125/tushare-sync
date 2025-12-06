#!/bin/bash
# Load project environment variables
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ -f "$SCRIPT_DIR/.env" ]; then
    . "$SCRIPT_DIR/.env"
fi

cd "$SCRIPT_DIR"
python stock_basic.py
python filter_stock_basic.py #过滤掉688，920，ST

python daily.py range
python daily_qfq.py workers 8
python apply_qfq.py

