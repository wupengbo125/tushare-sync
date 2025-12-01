#!/bin/bash
# Load project environment variables
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ -f "$SCRIPT_DIR/.env" ]; then
    . "$SCRIPT_DIR/.env"
fi

cd "$SCRIPT_DIR"
python stock_basic.py

#python daily.py range
#python daily_qfq.py workers 2
#python check_daily_qfq.py