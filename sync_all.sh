#!/bin/bash
# 加载项目环境变量
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$SCRIPT_DIR/.env" ]; then
    . "$SCRIPT_DIR/.env"
fi

cd "$SCRIPT_DIR"
python stock_basic.py

#python daily.py range
#python daily_qfq.py
#python check_daily_qfq.py
