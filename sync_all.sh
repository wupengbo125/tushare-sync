#!/bin/bash

python sync_stock_basic.py
# python sync_daily.py
python sync_daily.py range 20250920 20250924

python sync_stock_hot_rank.py
python sync_daily_qfq.py