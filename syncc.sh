#!/bin/bash
# Load project environment variables
export http_proxy=""
export https_proxy=""
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ -f "$SCRIPT_DIR/.env" ]; then
    . "$SCRIPT_DIR/.env"
fi


# 同步概念列表
cd "$SCRIPT_DIR"
python sync_em_concept_list.py

# 同步概念日线数据
python sync_em_concepts_daily_adata.py workers 10
# 同步失败的概念日线数据
for i in 1 2 3 ; do
python sync_em_concepts_daily_adata.py from-file failed-em-concepts-daily.txt
python sync_em_concepts_daily_ak.py from-file failed-em-concepts-daily.txt
done


# 同步股票的概念（周五）
if [ "$(date +%u)" = "6" ]; then
python sync_em_stock_concept_adata.py
fi

# python dividend_rank.py --workers 8 --allow-qfq-fallback
