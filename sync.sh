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
python apply_table.py daily_qfq_new daily_qfq
python sync_ths_concept_list.py
python sync_ths_concepts_adata.py workers 10

for i in 1 2 3 4 5 6 7; do
python ths-concept-failed-concepts.py
python sync_ths_concepts_ak.py from-file failed_concepts.txt
done
