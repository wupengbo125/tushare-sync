#!/bin/bash
# Load project environment variables
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ -f "$SCRIPT_DIR/.env" ]; then
    . "$SCRIPT_DIR/.env"
fi

cd "$SCRIPT_DIR"
python sync_ths_concept_list.py
python sync_ths_concepts_adata.py workers 10

for i in 1 2 3 4 5 6 7; do
python ths-concept-failed-concepts.py
python sync_ths_concepts_ak.py from-file failed_concepts.txt
done


python ths-hot-concept.py
# python ths-hot-concept.py -d 20251202 --no-sync
# python sync_stock_concept.py
# python sync_stock_concept_adata_ths.py