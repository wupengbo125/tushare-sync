import akshare as ak
df = ak.stock_board_concept_index_ths(symbol="阿尔茨海默概念", start_date="20210101", end_date="20251231")
df.to_csv("ths_concept_daily_sample_akshare.csv", index=False, encoding="utf-8-sig")
