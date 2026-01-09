import akshare as ak
#量价齐升
df = ak.stock_rank_ljqs_ths()

df.to_csv('ljqs_ths.csv', index=False, encoding='utf-8-sig')