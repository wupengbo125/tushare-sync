import adata

df = adata.sentiment.hot.pop_rank_100_east()
df.to_csv('hot-stock.csv', index=False, encoding='utf-8-sig')
