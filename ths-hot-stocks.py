import datetime
import adata
df = adata.sentiment.hot.pop_rank_100_east()
df.to_csv(f'./ths-hot-stocks/{datetime.datetime.now().strftime("%Y%m%d")}.csv', index=False, encoding='utf-8-sig')