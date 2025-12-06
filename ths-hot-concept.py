import adata
import datetime
df = adata.sentiment.hot.hot_concept_20_ths()
df.to_csv(f'./ths-hot-concept/{datetime.datetime.now().strftime("%Y%m%d")}.csv', index=False)
print(df)
