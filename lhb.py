import adata
import datetime
df = adata.sentiment.hot.list_a_list_daily(report_date='2025-12-05')
print(df)

df.to_csv(f'./lhb/{datetime.datetime.now().strftime("%Y%m%d")}.csv', index=False)