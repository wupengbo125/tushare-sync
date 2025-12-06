import adata

df = adata.sentiment.stock_lifting_last_month()
df.to_csv('unblock-stock.csv', index=False, encoding='utf-8-sig')
print(df)
