import adata
# df = adata.stock.info.get_concept_ths(stock_code="300033")
df = adata.sentiment.hot.hot_concept_20_ths()
print(df)
