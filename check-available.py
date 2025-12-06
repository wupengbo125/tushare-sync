import adata
df = adata.stock.market.get_market(stock_code='000001', start_date='2025-12-01', k_type=1, adjust_type=1)
print(df)
