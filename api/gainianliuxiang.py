import adata
df = adata.stock.market.all_capital_flow_east(days_type=10)
df.to_csv('capital_flow_east_10days.csv', index=False)
print(df)
