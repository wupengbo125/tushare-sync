import adata
df = adata.stock.market.all_capital_flow_east(days_type=1)
df.to_csv('capital_flow_east_10days.csv', index=False, encoding='utf-8-sig')
print(df)
