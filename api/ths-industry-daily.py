import akshare as ak

stock_board_industry_index_ths_df = ak.stock_board_industry_index_ths(symbol="元件", start_date="20260101", end_date="20260213")
print(stock_board_industry_index_ths_df)