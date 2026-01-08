import argparse

import adata


def main():
    parser = argparse.ArgumentParser(description="Query mine clearance data for a stock code.")
    parser.add_argument("stock_code", help="Stock code to query, e.g. 600790")
    args = parser.parse_args()

    df = adata.sentiment.mine.mine_clearance_tdx(stock_code=args.stock_code)
    print(df)


if __name__ == "__main__":
    main()
