import argparse
import sys
from typing import Optional

import adata
import akshare as ak


def normalize_symbol(stock_code: str) -> str:
    """Normalize a stock code to the akshare symbol format (e.g. SH600446)."""
    code = stock_code.strip().upper()
    if code.startswith(("SH", "SZ")):
        return code
    # Most Shanghai listings start with 6/9; Shenzhen with 0/2/3.
    prefix = "SH" if code.startswith(("6", "9")) else "SZ"
    return f"{prefix}{code}"


def print_section(title: str, df: Optional[object]) -> None:
    separator = "=" * 16
    print(f"\n{separator} {title} {separator}")
    if df is None:
        print("未获取到数据")
        return
    try:
        # Prefer a clean tabular output without the dataframe index.
        print(df.to_string(index=False))  # type: ignore[attr-defined]
    except Exception:
        print(df)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="汇总风险排雷、关联热度、同花顺概念、分红信息。"
    )
    parser.add_argument("stock_code", help="股票代码，例如 600446")
    args = parser.parse_args()

    stock_code = args.stock_code.strip()
    symbol = normalize_symbol(stock_code)

    try:
        mine_df = adata.sentiment.mine.mine_clearance_tdx(stock_code=stock_code)
    except Exception as exc:  # pragma: no cover - defensive
        print(f"获取风险排雷信息失败: {exc}", file=sys.stderr)
        mine_df = None

    try:
        relate_df = ak.stock_hot_rank_relate_em(symbol=symbol)
    except Exception as exc:  # pragma: no cover - defensive
        print(f"获取关联热度信息失败: {exc}", file=sys.stderr)
        relate_df = None

    try:
        concept_df = adata.stock.info.get_concept_ths(stock_code=stock_code)
    except Exception as exc:  # pragma: no cover - defensive
        print(f"获取同花顺概念信息失败: {exc}", file=sys.stderr)
        concept_df = None

    try:
        dividend_df = adata.stock.market.get_dividend(stock_code=stock_code)
    except Exception as exc:  # pragma: no cover - defensive
        print(f"获取分红信息失败: {exc}", file=sys.stderr)
        dividend_df = None

    print_section("风险排雷 (mine_clearance_tdx)", mine_df)
    print_section("关联热度 (stock_hot_rank_relate_em)", relate_df)
    print_section("同花顺概念 (get_concept_ths)", concept_df)
    print_section("分红信息 (get_dividend)", dividend_df)


if __name__ == "__main__":
    main()
