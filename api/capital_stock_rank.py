import argparse
import re
from pathlib import Path

import adata
import pandas as pd


_CODE_RE = re.compile(r"^\d{6}(\.[A-Za-z]{2})?$")


def _code_match_ratio(series: pd.Series) -> float:
    values = series.astype(str).str.strip()
    if len(values) == 0:
        return 0.0
    return values.str.match(_CODE_RE).mean()


def _pick_first_existing(df: pd.DataFrame, names: list[str]) -> str | None:
    for name in names:
        if name in df.columns:
            return name
    return None


def _read_csv_preserve_codes(path: Path) -> pd.DataFrame:
    header = pd.read_csv(path, encoding="utf-8-sig", nrows=0)
    cols = set(header.columns.astype(str))
    force_str_cols = [
        "stock_code",
        "stock_name",
        "ts_code",
        "code",
        "name",
        "index_code",
        "index_name",
        "concept_code",
    ]
    dtype = {c: str for c in force_str_cols if c in cols}
    return pd.read_csv(path, encoding="utf-8-sig", dtype=dtype)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Reorder concept capital-flow rows by stock frequency (most frequent stock first). "
            "Keeps the original columns/rows; only changes row order."
        )
    )
    parser.add_argument(
        "-i",
        "--input",
        default=None,
        help="Optional input CSV path (if omitted, fetch from adata API).",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=None,
        help="Output CSV path (default: api/capital_flow_east_<days_type>days_by_stock_sorted.csv).",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=50,
        help="Print top N rows to stdout (default: 50).",
    )
    parser.add_argument(
        "--days-type",
        type=int,
        default=1,
        help="adata all_capital_flow_east days_type (default: 1).",
    )
    parser.add_argument(
        "--save-raw",
        default=None,
        help="Optional path to save the raw fetched CSV (utf-8-sig).",
    )
    parser.add_argument(
        "--add-count-column",
        action="store_true",
        help="Add a temporary 'stock_row_count' column to the output CSV.",
    )
    args = parser.parse_args()

    if args.input:
        input_path = Path(args.input)
        df = _read_csv_preserve_codes(input_path)
    else:
        input_path = None
        df = adata.stock.market.all_capital_flow_east(days_type=args.days_type)
        if args.save_raw:
            Path(args.save_raw).parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(args.save_raw, index=False, encoding="utf-8-sig")

    if args.output:
        output_path = Path(args.output)
    else:
        output_path = Path(__file__).with_name(
            f"capital_flow_east_{args.days_type}days_by_stock_sorted.csv"
        )

    stock_code_col = _pick_first_existing(df, ["stock_code", "ts_code", "code"])
    stock_name_col = _pick_first_existing(df, ["stock_name", "name"])
    if not stock_code_col or not stock_name_col:
        raise SystemExit(
            f"Missing stock columns; got columns: {', '.join(df.columns.astype(str))}"
        )

    # Some outputs have stock_code/stock_name swapped; detect for counting only.
    stock_key_col = stock_code_col
    if _code_match_ratio(df[stock_code_col]) < _code_match_ratio(df[stock_name_col]):
        stock_key_col = stock_name_col

    work = df.copy()
    work["_orig_order"] = range(len(work))
    stock_key = work[stock_key_col].astype(str).str.strip()
    counts = stock_key.value_counts(dropna=False)
    work["_stock_row_count"] = stock_key.map(counts).fillna(0).astype(int)
    first_occurrence = (
        pd.DataFrame({"stock_key": stock_key, "_orig_order": work["_orig_order"]})
        .groupby("stock_key", dropna=False)["_orig_order"]
        .min()
    )
    work["_stock_first_occurrence"] = stock_key.map(first_occurrence).fillna(0).astype(int)

    out = work.sort_values(
        ["_stock_row_count", "_stock_first_occurrence", "_orig_order"],
        ascending=[False, True, True],
        kind="mergesort",
    )

    if not args.add_count_column:
        out = out.drop(columns=["_stock_row_count", "_stock_first_occurrence", "_orig_order"])
    else:
        out = out.drop(columns=["_stock_first_occurrence", "_orig_order"]).rename(
            columns={"_stock_row_count": "stock_row_count"}
        )

    # Rename headers to Chinese for readability.
    base_renames = {
        "index_code": "概念代码",
        "index_name": "概念名称",
        "change_pct": "涨跌幅(%)",
        "main_net_inflow": "主力净流入",
        "main_net_inflow_rate": "主力净占比(%)",
        "max_net_inflow": "超大单净流入",
        "max_net_inflow_rate": "超大单净占比(%)",
        "lg_net_inflow": "大单净流入",
        "lg_net_inflow_rate": "大单净占比(%)",
        "mid_net_inflow": "中单净流入",
        "mid_net_inflow_rate": "中单净占比(%)",
        "sm_net_inflow": "小单净流入",
        "sm_net_inflow_rate": "小单净占比(%)",
        "stock_row_count": "出现次数",
    }
    # Decide which stock column is "code" vs "name" (adata sometimes swaps them).
    stock_code_is_code = _code_match_ratio(df[stock_code_col]) >= _code_match_ratio(df[stock_name_col])
    stock_renames = (
        {stock_code_col: "股票代码", stock_name_col: "股票名称"}
        if stock_code_is_code
        else {stock_code_col: "股票名称", stock_name_col: "股票代码"}
    )
    out = out.rename(columns={**base_renames, **stock_renames})

    out.to_csv(output_path, index=False, encoding="utf-8-sig")

    print(f"input:  {input_path if input_path else '<adata API>'}")
    print(f"output: {output_path}")
    print(out.head(max(args.top, 0)).to_string(index=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
