#!/usr/bin/env python3
"""
分红排行脚本
遍历所有数据库中的 stock_basic 表，逐个拉取分红数据，
汇总最近5年的现金分红（方案中的“10股派X元”的 X），
按除权日收盘价折算分红收益率（默认用未复权 daily，必要时可选回退 daily_qfq），输出一份 CSV 展示近3/1/5年的收益率及排名（默认按近3年收益率倒序）。
"""
import argparse
import os
import sys
import logging
from datetime import datetime
from typing import Dict, List
import re
from datetime import timedelta
import concurrent.futures

import pandas as pd
from sqlalchemy import text

# 将当前目录加入路径，复用现有的数据库工具
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db_handler import get_db_handler  # noqa: E402

try:
    import adata
except ImportError as exc:  # pragma: no cover - 依赖由用户环境提供
    raise SystemExit("缺少 adata 库，请先安装：pip install adata") from exc

# 日志设置
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def detect_databases_with_stock_basic(engine) -> List[str]:
    """返回包含 stock_basic 表的数据库列表"""
    query = text(
        """
        SELECT DISTINCT table_schema
        FROM information_schema.tables
        WHERE table_name = 'stock_basic'
          AND table_schema NOT IN ('information_schema','mysql','performance_schema','sys')
        """
    )
    df = pd.read_sql(query, engine)
    # 兼容不同驱动返回的大写/小写列名
    cols = {c.lower(): c for c in df.columns}
    col = cols.get("table_schema")
    if col is None:
        logger.warning("information_schema 查询无 table_schema 列，返回列: %s", df.columns.tolist())
        return []
    dbs = df[col].dropna().unique().tolist()
    logger.info("找到包含 stock_basic 的数据库: %s", ", ".join(dbs) if dbs else "无")
    return dbs


def load_stocks_from_db(engine, database: str) -> pd.DataFrame:
    """从指定数据库读取股票基础信息"""
    query = text(f"SELECT ts_code, symbol, name FROM `{database}`.`stock_basic`")
    df = pd.read_sql(query, engine)
    df["source_db"] = database
    return df


def normalize_stock_code(row: pd.Series) -> str:
    """优先用 symbol，退回 ts_code（去掉交易所后缀）"""
    symbol = (row.get("symbol") or "").strip()
    if symbol:
        return symbol
    ts_code = (row.get("ts_code") or "").strip()
    if ts_code and "." in ts_code:
        return ts_code.split(".", 1)[0]
    return ts_code


def parse_cash_from_plan(plan: str) -> float:
    """
    从分红方案字符串中提取现金分红数字（单位：元/10股，保持原样不换算成每股）。
    典型格式：10股派2.36元、10股派39.74元，10股转赠...
    """
    if not isinstance(plan, str):
        return 0.0
    # 兼容中文逗号
    plan = plan.replace("，", ",")
    patterns = [
        r"派\s*([-+]?\d+(?:\.\d+)?)\s*元",
        r"派现(?:金)?\s*([-+]?\d+(?:\.\d+)?)\s*元",
        r"([-+]?\d+(?:\.\d+)?)\s*元",
    ]
    for pat in patterns:
        match = re.search(pat, plan)
        if match:
            try:
                return float(match.group(1))
            except Exception:
                break
    return 0.0


def fetch_close_prices(engine, ts_code: str, trade_dates: List[str], allow_qfq_fallback: bool = False) -> Dict[str, float]:
    """
    获取指定日期及其最近交易日（向前取最近一日）的收盘价。
    默认只用 daily（未复权）；若 allow_qfq_fallback=True 且 daily 缺失，则回退 daily_qfq。
    """
    safe_dates = [d for d in trade_dates if d and d.isdigit()]
    if not ts_code or not safe_dates:
        return {}

    # 取查询区间：最早日期往前多留 30 天，最晚日期为最大日期
    date_series = pd.to_datetime(safe_dates, format="%Y%m%d", errors="coerce").dropna()
    if date_series.empty:
        return {}
    start_date = (date_series.min() - timedelta(days=30)).strftime("%Y%m%d")
    end_date = date_series.max().strftime("%Y%m%d")

    def _query(table: str) -> pd.DataFrame:
        sql = text(
            f"""
            SELECT trade_date, close
            FROM {table}
            WHERE ts_code = :ts_code
              AND trade_date BETWEEN :start_date AND :end_date
            ORDER BY trade_date
            """
        )
        try:
            return pd.read_sql(sql, engine, params={"ts_code": ts_code, "start_date": start_date, "end_date": end_date})
        except Exception as exc:
            logger.warning("读取价格失败 %s %s: %s", table, ts_code, exc)
            return pd.DataFrame()

    df_price = _query("daily")
    if df_price.empty and allow_qfq_fallback:
        df_price = _query("daily_qfq")
    if df_price.empty:
        return {}

    df_price["trade_date_dt"] = pd.to_datetime(df_price["trade_date"], format="%Y%m%d", errors="coerce")
    df_price = df_price.dropna(subset=["trade_date_dt"]).sort_values("trade_date_dt")

    # 对目标日期向前寻找最近的可用交易日价格
    target_df = pd.DataFrame({"target_date": pd.to_datetime(safe_dates, format="%Y%m%d", errors="coerce")}).dropna()
    merged = pd.merge_asof(
        target_df.sort_values("target_date"),
        df_price[["trade_date_dt", "close"]].rename(columns={"trade_date_dt": "trade_date_dt"}),
        left_on="target_date",
        right_on="trade_date_dt",
        direction="backward",
    )

    price_map: Dict[str, float] = {}
    for _, row in merged.iterrows():
        if pd.notnull(row.get("close")) and pd.notnull(row.get("target_date")):
            price_map[row["target_date"].strftime("%Y%m%d")] = float(row["close"])
    return price_map


def compute_dividend_by_year(
    stock_code: str, ts_code: str, years: List[int], engine, allow_qfq_fallback: bool = False
) -> (Dict[int, float], Dict[int, float]):
    """获取单个股票的分红并按年度汇总：金额与对应收益率"""
    try:
        df = adata.stock.market.get_dividend(stock_code=stock_code)
    except Exception as exc:
        logger.warning("获取分红失败 %s: %s", stock_code, exc)
        return {}, {}

    if df is None or df.empty:
        return {}, {}

    df = df.copy()
    ex_div_dates = pd.to_datetime(df.get("ex_dividend_date"), errors="coerce")
    report_dates = pd.to_datetime(df.get("report_date"), errors="coerce")
    df["date_used"] = ex_div_dates.fillna(report_dates)
    # 没有任何日期的记录直接丢弃
    df = df.dropna(subset=["date_used"])
    df["year"] = df["date_used"].dt.year
    df["cash_dividend"] = df["dividend_plan"].apply(parse_cash_from_plan)
    df["trade_date"] = df["date_used"].dt.strftime("%Y%m%d")

    # 获取对应交易日的收盘价，计算单次分红收益率
    price_map = fetch_close_prices(
        engine,
        ts_code,
        df["trade_date"].dropna().unique().tolist(),
        allow_qfq_fallback=allow_qfq_fallback,
    )
    df["close_price"] = df["trade_date"].map(price_map)
    # 缺价的记录直接丢弃，避免把收益率算成0
    df = df.dropna(subset=["close_price"])
    df["yield_ratio"] = df.apply(
        lambda r: (r.cash_dividend / (r.close_price * 10)) if pd.notnull(r.close_price) and r.close_price > 0 else 0.0,
        axis=1,
    )

    df = df[df["year"].isin(years)]
    grouped_cash = df.groupby("year")["cash_dividend"].sum()
    grouped_yield = df.groupby("year")["yield_ratio"].sum()
    cash = {int(year): float(grouped_cash.get(year, 0.0)) for year in years}
    yields = {int(year): float(grouped_yield.get(year, 0.0)) for year in years}
    return cash, yields


def build_report(
    stocks: pd.DataFrame,
    years: List[int],
    engine,
    allow_qfq_fallback: bool = False,
    workers: int = 1,
    limit: int = None,
) -> pd.DataFrame:
    """遍历股票并生成分红汇总结果（单线程，每只都打印）"""
    records = []
    total_stocks = len(stocks) if not limit else min(len(stocks), limit)
    logger.info("准备处理 %s 只股票（去重后）", total_stocks)

    rows = list(stocks.itertuples(index=False))
    if limit:
        rows = rows[:limit]

    def _process(row_tuple):
        row_dict = row_tuple._asdict()
        code = normalize_stock_code(pd.Series(row_dict))
        if not code:
            return None, f"跳过无效代码: {row_dict}"

        cash_by_year, yield_by_year = compute_dividend_by_year(
            code, row_dict.get("ts_code", ""), years, engine, allow_qfq_fallback=allow_qfq_fallback
        )
        if not cash_by_year:
            return None, f"无分红数据: {code} {row_dict.get('name') or ''}"

        record: Dict[str, object] = {
            "stock_code": code,
            "ts_code": row_dict.get("ts_code", "") or "",
            "name": row_dict.get("name", "") or "",
            "source_db": row_dict.get("source_db", "") or "",
        }
        yield_5y = sum(yield_by_year.get(y, 0.0) for y in years)
        yield_3y = sum(yield_by_year.get(y, 0.0) for y in years[:3])
        yield_1y = yield_by_year.get(years[0], 0.0)
        record["yield_5y_pct"] = round(yield_5y * 100, 4)
        record["yield_3y_pct"] = round(yield_3y * 100, 4)
        record["yield_1y_pct"] = round(yield_1y * 100, 4)
        return record, None

    if workers and workers > 1:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
            for idx, (record, msg) in enumerate(executor.map(_process, rows), start=1):
                if msg:
                    logger.info(msg)
                if record:
                    records.append(record)
                logger.info("已处理 %d/%d %s %s", idx, total_stocks, record["stock_code"] if record else "", record["name"] if record else "")
    else:
        for idx, row in enumerate(rows, start=1):
            record, msg = _process(row)
            if msg:
                logger.info(msg)
            if record:
                records.append(record)
            logger.info("已处理 %d/%d %s %s", idx, total_stocks, record["stock_code"] if record else "", record["name"] if record else "")

    df = pd.DataFrame(records)
    if not df.empty:
        # 默认按近3年收益率倒序，其次近1年收益率，再其次近5年收益率
        df.sort_values(
            by=["yield_3y_pct", "yield_1y_pct", "yield_5y_pct"],
            ascending=False,
            inplace=True,
        )
        # 生成收益率排名（同额同名次）
        df["rank_yield_5y"] = df["yield_5y_pct"].rank(method="min", ascending=False).astype(int)
        df["rank_yield_3y"] = df["yield_3y_pct"].rank(method="min", ascending=False).astype(int)
        df["rank_yield_1y"] = df["yield_1y_pct"].rank(method="min", ascending=False).astype(int)
    return df


def main():
    parser = argparse.ArgumentParser(
        description="汇总近5年分红并导出CSV（单位：分红方案中的元/10股）"
    )
    parser.add_argument(
        "--databases",
        help="手动指定数据库（逗号分隔），默认自动发现所有含 stock_basic 的库",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="仅处理前N只股票用于快速试跑",
    )
    parser.add_argument(
        "--allow-qfq-fallback",
        action="store_true",
        help="当 daily 缺少除权日价格时，允许回退 daily_qfq（前复权）作为参考价",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="线程数，>1 时开启多线程提速（默认 1 单线程）",
    )
    args = parser.parse_args()

    db_handler = get_db_handler()
    engine = db_handler.get_engine()

    if args.databases:
        databases = [db.strip() for db in args.databases.split(",") if db.strip()]
    else:
        databases = detect_databases_with_stock_basic(engine)

    if not databases:
        logger.error("未找到任何包含 stock_basic 的数据库")
        sys.exit(1)

    stock_frames = []
    for db_name in databases:
        try:
            stock_frames.append(load_stocks_from_db(engine, db_name))
            logger.info("从 %s 读取到 %d 条股票", db_name, len(stock_frames[-1]))
        except Exception as exc:
            logger.warning("读取数据库 %s 失败: %s", db_name, exc)

    if not stock_frames:
        logger.error("未能从任何数据库读取到股票数据")
        sys.exit(1)

    all_stocks = pd.concat(stock_frames, ignore_index=True)
    all_stocks["stock_code"] = all_stocks.apply(normalize_stock_code, axis=1)
    all_stocks = all_stocks.drop_duplicates(subset=["stock_code"])

    current_year = datetime.now().year
    years = list(range(current_year, current_year - 5, -1))

    report_df = build_report(
        all_stocks,
        years,
        engine,
        allow_qfq_fallback=args.allow_qfq_fallback,
        workers=max(1, args.workers),
        limit=args.limit,
    )
    if report_df.empty:
        logger.warning("没有可输出的分红记录")
        sys.exit(0)

    output_path = "dividend_last5y.csv"
    # 使用带 BOM 的 UTF-8 方便 Excel 正常显示中文
    report_df.to_csv(output_path, index=False, encoding="utf-8-sig")
    logger.info("已写出CSV: %s (共 %d 行)", output_path, len(report_df))


if __name__ == "__main__":
    main()
