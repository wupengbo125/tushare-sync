#!/usr/bin/env python3
"""
同步最近 N 天 A 股分钟级别数据（支持 1/5/15/30/60 分钟）到 *_new 表
"""
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import akshare as ak
import pandas as pd
import warnings
from sqlalchemy import text, inspect
from tqdm import tqdm

# 添加当前目录到 Python 路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db_handler import get_db_handler  # noqa: E402

warnings.filterwarnings("ignore", category=FutureWarning, module=r"akshare")

DEFAULT_WORKERS = 1
MAX_RETRIES = 3
DEFAULT_PERIOD = 30
VALID_PERIODS = {1, 5, 15, 30, 60}
START_OFFSET_DAYS = 30  # 最近 N 天，可直接修改
AK_SOURCE = "akshare"


def get_table_names(period: int) -> Tuple[str, str]:
    """根据周期生成表名，支持环境变量覆盖."""
    env_key = f"AK_STOCK_{period}M_TABLE"
    table = os.getenv(env_key, f"stock_{period}m")
    table_new = os.getenv(f"{env_key}_NEW", f"{table}_new")
    return table, table_new


def ts_code_to_symbol(ts_code: str) -> Optional[str]:
    """转换 000001.SZ -> sz000001，供 akshare 使用."""
    if not ts_code or "." not in ts_code:
        return None
    code, exchange = ts_code.split(".")
    exchange = exchange.upper()
    mapping = {"SZ": "sz", "SH": "sh", "BJ": "bj"}
    prefix = mapping.get(exchange)
    if not prefix:
        return None
    return f"{prefix}{code}"


def load_stock_codes(db_handler) -> List[str]:
    try:
        query = "SELECT ts_code FROM stock_basic"
        df = pd.read_sql(query, con=db_handler.get_engine())
        return df["ts_code"].dropna().tolist()
    except Exception as exc:  # pylint: disable=broad-except
        print(f"读取股票列表失败: {exc}")
        return []


def calculate_start_time() -> datetime:
    """以当天 00:00 为基准向前推 START_OFFSET_DAYS 天."""
    now = datetime.now()
    base = now.replace(hour=0, minute=0, second=0, microsecond=0)
    return base - timedelta(days=START_OFFSET_DAYS)


def fetch_minute_data(
    ts_code: str,
    ak_symbol: str,
    start_time: datetime,
    period: int,
) -> pd.DataFrame:
    """调用 akshare 获取分钟级别数据并整理."""
    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            df = ak.stock_zh_a_minute(symbol=ak_symbol, period=str(period), adjust="qfq")
            if df is None or df.empty:
                return pd.DataFrame()

            df = df.copy()
            df["trade_time"] = pd.to_datetime(df["day"], errors="coerce")
            df = df.dropna(subset=["trade_time"])
            df = df[df["trade_time"] >= start_time]
            if df.empty:
                return df

            numeric_cols = ["open", "high", "low", "close", "volume"]
            for col in numeric_cols:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df.dropna(subset=["close"])
            if df.empty:
                return df

            df["amount"] = df["volume"] * df["close"]
            df["ts_code"] = ts_code
            df["trade_time"] = df["trade_time"].dt.strftime("%Y-%m-%d %H:%M:%S")
            df["source"] = AK_SOURCE
            df["updated_at"] = pd.Timestamp.utcnow()

            ordered_columns = [
                "ts_code",
                "trade_time",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "amount",
                "source",
                "updated_at",
            ]
            return df[ordered_columns]
        except Exception as exc:  # pylint: disable=broad-except
            last_error = exc
            time.sleep((attempt + 1) * 2)

    print(f"{ts_code} 数据获取失败: {last_error}")
    return pd.DataFrame()


def drop_target_table(db_handler, table_new: str):
    try:
        with db_handler.get_engine().connect() as conn:
            print(f"清空或创建 {table_new} 表...")
            conn.execute(text(f"DROP TABLE IF EXISTS {table_new}"))
            conn.commit()
        with db_handler._table_lock:  # noqa: SLF001
            db_handler._existing_tables.discard(table_new)  # noqa: SLF001
    except Exception as exc:  # pylint: disable=broad-except
        print(f"清理 {table_new} 失败: {exc}")


def sync_stock_minute(period: int, max_workers: int = DEFAULT_WORKERS) -> bool:
    table, table_new = get_table_names(period)
    print("=" * 60)
    print(f"同步最近 {START_OFFSET_DAYS} 天 {period} 分钟数据 → 写入 {table_new}")
    print("=" * 60)

    try:
        db_handler = get_db_handler()
        stock_codes = load_stock_codes(db_handler)
        if not stock_codes:
            print("未获取到股票代码")
            return False

        stock_items: List[Tuple[str, str]] = []
        for ts_code in stock_codes:
            symbol = ts_code_to_symbol(ts_code)
            if symbol:
                stock_items.append((ts_code, symbol))

        if not stock_items:
            print("没有可用的标的")
            return False

        start_time = calculate_start_time()
        print(f"起始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"股票数量: {len(stock_items)}")

        drop_target_table(db_handler, table_new)

        total_records = 0
        success_count = 0
        failed_items: List[str] = []
        first_batch = True

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map: Dict = {
                executor.submit(fetch_minute_data, ts_code, symbol, start_time, period): ts_code
                for ts_code, symbol in stock_items
            }

            with tqdm(total=len(future_map), desc="同步进度", unit="stock") as pbar:
                for future in as_completed(future_map):
                    ts_code = future_map[future]
                    try:
                        df = future.result()
                    except Exception as exc:  # pylint: disable=broad-except
                        failed_items.append(f"{ts_code}:{exc}")
                        pbar.update(1)
                        continue

                    if df.empty:
                        failed_items.append(f"{ts_code}:无数据")
                        pbar.update(1)
                        continue

                    try:
                        if first_batch:
                            df.to_sql(table_new, db_handler.get_engine(), if_exists="replace", index=False)
                            db_handler._create_indexes(table_new, df.columns.tolist())  # noqa: SLF001
                            first_batch = False
                        else:
                            df.to_sql(table_new, db_handler.get_engine(), if_exists="append", index=False)

                        total_records += len(df)
                        success_count += 1
                    except Exception as exc:  # pylint: disable=broad-except
                        failed_items.append(f"{ts_code}:{exc}")

                    pbar.update(1)
                    pbar.set_postfix({"成功": success_count, "记录": total_records})

        print("-" * 60)
        print(f"同步结束: 成功 {success_count}, 失败 {len(failed_items)}, 总记录 {total_records}")
        if failed_items:
            preview = failed_items[:10]
            print("失败示例:")
            for item in preview:
                print(f"  {item}")
            if len(failed_items) > len(preview):
                print(f"  ... 其余 {len(failed_items) - len(preview)} 条")

        return True

    except Exception as exc:  # pylint: disable=broad-except
        print(f"同步失败: {exc}")
        return False


def ensure_new_table_placeholder(db_handler, table_new: str):
    inspector = inspect(db_handler.get_engine())
    if inspector.has_table(table_new):
        return
    with db_handler.get_engine().connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {table_new}"))
        conn.commit()
    with db_handler._table_lock:  # noqa: SLF001
        db_handler._existing_tables.discard(table_new)  # noqa: SLF001


def sync_single_stock(ts_code: str, period: int) -> bool:
    table, table_new = get_table_names(period)
    print("=" * 60)
    print(f"同步单个股票 {ts_code}（{period} 分钟）→ 写入 {table_new}")
    print("=" * 60)

    symbol = ts_code_to_symbol(ts_code)
    if not symbol:
        print("无法识别的 ts_code")
        return False

    try:
        db_handler = get_db_handler()
        ensure_new_table_placeholder(db_handler, table_new)
        start_time = calculate_start_time()
        df = fetch_minute_data(ts_code, symbol, start_time, period)
        if df.empty:
            print("无可写入数据")
            return True

        inspector = inspect(db_handler.get_engine())
        if not inspector.has_table(table_new):
            df.to_sql(table_new, db_handler.get_engine(), if_exists="replace", index=False)
            db_handler._create_indexes(table_new, df.columns.tolist())  # noqa: SLF001
        else:
            df.to_sql(table_new, db_handler.get_engine(), if_exists="append", index=False)

        print(f"{ts_code} 同步完成，{len(df)} 条记录")
        return True
    except Exception as exc:  # pylint: disable=broad-except
        print(f"同步失败: {exc}")
        return False


def parse_cli_args(argv: List[str]) -> Tuple[int, int, Optional[str]]:
    workers = DEFAULT_WORKERS
    period = DEFAULT_PERIOD
    stock_code: Optional[str] = None

    idx = 1
    while idx < len(argv):
        arg = argv[idx]
        if arg == "workers" and idx + 1 < len(argv):
            workers = int(argv[idx + 1])
            idx += 2
        elif arg == "period" and idx + 1 < len(argv):
            period = int(argv[idx + 1])
            idx += 2
        elif stock_code is None:
            stock_code = arg
            idx += 1
        else:
            raise ValueError(
                "参数错误。用法: python ak_stock_minute.py [ts_code] workers N period {1|5|15|30|60}"
            )

    if period not in VALID_PERIODS:
        raise ValueError(f"仅支持 {sorted(VALID_PERIODS)} 分钟周期")

    return workers, period, stock_code


def main(argv: List[str]) -> bool:
    workers, period, stock_code = parse_cli_args(argv)
    if stock_code:
        return sync_single_stock(stock_code, period)
    return sync_stock_minute(period, workers)


if __name__ == "__main__":
    try:
        success = main(sys.argv)
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n用户中断")
        sys.exit(0)
    except Exception as err:  # pylint: disable=broad-except
        print(f"程序错误: {err}")
        sys.exit(1)
