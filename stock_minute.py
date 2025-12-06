#!/usr/bin/env python3
"""
使用 AData / 东财分钟级行情接口, 同步最近 N 天 A 股分钟数据到 *_new 表
"""
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import adata
import pandas as pd
import requests
from sqlalchemy import text
from tqdm import tqdm

# 添加当前目录到 Python 路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db_handler import get_db_handler  # noqa: E402

DEFAULT_WORKERS = 1
MAX_RETRIES = 3
DEFAULT_PERIOD = 30
VALID_PERIODS = {1, 5, 15, 30, 60}
START_OFFSET_DAYS = 30
ADJUST_TYPE = 1
DATA_SOURCE = "adata_east"


def get_table_names(period: int) -> Tuple[str, str]:
    """根据周期生成表名."""
    table = f"stock_{period}m"
    table_new = f"{table}_new"
    return table, table_new


def ts_code_to_stock_code(ts_code: str) -> Optional[str]:
    """转换 000001.SZ -> 000001."""
    if not ts_code or "." not in ts_code:
        return None
    code, _ = ts_code.split(".")
    return code


def load_stock_codes(db_handler, filter_code: Optional[str] = None) -> List[str]:
    """从 stock_basic 读取 ts_code 列表."""
    try:
        query = "SELECT ts_code FROM stock_basic"
        df = pd.read_sql(query, con=db_handler.get_engine())
        codes = df["ts_code"].dropna().tolist()
        if filter_code:
            codes = [code for code in codes if code == filter_code]
        return codes
    except Exception as exc:  # pylint: disable=broad-except
        print(f"读取股票列表失败: {exc}")
        return []


def calculate_time_range(ts_filter: Optional[str] = None) -> Tuple[datetime, datetime]:
    """计算起止时间."""
    now = datetime.now()
    base = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start = base - timedelta(days=START_OFFSET_DAYS)
    return start, now


def _fetch_minute_via_adata(
    stock_code: str,
    start_time: datetime,
    end_time: datetime,
    period: int,
) -> pd.DataFrame:
    """调用 AData 的 get_market 接口获取 5/15/30/60 分钟数据."""
    start_date = start_time.strftime("%Y-%m-%d")
    end_date = end_time.strftime("%Y-%m-%d")
    df = adata.stock.market.get_market(
        stock_code=stock_code,
        start_date=start_date,
        end_date=end_date,
        k_type=period,
        adjust_type=ADJUST_TYPE,
    )
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    df["trade_time"] = pd.to_datetime(df["trade_time"], errors="coerce")
    df = df.dropna(subset=["trade_time"])
    df = df[(df["trade_time"] >= start_time) & (df["trade_time"] <= end_time)]
    if df.empty:
        return df

    rename_map = {
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "volume": "volume",
        "amount": "amount",
    }
    df = df.rename(columns=rename_map)
    numeric_cols = ["open", "high", "low", "close", "volume", "amount"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["close"])
    df["trade_time"] = df["trade_time"].dt.strftime("%Y-%m-%d %H:%M:%S")
    return df[["trade_time", "open", "high", "low", "close", "volume", "amount"]]


def _fetch_minute_one_minute(
    stock_code: str,
    start_time: datetime,
    end_time: datetime,
) -> pd.DataFrame:
    """调用东财分钟接口获取 1 分钟数据."""
    se_cid = 1 if stock_code.startswith("6") else 0
    beg = start_time.strftime("%Y%m%d%H%M%S")
    end = end_time.strftime("%Y%m%d%H%M%S")
    params = {
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61,f116",
        "ut": "7eea3edcaed734bea9cbfc24409ed989",
        "klt": "1",
        "fqt": str(ADJUST_TYPE),
        "secid": f"{se_cid}.{stock_code}",
        "beg": beg,
        "end": end,
    }
    url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
    res = requests.get(url, params=params, timeout=15)
    res.raise_for_status()
    data_json = res.json()
    if not data_json.get("data") or not data_json["data"].get("klines"):
        return pd.DataFrame()

    data = []
    for item in data_json["data"]["klines"]:
        parts = item.split(",")
        if len(parts) < 7:
            continue
        data.append(
            {
                "trade_time": parts[0],
                "open": parts[1],
                "close": parts[2],
                "high": parts[3],
                "low": parts[4],
                "volume": parts[5],
                "amount": parts[6],
            }
        )
    df = pd.DataFrame(data)
    if df.empty:
        return df

    df["trade_time"] = pd.to_datetime(df["trade_time"], errors="coerce")
    df = df.dropna(subset=["trade_time"])
    df = df[(df["trade_time"] >= start_time) & (df["trade_time"] <= end_time)]
    if df.empty:
        return df

    numeric_cols = ["open", "high", "low", "close", "volume", "amount"]
    df["high"] = df["high"].fillna(df["close"])
    df["low"] = df["low"].fillna(df["close"])
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.dropna(subset=["close"])
    df["volume"] = (df["volume"] * 100).astype(float)
    df["trade_time"] = df["trade_time"].dt.strftime("%Y-%m-%d %H:%M:%S")
    return df[["trade_time", "open", "high", "low", "close", "volume", "amount"]]


def fetch_minute_data(
    ts_code: str,
    stock_code: str,
    start_time: datetime,
    end_time: datetime,
    period: int,
) -> pd.DataFrame:
    """根据周期选择数据源."""
    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            if period == 1:
                df = _fetch_minute_one_minute(stock_code, start_time, end_time)
            else:
                df = _fetch_minute_via_adata(stock_code, start_time, end_time, period)
            if df is None or df.empty:
                return pd.DataFrame()
            df = df.copy()
            df["ts_code"] = ts_code
            df["source"] = DATA_SOURCE
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
    """清空新表."""
    try:
        with db_handler.get_engine().connect() as conn:
            print(f"清空或创建 {table_new} 表...")
            conn.execute(text(f"DROP TABLE IF EXISTS {table_new}"))
            conn.commit()
        with db_handler._table_lock:  # noqa: SLF001
            db_handler._existing_tables.discard(table_new)  # noqa: SLF001
    except Exception as exc:  # pylint: disable=broad-except
        print(f"清理 {table_new} 失败: {exc}")


def sync_stock_minute(period: int, ts_filter: Optional[str], max_workers: int = DEFAULT_WORKERS) -> bool:
    """主流程."""
    if period not in VALID_PERIODS:
        raise ValueError(f"仅支持 {sorted(VALID_PERIODS)} 分钟周期")

    table, table_new = get_table_names(period)
    print("=" * 60)
    print(f"同步最近 {START_OFFSET_DAYS} 天 {period} 分钟数据 → 写入 {table_new}")
    if ts_filter:
        print(f"仅同步: {ts_filter}")
    print("=" * 60)

    try:
        db_handler = get_db_handler()
        stock_codes = load_stock_codes(db_handler, ts_filter)
        if not stock_codes:
            print("未获取到股票代码")
            return False

        stock_items: List[Tuple[str, str]] = []
        for ts_code in stock_codes:
            stock_code = ts_code_to_stock_code(ts_code)
            if stock_code:
                stock_items.append((ts_code, stock_code))

        if not stock_items:
            print("没有可用的标的")
            return False

        start_time, end_time = calculate_time_range(ts_filter)
        print(f"起始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"股票数量: {len(stock_items)}")

        drop_target_table(db_handler, table_new)

        total_records = 0
        success_count = 0
        failed_items: List[str] = []
        first_batch = True

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_map: Dict = {
                executor.submit(fetch_minute_data, ts_code, stock_code, start_time, end_time, period): ts_code
                for ts_code, stock_code in stock_items
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
                        failed_items.append(f"{ts_code}:写入失败:{exc}")

                    pbar.update(1)

        print("-" * 60)
        if first_batch:
            print(f"未能写入任何数据到 {table_new}")
        else:
            print(f"完成：成功 {success_count}/{len(stock_items)}，累计 {total_records} 条记录")
            print(f"数据已写入 {table_new}，请使用 apply_stock_minute.py 应用")

        if failed_items:
            print("失败列表示例：")
            for item in failed_items[:20]:
                print(f"  - {item}")
            if len(failed_items) > 20:
                print(f"  ... 其余 {len(failed_items) - 20} 条")

        return not first_batch

    except Exception as exc:  # pylint: disable=broad-except
        print(f"执行异常: {exc}")
        return False


def parse_cli_args(argv: List[str]) -> Tuple[int, Optional[str], int]:
    """解析命令行参数."""
    workers = DEFAULT_WORKERS
    period = DEFAULT_PERIOD
    ts_filter = None

    idx = 1
    length = len(argv)
    while idx < length:
        arg = argv[idx]
        if arg == "workers" and idx + 1 < length:
            workers = int(argv[idx + 1])
            idx += 2
        elif arg == "period" and idx + 1 < length:
            period = int(argv[idx + 1])
            idx += 2
        elif arg == "ts_code" and idx + 1 < length:
            ts_filter = argv[idx + 1]
            idx += 2
        else:
            raise ValueError(
                "参数错误。用法: python stock_minute.py [ts_code 000001.SZ] workers N period {1|5|15|30|60}"
            )
    return workers, ts_filter, period


def main() -> bool:
    workers, ts_filter, period = parse_cli_args(sys.argv)
    return sync_stock_minute(period, ts_filter, workers)


if __name__ == "__main__":
    try:
        ok = main()
        sys.exit(0 if ok else 1)
    except KeyboardInterrupt:
        print("\n用户中断")
        sys.exit(0)
    except Exception as err:  # pylint: disable=broad-except
        print(f"程序错误: {err}")
        sys.exit(1)
