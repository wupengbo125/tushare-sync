#!/usr/bin/env python3
"""
使用 AData / 东财分钟级行情接口, 同步最近 N 天 A 股分钟数据到 *_new 表
"""
import os
import sys
import time
from datetime import datetime, timedelta
from typing import List, Optional, Tuple

import adata
import pandas as pd
import requests
from sqlalchemy import inspect
from tqdm import tqdm

# 添加当前目录到 Python 路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db_handler import get_db_handler  # noqa: E402

MAX_RETRIES = 3
DEFAULT_PERIOD = 30
VALID_PERIODS = {1, 5, 15, 30, 60}
START_OFFSET_DAYS = 30
ADJUST_TYPE = 1
DATA_SOURCE = "adata_east"
FAILED_FILE = "failed_stock_minute.txt"


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


def load_stock_codes(db_handler) -> List[str]:
    """从 stock_basic 读取 ts_code 列表."""
    try:
        query = "SELECT ts_code FROM stock_basic"
        df = pd.read_sql(query, con=db_handler.get_engine())
        return df["ts_code"].dropna().tolist()
    except Exception as exc:  # pylint: disable=broad-except
        print(f"读取股票列表失败: {exc}")
        return []


def calculate_time_range() -> Tuple[datetime, datetime]:
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


def read_codes_from_file(file_path: str) -> List[str]:
    """从文本文件读取 ts_code 列表."""
    if not os.path.isfile(file_path):
        print(f"失败文件 {file_path} 不存在")
        return []
    codes: List[str] = []
    with open(file_path, "r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            code = line.split()[0]
            codes.append(code)
    return codes


def write_failed_codes(failed_items: List[Tuple[str, str]], file_path: str):
    """将失败的股票写入文件."""
    if not failed_items:
        if os.path.isfile(file_path):
            os.remove(file_path)
            print(f"无失败项，已删除 {file_path}")
        else:
            print("无失败项，不生成失败文件")
        return

    try:
        with open(file_path, "w", encoding="utf-8") as file:
            for code, reason in failed_items:
                file.write(f"{code}\t{reason}\n")
        print(f"失败列表已写入 {file_path}（{len(failed_items)} 条）")
    except Exception as exc:  # pylint: disable=broad-except
        print(f"写入失败文件 {file_path} 出错: {exc}")


def sync_stock_minute(
    period: int,
    sleep_interval: float,
    target_codes: Optional[List[str]],
    failed_file: str,
) -> bool:
    """主流程."""
    if period not in VALID_PERIODS:
        raise ValueError(f"仅支持 {sorted(VALID_PERIODS)} 分钟周期")

    table, table_new = get_table_names(period)
    print("=" * 60)
    print(f"同步最近 {START_OFFSET_DAYS} 天 {period} 分钟数据 → 写入 {table_new}")
    if target_codes is not None:
        print(f"仅同步指定的 {len(target_codes)} 个标的")
    print("=" * 60)

    try:
        db_handler = get_db_handler()
        if target_codes is not None:
            stock_codes = target_codes
        else:
            stock_codes = load_stock_codes(db_handler)
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

        start_time, end_time = calculate_time_range()
        print(f"起始时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"结束时间: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"股票数量: {len(stock_items)}")

        engine = db_handler.get_engine()
        inspector = inspect(engine)
        table_exists = inspector.has_table(table_new)
        if table_exists:
            print(f"{table_new} 已存在，将以追加方式写入")
        else:
            print(f"{table_new} 不存在，将自动创建")
        table_ready = table_exists

        total_records = 0
        success_count = 0
        failed_items: List[Tuple[str, str]] = []
        any_written = False

        total_tasks = len(stock_items)
        with tqdm(total=total_tasks, desc="同步进度", unit="stock") as pbar:
            aborted = False
            for index, (ts_code, stock_code) in enumerate(stock_items, start=1):
                try:
                    df = fetch_minute_data(ts_code, stock_code, start_time, end_time, period)
                except Exception as exc:  # pylint: disable=broad-except
                    failed_items.append((ts_code, f"请求失败:{exc}"))
                    print(f"[{index}/{total_tasks}] {ts_code} 请求失败: {exc}")
                    pbar.update(1)
                    if sleep_interval > 0:
                        time.sleep(sleep_interval)
                    aborted = True
                    abort_reason = f"请求失败:{exc}"
                    break

                if df.empty:
                    failed_items.append((ts_code, "无数据"))
                    print(f"[{index}/{total_tasks}] {ts_code} 返回空数据（源无记录）")
                    pbar.update(1)
                    if sleep_interval > 0:
                        time.sleep(sleep_interval)
                    aborted = True
                    abort_reason = "无数据"
                    break

                try:
                    if not table_ready:
                        df.to_sql(table_new, engine, if_exists="replace", index=False)
                        db_handler._create_indexes(table_new, df.columns.tolist())  # noqa: SLF001
                        with db_handler._table_lock:  # noqa: SLF001
                            db_handler._existing_tables.add(table_new)  # noqa: SLF001
                        table_ready = True
                    else:
                        df.to_sql(table_new, engine, if_exists="append", index=False)
                    total_records += len(df)
                    success_count += 1
                    any_written = True
                except Exception as exc:  # pylint: disable=broad-except
                    failed_items.append((ts_code, f"写入失败:{exc}"))
                    print(f"[{index}/{total_tasks}] {ts_code} 写入失败: {exc}")
                    aborted = True
                    abort_reason = f"写入失败:{exc}"
                    pbar.update(1)
                    if sleep_interval > 0:
                        time.sleep(sleep_interval)
                    break

                pbar.update(1)
                if sleep_interval > 0:
                    time.sleep(sleep_interval)

            if aborted:
                remaining = stock_items[index:]
                remaining_count = len(remaining)
                if remaining_count > 0:
                    print(f"检测到失败({abort_reason})，剩余 {remaining_count} 个标的自动标记为失败")
                    for rest_ts, _ in remaining:
                        failed_items.append((rest_ts, "跳过:前序失败"))
                    pbar.update(remaining_count)

        print("-" * 60)
        if not any_written:
            print(f"未能写入任何数据到 {table_new}")
            if failed_items:
                print("全部任务返回空或失败，请检查数据源或时间范围。")
        else:
            print(f"完成：成功 {success_count}/{len(stock_items)}，累计 {total_records} 条记录")
            print(f"数据已写入 {table_new}，请使用 apply_stock_minute.py 应用")

        if failed_items:
            print("失败列表示例：")
            preview = failed_items[:20]
            for code, reason in preview:
                print(f"  - {code}: {reason}")
            if len(failed_items) > len(preview):
                print(f"  ... 其余 {len(failed_items) - len(preview)} 条")

        write_failed_codes(failed_items, failed_file)

        return any_written

    except Exception as exc:  # pylint: disable=broad-except
        print(f"执行异常: {exc}")
        return False


def parse_cli_args(argv: List[str]) -> Tuple[int, Optional[str], float, Optional[str], str]:
    """解析命令行参数."""
    period = DEFAULT_PERIOD
    ts_filter = None
    sleep_interval = 0.0
    from_file = None
    failed_file = FAILED_FILE

    idx = 1
    length = len(argv)
    while idx < length:
        arg = argv[idx]
        if arg == "period" and idx + 1 < length:
            period = int(argv[idx + 1])
            idx += 2
        elif arg == "ts_code" and idx + 1 < length:
            ts_filter = argv[idx + 1]
            idx += 2
        elif arg == "sleep" and idx + 1 < length:
            sleep_interval = float(argv[idx + 1])
            idx += 2
        elif arg == "from-file" and idx + 1 < length:
            from_file = argv[idx + 1]
            idx += 2
        elif arg == "failed-file" and idx + 1 < length:
            failed_file = argv[idx + 1]
            idx += 2
        else:
            raise ValueError(
                "参数错误。用法: python stock_minute.py [ts_code 000001.SZ] "
                "period {1|5|15|30|60} sleep 0.5 [from-file failed.txt] [failed-file path]"
            )
    return period, ts_filter, sleep_interval, from_file, failed_file


def main() -> bool:
    period, ts_filter, sleep_interval, from_file, failed_file = parse_cli_args(sys.argv)

    target_codes: Optional[List[str]] = None
    if from_file:
        target_codes = read_codes_from_file(from_file)
        if not target_codes:
            print("失败文件中没有可用代码，结束。")
            return False
    elif ts_filter:
        target_codes = [ts_filter]

    return sync_stock_minute(period, sleep_interval, target_codes, failed_file)


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
