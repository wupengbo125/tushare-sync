#!/usr/bin/env python3
"""
使用 AkShare 同步同花顺行业指数（日/周/月）行情数据。

依赖行业列表表：ths_industry_list（由 sync_ths_industry_list.py 生成）
目标表：ths_industry_daily
"""

import os
import sys
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import akshare as ak
import pandas as pd
from sqlalchemy import text

# 添加当前目录到 Python 路径，复用 db_handler
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db_handler import get_db_handler  # noqa: E402

# -----------------------------
# 可配置参数（方便后续调节）
# -----------------------------
# 动态计算开始时间：当前时间前推 3 年
START_DATE = (pd.Timestamp.now() - pd.DateOffset(years=3)).strftime("%Y%m%d")
END_DATE = None  # 若为空则自动取最近交易日
TABLE_NAME = "ths_industry_daily"
INDUSTRY_META_TABLE = "ths_industry_list"
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 15.0
REQUEST_INTERVAL_SECONDS = 0.5  # Add delay to be polite
FAILED_OUTPUT_PATH = "failed-ths-industry-daily.txt"
SKIP_NAMES_FILE = "ljg_ths.txt" # Different skip file if needed
INDUSTRY_NAME_FILTER: List[str] = []
DATA_SOURCE = "akshare_ths"

REQUIRED_COLUMNS = {"trade_date", "open", "high", "low", "close", "volume", "amount"}
NUMERIC_COLUMNS = ["open", "high", "low", "close", "volume", "amount"]


def normalize_ymd(date_text: str) -> str:
    """将任意格式日期转换为 YYYYMMDD."""
    if not date_text:
        raise ValueError("日期不能为空")
    digits = "".join(ch for ch in str(date_text) if ch.isdigit())
    if len(digits) != 8:
        raise ValueError(f"无法识别的日期格式: {date_text}")
    return datetime.strptime(digits, "%Y%m%d").strftime("%Y%m%d")


def _load_trade_calendar(year: int) -> pd.DataFrame:
    """读取指定年份的交易日历."""
    try:
        import adata  # 本项目里已使用 AData，复用其交易日历来取最近交易日

        return adata.stock.info.trade_calendar(year=year)
    except Exception as exc:  # pylint: disable=broad-except
        print(f"加载 {year} 年交易日历失败: {exc}")
        return pd.DataFrame()


def get_latest_trade_date() -> str:
    """使用 AData 交易日历获取最近已开市的日期（避免 AkShare 接口差异）."""
    print("正在获取最近交易日 (AData)...")
    today = pd.Timestamp.today().normalize()
    calendars: List[pd.DataFrame] = []
    for year in {today.year, today.year - 1}:
        df = _load_trade_calendar(year)
        if not df.empty:
            calendars.append(df)

    if not calendars:
        print("无法获取交易日历，使用当作为结束日期")
        return today.strftime("%Y%m%d")

    cal_df = pd.concat(calendars, ignore_index=True)
    cal_df["trade_date"] = pd.to_datetime(cal_df["trade_date"], errors="coerce")
    cal_df = cal_df.dropna(subset=["trade_date"])
    cal_df = cal_df[cal_df["trade_status"].astype(int) == 1]
    valid = cal_df[cal_df["trade_date"] <= today]
    if valid.empty:
        raise RuntimeError("交易日历中没有早于今天的日期")
    latest = valid.iloc[-1]["trade_date"]
    return latest.strftime("%Y%m%d")


def fetch_industry_daily(industry: Dict[str, str], start_date: str, end_date: str) -> pd.DataFrame:
    """调用 AkShare 获取同花顺行业指数行情数据."""
    industry_name = industry.get("industry_name", "").strip()
    if not industry_name:
        return pd.DataFrame()

    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            # AkShare 接口以“行业名称”作为 symbol 入参
            df = ak.stock_board_industry_index_ths(
                symbol=industry_name,
                start_date=start_date,
                end_date=end_date,
            )
            if df is None or df.empty:
                return pd.DataFrame()
            return df
        except Exception as exc:  # pylint: disable=broad-except
            last_error = exc
            wait_time = RETRY_BACKOFF_SECONDS * (attempt + 1)
            print(f"{industry_name} 获取失败，{wait_time:.1f}s 后重试... ({exc})")
            time.sleep(wait_time)

    print(f"{industry_name} 数据获取失败，跳过。错误: {last_error}")
    return pd.DataFrame()


def prepare_daily_records(raw_df: pd.DataFrame, industry: Dict[str, str]) -> pd.DataFrame:
    """将 AkShare 返回字段映射为统一结构并添加行业信息."""
    if raw_df.empty:
        return raw_df

    # AkShare columns: ['日期', '开盘价', '最高价', '最低价', '收盘价', '成交量', '成交额']
    col_map = {
        "日期": "trade_date",
        "开盘价": "open",
        "最高价": "high",
        "最低价": "low",
        "收盘价": "close",
        "成交量": "volume",
        "成交额": "amount",
    }
    missing = set(col_map.keys()) - set(raw_df.columns)
    if missing:
        raise RuntimeError(f"返回数据缺少必要字段: {missing}. 实际字段: {raw_df.columns.tolist()}")

    df = raw_df.rename(columns=col_map).copy()
    
    # 转换日期格式
    if pd.api.types.is_object_dtype(df["trade_date"]) or pd.api.types.is_datetime64_any_dtype(df["trade_date"]):
        df["trade_date"] = pd.to_datetime(df["trade_date"]).dt.strftime("%Y%m%d")

    df = df.dropna(subset=["trade_date"])

    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["close"])

    df["industry_name"] = industry["industry_name"]
    df["industry_code"] = industry.get("industry_code", "")
    df["source"] = DATA_SOURCE
    df["updated_at"] = pd.Timestamp.utcnow()

    missing_cols = REQUIRED_COLUMNS - set(df.columns)
    if missing_cols:
        raise RuntimeError(f"字段映射后仍缺少必要字段: {missing_cols}")

    ordered_columns = [
        "industry_code",
        "industry_name",
        "trade_date",
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


def load_industries_from_db(table_name: str, filters: Optional[List[str]], db_handler) -> List[Dict[str, str]]:
    """从数据库读取行业列表."""
    try:
        df = pd.read_sql_table(table_name, con=db_handler.get_engine())
    except Exception as exc:  # pylint: disable=broad-except
        print(f"读取行业表 {table_name} 失败: {exc}")
        return []

    if df.empty or "industry_name" not in df.columns:
        return []

    df = df.dropna(subset=["industry_name"])
    if filters:
        filter_set = set(filters)
        df = df[df["industry_name"].isin(filter_set)]
        missing = filter_set - set(df["industry_name"].tolist())
        for name in missing:
            print(f"警告: 数据库中未找到行业 {name}")

    required_columns = {"industry_name", "industry_code"}
    if not required_columns.issubset(df.columns):
        print("行业表缺少必要字段")
        return []

    df = df[df["industry_name"] != ""]
    return df[["industry_name", "industry_code"]].to_dict("records")


def process_industry_task(
    industry: Dict[str, str], start_date: str, end_date: str
) -> Tuple[Dict[str, str], Optional[pd.DataFrame], Optional[str]]:
    """抓取并整理单个行业数据."""
    raw_df = fetch_industry_daily(industry, start_date, end_date)
    if raw_df.empty:
        return industry, None, "返回数据为空"

    try:
        prepared_df = prepare_daily_records(raw_df, industry)
    except Exception as exc:  # pylint: disable=broad-except
        return industry, None, f"数据处理失败: {exc}"

    if prepared_df.empty:
        return industry, None, "整理后无有效数据"

    return industry, prepared_df, None


def read_industry_names_from_file(file_path: str) -> List[str]:
    """从文本文件读取行业名称."""
    try:
        with open(file_path, "r", encoding="utf-8") as file:
            names: List[str] = []
            seen = set()
            for line in file:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                name = line.split("\t", 1)[0].strip()
                if not name or name in seen:
                    continue
                names.append(name)
                seen.add(name)
        return names
    except Exception as exc:  # pylint: disable=broad-except
        print(f"读取行业文件失败 {file_path}: {exc}")
        return []


def write_failed_industries(failed_list: List[Tuple[str, str]], file_path: str):
    """将失败行业写入文件."""
    try:
        with open(file_path, "w", encoding="utf-8") as file:
            for name, reason in failed_list:
                file.write(f"{name}\t{reason}\n")
        print(f"失败列表已写入 {file_path}")
    except Exception as exc:  # pylint: disable=broad-except
        print(f"写入失败列表文件出错: {exc}")


def load_skip_names(file_path: str) -> List[str]:
    """读取需要跳过的行业名称."""
    if not file_path:
        return []
    if not os.path.isfile(file_path):
        return []
    names = read_industry_names_from_file(file_path)
    if names:
        print(f"将跳过 {len(names)} 个行业（来自 {file_path}）")
    return names


def sync_industries(industry_file: Optional[str] = None) -> bool:
    """主流程入口."""
    start_date = normalize_ymd(START_DATE)
    end_date = normalize_ymd(END_DATE) if END_DATE else get_latest_trade_date()
    
    print("=" * 60)
    print(f"[AkShare] 同步同花顺行业指数行情: {start_date} -> {end_date}")
    print(f"线程模式: 单线程")
    print(f"source={DATA_SOURCE}")
    print("=" * 60)

    db_handler = get_db_handler()

    file_filters = None
    if industry_file:
        file_filters = read_industry_names_from_file(industry_file)
        if not file_filters:
            print(f"行业文件 {industry_file} 中没有可用行业，结束。")
            return False
        print(f"从文件加载 {len(file_filters)} 个行业，将仅同步这些行业。")

    if file_filters:
        filters = file_filters
    elif INDUSTRY_NAME_FILTER:
        filters = INDUSTRY_NAME_FILTER
    else:
        filters = None

    skip_names = set(load_skip_names(SKIP_NAMES_FILE))

    industries = load_industries_from_db(INDUSTRY_META_TABLE, filters, db_handler)
    
    if not industries:
        print(f"数据库中未找到任何行业数据，请先运行 sync_ths_industry_list.py。")
        return False

    if skip_names:
        before = len(industries)
        industries = [ind for ind in industries if ind["industry_name"] not in skip_names]
        skipped_count = before - len(industries)
        if skipped_count > 0:
            print(f"跳过 {skipped_count} 个行业（配置文件: {SKIP_NAMES_FILE}）")
        if not industries:
            print("所有行业均在跳过列表中，无需同步。")
            return True

    print(f"行业来源表: {INDUSTRY_META_TABLE}（共 {len(industries)} 个行业）")

    engine = db_handler.get_engine()
    try:
        with engine.connect() as conn:
            print(f"删除旧表: {TABLE_NAME} ...")
            conn.execute(text(f"DROP TABLE IF EXISTS {TABLE_NAME}"))
            conn.commit()
    except Exception as exc:  # pylint: disable=broad-except
        print(f"删除旧表 {TABLE_NAME} 失败: {exc}")

    with db_handler._table_lock:  # noqa: SLF001
        db_handler._existing_tables.discard(TABLE_NAME)  # noqa: SLF001

    first_batch = True
    total_records = 0
    success_industries = 0
    failed_industries: List[Tuple[str, str]] = []

    total = len(industries)
    for i, industry in enumerate(industries):
        finished = i + 1
        industry_name = industry["industry_name"]
        
        try:
            _, prepared_df, error_msg = process_industry_task(industry, start_date, end_date)
        except Exception as exc:
            error_msg = f"任务异常: {exc}"
            prepared_df = None

        if error_msg or prepared_df is None:
            failed_industries.append((industry_name, error_msg or "未知错误"))
            print(f"[{finished}/{total}] {industry_name} 失败: {error_msg}")
            
            if REQUEST_INTERVAL_SECONDS > 0:
                time.sleep(REQUEST_INTERVAL_SECONDS)
            continue

        try:
            if first_batch:
                prepared_df.to_sql(TABLE_NAME, engine, if_exists="replace", index=False)
                db_handler._create_indexes(TABLE_NAME, prepared_df.columns.tolist())  # noqa: SLF001
                first_batch = False
            else:
                prepared_df.to_sql(TABLE_NAME, engine, if_exists="append", index=False)

            record_count = len(prepared_df)
            total_records += record_count
            success_industries += 1
            print(f"[{finished}/{total}] {industry_name} 同步完成，{record_count} 条记录")
        except Exception as exc:  # pylint: disable=broad-except
            failed_industries.append((industry_name, f"写入失败: {exc}"))
            print(f"[{finished}/{total}] {industry_name} 写入数据库失败: {exc}")

        if REQUEST_INTERVAL_SECONDS > 0:
            time.sleep(REQUEST_INTERVAL_SECONDS)

    print("-" * 60)
    if total_records == 0:
        print(f"未能写入任何数据到 {TABLE_NAME}")
    else:
        print(f"同步完成：成功行业 {success_industries}/{len(industries)}，累计 {total_records} 条记录")
        print(f"数据已写入 {TABLE_NAME}")

    if failed_industries:
        write_failed_industries(failed_industries, FAILED_OUTPUT_PATH)
        preview = failed_industries[:10]
        print(f"失败行业 {len(failed_industries)} 个，示例：")
        for name, reason in preview:
            print(f"  - {name}: {reason}")
        if len(failed_industries) > len(preview):
            print(f"  ... 其余 {len(failed_industries) - len(preview)} 个（详见 {FAILED_OUTPUT_PATH}）")

    return total_records > 0


def parse_cli_args(argv: List[str]) -> Optional[str]:
    """解析命令行参数."""
    industry_file = None

    idx = 1
    length = len(argv)
    while idx < length:
        arg = argv[idx]
        if arg == "workers" and idx + 1 < length:
            # 忽略 workers 参数
            print("提示: 已强制改为单线程模式，忽略 workers 参数")
            idx += 2
        elif arg == "from-file" and idx + 1 < length:
            industry_file = argv[idx + 1]
            idx += 2
        else:
            raise ValueError(
                "参数错误。用法示例: "
                "python sync_ths_industry_daily.py from-file failed-ths-industry-daily.txt"
            )
    return industry_file


def main() -> bool:
    industry_file = parse_cli_args(sys.argv)
    return sync_industries(industry_file)


if __name__ == "__main__":
    try:
        success = main()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n用户中断")
        sys.exit(0)
    except Exception as err:  # pylint: disable=broad-except
        print(f"程序错误: {err}")
        sys.exit(1)
