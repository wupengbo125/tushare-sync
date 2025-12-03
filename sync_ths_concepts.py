#!/usr/bin/env python3
"""
同步同花顺概念指数日线数据到本地数据库
"""
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import akshare as ak
import pandas as pd
from sqlalchemy import text, inspect

# 添加当前目录到 Python 路径，复用 db_handler
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db_handler import get_db_handler  # noqa: E402

# -----------------------------
# 可配置参数（方便后续调节）
# -----------------------------
START_DATE = os.getenv("THS_CONCEPT_START_DATE", "20210101")
END_DATE = os.getenv("THS_CONCEPT_END_DATE")  # 若为空则自动取最近交易日
TABLE_NAME = os.getenv("THS_CONCEPT_TABLE", "ths_concept_index_daily")
TABLE_NAME_NEW = os.getenv("THS_CONCEPT_TABLE_NEW", f"{TABLE_NAME}_new")
CONCEPT_META_TABLE = os.getenv("THS_CONCEPT_META_TABLE", "ths_concept_list")
CONCEPT_META_TABLE_NEW = os.getenv("THS_CONCEPT_META_TABLE_NEW", f"{CONCEPT_META_TABLE}_new")
MAX_RETRIES = int(os.getenv("THS_CONCEPT_MAX_RETRIES", "3"))
RETRY_BACKOFF_SECONDS = float(os.getenv("THS_CONCEPT_RETRY_BACKOFF", "15"))
REQUEST_INTERVAL_SECONDS = float(os.getenv("THS_CONCEPT_REQUEST_INTERVAL", "0"))
DEFAULT_WORKERS = max(1, int(os.getenv("THS_CONCEPT_WORKERS", "10")))
FAILED_OUTPUT_PATH = os.getenv("THS_CONCEPT_FAILED_FILE", "failed_concepts.txt")
CONCEPT_NAME_FILTER = [
    name.strip() for name in os.getenv("THS_CONCEPT_NAME_FILTER", "").split(",") if name.strip()
]
DATA_SOURCE = "tonghuashun"

COLUMN_MAPPING = {
    "日期": "trade_date",
    "开盘价": "open",
    "最高价": "high",
    "最低价": "low",
    "收盘价": "close",
    "成交量": "volume",
    "成交额": "amount",
}
NUMERIC_COLUMNS = ["open", "high", "low", "close", "volume", "amount"]


def normalize_ymd(date_text: str) -> str:
    """将任意格式日期转换为 YYYYMMDD."""
    if not date_text:
        raise ValueError("日期不能为空")
    digits = "".join(ch for ch in str(date_text) if ch.isdigit())
    if len(digits) != 8:
        raise ValueError(f"无法识别的日期格式: {date_text}")
    return datetime.strptime(digits, "%Y%m%d").strftime("%Y%m%d")


def get_latest_trade_date() -> str:
    """使用新浪交易日历获取最近已开市的日期."""
    print("正在获取最近交易日...")
    cal_df = ak.tool_trade_date_hist_sina()
    cal_df["trade_date"] = pd.to_datetime(cal_df["trade_date"], errors="coerce")
    cal_df = cal_df.dropna(subset=["trade_date"])
    today = pd.Timestamp.today().normalize()
    valid = cal_df[cal_df["trade_date"] <= today]
    if valid.empty:
        raise RuntimeError("无法获取有效的交易日历数据")
    latest = valid.iloc[-1]["trade_date"]
    return latest.strftime("%Y%m%d")


def fetch_concept_daily(concept_name: str, start_date: str, end_date: str) -> pd.DataFrame:
    """通过 akshare 获取同花顺概念指数的日线数据."""
    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            return ak.stock_board_concept_index_ths(
                symbol=concept_name,
                start_date=start_date,
                end_date=end_date,
            )
        except Exception as exc:  # pylint: disable=broad-except
            last_error = exc
            wait_time = RETRY_BACKOFF_SECONDS * (attempt + 1)
            print(f"{concept_name} 数据获取失败，{wait_time:.1f} 秒后重试... ({exc})")
            time.sleep(wait_time)

    print(f"{concept_name} 数据获取失败，跳过。错误: {last_error}")
    return pd.DataFrame()


def prepare_daily_records(raw_df: pd.DataFrame, concept: Dict[str, str]) -> pd.DataFrame:
    """重命名字段并添加概念信息."""
    if raw_df.empty:
        return raw_df

    df = raw_df.rename(columns=COLUMN_MAPPING)
    missing_cols = set(COLUMN_MAPPING.values()) - set(df.columns)
    if missing_cols:
        raise RuntimeError(f"返回数据缺少必要字段: {missing_cols}")

    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df = df.dropna(subset=["trade_date"])
    df["trade_date"] = df["trade_date"].dt.strftime("%Y%m%d")

    for col in NUMERIC_COLUMNS:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna(subset=["close"])

    df["concept_name"] = concept["concept_name"]
    df["concept_code"] = concept["concept_code"]
    df["source"] = DATA_SOURCE
    df["updated_at"] = pd.Timestamp.utcnow()

    ordered_columns = [
        "concept_code",
        "concept_name",
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


def load_concepts_from_db(table_name: str, filters: Optional[List[str]], db_handler) -> List[Dict[str, str]]:
    """从数据库读取概念列表."""
    try:
        df = pd.read_sql_table(table_name, con=db_handler.get_engine())
    except Exception as exc:  # pylint: disable=broad-except
        print(f"读取概念表 {table_name} 失败: {exc}")
        return []

    if df.empty or "concept_name" not in df.columns:
        return []

    df = df.dropna(subset=["concept_name"])
    if filters:
        filter_set = set(filters)
        df = df[df["concept_name"].isin(filter_set)]
        missing = filter_set - set(df["concept_name"].tolist())
        for name in missing:
            print(f"警告: 数据库中未找到概念 {name}")

    required_columns = {"concept_name", "concept_code"}
    if not required_columns.issubset(df.columns):
        print("概念表缺少必要字段")
        return []

    df = df[df["concept_name"] != ""]
    return df[["concept_name", "concept_code"]].to_dict("records")


def process_concept_task(
    concept: Dict[str, str],
    start_date: str,
    end_date: str,
) -> Tuple[Dict[str, str], Optional[pd.DataFrame], Optional[str]]:
    """线程任务：抓取并整理单个概念数据."""
    concept_name = concept["concept_name"]
    raw_df = fetch_concept_daily(concept_name, start_date, end_date)
    if raw_df.empty:
        return concept, None, "返回数据为空"

    try:
        prepared_df = prepare_daily_records(raw_df, concept)
    except Exception as exc:  # pylint: disable=broad-except
        return concept, None, f"数据处理失败: {exc}"

    if prepared_df.empty:
        return concept, None, "整理后无有效数据"

    return concept, prepared_df, None


def read_concept_names_from_file(file_path: str) -> List[str]:
    """从文本文件读取概念名称."""
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
        print(f"读取概念文件失败 {file_path}: {exc}")
        return []


def write_failed_concepts(failed_list: List[Tuple[str, str]], file_path: str):
    """将失败概念写入文件."""
    try:
        with open(file_path, "w", encoding="utf-8") as file:
            for name, reason in failed_list:
                file.write(f"{name}\t{reason}\n")
        print(f"失败列表已写入 {file_path}")
    except Exception as exc:  # pylint: disable=broad-except
        print(f"写入失败列表文件出错: {exc}")


def sync_concepts(max_workers: int = DEFAULT_WORKERS, concept_file: Optional[str] = None):
    """主流程入口."""
    start_date = normalize_ymd(START_DATE)
    end_date = normalize_ymd(END_DATE) if END_DATE else get_latest_trade_date()
    max_workers = max(1, int(max_workers))
    print("=" * 60)
    print(f"同步同花顺概念指数数据: {start_date} -> {end_date}")
    print(f"线程数: {max_workers}")
    print("=" * 60)

    db_handler = get_db_handler()

    file_filters = None
    if concept_file:
        file_filters = read_concept_names_from_file(concept_file)
        if not file_filters:
            print(f"概念文件 {concept_file} 中没有可用概念，结束。")
            return False
        print(f"从文件加载 {len(file_filters)} 个概念，将仅同步这些概念。")

    if file_filters:
        filters = file_filters
    elif CONCEPT_NAME_FILTER:
        filters = CONCEPT_NAME_FILTER
    else:
        filters = None

    concepts = load_concepts_from_db(CONCEPT_META_TABLE, filters, db_handler)
    meta_table_used = CONCEPT_META_TABLE
    if not concepts:
        print(f"正式概念表 {CONCEPT_META_TABLE} 无数据，尝试读取 {CONCEPT_META_TABLE_NEW} ...")
        concepts = load_concepts_from_db(CONCEPT_META_TABLE_NEW, filters, db_handler)
        if concepts:
            meta_table_used = CONCEPT_META_TABLE_NEW

    if not concepts:
        print("数据库中未找到任何概念数据，请先运行 sync_ths_concept_list.py 并应用。")
        return False
    else:
        print(f"概念来源表: {meta_table_used}（共 {len(concepts)} 个概念）")

    engine = db_handler.get_engine()
    inspector = inspect(engine)
    table_exists = inspector.has_table(TABLE_NAME_NEW)
    initial_run = concept_file is None

    if initial_run:
        try:
            with engine.connect() as conn:
                print(f"清理旧的新表: {TABLE_NAME_NEW} ...")
                conn.execute(text(f"DROP TABLE IF EXISTS {TABLE_NAME_NEW}"))
                conn.commit()
        except Exception as exc:  # pylint: disable=broad-except
            print(f"清理 {TABLE_NAME_NEW} 失败: {exc}")

        with db_handler._table_lock:  # noqa: SLF001
            if TABLE_NAME_NEW in db_handler._existing_tables:  # noqa: SLF001
                db_handler._existing_tables.remove(TABLE_NAME_NEW)  # noqa: SLF001

        table_exists = False
        print(f"准备全量写入 {TABLE_NAME_NEW}")
    else:
        if table_exists:
            print(f"{TABLE_NAME_NEW} 已存在，本次运行将追加同步概念")
        else:
            print(f"{TABLE_NAME_NEW} 不存在，将重新创建该表")

    total_records = 0
    success_concepts = 0
    failed_concepts: List[Tuple[str, str]] = []
    first_batch = not table_exists

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(process_concept_task, concept, start_date, end_date): concept
            for concept in concepts
        }

        finished = 0
        total = len(future_map)
        for future in as_completed(future_map):
            finished += 1
            concept = future_map[future]
            concept_name = concept["concept_name"]

            try:
                _, prepared_df, error_msg = future.result()
            except Exception as exc:  # pylint: disable=broad-except
                error_msg = f"任务异常: {exc}"
                prepared_df = None

            if error_msg or prepared_df is None:
                failed_concepts.append((concept_name, error_msg))
                print(f"[{finished}/{total}] {concept_name} 失败: {error_msg}")
                continue

            try:
                if first_batch:
                    prepared_df.to_sql(TABLE_NAME_NEW, engine, if_exists="replace", index=False)
                    db_handler._create_indexes(TABLE_NAME_NEW, prepared_df.columns.tolist())  # noqa: SLF001
                    first_batch = False
                else:
                    prepared_df.to_sql(TABLE_NAME_NEW, engine, if_exists="append", index=False)

                record_count = len(prepared_df)
                total_records += record_count
                success_concepts += 1
                print(f"[{finished}/{total}] {concept_name} 同步完成，{record_count} 条记录")
            except Exception as exc:  # pylint: disable=broad-except
                failed_concepts.append((concept_name, f"写入失败: {exc}"))
                print(f"[{finished}/{total}] {concept_name} 写入数据库失败: {exc}")

            if REQUEST_INTERVAL_SECONDS > 0:
                time.sleep(REQUEST_INTERVAL_SECONDS)

    print("-" * 60)
    if total_records == 0:
        print(f"未能写入任何数据到 {TABLE_NAME_NEW}")
    else:
        print(f"同步完成：成功概念 {success_concepts}/{len(concepts)}，累计 {total_records} 条记录")
        print(f"数据已写入 {TABLE_NAME_NEW}，请检查后运行 apply-ths-concept-new.py 将其替换为 {TABLE_NAME}")

    if failed_concepts:
        write_failed_concepts(failed_concepts, FAILED_OUTPUT_PATH)
        preview = failed_concepts[:10]
        print(f"失败概念 {len(failed_concepts)} 个，示例：")
        for name, reason in preview:
            print(f"  - {name}: {reason}")
        if len(failed_concepts) > len(preview):
            print(f"  ... 其余 {len(failed_concepts) - len(preview)} 个（详见 {FAILED_OUTPUT_PATH}）")
    return total_records > 0


def parse_cli_args(argv: List[str]) -> Tuple[int, Optional[str]]:
    """解析命令行参数."""
    workers = DEFAULT_WORKERS
    concept_file = None

    idx = 1
    length = len(argv)
    while idx < length:
        arg = argv[idx]
        if arg == "workers" and idx + 1 < length:
            workers = int(argv[idx + 1])
            idx += 2
        elif arg == "from-file" and idx + 1 < length:
            concept_file = argv[idx + 1]
            idx += 2
        else:
            raise ValueError(
                "参数错误。用法示例: "
                "python sync_ths_concepts.py workers 8 from-file failed_concepts.txt"
            )
    return workers, concept_file


def main() -> bool:
    workers, concept_file = parse_cli_args(sys.argv)
    return sync_concepts(workers, concept_file)


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
