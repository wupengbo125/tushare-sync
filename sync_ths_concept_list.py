#!/usr/bin/env python3
"""
同步同花顺概念列表到数据库（直接覆盖正式表）
"""
import os
import sys
import time
from typing import Dict, List, Optional

import pandas as pd
import pywencai
from sqlalchemy import text

# 添加当前目录到 Python 路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db_handler import get_db_handler  # noqa: E402

# -----------------------------
# 可配置参数
# -----------------------------
CONCEPT_META_TABLE = "ths_concept_list"
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5.0
CONCEPT_NAME_FILTER: List[str] = []
DATA_SOURCE = "tonghuashun"


def fetch_concept_list(filters: Optional[List[str]] = None) -> List[Dict[str, str]]:
    """调用问财接口获取同花顺概念列表."""
    print("正在获取同花顺概念列表...")
    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            df = pywencai.get(
                query="同花顺概念指数",
                query_type="zhishu",
                sort_order="desc",
                loop=True,
            )
            if df is None or df.empty:
                raise ValueError("返回数据为空")
            break
        except Exception as exc:  # pylint: disable=broad-except
            last_error = exc
            wait_time = RETRY_BACKOFF_SECONDS * (attempt + 1)
            print(f"获取概念列表失败，{wait_time:.1f} 秒后重试... ({exc})")
            time.sleep(wait_time)
    else:
        raise RuntimeError(f"获取同花顺概念列表失败: {last_error}")

    required_columns = {"指数简称", "指数代码", "code"}
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise RuntimeError(f"概念列表缺少必要字段: {missing_columns}")

    records: List[Dict[str, str]] = []
    seen_names = set()
    filter_set = set(filters or [])

    for _, row in df.iterrows():
        concept_name = str(row.get("指数简称", "")).strip()
        if not concept_name or concept_name.lower() == "nan":
            continue
        if filters and concept_name not in filter_set:
            continue

        concept_code = str(row.get("指数代码", "")).strip()
        short_code = str(row.get("code", "")).strip()

        for candidate in (concept_code, short_code, concept_name):
            if candidate and candidate.lower() != "nan":
                concept_code = candidate
                break

        if concept_name in seen_names:
            continue

        records.append(
            {
                "concept_name": concept_name,
                "concept_code": concept_code,
            }
        )
        seen_names.add(concept_name)

    if filters:
        missing = filter_set - {item["concept_name"] for item in records}
        for name in missing:
            print(f"警告: 未在问财结果中找到概念 {name}")

    print(f"共获取到 {len(records)} 个概念")
    return records


def build_concept_meta_dataframe(records: List[Dict[str, str]]) -> pd.DataFrame:
    """根据问财返回的数据构建概念元数据表."""
    df = pd.DataFrame(records)
    if df.empty:
        return df

    df = df.dropna(subset=["concept_name"])
    df["concept_name"] = df["concept_name"].astype(str).str.strip()
    df["concept_code"] = df["concept_code"].astype(str).str.strip()
    df = df[df["concept_name"] != ""]

    df["source"] = DATA_SOURCE
    df["updated_at"] = pd.Timestamp.utcnow()

    return df[["concept_code", "concept_name", "source", "updated_at"]]


def sync_concept_list():
    print("=" * 60)
    print("同步同花顺概念列表（覆盖正式表）")
    print("=" * 60)

    try:
        db_handler = get_db_handler()
        records = fetch_concept_list(CONCEPT_NAME_FILTER if CONCEPT_NAME_FILTER else None)
        df = build_concept_meta_dataframe(records)
        if df.empty:
            print("概念列表为空，结束。")
            return False

        engine = db_handler.get_engine()
        with engine.connect() as conn:
            print(f"删除旧表 {CONCEPT_META_TABLE}（若存在）...")
            conn.execute(text(f"DROP TABLE IF EXISTS {CONCEPT_META_TABLE}"))
            conn.commit()

        df.to_sql(CONCEPT_META_TABLE, engine, if_exists="replace", index=False)
        db_handler._create_indexes(CONCEPT_META_TABLE, df.columns.tolist())  # noqa: SLF001
        with db_handler._table_lock:  # noqa: SLF001
            db_handler._existing_tables.add(CONCEPT_META_TABLE)  # noqa: SLF001

        print(f"概念列表已写入 {CONCEPT_META_TABLE}，共 {len(df)} 条记录")
        print("操作完成，如需同步行情请运行 sync_ths_concepts.py")
        return True

    except Exception as exc:  # pylint: disable=broad-except
        print(f"同步概念列表失败: {exc}")
        return False


if __name__ == "__main__":
    try:
        success = sync_concept_list()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n用户中断")
        sys.exit(0)
    except Exception as err:  # pylint: disable=broad-except
        print(f"程序错误: {err}")
        sys.exit(1)
