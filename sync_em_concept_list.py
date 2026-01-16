#!/usr/bin/env python3
"""
同步东方财富概念列表到数据库（直接覆盖正式表）
"""
import os
import sys
import time
from typing import Dict, List, Optional

import pandas as pd
import akshare as ak
from sqlalchemy import text

# 添加当前目录到 Python 路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db_handler import get_db_handler  # noqa: E402

# -----------------------------
# 可配置参数
# -----------------------------
CONCEPT_META_TABLE = "em_concept_list"
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5.0
DATA_SOURCE = "eastmoney"


def fetch_concept_list() -> List[Dict[str, str]]:
    """调用 Akshare 接口获取东方财富概念列表."""
    print("正在获取东方财富概念列表...")
    last_error = None
    df = None
    for attempt in range(MAX_RETRIES):
        try:
            df = ak.stock_board_concept_name_em()
            if df is None or df.empty:
                raise ValueError("返回数据为空")
            break
        except Exception as exc:  # pylint: disable=broad-except
            last_error = exc
            wait_time = RETRY_BACKOFF_SECONDS * (attempt + 1)
            print(f"获取概念列表失败，{wait_time:.1f} 秒后重试... ({exc})")
            time.sleep(wait_time)
    
    if df is None:
        raise RuntimeError(f"获取东方财富概念列表失败: {last_error}")

    # Columns: ['排名', '板块名称', '板块代码', '最新价', '涨跌额', '涨跌幅', '总市值', '换手率', '涨跌家数', '领涨股票', '领涨股票-涨跌幅']
    required_columns = {"板块名称", "板块代码"}
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        raise RuntimeError(f"概念列表缺少必要字段: {missing_columns}. 实际字段: {df.columns.tolist()}")

    records: List[Dict[str, str]] = []
    seen_names = set()

    for _, row in df.iterrows():
        concept_name = str(row.get("板块名称", "")).strip()
        concept_code = str(row.get("板块代码", "")).strip()

        if not concept_name or concept_name.lower() == "nan":
            continue
        
        if concept_name in seen_names:
            continue

        records.append(
            {
                "concept_name": concept_name,
                "concept_code": concept_code,
            }
        )
        seen_names.add(concept_name)

    print(f"共获取到 {len(records)} 个概念")
    return records


def build_concept_meta_dataframe(records: List[Dict[str, str]]) -> pd.DataFrame:
    """构建概念元数据表."""
    df = pd.DataFrame(records)
    if df.empty:
        return df

    df["source"] = DATA_SOURCE
    df["updated_at"] = pd.Timestamp.utcnow()

    return df[["concept_code", "concept_name", "source", "updated_at"]]


def sync_concept_list():
    print("=" * 60)
    print("同步东方财富概念列表（覆盖正式表）")
    print("=" * 60)

    try:
        db_handler = get_db_handler()
        records = fetch_concept_list()
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
        try:
            db_handler._create_indexes(CONCEPT_META_TABLE, df.columns.tolist())  # noqa: SLF001
        except Exception as e:
            print(f"索引创建警告: {e}")
            
        with db_handler._table_lock:  # noqa: SLF001
            db_handler._existing_tables.add(CONCEPT_META_TABLE)  # noqa: SLF001

        print(f"概念列表已写入 {CONCEPT_META_TABLE}，共 {len(df)} 条记录")
        print("操作完成")
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
