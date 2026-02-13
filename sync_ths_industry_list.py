#!/usr/bin/env python3
"""
同步同花顺行业列表到数据库（直接覆盖正式表）
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
INDUSTRY_META_TABLE = "ths_industry_list"
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 5.0
DATA_SOURCE = "ths"


def fetch_industry_list() -> List[Dict[str, str]]:
    """调用 Akshare 接口获取同花顺行业列表."""
    print("正在获取同花顺行业列表...")
    last_error = None
    df = None
    for attempt in range(MAX_RETRIES):
        try:
            # 使用 ak.stock_board_industry_name_ths 获取同花顺行业列表
            df = ak.stock_board_industry_name_ths()
            if df is None or df.empty:
                raise ValueError("返回数据为空")
            break
        except Exception as exc:  # pylint: disable=broad-except
            last_error = exc
            wait_time = RETRY_BACKOFF_SECONDS * (attempt + 1)
            print(f"获取行业列表失败，{wait_time:.1f} 秒后重试... ({exc})")
            time.sleep(wait_time)
    
    if df is None:
        raise RuntimeError(f"获取同花顺行业列表失败: {last_error}")

    # Columns: ['name', 'code']
    # Output columns mapping: name -> industry_name, code -> industry_code
    required_columns = {"name", "code"}
    missing_columns = required_columns - set(df.columns)
    if missing_columns:
        # Sometimes columns might differ, let's notify
        raise RuntimeError(f"行业列表缺少必要字段: {missing_columns}. 实际字段: {df.columns.tolist()}")

    records: List[Dict[str, str]] = []
    seen_names = set()

    for _, row in df.iterrows():
        industry_name = str(row.get("name", "")).strip()
        industry_code = str(row.get("code", "")).strip()

        if not industry_name or industry_name.lower() == "nan":
            continue
        
        if industry_name in seen_names:
            continue

        records.append(
            {
                "industry_name": industry_name,
                "industry_code": industry_code,
            }
        )
        seen_names.add(industry_name)

    print(f"共获取到 {len(records)} 个行业")
    return records


def build_industry_meta_dataframe(records: List[Dict[str, str]]) -> pd.DataFrame:
    """构建行业元数据表."""
    df = pd.DataFrame(records)
    if df.empty:
        return df

    df["source"] = DATA_SOURCE
    df["updated_at"] = pd.Timestamp.utcnow()

    return df[["industry_code", "industry_name", "source", "updated_at"]]


def sync_industry_list():
    print("=" * 60)
    print("同步同花顺行业列表（覆盖正式表）")
    print("=" * 60)

    try:
        db_handler = get_db_handler()
        records = fetch_industry_list()
        df = build_industry_meta_dataframe(records)
        if df.empty:
            print("行业列表为空，结束。")
            return False

        engine = db_handler.get_engine()
        with engine.connect() as conn:
            print(f"删除旧表 {INDUSTRY_META_TABLE}（若存在）...")
            conn.execute(text(f"DROP TABLE IF EXISTS {INDUSTRY_META_TABLE}"))
            conn.commit()

        df.to_sql(INDUSTRY_META_TABLE, engine, if_exists="replace", index=False)
        try:
            db_handler._create_indexes(INDUSTRY_META_TABLE, df.columns.tolist())  # noqa: SLF001
        except Exception as e:
            print(f"索引创建警告: {e}")
            
        with db_handler._table_lock:  # noqa: SLF001
            db_handler._existing_tables.add(INDUSTRY_META_TABLE)  # noqa: SLF001

        print(f"行业列表已写入 {INDUSTRY_META_TABLE}，共 {len(df)} 条记录")
        print("操作完成")
        return True

    except Exception as exc:  # pylint: disable=broad-except
        print(f"同步行业列表失败: {exc}")
        return False


if __name__ == "__main__":
    try:
        success = sync_industry_list()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n用户中断")
        sys.exit(0)
    except Exception as err:  # pylint: disable=broad-except
        print(f"程序错误: {err}")
        sys.exit(1)
