#!/usr/bin/env python3
"""
为 stock_basic 中的股票写入所有概念，存入新表 stock_concept。
数据来源：ak.stock_hot_keyword_em（东财热门关键词），仅存概念和排名，不存热度。
"""
import argparse
import time
from typing import List, Dict, Optional

import akshare as ak
import pandas as pd
from sqlalchemy import text

from db_handler import get_db_handler

TABLE_NAME = "stock_concept"
SOURCE = "ak.stock_hot_keyword_em"


def ensure_table(engine) -> None:
    """创建 stock_concept 表（如不存在）。"""
    create_sql = f"""
    CREATE TABLE IF NOT EXISTS `{TABLE_NAME}` (
        `id` BIGINT AUTO_INCREMENT PRIMARY KEY,
        `ts_code` VARCHAR(20) NOT NULL,
        `symbol` VARCHAR(20) NOT NULL,
        `concept` VARCHAR(100) NOT NULL,
        `concept_code` VARCHAR(20) DEFAULT NULL,
        `concept_rank` TINYINT NOT NULL,
        `source` VARCHAR(64) DEFAULT NULL,
        `created_at` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        UNIQUE KEY `uniq_ts_concept` (`ts_code`, `concept`),
        KEY `idx_ts_code` (`ts_code`),
        KEY `idx_concept` (`concept`)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
    """
    with engine.connect() as conn:
        conn.execute(text(create_sql))
        conn.commit()


def load_stocks(engine, limit: Optional[int], offset: int) -> pd.DataFrame:
    sql = """
    SELECT ts_code, symbol, name, exchange
    FROM stock_basic
    ORDER BY ts_code
    """
    params = {}
    if limit is not None:
        sql += " LIMIT :limit"
        params["limit"] = limit
        if offset:
            sql += " OFFSET :offset"
            params["offset"] = offset
    elif offset:
        # MySQL OFFSET 需要 LIMIT，给一个足够大的值
        sql += " LIMIT 18446744073709551615 OFFSET :offset"
        params["offset"] = offset

    return pd.read_sql(sql, engine, params=params)


def normalize_symbol(symbol: str, exchange: str) -> Optional[str]:
    """转为东财股票代码格式，如 SZ000001 / SH600000。"""
    if not symbol or not exchange:
        return None
    prefix = None
    exchange = exchange.upper()
    if exchange == "SZSE":
        prefix = "SZ"
    elif exchange == "SSE":
        prefix = "SH"
    if prefix is None:
        return None
    return f"{prefix}{symbol}"


def fetch_concepts(em_symbol: str, retries: int, pause: float) -> Optional[pd.DataFrame]:
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            return ak.stock_hot_keyword_em(symbol=em_symbol)
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            if attempt < retries:
                time.sleep(pause * attempt)
    print(f"[WARN] {em_symbol} 概念获取失败: {last_err}")
    return None


def extract_top_concepts(df: pd.DataFrame, max_items: Optional[int] = None) -> List[Dict]:
    """从接口结果提取概念，去重去空。当 max_items=None 时提取所有概念，否则提取前 N 个。"""
    if df is None or df.empty or "概念名称" not in df.columns:
        return []
    df = df.dropna(subset=["概念名称"]).copy()
    df["概念名称"] = df["概念名称"].astype(str).str.strip()
    if "概念代码" in df.columns:
        df["概念代码"] = df["概念代码"].astype(str).str.strip()

    concepts: List[Dict] = []
    seen = set()
    for _, row in df.iterrows():
        concept_name = row.get("概念名称", "")
        if not concept_name or concept_name in seen:
            continue
        seen.add(concept_name)
        concept_code = row.get("概念代码") if "概念代码" in row else None
        if concept_code and str(concept_code).lower() != "nan":
            concept_code = str(concept_code)
        else:
            concept_code = None
        concepts.append(
            {
                "concept": concept_name,
                "concept_code": concept_code,
            }
        )
        if max_items is not None and len(concepts) >= max_items:
            break
    return concepts


def replace_concepts(engine, ts_code: str, symbol: str, concepts: List[Dict]) -> None:
    """覆盖写入某只股票的概念列表（先删后插）。"""
    if not concepts:
        return
    rows = [
        {
            "ts_code": ts_code,
            "symbol": symbol,
            "concept": item["concept"],
            "concept_code": item.get("concept_code"),
            "concept_rank": idx,
            "source": SOURCE,
        }
        for idx, item in enumerate(concepts, start=1)
    ]
    with engine.begin() as conn:
        conn.execute(text(f"DELETE FROM {TABLE_NAME} WHERE ts_code = :ts_code"), {"ts_code": ts_code})
        conn.execute(
            text(
                f"""
                INSERT INTO {TABLE_NAME}
                (ts_code, symbol, concept, concept_code, concept_rank, source)
                VALUES (:ts_code, :symbol, :concept, :concept_code, :concept_rank, :source)
                """
            ),
            rows,
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="为股票写入所有概念（覆盖写入）。")
    parser.add_argument("--limit", type=int, default=None, help="最多处理多少只股票")
    parser.add_argument("--offset", type=int, default=0, help="起始偏移，用于断点续跑")
    parser.add_argument("--sleep", type=float, default=0.2, help="每次请求后的休眠秒数")
    parser.add_argument("--retries", type=int, default=3, help="接口失败重试次数")
    parser.add_argument("--max-concepts", type=int, default=None, help="每只股票最多保存的概念数量（默认=None，保存所有概念）")
    args = parser.parse_args()

    handler = get_db_handler()
    engine = handler.get_engine()
    ensure_table(engine)

    stocks_df = load_stocks(engine, args.limit, args.offset)
    total = len(stocks_df)
    print(f"待处理股票：{total} 条")

    for idx, row in stocks_df.iterrows():
        ts_code = row["ts_code"]
        symbol = row["symbol"]
        exchange = row["exchange"]
        em_symbol = normalize_symbol(symbol, exchange)
        if not em_symbol:
            print(f"[{idx + 1}/{total}] {ts_code} 无法识别市场，跳过")
            continue

        print(f"[{idx + 1}/{total}] {ts_code} ({em_symbol}) 获取概念...")
        df = fetch_concepts(em_symbol, retries=args.retries, pause=args.sleep)
        concepts = extract_top_concepts(df, args.max_concepts)

        if not concepts:
            print(f"    无概念数据，跳过写入")
            time.sleep(args.sleep)
            continue

        replace_concepts(engine, ts_code, symbol, concepts)
        print(f"    写入 {len(concepts)} 条概念")
        time.sleep(args.sleep)


if __name__ == "__main__":
    main()
