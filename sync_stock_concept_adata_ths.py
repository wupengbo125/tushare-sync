#!/usr/bin/env python3
"""
使用 AData 的 get_concept_ths 接口，为 stock_basic 中的股票写入概念列表（前 N 个）。
写入表结构与 sync_stock_concept.py 保持一致，目标表：ths_stock_concept。
"""
import argparse
import time
from typing import Dict, List, Optional

import adata
import pandas as pd
from sqlalchemy import text

from db_handler import get_db_handler

TABLE_NAME = "ths_stock_concept"
MAX_CONCEPTS = 5
SOURCE = "adata.stock.info.get_concept_ths"


def ensure_table(engine) -> None:
    """创建 ths_stock_concept 表（如不存在）。"""
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

    stmt = text(sql) if params else sql
    return pd.read_sql(stmt, engine, params=params)


def normalize_stock_code(symbol: str) -> Optional[str]:
    """转为 AData 需要的纯 6 位股票代码。"""
    if not symbol:
        return None
    code = str(symbol).strip()
    if not code:
        return None
    # 保留数字并左填充至 6 位，部分新股可能为 7 位（科创/北交所），按原样返回
    digits = "".join(ch for ch in code if ch.isdigit())
    if not digits:
        return None
    if len(digits) == 6:
        return digits
    if len(digits) > 6:
        return digits
    return digits.zfill(6)


def fetch_concepts(stock_code: str, retries: int, pause: float) -> Optional[pd.DataFrame]:
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            df = adata.stock.info.get_concept_ths(stock_code=stock_code)
            return df
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            if attempt < retries:
                time.sleep(pause * attempt)
    print(f"[WARN] {stock_code} 概念获取失败: {last_err}")
    return None


def extract_top_concepts(df: pd.DataFrame, max_items: int) -> List[Dict]:
    """从接口结果提取前 N 个概念，去重去空。"""
    if df is None or df.empty or "name" not in df.columns:
        return []
    df = df.dropna(subset=["name"]).copy()
    df["name"] = df["name"].astype(str).str.strip()
    if "concept_code" in df.columns:
        df["concept_code"] = df["concept_code"].astype(str).str.strip()

    concepts: List[Dict] = []
    seen = set()
    for _, row in df.iterrows():
        concept_name = row.get("name", "")
        if not concept_name or concept_name in seen:
            continue
        seen.add(concept_name)
        concept_code = row.get("concept_code") if "concept_code" in df.columns else None
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
        if len(concepts) >= max_items:
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
    parser = argparse.ArgumentParser(description="为股票写入前 5 个概念（AData 同花顺接口，覆盖写入）。")
    parser.add_argument("--limit", type=int, default=None, help="最多处理多少只股票")
    parser.add_argument("--offset", type=int, default=0, help="起始偏移，用于断点续跑")
    parser.add_argument("--sleep", type=float, default=0.2, help="每次请求后的休眠秒数")
    parser.add_argument("--retries", type=int, default=3, help="接口失败重试次数")
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
        stock_code = normalize_stock_code(symbol)
        if not stock_code:
            print(f"[{idx + 1}/{total}] {ts_code} 无法识别股票代码，跳过")
            continue

        print(f"[{idx + 1}/{total}] {ts_code} ({stock_code}) 获取概念...")
        df = fetch_concepts(stock_code, retries=args.retries, pause=args.sleep)
        concepts = extract_top_concepts(df, MAX_CONCEPTS)

        if not concepts:
            print("    无概念数据，跳过写入")
            time.sleep(args.sleep)
            continue

        replace_concepts(engine, ts_code, symbol, concepts)
        print(f"    写入 {len(concepts)} 条概念")
        time.sleep(args.sleep)


if __name__ == "__main__":
    main()
