#!/usr/bin/env python3
"""
使用 AData 的 get_concept_ths 接口，为 stock_basic 中的股票写入所有概念。
写入表结构与 sync_stock_concept.py 保持一致，目标表：ths_stock_concept。
支持多线程并发处理。
支持失败重试（-c 参数从失败文件读取）。
"""
import argparse
import datetime
import time
import os
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

import adata
import pandas as pd
from sqlalchemy import text

from db_handler import get_db_handler

TABLE_NAME = "ths_stock_concept"
SOURCE = "adata.stock.info.get_concept_ths"
FAILED_FILE = "failed-stock-concept-adata-ths.txt"


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


def load_failed_stocks(file_path: str) -> List[Dict]:
    """从失败文件加载股票列表。"""
    if not os.path.exists(file_path):
        print(f"失败文件不存在: {file_path}")
        return []

    failed_stocks = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                # 每行格式: ts_code,symbol,stock_code 或 ts_code
                parts = line.split(',')
                if len(parts) >= 2:
                    failed_stocks.append({
                        "ts_code": parts[0],
                        "symbol": parts[1],
                        "stock_code": parts[2] if len(parts) > 2 else normalize_stock_code(parts[1])
                    })
                else:
                    # 只有 ts_code，需要查询数据库
                    ts_code = line
                    failed_stocks.append({"ts_code": ts_code})

    print(f"从失败文件加载 {len(failed_stocks)} 只股票")
    return failed_stocks


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


def extract_top_concepts(df: pd.DataFrame, max_items: Optional[int] = None) -> List[Dict]:
    """从接口结果提取概念，去重去空。当 max_items=None 时提取所有概念，否则提取前 N 个。"""
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
        if max_items is not None and len(concepts) >= max_items:
            break
    return concepts


def replace_concepts(engine, ts_code: str, symbol: str, concepts: List[Dict]) -> None:
    """覆盖写入某只股票的概念列表（先删后插）。"""
    if not concepts:
        return

    # 使用 autocommit 模式，减少锁持有时间，避免多线程死锁
    with engine.connect() as conn:
        # 第一步：删除该股票的所有旧概念
        conn.execute(text(f"DELETE FROM {TABLE_NAME} WHERE ts_code = :ts_code"), {"ts_code": ts_code})
        conn.commit()

        # 第二步：批量插入新概念
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
        conn.commit()


def process_stock(
    ts_code: str,
    symbol: str,
    stock_code: str,
    retries: int,
    pause: float,
    max_concepts: Optional[int],
    db_handler,
    progress_counter: List[int],
    progress_lock: threading.Lock
) -> Dict:
    """处理单只股票的概念获取和写入（线程安全）。"""
    result = {
        "ts_code": ts_code,
        "success": False,
        "concepts_count": 0,
        "error": None
    }

    try:
        # 每个线程创建自己的数据库连接
        engine = db_handler.get_engine()

        # 获取概念
        df = fetch_concepts(stock_code, retries=retries, pause=pause)
        concepts = extract_top_concepts(df, max_concepts)

        if not concepts:
            result["error"] = "无概念数据"
            return result

        # 写入数据库（带死锁重试）
        db_retry = 0
        max_db_retry = 3
        while db_retry < max_db_retry:
            try:
                replace_concepts(engine, ts_code, symbol, concepts)
                result["success"] = True
                result["concepts_count"] = len(concepts)
                break
            except Exception as db_err:
                db_retry += 1
                if "Deadlock" in str(db_err) and db_retry < max_db_retry:
                    # 死锁重试
                    time.sleep(0.1 * db_retry)
                else:
                    raise

    except Exception as e:
        result["error"] = str(e)

    finally:
        # 每次处理完后 sleep，避免请求过快
        if result["success"] or result.get("error") != "无概念数据":
            time.sleep(pause)

        # 更新进度计数器（线程安全）
        with progress_lock:
            progress_counter[0] += 1
            current = progress_counter[0]
            total = progress_counter[1]
            status = "✓" if result["success"] else "✗"
            concepts_info = f"写入 {result['concepts_count']} 条概念" if result["success"] else result["error"]
            print(f"[{current}/{total}] {ts_code} ({stock_code}) {status} - {concepts_info}")

    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="为股票写入所有概念（AData 同花顺接口，覆盖写入，支持多线程）。")
    parser.add_argument("-c", "--continue", action="store_true", dest="continue_mode",
                        help="从失败文件读取股票列表进行重试")
    parser.add_argument("--limit", type=int, default=None, help="最多处理多少只股票")
    parser.add_argument("--offset", type=int, default=0, help="起始偏移，用于断点续跑")
    parser.add_argument("--sleep", type=float, default=0.2, help="接口失败重试时的休眠秒数")
    parser.add_argument("--retries", type=int, default=3, help="接口失败重试次数")
    parser.add_argument("--max-concepts", type=int, default=None, help="每只股票最多保存的概念数量（默认=None，保存所有概念）")
    parser.add_argument("--workers", type=int, default=1, help="并发线程数（默认=1）")
    args = parser.parse_args()

    # 仅在周三运行；非周三直接退出（返回码为 0，以便调度器不视为失败）
    # Python 的 weekday(): 周一=0, 周二=1, 周三=2, ... 周日=6
    if datetime.datetime.now().weekday() != 2:
        print("今天不是周三，任务跳过并退出。")
        return

    handler = get_db_handler()
    engine = handler.get_engine()
    ensure_table(engine)

    # 准备任务列表
    tasks = []

    if args.continue_mode:
        # 从失败文件读取
        print(f"从失败文件读取: {FAILED_FILE}")
        failed_list = load_failed_stocks(FAILED_FILE)

        # 如果失败文件中只有 ts_code，需要从数据库查询完整信息
        for item in failed_list:
            if "symbol" not in item or "stock_code" not in item:
                # 从数据库查询
                sql = text("SELECT ts_code, symbol, name, exchange FROM stock_basic WHERE ts_code = :ts_code")
                with engine.connect() as conn:
                    result = conn.execute(sql, {"ts_code": item["ts_code"]}).fetchone()
                if result:
                    ts_code, symbol, name, exchange = result
                    stock_code = normalize_stock_code(symbol)
                    if stock_code:
                        tasks.append({
                            "ts_code": ts_code,
                            "symbol": symbol,
                            "stock_code": stock_code,
                        })
                else:
                    print(f"[WARN] 数据库中未找到股票: {item['ts_code']}")
            else:
                # 已有完整信息
                tasks.append(item)
    else:
        # 从数据库加载股票
        stocks_df = load_stocks(engine, args.limit, args.offset)
        for _, row in stocks_df.iterrows():
            ts_code = row["ts_code"]
            symbol = row["symbol"]
            stock_code = normalize_stock_code(symbol)
            if not stock_code:
                print(f"[WARN] {ts_code} 无法识别股票代码，跳过")
                continue

            tasks.append({
                "ts_code": ts_code,
                "symbol": symbol,
                "stock_code": stock_code,
            })

    total = len(tasks)
    print(f"待处理股票：{total} 条")
    print(f"使用 {args.workers} 个线程并发处理\n")

    if total == 0:
        print("没有需要处理的股票")
        return

    # 线程安全的进度计数器和失败列表
    progress_counter = [0, total]
    progress_lock = threading.Lock()
    failed_stocks = []
    failed_lock = threading.Lock()

    # 使用线程池并发处理
    success_count = 0
    fail_count = 0

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        # 提交所有任务
        futures = {
            executor.submit(
                process_stock,
                task["ts_code"],
                task["symbol"],
                task["stock_code"],
                args.retries,
                args.sleep,
                args.max_concepts,
                handler,
                progress_counter,
                progress_lock
            ): task for task in tasks
        }

        # 等待所有任务完成
        for future in as_completed(futures):
            try:
                result = future.result()
                if result["success"]:
                    success_count += 1
                else:
                    fail_count += 1
                    # 记录失败的股票（线程安全）
                    with failed_lock:
                        failed_stocks.append(result)
            except Exception as e:
                fail_count += 1
                print(f"[ERROR] 任务执行异常: {e}")

    # 保存失败的股票列表
    if failed_stocks:
        with open(FAILED_FILE, 'w', encoding='utf-8') as f:
            for item in failed_stocks:
                # 格式: ts_code,symbol,stock_code
                stock_code = normalize_stock_code(item.get("symbol", ""))
                if stock_code:
                    f.write(f"{item['ts_code']},{item.get('symbol', '')},{stock_code}\n")
                else:
                    f.write(f"{item['ts_code']}\n")
        print(f"\n失败列表已保存到: {FAILED_FILE}")
    else:
        # 如果全部成功，删除失败文件
        if os.path.exists(FAILED_FILE):
            os.remove(FAILED_FILE)
            print(f"\n全部成功，已删除失败文件: {FAILED_FILE}")

    # 输出统计
    print(f"\n处理完成！")
    print(f"成功: {success_count} 只")
    print(f"失败: {fail_count} 只")
    print(f"总计: {success_count + fail_count} 只")

    if fail_count > 0:
        print(f"\n提示: 使用 -c 参数重试失败的股票")
        print(f"python3 {os.path.basename(__file__)} -c --workers {args.workers}")


if __name__ == "__main__":
    main()
