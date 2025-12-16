#!/usr/bin/env python3
"""
检查概念指数日线表的最新交易日，并找出该交易日缺失数据的概念板块。

默认表名与 board.py 保持一致：
- 概念列表表：ths_concept_list（concept_code, concept_name）
- 概念日线表：ths_concept_index_daily（concept_code, trade_date, ...）
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd

DB_HOST = os.getenv("MYSQL_HOST", "localhost")
DB_PORT = int(os.getenv("MYSQL_PORT", "3306"))
DB_USER = os.getenv("MYSQL_USER", "root")
DB_PASSWORD = os.getenv("MYSQL_PASSWORD") or os.getenv("MYSQL_ROOT_PASSWORD", "")
DB_NAME = os.getenv("MYSQL_DATABASE", "tushare_sync")
DB_TABLE_CONCEPT_LIST = os.getenv("BOARD_CONCEPT_LIST_TABLE", "ths_concept_list")
DB_TABLE_CONCEPT_DAILY = os.getenv("BOARD_CONCEPT_DAILY_TABLE", "ths_concept_index_daily")


def get_db_engine():
    try:
        from sqlalchemy import create_engine
    except ImportError as exc:  # pragma: no cover
        raise SystemExit("缺少依赖 `sqlalchemy`，请先执行 `pip install sqlalchemy pymysql`。") from exc
    try:
        import pymysql  # noqa: F401
    except ImportError as exc:  # pragma: no cover
        raise SystemExit("缺少依赖 `pymysql`，请先执行 `pip install sqlalchemy pymysql`。") from exc

    dsn = f"mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4"
    return create_engine(dsn, pool_pre_ping=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="检查最新交易日缺失的概念指数日线数据")
    parser.add_argument(
        "--delete",
        action="store_true",
        help="删除数据不全的概念（同时从概念列表表与概念日线表中删除）",
    )
    return parser.parse_args()


def main() -> None:
    from sqlalchemy import bindparam, text

    args = parse_args()
    engine = get_db_engine()

    latest_df = pd.read_sql(
        text(f"SELECT MAX(trade_date) AS max_d FROM {DB_TABLE_CONCEPT_DAILY}"),
        con=engine,
    )
    max_d = str(latest_df.iloc[0]["max_d"]) if not latest_df.empty else ""
    if not max_d or max_d.lower() == "none":
        raise SystemExit(f"{DB_TABLE_CONCEPT_DAILY} 表为空，无法获取最新交易日。")

    concepts = pd.read_sql(
        text(
            f"SELECT concept_code, concept_name FROM {DB_TABLE_CONCEPT_LIST} ORDER BY concept_code"
        ),
        con=engine,
    )
    concepts = concepts.dropna(subset=["concept_code", "concept_name"]).copy()
    concepts["concept_code"] = concepts["concept_code"].astype(str)
    if concepts.empty:
        raise SystemExit(f"{DB_TABLE_CONCEPT_LIST} 表为空，无法对照检查。")

    have_df = pd.read_sql(
        text(
            f"SELECT DISTINCT concept_code FROM {DB_TABLE_CONCEPT_DAILY} WHERE trade_date = :d"
        ),
        con=engine,
        params={"d": max_d},
    )
    have_codes = set(have_df["concept_code"].dropna().astype(str).tolist()) if not have_df.empty else set()

    all_codes = set(concepts["concept_code"].tolist())
    missing_codes = sorted(all_codes - have_codes)

    if args.delete and missing_codes:
        output_path = Path("deleted_concepts.txt")
        name_map = dict(zip(concepts["concept_code"], concepts["concept_name"]))
        lines = [f"{name_map.get(code, '')}\t{code}" for code in missing_codes]
        output_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")

        stmt_daily = text(
            f"DELETE FROM {DB_TABLE_CONCEPT_DAILY} WHERE concept_code IN :codes"
        ).bindparams(bindparam("codes", expanding=True))
        stmt_list = text(
            f"DELETE FROM {DB_TABLE_CONCEPT_LIST} WHERE concept_code IN :codes"
        ).bindparams(bindparam("codes", expanding=True))
        with engine.begin() as conn:
            deleted_daily = conn.execute(stmt_daily, {"codes": missing_codes}).rowcount
            deleted_list = conn.execute(stmt_list, {"codes": missing_codes}).rowcount
        print(
            f"最新交易日 {max_d} 缺失 {len(missing_codes)} 个概念，已删除："
            f"{DB_TABLE_CONCEPT_DAILY}={deleted_daily}, {DB_TABLE_CONCEPT_LIST}={deleted_list}；"
            f"明细已写入 {output_path.resolve()}"
        )
        return

    if args.delete:
        print(f"最新交易日 {max_d} 缺失 0 个概念，无需删除。")
        return

    output_path = Path("failed_concepts.txt")
    lines: list[str] = []
    if missing_codes:
        name_map = dict(zip(concepts["concept_code"], concepts["concept_name"]))
        for code in missing_codes:
            lines.append(f"{name_map.get(code, '')}\t{code}")
    output_path.write_text("\n".join(lines) + ("\n" if lines else ""), encoding="utf-8")
    print(f"最新交易日 {max_d} 缺失 {len(missing_codes)} 个概念，已写入 {output_path.resolve()}")


if __name__ == "__main__":
    main()
