#!/usr/bin/env python3
"""Fetch TongHuaShun (i问财) hot rank for a single A-share code."""

from __future__ import annotations

import argparse
import re
import sys
from typing import Any, Dict, Optional

import pywencai


def normalize_code(code: str) -> str:
    digits = re.sub(r"\D", "", code)
    if len(digits) != 6:
        raise ValueError(f"股票代码需要 6 位数字，比如 000001，收到: {code!r}")
    return digits


def parse_rank_from_text(txt: str) -> Dict[str, Optional[Any]]:
    """Extract rank and heat number from the html snippet."""
    rank = None
    heat = None
    if txt:
        m_rank = re.search(r"排名<span[^>]*>([0-9.,]+)", txt)
        if m_rank:
            try:
                rank = int(float(m_rank.group(1).replace(",", "")))
            except ValueError:
                rank = None
        m_heat = re.search(r"热度<span[^>]*>([^<]+)", txt)
        if m_heat:
            heat = m_heat.group(1).strip()
    return {"rank": rank, "heat": heat}


def fetch_hot_rank(code: str) -> Dict[str, Any]:
    query = f"{code} 人气排名"
    res = pywencai.get(query=query, query_type="stock")

    txt = res.get("txt1", "") or ""
    parsed = parse_rank_from_text(txt)

    # grade1 sometimes包含总样本数和排名，部分标的没有该字段
    total = None
    rank = parsed["rank"]
    if "grade1" in res and hasattr(res["grade1"], "iloc") and len(res["grade1"]):
        df = res["grade1"]
        if "count" in df.columns:
            try:
                total = int(df.iloc[0]["count"])
            except Exception:
                total = None
        if rank is None and "latest_rank" in df.columns:
            try:
                rank = int(df.iloc[0]["latest_rank"])
            except Exception:
                rank = None

    clean_txt = re.sub(r"<[^>]+>", "", txt).strip() or None

    if rank is None:
        raise RuntimeError("未能从问财返回数据中解析到排名，请稍后重试或换个股票代码")

    return {
        "code": code,
        "rank": rank,
        "heat": parsed["heat"],
        "total": total,
        "raw_text": clean_txt,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="获取同花顺(问财)个股热度排名，返回最新排名数字。"
    )
    parser.add_argument("code", help="6 位股票代码，例如 000001")
    args = parser.parse_args()

    try:
        code = normalize_code(args.code)
        result = fetch_hot_rank(code)
    except Exception as exc:  # noqa: BLE001
        sys.stderr.write(f"获取失败: {exc}\\n")
        return 1

    total_part = f"/{result['total']}" if result["total"] else ""
    print(f"{result['code']} 当前同花顺个股热度排名: {result['rank']}{total_part}")
    if result["heat"]:
        print(f"热度值: {result['heat']}")
    if result["raw_text"]:
        print(f"问财摘要: {result['raw_text']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
