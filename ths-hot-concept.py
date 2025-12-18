#!/usr/bin/env python3
"""
同花顺热门概念同步（只保留：连续* / n天m次上榜）
"""
from __future__ import annotations

import argparse
import os
import sys
import re
import datetime
import adata

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from db_handler import get_db_handler


HOT_TAG_N_M_PATTERN = r'^\d+天\d+次上榜$'


def _validate_yyyymmdd(value: str) -> str:
    if not re.fullmatch(r"\d{8}", value):
        raise argparse.ArgumentTypeError(f"日期格式错误: {value!r}，需要 YYYYMMDD")
    return value


def _parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="同步同花顺热门概念到数据库")
    parser.add_argument(
        "-d",
        "--delete-before",
        type=_validate_yyyymmdd,
        help="删除 trade_date 小于该日期(YYYYMMDD) 的入库记录",
    )
    parser.add_argument(
        "--no-sync",
        action="store_true",
        help="只执行删除，不执行当天同步",
    )
    return parser.parse_args(argv[1:])


def delete_records_before(before_date: str) -> bool:
    try:
        db_handler = get_db_handler()
        engine = db_handler.get_engine()

        from sqlalchemy import inspect, text  # 局部导入避免非必要依赖暴露到脚本入口

        if not inspect(engine).has_table("ths_hot_concept"):
            print("表 ths_hot_concept 不存在，跳过删除")
            return True

        with engine.connect() as conn:
            result = conn.execute(
                text("DELETE FROM `ths_hot_concept` WHERE trade_date < :before_date"),
                {"before_date": before_date},
            )
            conn.commit()

        print(f"已删除 trade_date < {before_date} 的记录: {result.rowcount} 行")
        return True
    except Exception as e:
        print(f"删除失败: {e}")
        return False


def sync_ths_hot_concept() -> bool:
    trade_date = datetime.datetime.now().strftime("%Y%m%d")
    print("=" * 50)
    print(f"同步同花顺热门概念: {trade_date}")
    print("=" * 50)

    try:
        df = adata.sentiment.hot.hot_concept_20_ths()
        if df is None or df.empty:
            print("警告: 获取到的数据为空")
            return False

        df = df.copy()
        df["trade_date"] = trade_date

        if "concept_code" in df.columns:
            df["concept_code"] = df["concept_code"].astype(str)

        if "hot_tag" not in df.columns:
            print("错误: 数据缺少 hot_tag 字段，无法过滤")
            return False

        hot_tag = df["hot_tag"].astype(str)
        keep_mask = df["hot_tag"].notna() & (
            hot_tag.str.startswith("连续") | hot_tag.str.match(HOT_TAG_N_M_PATTERN)
        )
        kept = df.loc[keep_mask].copy()

        if kept.empty:
            print("提示: 当天无满足条件的数据（连续* / n天m次上榜），跳过入库")
            return True

        # 统一列顺序，避免后续 temp_table SELECT * 列顺序不一致
        preferred_cols = [
            "concept_code",
            "concept_name",
            "rank",
            "change_pct",
            "hot_value",
            "hot_tag",
            "trade_date",
        ]
        cols = [c for c in preferred_cols if c in kept.columns] + [
            c for c in kept.columns if c not in preferred_cols
        ]
        kept = kept[cols]

        # 可选：仍输出当天 CSV 便于人工核对
        os.makedirs("./ths-hot-concept", exist_ok=True)
        kept.to_csv(
            f"./ths-hot-concept/{trade_date}.csv",
            index=False,
            encoding="utf-8-sig",
        )

        db_handler = get_db_handler()
        ok = db_handler.insert_data("ths_hot_concept", kept, trade_date)
        print("同步完成" if ok else "同步失败")
        return bool(ok)

    except Exception as e:
        print(f"同步失败: {e}")
        return False


if __name__ == "__main__":
    try:
        args = _parse_args(sys.argv)

        success = True
        if args.delete_before:
            success = delete_records_before(args.delete_before) and success

        if not args.no_sync:
            success = sync_ths_hot_concept() and success

        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n用户中断")
        sys.exit(0)
    except Exception as e:
        print(f"程序错误: {e}")
        sys.exit(1)
