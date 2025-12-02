#!/usr/bin/env python3
"""
清理 stock_basic 表
删除名称包含“ST”的股票，以及 ts_code 以 688 或 9 开头的记录
"""
import os
import sys
from sqlalchemy import text

# 添加当前目录到 Python 路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db_handler import get_db_handler


def delete_unwanted_stocks():
    print("=" * 60)
    print("清理 stock_basic：删除 ST 股票及 688*/9* 股票")
    print("=" * 60)

    try:
        db_handler = get_db_handler()
        engine = db_handler.get_engine()

        params = {
            "st_pattern": "%ST%",
            "kc_pattern": "688%",
            "b_pattern": "9%"
        }

        count_sql = text("""
            SELECT
                SUM(name LIKE :st_pattern) AS st_count,
                SUM(ts_code LIKE :kc_pattern) AS kc_count,
                SUM(ts_code LIKE :b_pattern) AS b_count,
                SUM(
                    (name LIKE :st_pattern)
                    OR (ts_code LIKE :kc_pattern)
                    OR (ts_code LIKE :b_pattern)
                ) AS total_count
            FROM stock_basic
        """)

        delete_sql = text("""
            DELETE FROM stock_basic
            WHERE name LIKE :st_pattern
               OR ts_code LIKE :kc_pattern
               OR ts_code LIKE :b_pattern
        """)

        with engine.connect() as conn:
            result = conn.execute(count_sql, params).mappings().one()
            total = result["total_count"] or 0

            print(f"命中 ST 股票: {result['st_count'] or 0}")
            print(f"命中 688* 股票: {result['kc_count'] or 0}")
            print(f"命中 9* 股票: {result['b_count'] or 0}")

            if not total:
                print("无匹配记录，跳过删除")
                return True

            print(f"总计 {total} 条记录即将被删除...")
            conn.execute(delete_sql, params)
            conn.commit()
            print("删除完成")

        return True

    except Exception as e:
        print(f"删除失败: {e}")
        return False


if __name__ == "__main__":
    success = delete_unwanted_stocks()
    sys.exit(0 if success else 1)
