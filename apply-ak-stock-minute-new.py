#!/usr/bin/env python3
"""
删除旧 stock_{period}m 表，并将 stock_{period}m_new 改名为正式表
"""
import os
import sys
from typing import List

from sqlalchemy import inspect, text

# 添加当前目录到 Python 路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db_handler import get_db_handler  # noqa: E402

DEFAULT_PERIOD = 30
VALID_PERIODS = {1, 5, 15, 30, 60}


def get_table_names(period: int) -> List[str]:
    env_key = f"AK_STOCK_{period}M_TABLE"
    table = os.getenv(env_key, f"stock_{period}m")
    table_new = os.getenv(f"{env_key}_NEW", f"{table}_new")
    return table, table_new


def parse_cli_args(argv: List[str]) -> int:
    period = DEFAULT_PERIOD
    idx = 1
    while idx < len(argv):
        arg = argv[idx]
        if arg == "period" and idx + 1 < len(argv):
            period = int(argv[idx + 1])
            idx += 2
        else:
            raise ValueError(
                "参数错误。用法: python apply-ak-stock-minute-new.py period {1|5|15|30|60}"
            )
    if period not in VALID_PERIODS:
        raise ValueError(f"仅支持 {sorted(VALID_PERIODS)} 分钟周期")
    return period


def apply_stock_table(period: int) -> bool:
    table, table_new = get_table_names(period)
    print("=" * 60)
    print(f"删除旧 {table} 表，并重命名 {table_new} → {table}")
    print("=" * 60)

    try:
        db_handler = get_db_handler()
        engine = db_handler.get_engine()
        inspector = inspect(engine)

        with engine.connect() as conn:
            if inspector.has_table(table):
                print(f"删除旧表: {table} ...")
                conn.execute(text(f"DROP TABLE {table}"))
                conn.commit()
                print(f"已删除 {table}")

            if not inspector.has_table(table_new):
                print(f"错误: 新表 {table_new} 不存在，无法重命名")
                return False

            print(f"重命名 {table_new} → {table} ...")
            conn.execute(text(f"ALTER TABLE {table_new} RENAME TO {table}"))
            conn.commit()
            print("重命名成功")

        with db_handler._table_lock:  # noqa: SLF001
            db_handler._existing_tables.discard(table)  # noqa: SLF001
            db_handler._existing_tables.discard(table_new)  # noqa: SLF001

        print("操作完成")
        return True

    except Exception as exc:  # pylint: disable=broad-except
        print(f"执行失败: {exc}")
        return False


if __name__ == "__main__":
    try:
        period_value = parse_cli_args(sys.argv)
        success = apply_stock_table(period_value)
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n用户中断")
        sys.exit(0)
    except Exception as err:  # pylint: disable=broad-except
        print(f"程序错误: {err}")
        sys.exit(1)
