#!/usr/bin/env python3
"""
通用 apply 脚本：把新表重命名为旧表。
用法: python apply_table.py <新表名> <旧表名>
"""
from __future__ import annotations

import argparse
import os
import re
import sys
from typing import Tuple

from sqlalchemy import inspect, text

# 添加当前目录到 Python 路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db_handler import get_db_handler  # noqa: E402

VALID_TABLE_NAME = re.compile(r"^[A-Za-z0-9_]+$")


def validate_table_name(value: str) -> str:
    """仅允许由字母、数字或下划线组成的表名。"""
    if not VALID_TABLE_NAME.fullmatch(value):
        msg = f"非法表名: {value!r}，仅允许字母、数字与下划线"
        raise argparse.ArgumentTypeError(msg)
    return value


def parse_args(argv: list[str]) -> Tuple[str, str]:
    parser = argparse.ArgumentParser(
        description="删除旧表并将新表重命名为旧表"
    )
    parser.add_argument("new_table", type=validate_table_name, help="新表名")
    parser.add_argument("old_table", type=validate_table_name, help="旧表名")
    args = parser.parse_args(argv[1:])
    return args.new_table, args.old_table


def apply_table(new_table: str, old_table: str) -> bool:
    print("=" * 60)
    print(f"删除旧 {old_table} 表，并重命名 {new_table} → {old_table}")
    print("=" * 60)

    if new_table == old_table:
        print("旧表名与新表名相同，无需操作")
        return True

    try:
        db_handler = get_db_handler()
        engine = db_handler.get_engine()
        inspector = inspect(engine)

        with engine.connect() as conn:
            if inspector.has_table(old_table):
                print(f"删除旧表: {old_table} ...")
                conn.execute(text(f"DROP TABLE `{old_table}`"))
                conn.commit()
                print(f"已删除 {old_table}")

            if not inspector.has_table(new_table):
                print(f"错误: 新表 {new_table} 不存在，无法重命名")
                return False

            print(f"重命名 {new_table} → {old_table} ...")
            conn.execute(text(f"ALTER TABLE `{new_table}` RENAME TO `{old_table}`"))
            conn.commit()
            print("重命名成功")

        table_lock = getattr(db_handler, "_table_lock", None)  # noqa: SLF001
        existing_tables = getattr(db_handler, "_existing_tables", None)  # noqa: SLF001
        if table_lock and existing_tables is not None:
            with table_lock:
                existing_tables.discard(new_table)
                existing_tables.discard(old_table)

        print("操作完成")
        return True

    except Exception as exc:  # pylint: disable=broad-except
        print(f"执行失败: {exc}")
        return False


def main(argv: list[str]) -> int:
    new_table, old_table = parse_args(argv)
    success = apply_table(new_table, old_table)
    return 0 if success else 1


if __name__ == "__main__":
    try:
        sys.exit(main(sys.argv))
    except KeyboardInterrupt:
        print("\n用户中断")
        sys.exit(0)
