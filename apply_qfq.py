#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
删除旧 daily_qfq 表，并把 daily_qfq_new 改名为 daily_qfq
"""
import sys
import os
from sqlalchemy import inspect, text

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db_handler import get_db_handler


def rename_daily_qfq_table():
    print("=" * 60)
    print("删除旧 daily_qfq 表，并重命名 daily_qfq_new → daily_qfq")
    print("=" * 60)

    try:
        db_handler = get_db_handler()
        engine = db_handler.get_engine()
        inspector = inspect(engine)

        with engine.connect() as conn:
            # 1️⃣ 删除旧表 daily_qfq
            if inspector.has_table("daily_qfq"):
                print("删除旧表: daily_qfq ...")
                conn.execute(text("DROP TABLE daily_qfq"))
                conn.commit()
                print("已删除 daily_qfq")

            # 2️⃣ 检查新表 daily_qfq_new 是否存在
            if not inspector.has_table("daily_qfq_new"):
                print("错误: 新表 daily_qfq_new 不存在，无法重命名")
                return False

            # 3️⃣ 重命名 daily_qfq_new → daily_qfq
            print("重命名 daily_qfq_new → daily_qfq ...")
            conn.execute(text("ALTER TABLE daily_qfq_new RENAME TO daily_qfq"))
            conn.commit()
            print("重命名成功")

        # 清空表缓存（如果 db_handler 有缓存表名）
        with db_handler._table_lock:
            if "daily_qfq" in db_handler._existing_tables:
                db_handler._existing_tables.remove("daily_qfq")
            if "daily_qfq_new" in db_handler._existing_tables:
                db_handler._existing_tables.remove("daily_qfq_new")

        print("操作完成")
        return True

    except Exception as e:
        print(f"执行失败: {e}")
        return False


if __name__ == "__main__":
    success = rename_daily_qfq_table()
    sys.exit(0 if success else 1)
