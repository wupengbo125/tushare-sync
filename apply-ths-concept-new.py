#!/usr/bin/env python3
"""
将 ths_concept 系列新表替换为正式表
"""
import os
import sys
from sqlalchemy import inspect, text

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db_handler import get_db_handler  # noqa: E402

CONCEPT_TABLE = os.getenv("THS_CONCEPT_TABLE", "ths_concept_index_daily")
CONCEPT_TABLE_NEW = os.getenv("THS_CONCEPT_TABLE_NEW", f"{CONCEPT_TABLE}_new")
CONCEPT_META_TABLE = os.getenv("THS_CONCEPT_META_TABLE", "ths_concept_list")
CONCEPT_META_TABLE_NEW = os.getenv("THS_CONCEPT_META_TABLE_NEW", f"{CONCEPT_META_TABLE}_new")


def rename_table(engine, old_name: str, new_name: str) -> bool:
    """删除旧表并将新表重命名为正式表."""
    inspector = inspect(engine)
    if not inspector.has_table(old_name):
        print(f"未发现新表 {old_name}，跳过 {new_name} 替换")
        return True

    with engine.connect() as conn:
        inspector = inspect(engine)
        if inspector.has_table(new_name):
            print(f"删除旧表: {new_name} ...")
            conn.execute(text(f"DROP TABLE {new_name}"))
            conn.commit()

        print(f"重命名 {old_name} → {new_name} ...")
        conn.execute(text(f"ALTER TABLE {old_name} RENAME TO {new_name}"))
        conn.commit()
        print(f"{new_name} 替换完成")
    return True


def apply_concept_tables():
    print("=" * 60)
    print("应用同花顺概念新表 → 正式表")
    print("=" * 60)

    try:
        db_handler = get_db_handler()
        engine = db_handler.get_engine()
        ok_daily = rename_table(engine, CONCEPT_TABLE_NEW, CONCEPT_TABLE)
        ok_meta = rename_table(engine, CONCEPT_META_TABLE_NEW, CONCEPT_META_TABLE)

        if not (ok_daily and ok_meta):
            print("部分表替换失败，请检查日志")
            return False

        with db_handler._table_lock:  # noqa: SLF001
            for name in (CONCEPT_TABLE, CONCEPT_TABLE_NEW, CONCEPT_META_TABLE, CONCEPT_META_TABLE_NEW):
                db_handler._existing_tables.discard(name)  # noqa: SLF001

        print("概念数据表替换完成")
        return True

    except Exception as exc:  # pylint: disable=broad-except
        print(f"执行失败: {exc}")
        return False


if __name__ == "__main__":
    success = apply_concept_tables()
    sys.exit(0 if success else 1)
