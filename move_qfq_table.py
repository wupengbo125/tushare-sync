#!/usr/bin/env python3
"""
前复权表切换脚本
将 daily_qfq_new 表重命名为 daily_qfq 表
"""

import os
import sys
from sqlalchemy import text

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db_handler import get_db_handler


def move_qfq_table():
    """将 daily_qfq_new 表重命名为 daily_qfq 表"""
    print("=" * 50)
    print("前复权表切换")
    print("=" * 50)

    try:
        # 初始化数据库处理器
        db_handler = get_db_handler()
        engine = db_handler.get_engine()

        # 检查新表是否存在
        from sqlalchemy import inspect
        inspector = inspect(engine)

        if not inspector.has_table('daily_qfq_new'):
            print("错误: daily_qfq_new 表不存在")
            return False

        # 开始事务
        with engine.connect() as conn:
            # 如果旧表存在，先删除
            if inspector.has_table('daily_qfq'):
                print("删除旧的 daily_qfq 表...")
                conn.execute(text('DROP TABLE daily_qfq'))
                conn.commit()
                print("已删除旧表")

            # 重命名新表
            print("将 daily_qfq_new 重命名为 daily_qfq...")
            conn.execute(text('ALTER TABLE daily_qfq_new RENAME TO daily_qfq'))
            conn.commit()
            print("表切换完成!")

        return True

    except Exception as e:
        print(f"表切换失败: {e}")
        return False


def show_table_status():
    """显示表状态"""
    try:
        db_handler = get_db_handler()
        engine = db_handler.get_engine()
        from sqlalchemy import inspect
        inspector = inspect(engine)

        print("表状态:")
        if inspector.has_table('daily_qfq'):
            # 获取记录数
            with engine.connect() as conn:
                result = conn.execute(text('SELECT COUNT(*) as count FROM daily_qfq'))
                count = result.fetchone()[0]
                print(f"  daily_qfq: 存在 ({count} 条记录)")
        else:
            print("  daily_qfq: 不存在")

        if inspector.has_table('daily_qfq_new'):
            # 获取记录数
            with engine.connect() as conn:
                result = conn.execute(text('SELECT COUNT(*) as count FROM daily_qfq_new'))
                count = result.fetchone()[0]
                print(f"  daily_qfq_new: 存在 ({count} 条记录)")
        else:
            print("  daily_qfq_new: 不存在")

    except Exception as e:
        print(f"获取表状态失败: {e}")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "status":
        show_table_status()
        sys.exit(0)

    try:
        success = move_qfq_table()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n用户中断")
        sys.exit(0)
    except Exception as e:
        print(f"程序错误: {e}")
        sys.exit(1)