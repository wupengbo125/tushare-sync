#!/usr/bin/env python3
"""
股票人气排名同步
"""
import os
import sys
import time
import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import text

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db_handler import get_db_handler

def sync_stock_hot_rank(keep_days=90):
    """同步股票人气排名"""
    print("=" * 50)
    print("同步股票人气排名")
    print("=" * 50)

    try:
        # 初始化
        db_handler = get_db_handler()
        engine = db_handler.get_engine()

        # 清理旧数据（如果表存在）
        cutoff_date = datetime.now() - timedelta(days=keep_days)
        delete_query = text("DELETE FROM stock_hot_rank WHERE record_date < :record_date")

        try:
            with engine.connect() as connection:
                result = connection.execute(delete_query, {'record_date': cutoff_date.date()})
                connection.commit()
                deleted_count = result.rowcount
            print(f"清理了 {deleted_count} 条 {keep_days} 天前的数据")
        except Exception as e:
            print(f"清理旧数据失败（可能是首次运行）: {e}")
            deleted_count = 0

        # 获取人气排名数据
        print("正在获取股票人气排名数据...")
        max_retries = 2
        rank_stocks = None

        for attempt in range(max_retries):
            try:
                print(f"尝试获取数据 (第 {attempt + 1} 次)...")
                # 尝试获取数据
                rank_stocks = ak.stock_hot_rank_em()

                if rank_stocks is not None and not rank_stocks.empty:
                    print(f"成功获取 {len(rank_stocks)} 条数据")
                    break
                else:
                    raise Exception("获取到的数据为空")

            except Exception as e:
                if attempt < max_retries - 1:
                    wait_time = 3
                    print(f"获取失败，{wait_time}秒后重试... (错误: {e})")
                    time.sleep(wait_time)
                else:
                    print(f"获取股票人气排名失败: {e}")
                    return False

        if rank_stocks.empty:
            print("获取到的数据为空")
            return False

        # 添加记录日期和时间
        rank_stocks['record_date'] = datetime.now().date()
        rank_stocks['record_time'] = datetime.now().time()

        # 重命名列
        rank_stocks.columns = [
            'ranking', 'stock_code', 'stock_name',
            'latest_price', 'price_change', 'price_change_percent',
            'record_date', 'record_time'
        ]

        print(f"获取到 {len(rank_stocks)} 条股票人气排名数据")

        # 插入数据库
        success = db_handler.insert_data('stock_hot_rank', rank_stocks)
        if success:
            print("股票人气排名同步完成")
            return True
        else:
            print("股票人气排名同步失败")
            return False

    except Exception as e:
        print(f"同步失败: {e}")
        return False

def schedule_sync(interval_minutes=30, max_hours=8):
    """定时同步"""
    print(f"定时同步模式，间隔 {interval_minutes} 分钟，最大运行 {max_hours} 小时")

    try:
        start_time = datetime.now()
        sync_count = 0

        while True:
            current_time = datetime.now()
            elapsed_hours = (current_time - start_time).total_seconds() / 3600

            if elapsed_hours >= max_hours:
                print(f"达到最大运行时间 {max_hours} 小时，停止")
                break

            print(f"开始第 {sync_count + 1} 次同步...")
            success = sync_stock_hot_rank()

            if success:
                sync_count += 1
                print(f"第 {sync_count} 次同步完成")
            else:
                print(f"第 {sync_count + 1} 次同步失败")

            # 计算下次同步时间
            next_sync = current_time + timedelta(minutes=interval_minutes)
            print(f"下次同步时间: {next_sync.strftime('%Y-%m-%d %H:%M:%S')}")

            # 等待
            import time
            time.sleep(interval_minutes * 60)

        print(f"定时同步结束，共同步 {sync_count} 次")
        return True

    except KeyboardInterrupt:
        print("用户中断定时同步")
        return True
    except Exception as e:
        print(f"定时同步失败: {e}")
        return False

if __name__ == "__main__":
    try:
        if len(sys.argv) > 1:
            if sys.argv[1] == "schedule":
                interval_minutes = int(sys.argv[2]) if len(sys.argv) >= 3 else 30
                max_hours = int(sys.argv[3]) if len(sys.argv) >= 4 else 8
                success = schedule_sync(interval_minutes, max_hours)
            elif sys.argv[1] == "keep":
                keep_days = int(sys.argv[2]) if len(sys.argv) >= 3 else 90
                success = sync_stock_hot_rank(keep_days)
            else:
                print("用法:")
                print("  python sync_stock_hot_rank.py              # 单次同步")
                print("  python sync_stock_hot_rank.py schedule 分钟 小时  # 定时同步")
                print("  python sync_stock_hot_rank.py keep 天数       # 指定保留天数")
                sys.exit(1)
        else:
            success = sync_stock_hot_rank()

        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n用户中断")
        sys.exit(0)
    except Exception as e:
        print(f"程序错误: {e}")
        sys.exit(1)