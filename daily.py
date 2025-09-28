#!/usr/bin/env python3
"""
日线数据同步
"""
import os
import sys
import time
import tushare as ts
import pandas as pd
from datetime import datetime, timedelta
import warnings

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db_handler import get_db_handler

# Suppress FutureWarning emitted by tushare.pro.data_pro about Series.fillna(method=...)
# The warning originates inside the third-party package; we filter it here to keep logs clean.
warnings.filterwarnings(
    "ignore",
    category=FutureWarning,
    module=r"tushare\.pro\.data_pro"
)

def get_trade_dates(pro, start_date, end_date):
    """获取交易日历"""
    try:
        trade_cal = pro.trade_cal(
            exchange='SSE',
            is_open='1',
            start_date=start_date,
            end_date=end_date,
            fields='cal_date'
        )
        return trade_cal['cal_date'].tolist()
    except Exception as e:
        print(f"获取交易日历失败: {e}")
        return []

def get_daily_data(pro, trade_date, max_retries=3):
    """获取日线数据"""
    for attempt in range(max_retries):
        try:
            print(f"获取 {trade_date} 日线数据...")
            df = pro.daily(trade_date=trade_date)
            return df
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2
                print(f"获取失败，{wait_time}秒后重试...")
                time.sleep(wait_time)
            else:
                print(f"获取 {trade_date} 数据失败: {e}")
    return pd.DataFrame()

def sync_daily():
    """同步日线数据"""
    print("=" * 50)
    print("同步日线数据")
    print("=" * 50)

    try:
        # 初始化
        db_handler = get_db_handler()

        # 检查TUSHARE_TOKEN
        token = os.getenv('TUSHARE_TOKEN')
        if not token:
            print("错误: 请设置 TUSHARE_TOKEN 环境变量")
            return False

        # 初始化Tushare
        ts.set_token(token)
        pro = ts.pro_api()

        # 获取起始日期
        max_date = db_handler.get_max_date('daily')
        if max_date:
            start_date = (pd.to_datetime(str(max_date)) + timedelta(days=1)).strftime('%Y%m%d')
            print(f"数据库最新日期: {max_date}, 从 {start_date} 开始同步")
        else:
            start_date = '20190101'
            print(f"数据库为空，从 {start_date} 开始同步")

        # 获取结束日期
        end_date = datetime.now().strftime('%Y%m%d')
        print(f"同步到: {end_date}")

        # 获取交易日
        trade_dates = get_trade_dates(pro, start_date, end_date)
        if not trade_dates:
            print("没有找到需要同步的交易日")
            return False

        print(f"共需同步 {len(trade_dates)} 个交易日")

        # 同步数据
        total_records = 0
        success_dates = []

        for trade_date in trade_dates:
            try:
                # 检查是否已存在
                try:
                    check_query = f"SELECT COUNT(*) FROM daily WHERE trade_date = '{trade_date}'"
                    result = pd.read_sql(check_query, con=db_handler.get_engine())
                    if result.iloc[0, 0] > 0:
                        print(f"{trade_date} 数据已存在，跳过")
                        continue
                except Exception as e:
                    # 表不存在，继续执行
                    print(f"表不存在，开始同步 {trade_date} 的数据")

                # 获取数据
                df = get_daily_data(pro, trade_date)
                if df.empty:
                    print(f"{trade_date} 数据为空，跳过")
                    continue

                # 插入数据库
                success = db_handler.insert_data('daily', df, trade_date)
                if success:
                    total_records += len(df)
                    success_dates.append(trade_date)
                    print(f"{trade_date} 同步完成，{len(df)} 条记录")

                # 延迟避免请求过于频繁
                time.sleep(0.5)

            except Exception as e:
                print(f"同步 {trade_date} 失败: {e}")
                continue

        print(f"日线数据同步完成，共 {len(success_dates)} 个交易日，{total_records} 条记录")
        return True

    except Exception as e:
        print(f"同步失败: {e}")
        return False

def sync_date_range(start_date, end_date):
    """同步指定日期范围"""
    print("=" * 50)
    print(f"同步日线数据 ({start_date} - {end_date})")
    print("=" * 50)

    try:
        # 初始化
        db_handler = get_db_handler()

        # 检查TUSHARE_TOKEN
        token = os.getenv('TUSHARE_TOKEN')
        if not token:
            print("错误: 请设置 TUSHARE_TOKEN 环境变量")
            return False

        # 初始化Tushare
        ts.set_token(token)
        pro = ts.pro_api()

        # 获取交易日
        trade_dates = get_trade_dates(pro, start_date, end_date)
        if not trade_dates:
            print("指定日期范围内没有交易日")
            return False

        print(f"共需同步 {len(trade_dates)} 个交易日")

        # 同步数据
        total_records = 0
        success_dates = []

        for trade_date in trade_dates:
            try:
                # 检查是否已存在
                try:
                    check_query = f"SELECT COUNT(*) FROM daily WHERE trade_date = '{trade_date}'"
                    result = pd.read_sql(check_query, con=db_handler.get_engine())
                    if result.iloc[0, 0] > 0:
                        print(f"{trade_date} 数据已存在，跳过")
                        continue
                except Exception as e:
                    # 表不存在，继续执行
                    print(f"表不存在，开始同步 {trade_date} 的数据")

                # 获取数据
                df = get_daily_data(pro, trade_date)
                if df.empty:
                    print(f"{trade_date} 数据为空，跳过")
                    continue

                # 插入数据库
                success = db_handler.insert_data('daily', df, trade_date)
                if success:
                    total_records += len(df)
                    success_dates.append(trade_date)
                    print(f"{trade_date} 同步完成，{len(df)} 条记录")

                time.sleep(0.5)

            except Exception as e:
                print(f"同步 {trade_date} 失败: {e}")
                continue

        print(f"指定日期范围同步完成，共 {len(success_dates)} 个交易日，{total_records} 条记录")
        return True

    except Exception as e:
        print(f"同步失败: {e}")
        return False

if __name__ == "__main__":
    try:
        if len(sys.argv) > 1 and sys.argv[1] == "range" and len(sys.argv) == 4:
            success = sync_date_range(sys.argv[2], sys.argv[3])
        else:
            success = sync_daily()

        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n用户中断")
        sys.exit(0)
    except Exception as e:
        print(f"程序错误: {e}")
        sys.exit(1)