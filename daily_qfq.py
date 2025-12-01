#!/usr/bin/env python3
"""
前复权日线数据同步
"""
import os
import sys
import time
import tushare as ts
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import inspect, text
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

def get_latest_trade_date(pro):
    """获取最新交易日"""
    try:
        now = datetime.now()
        today = now.strftime('%Y%m%d')
        morning_9am = now.replace(hour=16, minute=0, second=0, microsecond=0)
        start_date = (now - timedelta(days=30)).strftime('%Y%m%d')

        trade_cal = pro.trade_cal(exchange='', start_date=start_date, end_date=today)
        trade_cal = trade_cal[trade_cal['is_open'] == 1]

        if trade_cal.empty:
            return today

        if now < morning_9am:
            return trade_cal.iloc[1]['cal_date']
        else:
            return trade_cal.iloc[0]['cal_date']

    except Exception as e:
        print(f"获取最新交易日失败: {e}")
        return datetime.now().strftime('%Y%m%d')

def get_stock_codes(db_handler):
    """获取所有股票代码"""
    try:
        query = "SELECT ts_code FROM stock_basic"
        result = pd.read_sql(query, con=db_handler.get_engine())
        return result['ts_code'].tolist()
    except Exception as e:
        print(f"获取股票代码失败: {e}")
        return []

def get_qfq_data(ts_code, start_date, max_retries=3):
    """获取前复权数据"""
    for attempt in range(max_retries):
        try:
            df = ts.pro_bar(ts_code=ts_code, adj='qfq', start_date=start_date)
            # ts.pro_bar may return None on some failures; normalize to empty DataFrame
            if df is None:
                return pd.DataFrame()

            if not df.empty:
                df['ts_code'] = ts_code
            return df
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * 2
                time.sleep(wait_time)
            else:
                print(f"获取 {ts_code} 数据失败: {e}")
    return pd.DataFrame()

def sync_daily_qfq(max_workers=16, target_table='daily_qfq_new'):
    """同步前复权日线数据"""
    print("=" * 50)
    print(f"同步前复权日线数据到表: {target_table}")
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

        # 获取股票代码
        stock_codes = get_stock_codes(db_handler)
        if not stock_codes:
            print("错误: 未找到股票代码，请先同步股票基本信息")
            return False

        print(f"共找到 {len(stock_codes)} 只股票")

        # 获取最新交易日
        end_date = get_latest_trade_date(pro)
        tomorrow = (datetime.strptime(end_date, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")
        print(f"同步到最新交易日: {end_date}")

        # 同步数据
        total_records = 0
        success_count = 0
        processed_count = 0

        # 不再删除旧表，直接同步到新表 daily_qfq_new
        target_table = 'daily_qfq_new'
        print(f"同步数据到新表: {target_table}")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # 提交任务 - 移除重复数据检查，直接从2019年开始同步
            future_to_stock = {}

            for ts_code in stock_codes:
                # 直接从2019年开始同步，不检查数据库中的最大日期
                start_date = '20190101'

                future = executor.submit(get_qfq_data, ts_code, start_date)
                future_to_stock[future] = ts_code

            print(f"提交了 {len(future_to_stock)} 个任务")

            # 处理结果
            first_batch = True
            for future in as_completed(future_to_stock):
                ts_code = future_to_stock[future]
                try:
                    df = future.result()
                    if not df.empty:
                        # 使用简单的 to_sql 方法，不需要重复数据处理
                        if first_batch:
                            # 第一个批次创建表，后续批次追加数据
                            df.to_sql(target_table, db_handler.get_engine(), if_exists='replace', index=False)
                            first_batch = False
                            # 创建索引
                            db_handler._create_indexes(target_table, df.columns.tolist())
                            print(f"创建 {target_table} 表及索引并插入第一批数据")
                        else:
                            df.to_sql(target_table, db_handler.get_engine(), if_exists='append', index=False)

                        total_records += len(df)
                        success_count += 1
                        print(f"{ts_code} 同步完成，{len(df)} 条记录")
                    else:
                        print(f"{ts_code} 无数据")
                except Exception as e:
                    print(f"{ts_code} 处理失败: {e}")

                processed_count += 1
                # 显示进度百分比
                percentage = (processed_count / len(future_to_stock)) * 100
                print(f"进度: {processed_count}/{len(future_to_stock)} ({percentage:.1f}%)")

        print(f"前复权数据同步完成:")
        print(f"  成功: {success_count} 只")
        print(f"  失败: {len(future_to_stock) - success_count} 只")
        print(f"  总记录: {total_records} 条")

        return True

    except Exception as e:
        print(f"同步失败: {e}")
        return False

def sync_single_stock(ts_code):
    """同步单个股票"""
    print("=" * 50)
    print(f"同步单个股票前复权数据: {ts_code}")
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

        # 获取起始日期
        max_date = db_handler.get_max_date(target_table)
        if max_date:
            start_date = (pd.to_datetime(str(max_date)) + timedelta(days=1)).strftime('%Y%m%d')
        else:
            start_date = '20190101'

        print(f"从 {start_date} 开始同步")

        # 获取数据
        df = get_qfq_data(ts_code, start_date)
        if df.empty:
            print("无新数据需要同步")
            return True

        # 插入数据库
        success = db_handler.insert_data(target_table, df, ts_code)
        if success:
            print(f"{ts_code} 同步完成，{len(df)} 条记录")
            return True
        else:
            print(f"{ts_code} 同步失败")
            return False

    except Exception as e:
        print(f"同步失败: {e}")
        return False

if __name__ == "__main__":
    try:
        if len(sys.argv) == 2 and sys.argv[1].startswith(('000', '001', '002', '300', '600', '601', '603', '605', '688', '689')):
            # 同步单个股票
            success = sync_single_stock(sys.argv[1])
        elif len(sys.argv) == 3 and sys.argv[1] == "workers":
            # 指定线程数
            max_workers = int(sys.argv[2])
            success = sync_daily_qfq(max_workers)
        elif len(sys.argv) == 3 and sys.argv[1] == "table":
            # 指定目标表
            target_table = sys.argv[2]
            success = sync_daily_qfq(target_table=target_table)
        elif len(sys.argv) == 5 and sys.argv[1] == "workers" and sys.argv[3] == "table":
            # 指定线程数和目标表
            max_workers = int(sys.argv[2])
            target_table = sys.argv[4]
            success = sync_daily_qfq(max_workers, target_table)
        else:
            # 默认同步
            success = sync_daily_qfq()

        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n用户中断")
        sys.exit(0)
    except Exception as e:
        print(f"程序错误: {e}")
        sys.exit(1)