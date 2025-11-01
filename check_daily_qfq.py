#!/usr/bin/env python3
"""
检查当日前复权日线数据
"""

import os
import sys
import tushare as ts
import pandas as pd
from datetime import datetime, timedelta
from db_handler import get_db_handler

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

def check_daily_qfq_data():
    """检查当日前复权日线数据"""
    print("=" * 50)
    print("检查当日前复权日线数据")
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

        # 获取最新交易日
        latest_trade_date = get_latest_trade_date(pro)
        print(f"最新交易日: {latest_trade_date}")

        # 查询当日数据
        try:
            query = f"SELECT COUNT(*) as count FROM daily_qfq WHERE trade_date = '{latest_trade_date}'"
            result = pd.read_sql(query, con=db_handler.get_engine())
            count = result.iloc[0]['count']

            if count > 0:
                print(f"当日数据检查完成: 共 {count} 条记录")

                # 显示部分数据示例
                sample_query = f"SELECT ts_code, trade_date, open, high, low, close, vol FROM daily_qfq WHERE trade_date = '{latest_trade_date}' LIMIT 5"
                sample_data = pd.read_sql(sample_query, con=db_handler.get_engine())
                print("\n数据示例:")
                print(sample_data.to_string(index=False))
            else:
                print(f"当日数据检查完成: 无数据记录")

            # 在最后打印最新交易日
            print(f"最新交易日: {latest_trade_date}")
            return True
        except Exception as e:
            print(f"查询当日数据失败: {e}")
            # 即使查询失败，也打印最新交易日
            print(f"最新交易日: {latest_trade_date}")
            return False

    except Exception as e:
        print(f"检查失败: {e}")
        return False

if __name__ == "__main__":
    try:
        success = check_daily_qfq_data()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n用户中断")
        sys.exit(0)
    except Exception as e:
        print(f"程序错误: {e}")
        sys.exit(1)