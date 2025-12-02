#!/usr/bin/env python3
"""
前复权日线数据同步（写入 new 表，不删除旧表）
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
from tqdm import tqdm

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db_handler import get_db_handler

warnings.filterwarnings("ignore", category=FutureWarning, module=r"tushare\.pro\.data_pro")


TARGET_TABLE = "daily_qfq_new"   # ************ ⬅⬅⬅ 现在写入 NEW 表 ************


def get_latest_trade_date(pro):
    try:
        now = datetime.now()
        today = now.strftime('%Y%m%d')
        morning_9am = now.replace(hour=16, minute=0, second=0, microsecond=0)
        start_date = (now - timedelta(days=30)).strftime('%Y%m%d')

        trade_cal = pro.trade_cal(exchange='', start_date=start_date, end_date=today)
        trade_cal = trade_cal[trade_cal['is_open'] == 1]

        if trade_cal.empty:
            return today

        return trade_cal.iloc[1]['cal_date'] if now < morning_9am else trade_cal.iloc[0]['cal_date']
    except:
        return datetime.now().strftime('%Y%m%d')


def get_stock_codes(db_handler):
    try:
        query = "SELECT ts_code FROM stock_basic"
        result = pd.read_sql(query, con=db_handler.get_engine())
        return result['ts_code'].tolist()
    except:
        return []


def get_qfq_data(ts_code, start_date, max_retries=3):
    for attempt in range(max_retries):
        try:
            df = ts.pro_bar(ts_code=ts_code, adj='qfq', start_date=start_date)
            if df is None:
                return pd.DataFrame()
            if not df.empty:
                df['ts_code'] = ts_code
            return df
        except:
            time.sleep((attempt + 1) * 2)
    return pd.DataFrame()


def need_sync_daily_qfq(db_handler, pro):
    """保留原逻辑，但检查 old 表 daily_qfq"""
    try:
        latest_trade_date = get_latest_trade_date(pro)
        total_stocks = len(get_stock_codes(db_handler))

        query = f"SELECT COUNT(DISTINCT ts_code) as count FROM daily_qfq WHERE trade_date = '{latest_trade_date}'"
        result = pd.read_sql(query, con=db_handler.get_engine())
        latest_count = result.iloc[0]['count']
        ratio = latest_count / total_stocks if total_stocks else 0

        if ratio >= 0.9:
            print(f"数据完整 {latest_count}/{total_stocks} ({ratio:.1%})")
            return False
        else:
            print(f"数据不完整 {latest_count}/{total_stocks} ({ratio:.1%})，需要同步")
            return True
    except:
        return True


def sync_daily_qfq(max_workers=16):
    print("=" * 50)
    print("同步前复权日线数据 → 写入 new 表，不删除旧表")
    print("=" * 50)

    try:
        db_handler = get_db_handler()

        token = os.getenv('TUSHARE_TOKEN')
        if not token:
            print("错误: 请设置 TUSHARE_TOKEN")
            return False

        ts.set_token(token)
        pro = ts.pro_api()

        stock_codes = get_stock_codes(db_handler)
        if not stock_codes:
            print("无股票代码")
            return False

        print(f"共 {len(stock_codes)} 只股票")

        end_date = get_latest_trade_date(pro)
        print(f"同步到: {end_date}")

        if not need_sync_daily_qfq(db_handler, pro):
            print("数据已最新，不同步")
            return True

        # ************ 🚫 不删除 old 表 daily_qfq ************
        # ************ ✔ 创建/覆盖 NEW 表 daily_qfq_new ************
        print(f"准备写入新表: {TARGET_TABLE}")

        # 确保 new 表是干净的
        with db_handler.get_engine().connect() as conn:
            print(f"清空或创建 {TARGET_TABLE} 表...")
            conn.execute(text(f"DROP TABLE IF EXISTS {TARGET_TABLE}"))
            conn.commit()

        # 清理缓存
        with db_handler._table_lock:
            if TARGET_TABLE in db_handler._existing_tables:
                db_handler._existing_tables.remove(TARGET_TABLE)

        total_records = 0
        success_count = 0

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            future_to_stock = {
                executor.submit(get_qfq_data, ts_code, '20190101'): ts_code
                for ts_code in stock_codes
            }

            print(f"已提交 {len(future_to_stock)} 个任务")

            first_batch = True
            processed = 0
            failed_codes = []

            with tqdm(total=len(future_to_stock), desc="同步进度", unit="stock") as pbar:
                for future in as_completed(future_to_stock):
                    ts_code = future_to_stock[future]
                    try:
                        df = future.result()
                        if not df.empty:
                            if first_batch:
                                df.to_sql(TARGET_TABLE, db_handler.get_engine(),
                                          if_exists='replace', index=False)

                                db_handler._create_indexes(TARGET_TABLE, df.columns.tolist())
                                first_batch = False
                            else:
                                df.to_sql(TARGET_TABLE, db_handler.get_engine(),
                                          if_exists='append', index=False)

                            total_records += len(df)
                            success_count += 1
                        else:
                            failed_codes.append(f"{ts_code}:无数据")

                    except Exception as e:
                        failed_codes.append(f"{ts_code}:{e}")

                    processed += 1
                    pbar.update(1)
                    pbar.set_postfix({"成功数": success_count, "已处理": processed})

        fail_count = len(failed_codes)
        print("同步完成:")
        print(f"  成功: {success_count}")
        print(f"  失败/无数据: {fail_count}")
        print(f"  总记录: {total_records}")
        if failed_codes:
            preview = failed_codes[:10]
            print("  失败样本:")
            for item in preview:
                print(f"    {item}")
            if fail_count > len(preview):
                print(f"    ... 其余 {fail_count - len(preview)} 条")

        return True

    except Exception as e:
        print(f"同步失败: {e}")
        return False


def sync_single_stock(ts_code):
    print("=" * 50)
    print(f"同步单个股票 → 写入 {TARGET_TABLE}")
    print("=" * 50)

    try:
        db_handler = get_db_handler()
        token = os.getenv('TUSHARE_TOKEN')
        if not token:
            print("请设置 TUSHARE_TOKEN")
            return False

        ts.set_token(token)

        max_date = db_handler.get_max_date(TARGET_TABLE)
        start_date = (pd.to_datetime(str(max_date)) + timedelta(days=1)).strftime('%Y%m%d') if max_date else '20190101'

        print(f"从 {start_date} 开始同步")

        df = get_qfq_data(ts_code, start_date)
        if df.empty:
            print("无新数据")
            return True

        ok = db_handler.insert_data(TARGET_TABLE, df, ts_code)
        print(f"{ts_code} 同步 {'成功' if ok else '失败'}")
        return ok

    except Exception as e:
        print(f"错误: {e}")
        return False


if __name__ == "__main__":
    try:
        if len(sys.argv) == 2 and sys.argv[1].startswith(
                ('000', '001', '002', '300', '600', '601', '603', '605', '688', '689')):
            success = sync_single_stock(sys.argv[1])
        elif len(sys.argv) == 3 and sys.argv[1] == "workers":
            success = sync_daily_qfq(int(sys.argv[2]))
        else:
            success = sync_daily_qfq()

        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n用户中断")
        sys.exit(0)
