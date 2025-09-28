#!/usr/bin/env python3
"""
股票基本信息同步
"""
import os
import sys
import tushare as ts
import pandas as pd
from datetime import datetime

# 添加当前目录到Python路径
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from db_handler import get_db_handler

def sync_stock_basic():
    """同步股票基本信息"""
    print("=" * 50)
    print("同步股票基本信息")
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

        print("正在获取股票基本信息...")

        # 获取股票基本信息
        df = pro.stock_basic(
            exchange='',
            list_status='L',
            fields='ts_code, symbol, name, area, industry, fullname, enname, cnspell, market, exchange, curr_type, list_status, list_date, delist_date, is_hs, act_name, act_ent_type'
        )

        if df.empty:
            print("警告: 获取到的数据为空")
            return False

        print(f"获取到 {len(df)} 条股票基本信息")

        # 清空现有数据
        print("清空现有数据...")
        db_handler.empty_table('stock_basic')

        # 插入新数据
        print("插入新数据...")
        success = db_handler.insert_data('stock_basic', df)

        if success:
            print("股票基本信息同步完成")
            return True
        else:
            print("股票基本信息同步失败")
            return False

    except Exception as e:
        print(f"同步失败: {e}")
        return False

if __name__ == "__main__":
    try:
        success = sync_stock_basic()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        print("\n用户中断")
        sys.exit(0)
    except Exception as e:
        print(f"程序错误: {e}")
        sys.exit(1)