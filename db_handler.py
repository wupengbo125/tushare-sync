#!/usr/bin/env python3
"""
数据库处理模块
支持自动创建表结构和索引
"""
import os
import sys
import logging
from typing import Optional, Dict, Any
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import Engine
import pandas as pd
from datetime import datetime

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('db_handler.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class DatabaseHandler:
    """数据库处理器"""

    def __init__(self, host: str = None, port: int = None, user: str = None,
                 password: str = None, database: str = None):
        """初始化数据库连接"""
        self.host = host or os.getenv('MYSQL_HOST', 'localhost')
        self.port = port or int(os.getenv('MYSQL_PORT', 3306))
        self.user = user or os.getenv('username', 'root')
        self.password = password or os.getenv('password', '')
        self.database = database or os.getenv('MYSQL_DATABASE', 'tushare_sync')

        self.engine = None
        self._connect()

    def _connect(self):
        """建立数据库连接"""
        try:
            connection_string = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}?charset=utf8mb4"
            self.engine = create_engine(connection_string, pool_pre_ping=True, pool_recycle=3600)

            # 测试连接
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))

            logger.info(f"数据库连接成功: {self.host}:{self.port}/{self.database}")

        except Exception as e:
            logger.error(f"数据库连接失败: {e}")
            raise

    def create_database_if_not_exists(self):
        """创建数据库（如果不存在）"""
        try:
            # 不指定数据库连接，用于创建数据库
            connection_string = f"mysql+pymysql://{self.user}:{self.password}@{self.host}:{self.port}"
            engine = create_engine(connection_string)

            with engine.connect() as conn:
                conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{self.database}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci"))
                conn.commit()

            logger.info(f"数据库 {self.database} 创建或已存在")

        except Exception as e:
            logger.error(f"创建数据库失败: {e}")
            raise

    def ensure_table_exists(self, table_name: str, df: pd.DataFrame):
        """确保表存在，如果不存在则创建（包括索引）"""
        try:
            inspector = inspect(self.engine)

            # 检查表是否存在
            if not inspector.has_table(table_name):
                logger.info(f"创建表: {table_name}")

                # 创建表
                df.to_sql(table_name, self.engine, if_exists='replace', index=False)

                # 创建索引
                self._create_indexes(table_name, df.columns.tolist())

            else:
                logger.info(f"表 {table_name} 已存在")

        except Exception as e:
            logger.error(f"确保表存在失败: {e}")
            raise

    def _create_indexes(self, table_name: str, columns: list):
        """为表创建索引"""
        index_definitions = {
            'stock_basic': [
                ('ts_code', 'PRIMARY KEY'),
                ('symbol', 'INDEX'),
                ('industry', 'INDEX'),
                ('area', 'INDEX'),
            ],
            'daily': [
                ('ts_code', 'INDEX'),
                ('trade_date', 'INDEX'),
                ('ts_code_trade_date', 'INDEX (ts_code, trade_date)'),
            ],
            'daily_qfq': [
                ('ts_code', 'INDEX'),
                ('trade_date', 'INDEX'),
                ('ts_code_trade_date', 'INDEX (ts_code, trade_date)'),
            ],
            'stock_hot_rank': [
                ('stock_code', 'INDEX'),
                ('record_date', 'INDEX'),
                ('ranking', 'INDEX'),
                ('stock_code_record_date', 'INDEX (stock_code, record_date)'),
            ]
        }

        if table_name in index_definitions:
            for index_def in index_definitions[table_name]:
                index_name = index_def[0]
                index_type = index_def[1]

                try:
                    if index_type == 'PRIMARY KEY':
                        # 主键已经在创建表时处理
                        continue
                    elif index_type.startswith('INDEX'):
                        # 检查索引是否已存在
                        inspector = inspect(self.engine)
                        existing_indexes = [idx['name'] for idx in inspector.get_indexes(table_name)]
                        index_name_full = f"idx_{table_name}_{index_name}"

                        if index_name_full not in existing_indexes:
                            if '(' in index_type:
                                # 复合索引
                                if table_name in ['daily', 'daily_qfq'] and 'ts_code' in index_type:
                                    # TEXT字段需要指定索引长度
                                    index_sql = f"CREATE INDEX {index_name_full} ON {table_name} (ts_code(20), trade_date(20))"
                                else:
                                    index_sql = f"CREATE INDEX {index_name_full} ON {table_name} {index_type.split('(', 1)[1]}"
                            else:
                                # 单列索引
                                if table_name == 'stock_basic' and index_name in ['symbol', 'industry', 'area']:
                                    # TEXT字段需要指定索引长度
                                    index_sql = f"CREATE INDEX {index_name_full} ON {table_name} ({index_name}(50))"
                                elif table_name in ['daily', 'daily_qfq'] and index_name in ['ts_code', 'trade_date']:
                                    # TEXT字段需要指定索引长度
                                    index_sql = f"CREATE INDEX {index_name_full} ON {table_name} ({index_name}(20))"
                                else:
                                    index_sql = f"CREATE INDEX {index_name_full} ON {table_name} ({index_name})"

                            try:
                                with self.engine.connect() as conn:
                                    conn.execute(text(index_sql))
                                    conn.commit()
                                logger.info(f"创建索引: {index_name_full}")
                            except Exception as e:
                                logger.warning(f"创建索引失败 {index_name_full}: {e}")
                        else:
                            logger.info(f"索引已存在: {index_name_full}")

                except Exception as e:
                    logger.warning(f"处理索引 {index_name} 时发生错误: {e}")

    def insert_data(self, table_name: str, data: pd.DataFrame, record_id: str = None):
        """插入数据到数据库"""
        try:
            if data.empty:
                logger.warning(f"数据为空，跳过插入: {table_name}")
                return False

            # 确保表存在
            self.ensure_table_exists(table_name, data)

            # 插入数据
            rows_inserted = len(data)
            data.to_sql(table_name, self.engine, if_exists='append', index=False)

            logger.info(f"成功插入 {rows_inserted} 行数据到 {table_name}")
            if record_id:
                logger.info(f"记录ID: {record_id}")

            return True

        except Exception as e:
            logger.error(f"插入数据失败 {table_name}: {e}")
            return False

    def empty_table(self, table_name: str):
        """清空表"""
        try:
            inspector = inspect(self.engine)
            if inspector.has_table(table_name):
                with self.engine.connect() as conn:
                    conn.execute(text(f"TRUNCATE TABLE {table_name}"))
                    conn.commit()
                logger.info(f"表 {table_name} 已清空")
            else:
                logger.info(f"表 {table_name} 不存在，跳过清空")
            return True
        except Exception as e:
            logger.error(f"清空表失败 {table_name}: {e}")
            return False

    def get_max_date(self, table_name: str, date_column: str = 'trade_date'):
        """获取表中的最大日期"""
        try:
            # 检查表是否存在
            inspector = inspect(self.engine)
            if table_name not in inspector.get_table_names():
                return None

            query = f"SELECT MAX({date_column}) as max_date FROM {table_name}"
            result = pd.read_sql(query, self.engine)
            max_date = result.iloc[0, 0]
            return max_date
        except Exception as e:
            logger.error(f"获取最大日期失败: {e}")
            return None

    def get_engine(self) -> Engine:
        """获取数据库引擎"""
        return self.engine

    def close(self):
        """关闭数据库连接"""
        if self.engine:
            self.engine.dispose()
            logger.info("数据库连接已关闭")

# 全局数据库处理器实例
_db_handler = None

def get_db_handler() -> DatabaseHandler:
    """获取数据库处理器实例"""
    global _db_handler
    if _db_handler is None:
        _db_handler = DatabaseHandler()
    return _db_handler

def get_db_engine():
    """获取数据库引擎（保持向后兼容）"""
    return get_db_handler().get_engine()

def insert_data(table_name: str, data: pd.DataFrame, record: str = None):
    """插入数据（保持向后兼容）"""
    return get_db_handler().insert_data(table_name, data, record)

def empty_table(table_name: str):
    """清空表（保持向后兼容）"""
    return get_db_handler().empty_table(table_name)

if __name__ == "__main__":
    # 测试数据库连接
    try:
        handler = get_db_handler()
        handler.create_database_if_not_exists()
        print("数据库连接测试成功!")
    except Exception as e:
        print(f"数据库连接测试失败: {e}")
        sys.exit(1)