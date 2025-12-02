#!/usr/bin/env python3
"""
数据库处理模块
支持自动创建表结构和索引
"""
import os
import sys
import logging
import threading
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
        # 使用root用户连接数据库
        self.user = 'root'
        self.password = password or os.getenv('MYSQL_ROOT_PASSWORD') or os.getenv('MYSQL_PASSWORD', '')
        self.database = database or os.getenv('MYSQL_DATABASE', 'tushare_sync')

        self.engine = None
        self._inspector = None
        # Set of known existing tables to avoid repeated DB inspections
        self._existing_tables = set()
        # Lock to protect _existing_tables and inspector updates (thread-safe)
        self._table_lock = threading.RLock()

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
            # Cache inspector and current table list to reduce repeated introspection
            try:
                self._inspector = inspect(self.engine)
                self._existing_tables = set(self._inspector.get_table_names())
            except Exception:
                # If inspector creation fails, leave cache empty; methods will fall back to fresh inspectors
                self._inspector = None
                self._existing_tables = set()

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
            # First check local cache to avoid DB round-trip
            with self._table_lock:
                if table_name in self._existing_tables:
                    # 缓存命中不必要频繁输出到 INFO，改为 DEBUG 以减少噪音
                    logger.debug(f"表 {table_name} 已存在 (缓存)")
                    return

            # Fallback to inspector check if cache doesn't contain the table
            inspector = self._inspector or inspect(self.engine)
            if not inspector.has_table(table_name):
                logger.info(f"创建表: {table_name}")

                # 创建表
                df.to_sql(table_name, self.engine, if_exists='replace', index=False)

                # 创建索引
                self._create_indexes(table_name, df.columns.tolist())

                # 更新缓存
                try:
                    with self._table_lock:
                        if self._inspector:
                            # refresh inspector internal state
                            self._existing_tables.add(table_name)
                        else:
                            # create and cache a fresh inspector
                            self._inspector = inspect(self.engine)
                            self._existing_tables = set(self._inspector.get_table_names())
                except Exception:
                    # ignore caching failure
                    pass
            else:
                logger.info(f"表 {table_name} 已存在")
                # Update cache to avoid future checks
                try:
                    with self._table_lock:
                        self._existing_tables.add(table_name)
                except Exception:
                    pass

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

        definition_table = table_name
        if definition_table not in index_definitions and definition_table.endswith('_new'):
            candidate = definition_table[:-4]
            if candidate in index_definitions:
                definition_table = candidate

        if definition_table in index_definitions:
            for index_def in index_definitions[definition_table]:
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
                                if definition_table in ['daily', 'daily_qfq'] and 'ts_code' in index_type:
                                    # TEXT字段需要指定索引长度
                                    index_sql = f"CREATE INDEX {index_name_full} ON {table_name} (ts_code(20), trade_date(20))"
                                else:
                                    columns_part = index_type.split('(', 1)[1]
                                    if not columns_part.startswith('('):
                                        columns_part = f"({columns_part}"
                                    index_sql = f"CREATE INDEX {index_name_full} ON {table_name} {columns_part}"
                            else:
                                # 单列索引
                                if table_name == 'stock_basic' and index_name in ['symbol', 'industry', 'area']:
                                    # TEXT字段需要指定索引长度
                                    index_sql = f"CREATE INDEX {index_name_full} ON {table_name} ({index_name}(50))"
                                elif definition_table in ['daily', 'daily_qfq'] and index_name in ['ts_code', 'trade_date']:
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
        """插入数据到数据库，自动避免重复"""
        try:
            if data.empty:
                logger.warning(f"数据为空，跳过插入: {table_name}")
                return False

            # 确保表存在：如果缓存命中则跳过额外检查以减少重复开销
            with self._table_lock:
                table_known = table_name in self._existing_tables

            if not table_known:
                self.ensure_table_exists(table_name, data)

            # 插入数据，避免重复
            rows_inserted = len(data)

            # 对于daily_qfq和daily表，使用INSERT IGNORE避免重复数据
            if table_name in ['daily_qfq', 'daily']:
                # 先创建临时表
                temp_table = f"temp_{table_name}_{int(datetime.now().timestamp())}"
                data.to_sql(temp_table, self.engine, if_exists='replace', index=False)

                # 使用INSERT IGNORE插入数据，利用唯一约束避免重复
                with self.engine.connect() as conn:
                    # 确保有唯一约束
                    try:
                        conn.execute(text(f"""
                            ALTER TABLE {table_name}
                            ADD UNIQUE KEY uniq_ts_trade_date (ts_code(20), trade_date(20))
                        """))
                        conn.commit()
                        logger.info(f"添加唯一约束到表 {table_name}")
                    except Exception as e:
                        # 约束可能已存在，忽略错误
                        logger.debug(f"添加唯一约束失败（可能已存在）: {e}")

                    # 使用INSERT IGNORE插入
                    insert_sql = f"""
                        INSERT IGNORE INTO {table_name}
                        SELECT * FROM {temp_table}
                    """
                    result = conn.execute(text(insert_sql))
                    conn.commit()
                    rows_affected = result.rowcount

                    # 删除临时表
                    conn.execute(text(f"DROP TABLE {temp_table}"))
                    conn.commit()

                    logger.info(f"成功插入 {rows_affected} 行数据到 {table_name}（跳过重复数据）")
            else:
                # 其他表使用原有逻辑
                data.to_sql(table_name, self.engine, if_exists='append', index=False)
                rows_affected = rows_inserted
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
            # Use cached table list if available
            if table_name not in self._existing_tables:
                # fallback to inspector check
                inspector = self._inspector or inspect(self.engine)
                exists = inspector.has_table(table_name)
            else:
                exists = True

            if exists:
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
            # 如果缓存中没有该表名，先做一次检查以避免后续重复检查
            if table_name not in self._existing_tables:
                inspector = self._inspector or inspect(self.engine)
                if not inspector.has_table(table_name):
                    return None
                # update cache
                try:
                    self._existing_tables.add(table_name)
                except Exception:
                    pass

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
