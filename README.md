# TushareSync 数据同步工具

简洁的股票数据同步工具，支持独立同步各个数据表。

## 目录结构

```
tushare-sync/
├── db_handler.py              # 数据库处理模块
├── sync_stock_basic.py        # 股票基本信息同步
├── sync_daily.py             # 日线数据同步
├── sync_daily_qfq.py         # 前复权日线数据同步
├── sync_stock_hot_rank.py    # 股票人气排名同步
├── sync_all.sh               # 全量同步脚本
├── start_mysql.sh            # Linux/Mac 启动MySQL脚本
├── requirements.txt          # Python依赖
└── README.md                # 说明文档
```

## 使用教程

### 1. 环境准备

#### 1.1 创建Anaconda环境

```bash
conda env remove -n tushare
# 创建新环境
conda create -n tushare python=3.10

# 激活环境
conda activate tushare

# 安装依赖
pip install -r requirements.txt
```

#### 1.2 启动MySQL

```bash
sh start_mysql.sh
```

#### 1.3 设置环境变量

```bash
export TUSHARE_TOKEN=your_token_here
export MYSQL_HOST=localhost
export MYSQL_PORT=3306
export username=your_mysql_username
export password=your_mysql_password
export MYSQL_DATABASE=tushare_sync
```

### 2. 数据同步

#### 2.1 单表同步

```bash
# 股票基本信息（全量同步）
python stock_basic.py

# 日线数据（从数据库最新日期+1天开始）
python daily.py

# 指定日期范围同步日线数据
python daily.py range 20240101 20240131

# 前复权日线数据（从数据库最新日期+1天开始）
python sync_daily_qfq.py

# 单个股票前复权数据
python sync_daily_qfq.py 000001.SZ

# 股票人气排名（当日数据，保留90天）
python sync_stock_hot_rank.py

# 定时同步人气排名（每30分钟一次，最多8小时）
python sync_stock_hot_rank.py schedule 30 8
```

#### 2.2 时间控制说明

**股票基本信息 (sync_stock_basic.py)**
- 全量同步，无时间控制

**日线数据 (sync_daily.py)**
- 默认：从数据库最新日期+1天开始同步到今天
- 空数据库：从20100101开始同步
- 可指定日期范围：`python sync_daily.py range 20240101 20240131`

**前复权日线数据 (sync_daily_qfq.py)**
- 默认：从数据库最新日期+1天开始同步到最新交易日
- 空数据库：从20100101开始同步
- 可指定单个股票：`python sync_daily_qfq.py 000001.SZ`

**股票人气排名 (sync_stock_hot_rank.py)**
- 只同步当天数据
- 自动保留最近90天数据
- 支持定时同步模式

#### 2.3 全量同步

```bash
# 同步所有表
sh sync_all.sh
```

### 3. 环境变量说明

#### 必需的环境变量

| 变量名 | 说明 | 示例值 |
|--------|------|--------|
| `TUSHARE_TOKEN` | Tushare API的访问令牌 | `your_token_here` |
| `username` | MySQL数据库用户名 | `your_mysql_username` |
| `password` | MySQL数据库密码 | `your_mysql_password` |

#### 可选的环境变量

| 变量名 | 说明 | 默认值 |
|--------|------|--------|
| `MYSQL_HOST` | MySQL服务器地址 | `localhost` |
| `MYSQL_PORT` | MySQL服务器端口 | `3306` |
| `MYSQL_DATABASE` | MySQL数据库名称 | `tushare_sync` |

### 4. 数据库优化

系统自动创建以下索引以优化查询性能：

- **stock_basic**: ts_code(主键), symbol, industry, area
- **daily**: ts_code, trade_date, (ts_code, trade_date)
- **daily_qfq**: ts_code, trade_date, (ts_code, trade_date)
- **stock_hot_rank**: stock_code, record_date, ranking, (stock_code, record_date)

系统根据Tushare返回的数据自动创建表结构，无需手动建表。

### 5. 日志文件

每个同步脚本都会生成日志文件：
- `db_handler.log` - 数据库操作日志
- 控制台输出 - 同步进度日志

### 6. 注意事项

1. 首次运行前请确保已设置TUSHARE_TOKEN
2. 建议先同步stock_basic表，其他表依赖其中的股票代码
3. 前复权数据同步较慢，建议在服务器上运行
4. 定时同步功能建议在后台运行

### 7. 故障排除

#### 数据库连接失败
- 检查MySQL容器是否运行
- 验证环境变量设置
- 检查端口是否被占用

#### Tushare API错误
- 验证TUSHARE_TOKEN是否正确
- 检查网络连接
- 确认API额度是否足够

#### 内存不足
- 减少同步的日期范围
- 降低前复权同步的线程数
- 增加系统内存

## 数据库索引

系统自动创建以下索引以优化查询性能：

- **stock_basic**: ts_code(主键), symbol, industry, area
- **daily**: ts_code, trade_date, (ts_code, trade_date)
- **daily_qfq**: ts_code, trade_date, (ts_code, trade_date)
- **stock_hot_rank**: stock_code, record_date, ranking, (stock_code, record_date)

## 自动表结构

系统根据Tushare返回的数据自动创建表结构，无需手动建表。

## 日志文件

每个同步脚本都会生成日志文件：
- `db_handler.log` - 数据库操作日志
- 控制台输出 - 同步进度日志

## 注意事项

1. 首次运行前请确保已设置TUSHARE_TOKEN
2. 建议先同步stock_basic表，其他表依赖其中的股票代码
3. 前复权数据同步较慢，建议在服务器上运行
4. 定时同步功能建议在后台运行

## 故障排除

### 数据库连接失败
- 检查MySQL容器是否运行
- 验证环境变量设置
- 检查端口是否被占用

### Tushare API错误
- 验证TUSHARE_TOKEN是否正确
- 检查网络连接
- 确认API额度是否足够

### 内存不足
- 减少同步的日期范围
- 降低前复权同步的线程数
- 增加系统内存