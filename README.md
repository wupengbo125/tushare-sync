# TushareSync 数据同步工具

简洁的股票数据同步工具，支持独立同步各个数据表。

## 目录结构

```
tushare-sync/
├── db_handler.py              # 数据库处理模块
├── stock_basic.py             # 股票基本信息同步
├── daily.py                   # 日线数据同步
├── daily_qfq.py               # 前复权日线数据同步
├── check_daily_qfq.py         # 检查前复权数据完整性
├── sync_all.sh                # 全量同步脚本
├── .env                       # 环境变量配置文件
├── sync_all.log               # cron执行日志
├── requirements.txt           # Python依赖
└── README.md                  # 说明文档
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

创建 `.env` 文件配置环境变量：

```bash
# 复制示例配置
cp .env.example .env
# 编辑 .env 文件，填入你的配置信息
```

或者手动创建 `.env` 文件：

```bash
# 数据库配置
export MYSQL_DATABASE=tushare_sync
export MYSQL_HOST=localhost
export MYSQL_PORT=3306
export MYSQL_USER=your_mysql_username
export MYSQL_PASSWORD=your_mysql_password
export MYSQL_ROOT_PASSWORD=your_root_password

# Tushare API配置
export TUSHARE_TOKEN=your_token_here

# Python 环境配置（可选）
export UV_INDEX_URL=https://mirrors.aliyun.com/pypi/simple/
```

**注意**：`.env` 文件包含敏感信息，已加入 `.gitignore`，不会提交到版本控制系统。

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
python daily_qfq.py

# 单个股票前复权数据
python daily_qfq.py 000001.SZ


```

#### 2.2 时间控制说明

**股票基本信息 (stock_basic.py)**
- 全量同步，无时间控制

**日线数据 (daily.py)**
- 默认：从数据库最新日期+1天开始同步到今天
- 空数据库：从20100101开始同步
- 可指定日期范围：`python daily.py range 20240101 20240131`

**前复权日线数据 (daily_qfq.py)**
- 默认：从数据库最新日期+1天开始同步到最新交易日
- 空数据库：从20100101开始同步
- 可指定单个股票：`python daily_qfq.py 000001.SZ`

**数据完整性检查 (check_daily_qfq.py)**
- 检查前复权数据的完整性和一致性

#### 2.3 全量同步

```bash
# 同步所有表
sh sync_all.sh
```

#### 2.4 自动化定时同步

使用 crontab 设置定时同步，每分钟执行一次数据同步：

```bash
# 编辑 crontab
crontab -e

# 添加以下行（请根据你的实际路径修改）
00 17 * * * /bin/bash -c 'source /home/pengbo/miniconda3/bin/activate base && cd /home/pengbo/github/tushare-sync/ && sh sync_all.sh' > /home/pengbo/github/tushare-sync/sync_all.log 2>&1
```


**查看 cron 日志**：

```bash
# 查看实时日志
tail -f /home/pengbo/github/tushare-sync/sync_all.log

```


### 3. 环境变量说明

#### 必需的环境变量

| 变量名 | 说明 | 示例值 |
|--------|------|--------|
| `TUSHARE_TOKEN` | Tushare API的访问令牌 | `your_token_here` |
| `MYSQL_USER` | MySQL数据库用户名 | `your_mysql_username` |
| `MYSQL_PASSWORD` | MySQL数据库密码 | `your_mysql_password` |

#### 可选的环境变量

| 变量名 | 说明 | 默认值 |
|--------|------|--------|
| `MYSQL_HOST` | MySQL服务器地址 | `localhost` |
| `MYSQL_PORT` | MySQL服务器端口 | `3306` |
| `MYSQL_DATABASE` | MySQL数据库名称 | `tushare_sync` |
| `MYSQL_ROOT_PASSWORD` | MySQL root密码 | - |
| `UV_INDEX_URL` | PyPI镜像地址 | `https://mirrors.aliyun.com/pypi/simple/` |

### 4. 数据库优化

系统自动创建以下索引以优化查询性能：

- **stock_basic**: ts_code(主键), symbol, industry, area
- **daily**: ts_code, trade_date, (ts_code, trade_date)
- **daily_qfq**: ts_code, trade_date, (ts_code, trade_date)

系统根据Tushare返回的数据自动创建表结构，无需手动建表。

### 5. 日志文件

每个同步脚本都会生成日志文件：
- `db_handler.log` - 数据库操作日志
- `sync_all.log` - crontab执行日志
- 控制台输出 - 同步进度日志

### 6. 注意事项

1. 首次运行前请确保已设置TUSHARE_TOKEN并创建 `.env` 文件
2. 建议先同步stock_basic表，其他表依赖其中的股票代码
3. 前复权数据同步较慢，建议在服务器上运行
4. 定时同步功能建议在后台运行
5. 使用 crontab 时注意环境变量加载问题（已在 `sync_all.sh` 中修复）

### 7. 故障排除

#### 数据库连接失败
- 检查MySQL服务是否运行
- 验证 `.env` 文件中的环境变量设置
- 检查端口是否被占用
- 确认数据库用户权限

#### Crontab 执行失败
- 检查 `sync_all.log` 日志文件
- 验证 conda 环境路径是否正确

#### Node.js `punycode` DeprecationWarning
- `sync_ths_concept_list.py` 使用的 `pywencai` 可能触发该告警，不影响同步结果
- 默认已抑制该类告警；如需显示可设置：`TUSHARE_SYNC_SHOW_NODE_DEPRECATION=1`
- 确认脚本文件权限 (`chmod +x sync_all.sh`)
- 检查 `.env` 文件是否存在且格式正确

#### Tushare API错误
- 验证TUSHARE_TOKEN是否正确
- 检查网络连接
- 确认API额度是否足够

#### 内存不足
- 减少同步的日期范围
- 降低前复权同步的线程数
- 增加系统内存
