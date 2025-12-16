# tushare-sync

把常用 A 股数据从 `tushare` / 同花顺（`pywencai` + `adata`/`akshare`）同步到 MySQL。

## 快速开始

```bash
pip install -r requirements.txt
./start_mysql.sh
```

创建 `.env`（`sync.sh` 会自动 `source`）：

```bash
export TUSHARE_TOKEN=xxx
export MYSQL_HOST=localhost
export MYSQL_PORT=3306
export MYSQL_USER=tushare_user
export MYSQL_PASSWORD=xxx
export MYSQL_DATABASE=tushare_sync
```

一键同步：

```bash
./sync.sh
```

## 常用脚本

- `stock_basic.py`：股票列表（全量覆盖）
- `filter_stock_basic.py`：清理 `ST` / `688*` / `9*`
- `daily.py`：日线（支持 `range YYYYMMDD YYYYMMDD`）
- `daily_qfq.py`：前复权日线（写入 `daily_qfq_new`；可用 `workers N`）
- `apply_table.py`：用 `new` 表替换旧表（例：`python apply_table.py daily_qfq_new daily_qfq`）
- `sync_ths_concept_list.py`：同花顺概念列表（覆盖 `ths_concept_list`）
- `sync_ths_concepts_adata.py` / `sync_ths_concepts_ak.py`：同花顺概念指数日线（支持 `workers N`、`from-file failed_concepts.txt`）
- `ths-concept-failed-concepts.py`：检查概念缺失；`--delete` 直接删除不全概念并写 `deleted_concepts.txt`

## 输出文件

- `db_handler.log`：数据库写入日志
- `failed_concepts.txt`：概念补数据列表
- `deleted_concepts.txt`：`--delete` 删除记录
