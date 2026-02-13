#!/bin/bash
# Load project environment variables
export http_proxy=""
export https_proxy=""
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
if [ -f "$SCRIPT_DIR/.env" ]; then
    . "$SCRIPT_DIR/.env"
fi

cd "$SCRIPT_DIR"

# 同步同花顺行业列表
echo "开始同步同花顺行业列表..."
python sync_ths_industry_list.py

# 同步同花顺行业日线数据
# 默认使用 4 个线程以避免触发 IP 限制
echo "开始同步同花顺行业日线数据..."
python sync_ths_industry_daily.py workers 1

# 重试失败的行业
# 如果 failed-ths-industry-daily.txt 存在且不为空，则尝试重试
for i in 1 2 3 ; do
    if [ -f "failed-ths-industry-daily.txt" ] && [ -s "failed-ths-industry-daily.txt" ]; then
        echo "正在重试失败的行业 (第 $i 次尝试)..."
        python sync_ths_industry_daily.py workers 1 from-file failed-ths-industry-daily.txt
    else
        break
    fi
done

echo "同步完成。"
