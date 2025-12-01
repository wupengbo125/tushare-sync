#!/bin/bash

# 前复权表切换脚本
# 将 daily_qfq_new 表重命名为 daily_qfq 表

set -e

echo "==========================================="
echo "      前复权表切换脚本"
echo "==========================================="

# 检查Python脚本是否存在
if [ ! -f "move_qfq_table.py" ]; then
    echo "错误: move_qfq_table.py 文件不存在"
    exit 1
fi

# 显示当前表状态
echo "当前表状态:"
python move_qfq_table.py status

echo ""
echo "执行表切换操作..."
echo ""

# 执行表切换
python move_qfq_table.py

if [ $? -eq 0 ]; then
    echo ""
    echo "表切换完成!"
    echo ""
    echo "切换后表状态:"
    python move_qfq_table.py status
else
    echo "表切换失败!"
    exit 1
fi