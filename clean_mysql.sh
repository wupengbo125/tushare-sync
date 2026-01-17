#!/bin/bash

# 一次性清理旧 MySQL 容器脚本（只删旧的，不创建新的）
# 用完就删这个脚本，以后直接跑你改好的启动脚本

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${RED}===========================================${NC}"
echo -e "${RED}     一次性旧容器清理脚本（Lucas专用）     ${NC}"
echo -e "${RED}===========================================${NC}"
echo -e "${YELLOW}这个脚本只干一件事：把旧容器停掉+删掉${NC}"
echo -e "${YELLOW}数据（./mysql_data）100%不动，以后用你改好的启动脚本就行${NC}"
echo ""

CONTAINER_NAME="${MYSQL_CONTAINER_NAME:-tushare-mysql}"

# 检查 Docker
if ! docker info >/dev/null 2>&1; then
    echo -e "${RED}Docker 没跑？先开 Docker${NC}"
    exit 1
fi

# 看有没有旧容器
if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo -e "${YELLOW}找到旧容器 ${CONTAINER_NAME}${NC}"

    # 如果在跑，先停
    if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        echo -e "${YELLOW}正在运行 → 停止...${NC}"
        docker stop "${CONTAINER_NAME}"
        sleep 2
        echo -e "${GREEN}已停止${NC}"
    else
        echo -e "${YELLOW}已经停了${NC}"
    fi

    # 删除
    echo -e "${YELLOW}删除旧容器...${NC}"
    docker rm "${CONTAINER_NAME}"
    echo -e "${GREEN}旧容器已彻底删除${NC}"
else
    echo -e "${YELLOW}没找到容器 ${CONTAINER_NAME}，已经是最干净状态了${NC}"
fi

echo ""
echo -e "${GREEN}清理完成！${NC}"
echo -e "${YELLOW}现在你可以直接运行你修改后的启动脚本（带 binlog 参数的那个）${NC}"
echo -e "比如： ./start-mysql.sh  （或你叫啥名字就跑啥）"
echo ""
echo -e "${YELLOW}建议下一步：${NC}"
echo "1. 跑启动脚本 → 新容器会用新参数启动"
echo "2. 进 MySQL 手动清一次旧 binlog（可选，但推荐）："
echo "   docker exec -it ${CONTAINER_NAME} mysql -uroot -p"
echo "   PURGE BINARY LOGS BEFORE NOW() - INTERVAL 1 DAY;"
echo ""
echo -e "${GREEN}搞定！这个脚本用完可以 rm 掉。${NC}"
echo -e "${RED}下次别再让我写带 run 的傻逼脚本了😂${NC}"