#!/bin/bash

# 一键启动MySQL服务脚本
# 使用Docker运行MySQL容器

set -e

# 从环境变量获取配置，设置合理的默认值
CONTAINER_NAME="${MYSQL_CONTAINER_NAME:-tushare-mysql}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD:-${MYSQL_PASSWORD}}"
MYSQL_DATABASE="${MYSQL_DATABASE:-tushare_sync}"
MYSQL_USER="${MYSQL_USER:-tushare_user}"
MYSQL_PASSWORD="${MYSQL_PASSWORD:-tushare_password}"
MYSQL_HOST="${MYSQL_HOST:-localhost}"
DATA_DIR="${MYSQL_DATA_DIR:-$PWD/mysql_data}"

# 检查MYSQL_USER是否设置为root，如果是则给出警告并重置为默认值
if [ "$MYSQL_USER" = "root" ]; then
    echo -e "${YELLOW}警告: MYSQL_USER不能设置为root${NC}"
    echo -e "${YELLOW}MySQL Docker镜像中，root用户必须使用MYSQL_ROOT_PASSWORD配置${NC}"
    MYSQL_USER="tushare_user"
    echo -e "${YELLOW}已重置MYSQL_USER为默认值: ${MYSQL_USER}${NC}"
fi

# 检查必需的环境变量是否设置
if [ -z "$MYSQL_PASSWORD" ] || [ -z "$MYSQL_DATABASE" ]; then
    echo -e "${RED}错误: 请设置所有必需的环境变量${NC}"
    echo -e "${YELLOW}必需的环境变量:${NC}"
    echo "  MYSQL_PASSWORD - MySQL密码（用于root用户和普通用户）"
    echo "  MYSQL_DATABASE - 数据库名称"
    echo ""
    echo -e "${YELLOW}示例:${NC}"
    echo "  export MYSQL_PASSWORD=root123"
    echo "  export MYSQL_DATABASE=tushare_sync"
    echo ""
    echo -e "${YELLOW}可选环境变量:${NC}"
    echo "  MYSQL_PORT - MySQL端口 (默认: 3306)"
    echo "  MYSQL_USER - 普通用户名 (默认: tushare_user)"
    echo "  MYSQL_ROOT_PASSWORD - Root密码 (默认: 与MYSQL_PASSWORD相同)"
    echo "  MYSQL_HOST - 主机地址 (默认: localhost)"
    echo "  MYSQL_CONTAINER_NAME - 容器名称 (默认: tushare-mysql)"
    echo "  MYSQL_DATA_DIR - 数据目录 (默认: $PWD/mysql_data)"
    exit 1
fi

# 彩色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # 无颜色

echo -e "${GREEN}===========================================${NC}"
echo -e "${GREEN}      TushareSync MySQL 服务启动脚本      ${NC}"
echo -e "${GREEN}===========================================${NC}"

# 检查Docker是否安装
# 使用 POSIX 兼容的重定向 (>/dev/null 2>&1) 代替 bash 专有的 &>，
# 以便在用 sh 调用脚本时也能正确工作。
if ! command -v docker >/dev/null 2>&1; then
    echo -e "${RED}错误: Docker 未安装${NC}"
    exit 1
fi

# 检查Docker是否运行
if ! docker info >/dev/null 2>&1; then
    echo -e "${RED}错误: Docker 服务未运行${NC}"
    exit 1
fi

# 检查容器是否已存在
if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    echo -e "${YELLOW}容器 ${CONTAINER_NAME} 已存在${NC}"

    # 检查容器是否运行
    if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        echo -e "${GREEN}容器正在运行${NC}"
        echo -e "${YELLOW}连接信息:${NC}"
        echo "  主机: localhost"
        echo "  端口: ${MYSQL_PORT}"
        echo "  数据库: ${MYSQL_DATABASE}"
        echo "  用户名: ${MYSQL_USER}"
        echo "  密码: ${MYSQL_PASSWORD}"
        exit 0
    else
        echo -e "${YELLOW}启动现有容器...${NC}"
        docker start "${CONTAINER_NAME}"
    fi
else
    echo -e "${YELLOW}创建新容器...${NC}"

    # 创建数据目录
    mkdir -p "${DATA_DIR}"

    # 运行新容器
    docker run -d \
        --name "${CONTAINER_NAME}" \
        -p "${MYSQL_PORT}:3306" \
        -e MYSQL_ROOT_PASSWORD="${MYSQL_ROOT_PASSWORD}" \
        -e MYSQL_DATABASE="${MYSQL_DATABASE}" \
        -e MYSQL_USER="${MYSQL_USER}" \
        -e MYSQL_PASSWORD="${MYSQL_PASSWORD}" \
        --restart unless-stopped \
        -v "tushare:/var/lib/mysql" \
        mysql:8.0 \
        --character-set-server=utf8mb4 \
        --collation-server=utf8mb4_unicode_ci \
        --default-time-zone='+8:00' \
        --binlog_expire_logs_seconds=86400 \
        --max_binlog_size=128M    
fi

# 等待MySQL启动
echo -e "${YELLOW}等待MySQL启动...${NC}"
for i in $(seq 1 30); do
    if docker exec "${CONTAINER_NAME}" mysqladmin ping -h localhost --silent; then
        echo -e "${GREEN}MySQL 已启动${NC}"
        break
    fi
    if [ $i -eq 30 ]; then
        echo -e "${RED}MySQL 启动超时${NC}"
        exit 1
    fi
    sleep 1
done

# 显示连接信息
echo -e "${GREEN}===========================================${NC}"
echo -e "${GREEN}          MySQL 连接信息${NC}"
echo -e "${GREEN}===========================================${NC}"
echo -e "主机: ${YELLOW}${MYSQL_HOST}${NC}"
echo -e "端口: ${YELLOW}${MYSQL_PORT}${NC}"
echo -e "数据库: ${YELLOW}${MYSQL_DATABASE}${NC}"
echo -e "用户名: ${YELLOW}${MYSQL_USER}${NC}"
echo -e "密码: ${YELLOW}********${NC}"
echo -e "Root密码: ${YELLOW}********${NC}"
echo ""
echo -e "${GREEN}环境变量设置:${NC}"
echo -e "export MYSQL_HOST=${MYSQL_HOST}"
echo -e "export MYSQL_PORT=${MYSQL_PORT}"
echo -e "export MYSQL_USER=${MYSQL_USER}"
echo -e "export MYSQL_PASSWORD=********"
echo -e "export MYSQL_DATABASE=${MYSQL_DATABASE}"
echo ""
echo -e "${YELLOW}常用命令:${NC}"
echo -e "停止容器: docker stop ${CONTAINER_NAME}"
echo -e "重启容器: docker restart ${CONTAINER_NAME}"
echo -e "查看日志: docker logs ${CONTAINER_NAME}"
echo -e "进入容器: docker exec -it ${CONTAINER_NAME} bash"
echo -e "${GREEN}===========================================${NC}"
