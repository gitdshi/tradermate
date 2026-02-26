#!/bin/bash
#
# TraderMate 数据同步服务启动脚本
# 使用方式: ./scripts/datasync_service.sh [command]
# 支持命令: start, stop, restart, logs, status

set -e

SERVICE_NAME="tradermate-datasync"
COMPOSE_FILE="docker-compose.yml"

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查 .env 文件是否存在
check_env() {
    if [ ! -f ".env" ]; then
        log_warn ".env 文件不存在"
        log_info "请从 .env.example 复制并配置您的环境变量:"
        log_info "  cp .env.example .env"
        log_info "  然后编辑 .env 文件填入真实值"
        return 1
    fi
    return 0
}

# 验证必需的环境变量
validate_env() {
    local missing=()

    [ -z "${MYSQL_PASSWORD}" ] && missing+=("MYSQL_PASSWORD")
    [ -z "${TUSHARE_TOKEN}" ] && missing+=("TUSHARE_TOKEN")

    if [ ${#missing[@]} -gt 0 ]; then
        log_error "缺少必需的环境变量: ${missing[*]}"
        log_info "请在 .env 文件中配置这些变量"
        return 1
    fi
    return 0
}

start() {
    log_info "启动 ${SERVICE_NAME}..."
    if ! check_env; then
        return 1
    fi
    if ! validate_env; then
        return 1
    fi

    if docker-compose -f "${COMPOSE_FILE}" up -d datasync_service; then
        log_info "${SERVICE_NAME} 启动成功"
    else
        log_error "${SERVICE_NAME} 启动失败"
        return 1
    fi
}

stop() {
    log_info "停止 ${SERVICE_NAME}..."
    if docker-compose -f "${COMPOSE_FILE}" stop datasync_service; then
        log_info "${SERVICE_NAME} 已停止"
    else
        log_error "停止失败"
        return 1
    fi
}

restart() {
    log_info "重启 ${SERVICE_NAME}..."
    if docker-compose -f "${COMPOSE_FILE}" restart datasync_service; then
        log_info "${SERVICE_NAME} 重启成功"
    else
        log_error "重启失败"
        return 1
    fi
}

logs() {
    docker-compose -f "${COMPOSE_FILE}" logs -f datasync_service
}

status() {
    if docker-compose -f "${COMPOSE_FILE}" ps datasync_service | grep -q "Up"; then
        log_info "${SERVICE_NAME} 正在运行"
        docker-compose -f "${COMPOSE_FILE}" ps datasync_service
    else
        log_warn "${SERVICE_NAME} 未运行"
    fi
}

case "${1:-status}" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart|reload)
        restart
        ;;
    logs)
        logs
        ;;
    status)
        status
        ;;
    *)
        echo "Usage: $0 {start|stop|restart|logs|status}"
        exit 1
        ;;
esac
