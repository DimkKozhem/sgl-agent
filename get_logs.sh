#!/bin/bash
# Скрипт для скачивания логов с production сервера
# Использование: ./get_logs.sh [тип_логов]
# Типы: app, nginx, systemd, all (по умолчанию)

SERVER="root@31.172.73.121"
PASSWORD="UpFRdRp0NDvMIYGQ"
LOCAL_DIR="./server_logs"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

LOG_TYPE="${1:-all}"

echo "📥 Скачивание логов с сервера..."
echo "Тип логов: $LOG_TYPE"
echo ""

mkdir -p "$LOCAL_DIR"

# Функция для выполнения команд на сервере
ssh_cmd() {
    sshpass -p "$PASSWORD" ssh -o StrictHostKeyChecking=no "$SERVER" "$@"
}

# Функция для скачивания файлов
download_file() {
    local remote_path="$1"
    local local_name="$2"
    
    echo "  Скачивание: $remote_path"
    sshpass -p "$PASSWORD" scp -o StrictHostKeyChecking=no \
        "${SERVER}:${remote_path}" \
        "${LOCAL_DIR}/${local_name}" 2>/dev/null
    
    if [ $? -eq 0 ]; then
        echo "  ✅ Сохранено: ${LOCAL_DIR}/${local_name}"
    else
        echo "  ⚠️  Не удалось скачать: $remote_path"
    fi
}

# Логи приложения (файлы)
if [ "$LOG_TYPE" = "app" ] || [ "$LOG_TYPE" = "all" ]; then
    echo "📝 Логи приложения SQL-agent..."
    
    # Получить список файлов логов
    LOG_FILES=$(ssh_cmd "ls -1 /opt/sql-agent/logs/sql_agent_*.log 2>/dev/null | tail -5")
    
    if [ -n "$LOG_FILES" ]; then
        for log_file in $LOG_FILES; do
            filename=$(basename "$log_file")
            download_file "$log_file" "app_${filename}"
        done
    else
        echo "  ⚠️  Логи приложения не найдены"
    fi
    echo ""
fi

# Логи systemd
if [ "$LOG_TYPE" = "systemd" ] || [ "$LOG_TYPE" = "all" ]; then
    echo "📝 Логи systemd (последние 1000 строк)..."
    
    ssh_cmd "journalctl -u sql-agent -n 1000 --no-pager" > "${LOCAL_DIR}/systemd_${TIMESTAMP}.log"
    
    if [ $? -eq 0 ]; then
        echo "  ✅ Сохранено: ${LOCAL_DIR}/systemd_${TIMESTAMP}.log"
    else
        echo "  ⚠️  Не удалось получить логи systemd"
    fi
    echo ""
fi

# Логи Nginx
if [ "$LOG_TYPE" = "nginx" ] || [ "$LOG_TYPE" = "all" ]; then
    echo "📝 Логи Nginx..."
    
    download_file "/var/log/nginx/sql-agent-access.log" "nginx_access_${TIMESTAMP}.log"
    download_file "/var/log/nginx/sql-agent-error.log" "nginx_error_${TIMESTAMP}.log"
    
    echo ""
fi

# Логи задач (task_logs)
if [ "$LOG_TYPE" = "tasks" ] || [ "$LOG_TYPE" = "all" ]; then
    echo "📝 Логи задач (последние 10)..."
    
    TASK_FILES=$(ssh_cmd "ls -1t /opt/sql-agent/task_logs/*.json 2>/dev/null | head -10")
    
    if [ -n "$TASK_FILES" ]; then
        mkdir -p "${LOCAL_DIR}/task_logs"
        for task_file in $TASK_FILES; do
            filename=$(basename "$task_file")
            download_file "$task_file" "task_logs/${filename}"
        done
    else
        echo "  ⚠️  Логи задач не найдены"
    fi
    echo ""
fi

echo "✅ Готово! Логи сохранены в: $LOCAL_DIR"
echo ""
echo "Содержимое:"
ls -lh "$LOCAL_DIR" | tail -n +2

