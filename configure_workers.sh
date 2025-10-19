#!/bin/bash
# Скрипт для настройки количества воркеров SQL-agent
# Использование: ./configure_workers.sh <количество_воркеров>

set -e

SERVER="root@31.172.73.121"
PASSWORD="UpFRdRp0NDvMIYGQ"

if [ -z "$1" ]; then
    echo "❌ Ошибка: не указано количество воркеров"
    echo "Использование: $0 <количество_воркеров>"
    echo ""
    echo "Примеры:"
    echo "  $0 2   # Для слабого сервера (1 CPU, 1 GB RAM)"
    echo "  $0 4   # Для среднего сервера (2 CPU, 2 GB RAM)"
    echo "  $0 6   # Для сильного сервера (4 CPU, 4 GB RAM)"
    exit 1
fi

WORKERS=$1

# Проверка корректности числа
if ! [[ "$WORKERS" =~ ^[0-9]+$ ]] || [ "$WORKERS" -lt 1 ] || [ "$WORKERS" -gt 20 ]; then
    echo "❌ Ошибка: количество воркеров должно быть от 1 до 20"
    exit 1
fi

echo "🔧 Настройка SQL-agent на $WORKERS воркеров..."
echo ""

# Получить информацию о сервере
echo "📊 Проверка ресурсов сервера..."
sshpass -p "$PASSWORD" ssh -o StrictHostKeyChecking=no "$SERVER" << 'EOF'
CPU=$(nproc)
RAM=$(free -m | grep Mem | awk '{print $2}')

echo "CPU: $CPU ядер"
echo "RAM: $RAM MB"
echo ""

# Рекомендации
if [ $CPU -eq 1 ] && [ $(($RAM / 1024)) -lt 2 ]; then
    echo "⚠️  Слабый сервер - рекомендуется max 2 воркера"
elif [ $CPU -ge 2 ] && [ $(($RAM / 1024)) -ge 2 ]; then
    echo "✅ Средний сервер - можно использовать 4-6 воркеров"
elif [ $CPU -ge 4 ] && [ $(($RAM / 1024)) -ge 4 ]; then
    echo "✅ Сильный сервер - можно использовать 8-10 воркеров"
fi
EOF

echo ""
read -p "Продолжить настройку на $WORKERS воркеров? (y/n) " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Отменено"
    exit 1
fi

# Изменение конфигурации
echo "📝 Изменение конфигурации..."

sshpass -p "$PASSWORD" ssh -o StrictHostKeyChecking=no "$SERVER" << EOF
cd /opt/sql-agent

# Создать бэкап
cp sql_agent/api.py sql_agent/api.py.backup.\$(date +%Y%m%d_%H%M%S)

# Изменить max_workers
sed -i "s/max_workers=[0-9]\\+/max_workers=$WORKERS/" sql_agent/api.py

# Проверить изменение
echo "Новая конфигурация:"
grep "max_workers=" sql_agent/api.py | head -1

echo ""
echo "✅ Конфигурация изменена"
EOF

# Перезапуск сервиса
echo ""
echo "🔄 Перезапуск сервиса..."
sshpass -p "$PASSWORD" ssh -o StrictHostKeyChecking=no "$SERVER" << 'EOF'
systemctl restart sql-agent
sleep 3
systemctl status sql-agent --no-pager | head -15
EOF

# Проверка
echo ""
echo "🧪 Проверка работоспособности..."
sleep 2

HEALTH=$(curl -s https://skripkahack.ru/health)
STATUS=$(echo "$HEALTH" | jq -r '.status' 2>/dev/null)

if [ "$STATUS" = "healthy" ]; then
    echo "✅ Сервис работает!"
    echo ""
    echo "📊 Информация:"
    echo "$HEALTH" | jq -r '
        "Версия: \(.version)",
        "Uptime: \(.uptime_seconds) секунд",
        "Max workers: \(.components.queue.max_size // "N/A")"
    ' 2>/dev/null || echo "$HEALTH"
else
    echo "⚠️  Сервис не отвечает корректно"
    echo "Ответ: $HEALTH"
    echo ""
    echo "Проверьте логи:"
    echo "  ssh root@31.172.73.121 'journalctl -u sql-agent -n 50'"
fi

echo ""
echo "✅ Настройка завершена!"
echo ""
echo "Полезные команды:"
echo "  Просмотр логов:   ssh root@31.172.73.121 'journalctl -u sql-agent -f'"
echo "  Статус сервиса:   ssh root@31.172.73.121 'systemctl status sql-agent'"
echo "  Откат изменений:  ssh root@31.172.73.121 'cd /opt/sql-agent && mv sql_agent/api.py.backup.* sql_agent/api.py && systemctl restart sql-agent'"

