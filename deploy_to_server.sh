#!/bin/bash
# Главный скрипт для развертывания SQL-agent на production сервере
# Запускается локально и выполняет все операции на удаленном сервере
#
# Протестировано на: Ubuntu 24.04 с HestiaCP
# Дата последнего обновления: 2025-10-19
#
# Что делает скрипт:
# 1. Устанавливает необходимые пакеты (Python, Nginx, Certbot)
# 2. Копирует файлы проекта через tar+ssh (совместимость с HestiaCP)
# 3. Настраивает виртуальное окружение и зависимости
# 4. Создает systemd сервис для автозапуска
# 5. Настраивает Nginx reverse proxy (совместимость с HestiaCP)
# 6. Получает SSL сертификат через Let's Encrypt (webroot метод)
# 7. Запускает и тестирует сервис

set -e

# Конфигурация сервера
SERVER_IP="31.172.73.121"
SERVER_USER="root"
DOMAIN="skripkahack.ru"
APP_DIR="/opt/sql-agent"
PROJECT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "🚀 Развертывание SQL-agent на сервере $DOMAIN ($SERVER_IP)"
echo "📁 Локальная директория проекта: $PROJECT_DIR"
echo ""

# Функция для выполнения команд на сервере
ssh_exec() {
    sshpass -p 'UpFRdRp0NDvMIYGQ' ssh -o StrictHostKeyChecking=no ${SERVER_USER}@${SERVER_IP} "$@"
}

# Проверка наличия sshpass
if ! command -v sshpass &> /dev/null; then
    echo "📦 Установка sshpass..."
    sudo apt-get update
    sudo apt-get install -y sshpass
fi

# Шаг 1: Подготовка сервера
echo "📦 Шаг 1/8: Подготовка сервера..."
ssh_exec "bash -s" << 'EOF'
set -e
apt-get update
DEBIAN_FRONTEND=noninteractive apt-get upgrade -y
DEBIAN_FRONTEND=noninteractive apt-get install -y \
    python3 \
    python3-pip \
    python3-venv \
    git \
    nginx \
    certbot \
    python3-certbot-nginx \
    htop \
    curl \
    build-essential \
    libpq-dev

echo "✅ Пакеты установлены"
EOF

# Шаг 2: Копирование файлов проекта
echo "📦 Шаг 2/8: Копирование файлов проекта на сервер..."
ssh_exec "mkdir -p $APP_DIR"

# Файлы для копирования (используем tar через ssh вместо scp)
echo "  Копирование основных файлов через tar..."
cd "$PROJECT_DIR"
tar czf - main.py requirements.txt README.md pytest.ini | \
    sshpass -p 'UpFRdRp0NDvMIYGQ' ssh -o StrictHostKeyChecking=no ${SERVER_USER}@${SERVER_IP} \
    "cd $APP_DIR && tar xzf -"

echo "  Копирование директорий через tar..."
tar czf - sql_agent datasets static tests | \
    sshpass -p 'UpFRdRp0NDvMIYGQ' ssh -o StrictHostKeyChecking=no ${SERVER_USER}@${SERVER_IP} \
    "cd $APP_DIR && tar xzf -"

echo "✅ Файлы скопированы"

# Шаг 3: Установка зависимостей Python
echo "📦 Шаг 3/8: Установка зависимостей Python..."
ssh_exec "bash -s" << EOF
set -e
cd $APP_DIR
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
echo "✅ Зависимости установлены"
EOF

# Шаг 4: Настройка переменных окружения
echo "📦 Шаг 4/8: Настройка переменных окружения..."
read -p "Введите ваш OPEN_ROUTER API ключ: " API_KEY

ssh_exec "bash -s" << EOF
cat > $APP_DIR/.env << ENVEOF
OPEN_ROUTER=$API_KEY
ENVEOF
chmod 600 $APP_DIR/.env
echo "✅ Файл .env создан"
EOF

# Шаг 5: Создание systemd сервиса
echo "📦 Шаг 5/8: Создание systemd сервиса..."
ssh_exec "bash -s" << 'EOF'
cat > /etc/systemd/system/sql-agent.service << 'SERVICEEOF'
[Unit]
Description=SQL-agent REST API for Database Optimization
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=root
Group=root
WorkingDirectory=/opt/sql-agent
Environment="PATH=/opt/sql-agent/venv/bin"
EnvironmentFile=/opt/sql-agent/.env
ExecStart=/opt/sql-agent/venv/bin/python /opt/sql-agent/main.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=sql-agent

[Install]
WantedBy=multi-user.target
SERVICEEOF

systemctl daemon-reload
systemctl enable sql-agent
echo "✅ Systemd сервис создан"
EOF

# Шаг 6: Настройка Nginx
echo "📦 Шаг 6/8: Настройка Nginx..."

# Копируем конфигурацию nginx
cat "$PROJECT_DIR/nginx.conf" | sshpass -p 'UpFRdRp0NDvMIYGQ' ssh -o StrictHostKeyChecking=no ${SERVER_USER}@${SERVER_IP} \
    "cat > /etc/nginx/sites-available/sql-agent"

ssh_exec "bash -s" << 'EOF'
set -e
# Создание директорий если не существуют
mkdir -p /etc/nginx/sites-available /etc/nginx/sites-enabled /etc/nginx/conf.d/domains

# Добавляем include для sites-enabled в nginx.conf если его нет
if ! grep -q 'include /etc/nginx/sites-enabled' /etc/nginx/nginx.conf; then
    sed -i '/http {/a\    include /etc/nginx/sites-enabled/*;' /etc/nginx/nginx.conf
fi

# Создаём симлинк
ln -sf /etc/nginx/sites-available/sql-agent /etc/nginx/sites-enabled/sql-agent

# Отключаем default_server на портах 80 и 443 (для совместимости с HestiaCP)
if [ -f /etc/nginx/conf.d/31.172.73.121.conf ]; then
    sed -i 's/listen 31.172.73.121:80 default_server;/listen 31.172.73.121:80;/' /etc/nginx/conf.d/31.172.73.121.conf
    sed -i 's/listen 31.172.73.121:443 default_server ssl;/listen 31.172.73.121:443 ssl;/' /etc/nginx/conf.d/31.172.73.121.conf
fi

# Проверка конфигурации
nginx -t

# Перезапуск nginx
systemctl reload nginx
echo "✅ Nginx настроен"
EOF

# Шаг 7: Настройка SSL сертификата
echo "📦 Шаг 7/8: Настройка SSL сертификата..."
read -p "Введите email для Let's Encrypt: " EMAIL

ssh_exec "bash -s" << EOF
set -e
# Создание директории для webroot
mkdir -p /var/www/html/.well-known/acme-challenge
chmod -R 755 /var/www/html

# Получение SSL сертификата через webroot
certbot certonly --webroot \
    -w /var/www/html \
    -d $DOMAIN \
    -d www.$DOMAIN \
    --non-interactive \
    --agree-tos \
    --email $EMAIL

# Обновление конфигурации Nginx с SSL
cat > /etc/nginx/sites-available/sql-agent << 'NGINXEOF'
# HTTP → HTTPS redirect
server {
    listen 80;
    listen [::]:80;
    server_name $DOMAIN www.$DOMAIN;
    
    location /.well-known/acme-challenge/ {
        root /var/www/html;
    }
    
    location / {
        return 301 https://\$host\$request_uri;
    }
}

# HTTPS server
server {
    listen 443 ssl;
    listen [::]:443 ssl;
    http2 on;
    server_name $DOMAIN www.$DOMAIN;
    
    ssl_certificate /etc/letsencrypt/live/$DOMAIN/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/$DOMAIN/privkey.pem;
    
    ssl_protocols TLSv1.2 TLSv1.3;
    ssl_ciphers HIGH:!aNULL:!MD5;
    ssl_prefer_server_ciphers on;
    
    access_log /var/log/nginx/sql-agent-access.log;
    error_log /var/log/nginx/sql-agent-error.log;
    
    client_max_body_size 100M;
    
    location / {
        proxy_pass http://127.0.0.1:8001;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        
        proxy_read_timeout 900s;
        proxy_connect_timeout 60s;
        proxy_send_timeout 60s;
        
        proxy_http_version 1.1;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
    }
    
    location /health {
        proxy_pass http://127.0.0.1:8001/health;
        access_log off;
        proxy_read_timeout 5s;
        proxy_connect_timeout 5s;
    }
}
NGINXEOF

# Проверка и перезагрузка nginx
nginx -t && systemctl reload nginx

echo "✅ SSL сертификат получен и Nginx настроен"
EOF

# Шаг 8: Запуск приложения
echo "📦 Шаг 8/8: Запуск приложения..."
ssh_exec "bash -s" << EOF
# Создание директорий для логов
mkdir -p $APP_DIR/logs $APP_DIR/task_logs
# Запуск сервиса
systemctl start sql-agent
# Проверка статуса
sleep 3
systemctl status sql-agent --no-pager
echo "✅ Приложение запущено"
EOF

echo ""
echo "🎉 Развертывание завершено успешно!"
echo ""

# Финальная проверка работоспособности
echo "🧪 Финальная проверка работоспособности..."
sleep 5

if curl -sf https://$DOMAIN/health > /dev/null 2>&1; then
    echo "✅ Сервис доступен и работает!"
    echo ""
    echo "📊 Информация о сервисе:"
    echo "  URL: https://$DOMAIN"
    echo "  Health Check: https://$DOMAIN/health"
    curl -s https://$DOMAIN/health | grep -q "healthy" && echo "  Статус: 🟢 Healthy"
else
    echo "⚠️  Сервис недоступен, проверьте логи:"
    echo "  ssh root@$SERVER_IP 'journalctl -u sql-agent -n 50'"
fi

echo ""
echo "🔍 Полезные команды:"
echo "  Проверка статуса: ssh root@$SERVER_IP 'systemctl status sql-agent'"
echo "  Просмотр логов: ssh root@$SERVER_IP 'journalctl -u sql-agent -f'"
echo "  Перезапуск: ssh root@$SERVER_IP 'systemctl restart sql-agent'"
echo ""
echo "📝 Документация:"
echo "  Подробная информация: ./DEPLOYMENT.md"
echo "  Быстрый старт: ./QUICKSTART.md"
echo "  Работа с логами: ./LOGS_GUIDE.md"
echo ""

