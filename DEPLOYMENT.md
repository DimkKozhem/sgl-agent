# 🚀 Информация о развертывании SQL-agent

## Сервер

- **IP адрес:** 31.172.73.121
- **Домен:** https://skripkahack.ru
- **SSH:** `ssh root@31.172.73.121`
- **ОС:** Ubuntu 24.04 (Linux 6.8.0-83-generic)

## Развертывание завершено: 19 октября 2025

### ✅ Установленные компоненты

1. **Python 3.12** с виртуальным окружением
2. **Nginx** (reverse proxy с SSL)
3. **SSL сертификат** от Let's Encrypt (действителен до 17 января 2026)
4. **Systemd сервис** для автозапуска приложения
5. **Все зависимости Python** из requirements.txt

### 📁 Структура на сервере

```
/opt/sql-agent/
├── main.py                 # Точка входа приложения
├── requirements.txt        # Зависимости Python
├── .env                   # Переменные окружения (API ключи)
├── venv/                  # Виртуальное окружение Python
├── sql_agent/             # Основной код приложения
├── datasets/              # Тестовые данные
├── static/                # Статические файлы
├── tests/                 # Тесты
├── logs/                  # Логи приложения (ротация каждый час)
└── task_logs/             # Логи задач (история выполнения)
```

### 🔧 Конфигурации

**Nginx:** `/etc/nginx/conf.d/domains/skripkahack.ru.conf`
- HTTP → HTTPS редирект
- Proxy pass на порт 8001
- Таймауты: 900s для длительных операций
- Максимальный размер запроса: 100MB

**Systemd:** `/etc/systemd/system/sql-agent.service`
- Автозапуск при старте системы
- Автоматический перезапуск при падении
- Логирование в systemd journal

### 🌐 API Endpoints

Все endpoints доступны через HTTPS:

- **Health Check:** https://skripkahack.ru/health
- **Metrics:** https://skripkahack.ru/metrics
- **Новая задача:** POST https://skripkahack.ru/new
- **Статус задачи:** GET https://skripkahack.ru/status?task_id={id}
- **Результат:** GET https://skripkahack.ru/getresult?task_id={id}

### 📊 Пример использования

```bash
# Health check
curl https://skripkahack.ru/health

# Создание задачи
curl -X POST https://skripkahack.ru/new \
  -H "Content-Type: application/json" \
  -d @datasets/linear_schema.json

# Проверка статуса
curl "https://skripkahack.ru/status?task_id=<ID>"

# Получение результата
curl "https://skripkahack.ru/getresult?task_id=<ID>"
```

## 🔧 Управление сервисом

### Основные команды

```bash
# SSH подключение
ssh root@31.172.73.121

# Проверка статуса
systemctl status sql-agent

# Просмотр логов (real-time)
journalctl -u sql-agent -f

# Перезапуск сервиса
systemctl restart sql-agent

# Остановка сервиса
systemctl stop sql-agent

# Запуск сервиса
systemctl start sql-agent

# Проверка Nginx
nginx -t
systemctl status nginx
```

### Логи

**Логи приложения:**
```bash
cd /opt/sql-agent
tail -f logs/sql_agent_*.log
```

**Логи Nginx:**
```bash
tail -f /var/log/nginx/sql-agent-access.log
tail -f /var/log/nginx/sql-agent-error.log
```

**Systemd логи:**
```bash
journalctl -u sql-agent -n 100 --no-pager
```

## 🔐 Безопасность

- ✅ HTTPS с валидным SSL сертификатом
- ✅ Автоматическое обновление сертификата (certbot timer)
- ✅ Приложение слушает только localhost:8001
- ✅ Nginx как reverse proxy (защита от прямого доступа)
- ✅ API ключи в защищенном .env файле (chmod 600)
- ✅ Firewall правила (порты 80, 443 открыты, 8001 закрыт)

## 🔄 Обновление приложения

```bash
# 1. Подключение к серверу
ssh root@31.172.73.121

# 2. Переход в директорию
cd /opt/sql-agent

# 3. Остановка сервиса
systemctl stop sql-agent

# 4. Обновление кода (например, через git pull)
# или копирование обновленных файлов

# 5. Обновление зависимостей (если изменились)
source venv/bin/activate
pip install -r requirements.txt
deactivate

# 6. Запуск сервиса
systemctl start sql-agent

# 7. Проверка статуса
systemctl status sql-agent
curl https://skripkahack.ru/health
```

## 📈 Мониторинг

### Проверка работоспособности

```bash
# Быстрая проверка
curl -s https://skripkahack.ru/health | jq .

# Детальные метрики
curl -s https://skripkahack.ru/metrics | jq .

# Статус сервиса
systemctl status sql-agent

# Использование ресурсов
top -p $(pgrep -f "python.*main.py")
```

### Важные метрики

- **Uptime:** Время работы без перезапуска
- **Tasks total:** Всего обработано задач
- **Queue size:** Текущий размер очереди (max 100)
- **Error rate:** Процент ошибок
- **Memory usage:** Потребление памяти

## ⚠️ Troubleshooting

### Сервис не запускается

```bash
# Проверить логи
journalctl -u sql-agent -n 50 --no-pager

# Проверить конфигурацию
cd /opt/sql-agent
cat .env

# Проверить права
ls -la /opt/sql-agent

# Попробовать запустить вручную
cd /opt/sql-agent
source venv/bin/activate
python main.py
```

### 502 Bad Gateway

```bash
# Проверить, запущено ли приложение
systemctl status sql-agent

# Проверить, слушает ли порт 8001
netstat -tlnp | grep 8001

# Проверить логи Nginx
tail -50 /var/log/nginx/sql-agent-error.log
```

### SSL сертификат истёк

```bash
# Проверить срок действия
certbot certificates

# Обновить вручную
certbot renew

# Перезагрузить Nginx
systemctl reload nginx
```

## 📝 Особенности конфигурации

На сервере установлена **HestiaCP** - система управления хостингом. 
Конфигурация SQL-agent размещена в `/etc/nginx/conf.d/domains/` 
для корректной работы с HestiaCP.

**Важно:** При изменении конфигурации Nginx всегда используйте:
```bash
nginx -t        # Проверка конфигурации
systemctl reload nginx  # Применение изменений
```

## 🎯 Производительность

**Текущие настройки:**
- Max workers: 10 (параллельных задач)
- Max queue size: 100 (задач в очереди)
- Task timeout: 15 минут
- Connection timeout: 900 секунд (Nginx)

**Ожидаемая производительность:**
- ~10-30 задач в минуту (зависит от сложности схем)
- Поддержка до 100 задач в очереди
- Автоматическая ротация логов каждый час

## ✅ Проверка развертывания

```bash
# 1. Проверка доступности
curl -I https://skripkahack.ru
# Должно вернуть: HTTP/2 200

# 2. Проверка Health
curl https://skripkahack.ru/health
# Должно вернуть JSON со status: "healthy"

# 3. Проверка Metrics
curl https://skripkahack.ru/metrics
# Должно вернуть JSON с метриками

# 4. Проверка SSL
echo | openssl s_client -connect skripkahack.ru:443 -servername skripkahack.ru 2>/dev/null | openssl x509 -noout -dates
# Должно показать действительные даты сертификата
```

---

**Развертывание выполнено:** 19 октября 2025  
**Версия:** 1.2.0  
**Статус:** ✅ Production Ready

