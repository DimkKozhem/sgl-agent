# 📋 Руководство по работе с логами SQL-agent

## 🚀 Быстрый старт

### Автоматическое скачивание всех логов

```bash
./get_logs.sh
```

Логи будут сохранены в `./server_logs/`

---

## 📝 Типы логов

### 1. Логи приложения SQL-agent

**Расположение на сервере:** `/opt/sql-agent/logs/sql_agent_*.log`

**Содержат:**
- События запуска/остановки приложения
- Обработка запросов API
- Работа LLM анализатора
- Ошибки выполнения
- Оценки качества оптимизаций

**Как получить:**
```bash
# Автоматически
./get_logs.sh app

# Вручную через SSH
ssh root@31.172.73.121 "cat /opt/sql-agent/logs/sql_agent_$(date +%Y%m%d)_*.log" > app.log

# Real-time просмотр
ssh root@31.172.73.121
tail -f /opt/sql-agent/logs/sql_agent_*.log
```

---

### 2. Логи systemd

**Содержат:**
- События systemd (запуск, остановка, перезапуск сервиса)
- Стандартный вывод приложения (stdout/stderr)
- Информацию о статусе процесса

**Как получить:**
```bash
# Автоматически
./get_logs.sh systemd

# Вручную через SSH
ssh root@31.172.73.121 "journalctl -u sql-agent -n 1000 --no-pager" > systemd.log

# Real-time просмотр
ssh root@31.172.73.121
journalctl -u sql-agent -f

# Последние 100 строк
ssh root@31.172.73.121 "journalctl -u sql-agent -n 100 --no-pager"

# За последний час
ssh root@31.172.73.121 "journalctl -u sql-agent --since '1 hour ago'"
```

---

### 3. Логи Nginx

**Расположение на сервере:**
- Access: `/var/log/nginx/sql-agent-access.log`
- Error: `/var/log/nginx/sql-agent-error.log`

**Содержат:**
- HTTP запросы к API
- Ошибки прокси
- SSL/TLS события
- Performance метрики

**Как получить:**
```bash
# Автоматически
./get_logs.sh nginx

# Вручную (access log)
sshpass -p 'UpFRdRp0NDvMIYGQ' scp root@31.172.73.121:/var/log/nginx/sql-agent-access.log ./

# Вручную (error log)
sshpass -p 'UpFRdRp0NDvMIYGQ' scp root@31.172.73.121:/var/log/nginx/sql-agent-error.log ./

# Real-time просмотр
ssh root@31.172.73.121
tail -f /var/log/nginx/sql-agent-access.log
```

---

### 4. Логи задач (Task logs)

**Расположение на сервере:** `/opt/sql-agent/task_logs/*.json`

**Содержат:**
- Полная история каждой задачи
- Входные данные (url, ddl, queries)
- Выходные данные (ddl, migrations, queries)
- Оценка качества оптимизации
- Время выполнения

**Как получить:**
```bash
# Автоматически (последние 10)
./get_logs.sh tasks

# Все логи задач
sshpass -p 'UpFRdRp0NDvMIYGQ' scp -r root@31.172.73.121:/opt/sql-agent/task_logs ./

# Конкретная задача
sshpass -p 'UpFRdRp0NDvMIYGQ' scp root@31.172.73.121:/opt/sql-agent/task_logs/<task_id>.json ./
```

---

## 🔍 Поиск в логах

### Поиск ошибок

```bash
# В логах приложения
ssh root@31.172.73.121 "grep ERROR /opt/sql-agent/logs/sql_agent_*.log"

# В systemd логах
ssh root@31.172.73.121 "journalctl -u sql-agent | grep ERROR"

# Критические ошибки
ssh root@31.172.73.121 "journalctl -u sql-agent -p err"
```

### Поиск по task_id

```bash
TASK_ID="ваш-task-id"

# В логах приложения
ssh root@31.172.73.121 "grep $TASK_ID /opt/sql-agent/logs/sql_agent_*.log"

# В systemd
ssh root@31.172.73.121 "journalctl -u sql-agent | grep $TASK_ID"
```

### Статистика запросов

```bash
# Количество запросов к API
ssh root@31.172.73.121 "wc -l /var/log/nginx/sql-agent-access.log"

# Топ IP адресов
ssh root@31.172.73.121 "awk '{print \$1}' /var/log/nginx/sql-agent-access.log | sort | uniq -c | sort -rn | head"

# Статус коды ответов
ssh root@31.172.73.121 "awk '{print \$9}' /var/log/nginx/sql-agent-access.log | sort | uniq -c"
```

---

## 📊 Анализ производительности

### Время выполнения задач

```bash
# Средняя оценка качества всех задач
ssh root@31.172.73.121 "jq '.output.quality_score' /opt/sql-agent/task_logs/*.json 2>/dev/null | awk '{sum+=\$1; count++} END {print \"Средняя оценка:\", sum/count}'"

# Задачи с низкой оценкой (< 60)
ssh root@31.172.73.121 "for f in /opt/sql-agent/task_logs/*.json; do score=\$(jq '.output.quality_score' \$f 2>/dev/null); if [ \$score -lt 60 ]; then echo \$f: \$score; fi; done"
```

### Uptime и перезапуски

```bash
# Uptime приложения
ssh root@31.172.73.121 "systemctl show sql-agent --property=ActiveEnterTimestamp"

# История перезапусков
ssh root@31.172.73.121 "journalctl -u sql-agent | grep 'Started SQL-agent'"
```

---

## 🛠️ Управление логами

### Ротация логов

Логи приложения автоматически ротируются каждый час. Старые файлы остаются до ручного удаления.

```bash
# Просмотр всех файлов логов
ssh root@31.172.73.121 "ls -lh /opt/sql-agent/logs/"

# Удаление логов старше 7 дней
ssh root@31.172.73.121 "find /opt/sql-agent/logs/ -name 'sql_agent_*.log' -mtime +7 -delete"

# Очистка старых task logs (старше 72 часов)
# (автоматически очищается фоновой задачей)
ssh root@31.172.73.121 "find /opt/sql-agent/task_logs/ -name '*.json' -mtime +3 -delete"
```

### Архивация логов

```bash
# Создать архив всех логов
ssh root@31.172.73.121 "cd /opt/sql-agent && tar czf logs_backup_$(date +%Y%m%d).tar.gz logs/ task_logs/"

# Скачать архив
sshpass -p 'UpFRdRp0NDvMIYGQ' scp root@31.172.73.121:/opt/sql-agent/logs_backup_*.tar.gz ./
```

---

## 📈 Мониторинг в реальном времени

### Мультиплексный просмотр (tmux)

```bash
ssh root@31.172.73.121

# Запустить tmux
tmux new -s logs

# Разделить экран (Ctrl+B, затем ")
# В первой панели:
journalctl -u sql-agent -f

# Переключиться на вторую панель (Ctrl+B, стрелка вниз)
tail -f /var/log/nginx/sql-agent-access.log

# Выход из tmux: Ctrl+B, затем D
```

### Watch статистики

```bash
ssh root@31.172.73.121

# Мониторинг метрик приложения
watch -n 5 'curl -s http://127.0.0.1:8001/metrics | jq .'

# Мониторинг использования ресурсов
watch -n 2 'ps aux | grep "python.*main.py"'
```

---

## 🚨 Алерты и уведомления

### Настройка email уведомлений при ошибках

```bash
ssh root@31.172.73.121

# Установить systemd-mail (если нужно)
apt-get install systemd-mail

# Настроить OnFailure в systemd сервисе
# (требует дополнительной конфигурации SMTP)
```

### Мониторинг через cron

```bash
# Создать скрипт проверки
cat > /usr/local/bin/check_sql_agent.sh << 'EOF'
#!/bin/bash
if ! systemctl is-active --quiet sql-agent; then
    echo "SQL-agent не работает!" | mail -s "Alert: SQL-agent down" admin@example.com
fi
EOF

chmod +x /usr/local/bin/check_sql_agent.sh

# Добавить в cron (проверка каждые 5 минут)
echo "*/5 * * * * /usr/local/bin/check_sql_agent.sh" | crontab -
```

---

## 💡 Примеры использования

### Пример 1: Поиск медленных запросов

```bash
# Найти задачи, которые выполнялись больше 60 секунд
ssh root@31.172.73.121 "journalctl -u sql-agent | grep 'Задача.*выполнена' | grep -oP 'время: \K[0-9.]+' | awk '\$1 > 60'"
```

### Пример 2: Топ ошибок

```bash
# Топ 10 самых частых ошибок
ssh root@31.172.73.121 "journalctl -u sql-agent -p err | grep -oP 'ERROR.*' | sort | uniq -c | sort -rn | head -10"
```

### Пример 3: Проверка здоровья

```bash
# Комплексная проверка
ssh root@31.172.73.121 << 'EOF'
echo "=== Статус сервиса ==="
systemctl status sql-agent --no-pager | head -10

echo -e "\n=== Последние ошибки ==="
journalctl -u sql-agent -p err -n 5 --no-pager

echo -e "\n=== Использование памяти ==="
ps aux | grep "python.*main.py" | awk '{print $6/1024 " MB"}'

echo -e "\n=== API Health ==="
curl -s http://127.0.0.1:8001/health | jq '.status, .uptime_seconds'
EOF
```

---

## 📞 Быстрые команды

```bash
# Скачать все логи
./get_logs.sh

# Просмотреть последние 50 строк
ssh root@31.172.73.121 "journalctl -u sql-agent -n 50 --no-pager"

# Найти ошибки за сегодня
ssh root@31.172.73.121 "journalctl -u sql-agent --since today -p err"

# Проверить health
curl https://skripkahack.ru/health

# Статистика задач
curl -s https://skripkahack.ru/metrics | jq '.tasks'
```

---

**Дополнительная информация:** [DEPLOYMENT.md](./DEPLOYMENT.md)

