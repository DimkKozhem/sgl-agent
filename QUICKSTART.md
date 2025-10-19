# 🚀 Быстрый старт SQL-agent на skripkahack.ru

## ✅ Сервис развернут и работает!

**URL:** https://skripkahack.ru

**Дата развертывания:** 19 октября 2025

---

## 🌐 Проверка работы

```bash
# Проверка доступности
curl https://skripkahack.ru/health

# Ожидаемый результат:
# {
#   "status": "healthy",
#   "version": "1.2.0",
#   "components": {...}
# }
```

---

## 📝 Пример использования

### 1. Создание задачи оптимизации

```bash
curl -X POST https://skripkahack.ru/new \
  -H "Content-Type: application/json" \
  -d '{
    "url": "jdbc:trino://localhost:8080?catalog=mydb",
    "ddl": [
      {
        "statement": "CREATE TABLE mydb.public.users (id INT, name VARCHAR(100), created_at DATE)"
      }
    ],
    "queries": [
      {
        "queryid": "user-search",
        "query": "SELECT * FROM mydb.public.users WHERE created_at > '\''2024-01-01'\''",
        "runquantity": 500,
        "executiontime": 12
      }
    ]
  }'

# Ответ: {"taskid": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"}
```

### 2. Проверка статуса задачи

```bash
TASK_ID="полученный-task-id"

curl "https://skripkahack.ru/status?task_id=$TASK_ID"

# Ответ: {"status": "RUNNING"} или {"status": "DONE"}
```

### 3. Получение результата

```bash
curl "https://skripkahack.ru/getresult?task_id=$TASK_ID" | jq . > result.json

# Результат содержит:
# - ddl: Оптимизированные схемы таблиц
# - migrations: Команды миграции
# - queries: Оптимизированные запросы
```

---

## 🔑 SSH доступ к серверу

```bash
ssh root@31.172.73.121
# Пароль: UpFRdRp0NDvMIYGQ
```

---

## 🔧 Управление сервисом

```bash
# Статус
systemctl status sql-agent

# Логи в реальном времени
journalctl -u sql-agent -f

# Перезапуск
systemctl restart sql-agent

# Остановка/Запуск
systemctl stop sql-agent
systemctl start sql-agent
```

---

## 📊 Метрики и мониторинг

```bash
# Общие метрики
curl https://skripkahack.ru/metrics | jq .

# Статус очереди
curl -s https://skripkahack.ru/metrics | jq '.queue'

# Статистика задач
curl -s https://skripkahack.ru/metrics | jq '.tasks'
```

---

## 📁 Важные пути на сервере

```
/opt/sql-agent/                     # Приложение
/opt/sql-agent/logs/                # Логи приложения
/opt/sql-agent/.env                 # API ключи
/etc/nginx/conf.d/domains/          # Конфигурация Nginx
/etc/systemd/system/sql-agent.service  # Systemd сервис
```

---

## ⚠️ Быстрое решение проблем

### Сервис не отвечает
```bash
ssh root@31.172.73.121
systemctl restart sql-agent
systemctl status sql-agent
```

### Посмотреть последние ошибки
```bash
ssh root@31.172.73.121
journalctl -u sql-agent -n 50 --no-pager
```

### Проверить использование ресурсов
```bash
ssh root@31.172.73.121
top -p $(pgrep -f "python.*main.py")
```

---

## 🔐 Безопасность

- ✅ HTTPS с валидным SSL сертификатом
- ✅ Автообновление сертификата (certbot)
- ✅ Приложение недоступно напрямую из интернета
- ✅ Nginx reverse proxy
- ✅ API ключи защищены

---

## 📚 Дополнительная информация

Подробная документация: [DEPLOYMENT.md](./DEPLOYMENT.md)

Техническая документация: [README.md](./README.md)

---

**Версия:** 1.2.0  
**Статус:** ✅ Production Ready  
**Домен:** https://skripkahack.ru  
**Сервер:** 31.172.73.121

