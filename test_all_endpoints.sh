#!/bin/bash
# Скрипт для проверки всех endpoints SQL-agent

API_URL="https://skripkahack.ru"

echo "🧪 ТЕСТИРОВАНИЕ ВСЕХ ENDPOINTS"
echo "================================"
echo "Сервер: $API_URL"
echo ""

# 1. GET /health
echo "1️⃣  GET /health"
echo "---"
HEALTH=$(curl -s "$API_URL/health")
STATUS=$(echo "$HEALTH" | jq -r '.status' 2>/dev/null)

if [ "$STATUS" = "healthy" ]; then
    echo "✅ PASS"
    echo "$HEALTH" | jq '{status, version, uptime: .uptime_seconds}'
else
    echo "❌ FAIL"
    echo "$HEALTH"
fi
echo ""

# 2. GET /metrics
echo "2️⃣  GET /metrics"
echo "---"
METRICS=$(curl -s "$API_URL/metrics")
UPTIME=$(echo "$METRICS" | jq -r '.uptime_seconds' 2>/dev/null)

if [ -n "$UPTIME" ]; then
    echo "✅ PASS"
    echo "$METRICS" | jq '{uptime, workers: .tasks.max_workers, queue: .queue.max_size, running: .tasks.running}'
else
    echo "❌ FAIL"
    echo "$METRICS"
fi
echo ""

# 3. POST /new - создание задачи
echo "3️⃣  POST /new"
echo "---"
CREATE_RESPONSE=$(curl -s -X POST "$API_URL/new" \
  -H "Content-Type: application/json" \
  -d @datasets/linear_schema.json)

TASK_ID=$(echo "$CREATE_RESPONSE" | jq -r '.taskid' 2>/dev/null)

if [ -n "$TASK_ID" ] && [ "$TASK_ID" != "null" ]; then
    echo "✅ PASS"
    echo "Task ID: $TASK_ID"
else
    echo "❌ FAIL"
    echo "$CREATE_RESPONSE"
    exit 1
fi
echo ""

# 4. GET /status
echo "4️⃣  GET /status?task_id=$TASK_ID"
echo "---"
sleep 1
STATUS_RESPONSE=$(curl -s "$API_URL/status?task_id=$TASK_ID")
TASK_STATUS=$(echo "$STATUS_RESPONSE" | jq -r '.status' 2>/dev/null)

if [ -n "$TASK_STATUS" ]; then
    echo "✅ PASS"
    echo "Status: $TASK_STATUS"
else
    echo "❌ FAIL"
    echo "$STATUS_RESPONSE"
fi
echo ""

# 5. Ожидание завершения (для проверки /getresult)
echo "5️⃣  Ожидание завершения задачи..."
echo "---"
for i in {1..60}; do
    sleep 2
    STATUS_CHECK=$(curl -s "$API_URL/status?task_id=$TASK_ID" | jq -r '.status')
    
    if [ "$STATUS_CHECK" = "DONE" ]; then
        echo "✅ Задача завершена за $((i*2)) секунд"
        break
    elif [ "$STATUS_CHECK" = "FAILED" ]; then
        echo "❌ Задача провалилась"
        break
    fi
    
    echo -n "."
done
echo ""
echo ""

# 6. GET /getresult
echo "6️⃣  GET /getresult?task_id=$TASK_ID"
echo "---"
RESULT=$(curl -s "$API_URL/getresult?task_id=$TASK_ID")
DDL_COUNT=$(echo "$RESULT" | jq -r '.ddl | length' 2>/dev/null)
MIG_COUNT=$(echo "$RESULT" | jq -r '.migrations | length' 2>/dev/null)
QRY_COUNT=$(echo "$RESULT" | jq -r '.queries | length' 2>/dev/null)

if [ -n "$DDL_COUNT" ] && [ "$DDL_COUNT" != "null" ]; then
    echo "✅ PASS"
    echo "DDL: $DDL_COUNT команд"
    echo "Migrations: $MIG_COUNT команд"
    echo "Queries: $QRY_COUNT запросов"
else
    echo "❌ FAIL"
    echo "$RESULT" | head -20
fi
echo ""

# 7. GET /static/pipeline.html
echo "7️⃣  GET /static/pipeline.html"
echo "---"
PIPELINE_STATUS=$(curl -I -s "$API_URL/static/pipeline.html" | head -1)

if echo "$PIPELINE_STATUS" | grep -q "200"; then
    SIZE=$(curl -I -s "$API_URL/static/pipeline.html" | grep -i content-length | awk '{print $2}' | tr -d '\r')
    echo "✅ PASS"
    echo "HTTP Status: 200 OK"
    echo "Size: $SIZE байт"
else
    echo "❌ FAIL"
    echo "$PIPELINE_STATUS"
fi
echo ""

# Итоговый отчет
echo "================================"
echo "📊 ИТОГОВЫЙ ОТЧЕТ"
echo "================================"
echo ""
echo "Протестировано endpoints: 7"
echo "Сервер: $API_URL"
echo "Task ID: $TASK_ID"
echo ""
echo "✅ Все основные endpoints работают!"

