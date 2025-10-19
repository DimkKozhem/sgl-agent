#!/bin/bash
# Скрипт проверки что сервер запущен с правильными настройками

echo "🔍 Проверка сервера SQL-agent..."
echo ""

# 1. Проверка что сервер запущен
if pgrep -f "python.*main.py" > /dev/null; then
    echo "✅ Сервер запущен (PID: $(pgrep -f 'python.*main.py'))"
else
    echo "❌ Сервер НЕ запущен!"
    echo "   Запустите: python main.py"
    exit 1
fi

# 2. Проверка что порт 8001 открыт
if netstat -tuln 2>/dev/null | grep -q ":8001 " || ss -tuln 2>/dev/null | grep -q ":8001 "; then
    echo "✅ Порт 8001 открыт"
else
    echo "⚠️  Порт 8001 не найден"
fi

# 3. Проверка health endpoint
echo ""
echo "🏥 Проверка /health endpoint..."
HEALTH=$(curl -s http://localhost:8001/health 2>/dev/null)
if [ $? -eq 0 ]; then
    STATUS=$(echo "$HEALTH" | jq -r '.status' 2>/dev/null || echo "unknown")
    echo "✅ Health check: $STATUS"
    echo "$HEALTH" | jq '.' 2>/dev/null || echo "$HEALTH"
else
    echo "❌ Health endpoint недоступен"
fi

# 4. Проверка что в main.py есть настройки для туннеля
echo ""
echo "🔧 Проверка настроек в main.py..."
if grep -q "timeout_keep_alive=300" main.py; then
    echo "✅ timeout_keep_alive=300 найден"
else
    echo "❌ timeout_keep_alive=300 НЕ НАЙДЕН!"
fi

if grep -q "h11_max_incomplete_event_size" main.py; then
    echo "✅ h11_max_incomplete_event_size найден"
else
    echo "❌ h11_max_incomplete_event_size НЕ НАЙДЕН!"
fi

if grep -q "Оптимизация для работы через туннель" main.py; then
    echo "✅ Строка про оптимизацию туннеля найдена"
else
    echo "❌ Строка про оптимизацию туннеля НЕ НАЙДЕНА!"
fi

# 5. Проверка последних логов
echo ""
echo "📋 Последние строки из логов:"
LAST_LOG=$(ls -t logs/sql_agent_*.log 2>/dev/null | head -1)
if [ -n "$LAST_LOG" ]; then
    echo "   Файл: $LAST_LOG"
    echo ""
    tail -15 "$LAST_LOG" | grep -E "(Оптимизация|Keep-alive|Буфер|Параллельных|Uvicorn running)" || \
        echo "   ⚠️  Не найдено упоминаний про настройки туннеля в логах!"
else
    echo "   ⚠️  Лог-файлы не найдены"
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📊 ИТОГО:"
echo ""

# Финальная проверка
if grep -q "h11_max_incomplete_event_size" main.py && pgrep -f "python.*main.py" > /dev/null; then
    echo "✅ Сервер запущен с настройками для туннеля"
    echo ""
    echo "🚀 Можно запускать тестовый скрипт!"
    echo "   Таймауты должны исчезнуть."
else
    echo "⚠️  Что-то не так!"
    echo ""
    echo "Рекомендации:"
    echo "1. Остановите сервер: pkill -f 'python.*main.py'"
    echo "2. Очистите кэш: find . -type d -name __pycache__ -exec rm -rf {} +"
    echo "3. Запустите снова: python main.py"
fi

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

