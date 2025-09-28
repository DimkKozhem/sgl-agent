# SQL-agent

Система для асинхронного анализа и оптимизации структуры базы данных с интеграцией LLM, разработанная для конкурса ЛЦТ 2025.

## 🎯 Описание

SQL-agent - это REST API сервис, который принимает:
- **DDL команды** для создания таблиц
- **SQL запросы** с метриками выполнения
- **JDBC URL** для подключения к базе данных

И возвращает рекомендации по оптимизации:
- Новые DDL команды для улучшения структуры
- Команды миграции данных
- Оптимизированные SQL запросы

## 🚀 Быстрый старт

```bash
# Установка зависимостей
pip install -r requirements.txt

# Настройка API ключей (обязательно для LLM)
cp env_example.txt .env
# Отредактируйте .env файл и добавьте ваш OPEN_ROUTER API ключ

# Запуск сервера
python main.py

# Тестирование с тестовыми данными
python test.py
```

## 📊 API Endpoints

- `GET /health` - Проверка состояния сервиса
- `POST /new` - Создание задачи оптимизации
- `GET /status?task_id=<id>` - Статус задачи
- `GET /getresult?task_id=<id>` - Результат задачи
- `GET /stats` - Статистика сервиса

## 🏗️ Архитектура

- **SimpleTaskManager** - Менеджер асинхронных задач с поддержкой LLM
- **LLMAnalyzer** - Анализатор БД с использованием qwen/qwen-2.5-7b-instruct
- **SimpleAPI** - REST API на FastAPI
- **Models** - Модели данных с валидацией
- **LogRotator** - Система ротации логов

## 🔧 Особенности

- ✅ **Интеграция с LLM** - использует qwen/qwen-2.5-7b-instruct для анализа БД
- ✅ **Автоматическая оценка качества** - google/gemini-2.5-flash-preview-09-2025 оценивает рекомендации по 10-балльной шкале
- ✅ **Асинхронная обработка задач** - до 4 одновременных задач
- ✅ **Fallback механизм** - автоматический переход к простой логике при ошибках LLM
- ✅ **Валидация данных** с Pydantic
- ✅ **Автоматическая ротация логов** - сохранение в файлы каждый час
- ✅ **Соответствие требованиям ТЗ** конкурса ЛЦТ 2025

## 📁 Структура проекта

```
sql_agent/
├── __init__.py              # Инициализация пакета
├── models.py                # Модели данных Pydantic
├── task_manager.py          # Менеджер асинхронных задач с LLM
├── llm_analyzer.py          # Анализатор БД с LLM
├── log_rotator.py           # Система ротации логов
└── api.py                   # REST API на FastAPI

datasets/                    # Тестовые данные
├── flights.json             # Данные авиаперелетов (20 запросов)
└── questsH.json             # Данные квест-платформы (10 запросов)

main.py                      # Главный модуль запуска
test.py                      # Система тестирования
LCT.ipynb                    # Эксперименты с LLM моделями
env_example.txt              # Пример конфигурации API ключей
requirements.txt             # Зависимости проекта
README.md                    # Документация
LOG_ROTATION.md              # Документация по ротации логов
```

## 🧪 Тестовые данные

### flights.json
- **1 таблица** с 50+ полями авиаперелетов
- **20 сложных запросов** для анализа задержек, маршрутов, авиакомпаний
- **~20,000 выполнений** запросов в общей сложности
- **Темы**: анализ задержек, эффективность авиакомпаний, сезонность

### questsH.json  
- **37 таблиц** в схеме квест-платформы
- **10 аналитических запросов** для бизнес-аналитики
- **Сложная схема**: авторы, клиенты, квесты, экскурсии, платежи
- **Темы**: конверсия, сегментация клиентов, анализ продаж

## 🔬 Эксперименты с LLM

Проведены эксперименты с **27 различными LLM моделями**:

### Топ-модели (8-9 баллов):
- `nvidia/nemotron-nano-9b-v2`
- `qwen/qwen-2.5-7b-instruct` 
- `mistralai/mistral-7b-instruct`

### Проблемные модели (0-3 балла):
- `google/gemini-flash-1.5-8b`
- `moonshotai/kimi-vl-a3b-thinking`
- `meta-llama/llama-guard-3-8b`

**Методология оценки**: google/gemini-2.5-flash-preview-09-2025 оценивает качество ответов LLM по 10-балльной шкале на основе корректности и полноты рекомендаций по оптимизации БД.

## 🤖 LLM Анализатор (LLMAnalyzer)

### 🧠 Обзор модуля

`LLMAnalyzer` - это ключевой компонент системы, который использует большие языковые модели для анализа структуры базы данных и генерации рекомендаций по оптимизации.

### 📋 Основные возможности

#### 1. **Анализ структуры БД**
- Изучение DDL команд и схемы данных
- Анализ типов данных и связей между таблицами
- Выявление потенциальных проблем в структуре

#### 2. **Оптимизация запросов**
- Переписывание SQL для улучшения производительности
- Предложения по созданию индексов
- Рекомендации по партиционированию

#### 3. **Стратегии оптимизации**
- Рекомендации по изменению типов полей
- Предложения по нормализации/денормализации
- Стратегии кэширования и материализации

#### 4. **Автоматическая оценка качества**
- Оценка каждой рекомендации по 10-балльной шкале
- Анализ корректности и полноты ответа
- Логирование оценок для мониторинга

### 🔧 Технические детали

#### Модели LLM:
- **Анализ**: `qwen/qwen-2.5-7b-instruct` - основная модель для анализа БД
- **Оценка**: `google/gemini-2.5-flash-preview-09-2025` - модель для оценки качества

#### Параметры запросов:
- **Температура**: 0.1 (низкая для стабильных результатов)
- **Максимум токенов**: 4000
- **API**: OpenRouter для доступа к моделям

### 📝 Структура класса

```python
class LLMAnalyzer:
    def __init__(self, api_key: Optional[str] = None, base_url: Optional[str] = None):
        """Инициализация с настройкой API ключей и моделей"""
        
    def analyze_database(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """Основной метод анализа БД"""
        
    def evaluate_response(self, task_input: str, output: str) -> int:
        """Оценка качества ответа по 10-балльной шкале"""
        
    def _get_analysis_prompt(self) -> str:
        """Генерация промпта для анализа"""
        
    def _create_fallback_result(self, llm_output: str, request_data: Dict) -> Dict:
        """Создание резервного результата при ошибках"""
```

### 🔄 Процесс работы

#### 1. **Инициализация**
```python
# В task_manager.py
self.llm_analyzer = LLMAnalyzer()
```

#### 2. **Анализ БД**
```python
# Подготовка данных
request_data = {
    "url": task.request.url,
    "ddl": task.request.ddl,
    "queries": task.request.queries
}

# Анализ с помощью LLM
llm_result = self.llm_analyzer.analyze_database(request_data)
```

#### 3. **Оценка качества**
```python
# Оценка ответа
quality_score = self.llm_analyzer.evaluate_response(
    task_input_str, 
    output_str
)
```

### 📤 Формат входных данных

```json
{
  "url": "jdbc:trino://example.com:8080?catalog=test",
  "ddl": [
    {
      "statement": "CREATE TABLE users (id INTEGER, name VARCHAR(255))"
    }
  ],
  "queries": [
    {
      "queryid": "query-1",
      "query": "SELECT * FROM users WHERE id = 1",
      "runquantity": 100,
      "executiontime": 5
    }
  ]
}
```

### 📥 Формат выходных данных

```json
{
  "ddl": [
    {
      "statement": "ALTER TABLE users ADD COLUMN email VARCHAR(255)"
    }
  ],
  "migrations": [
    {
      "statement": "UPDATE users SET email = 'default@example.com' WHERE email IS NULL"
    }
  ],
  "queries": [
    {
      "queryid": "query-1",
      "query": "SELECT id, name, email FROM users WHERE id = 1",
      "runquantity": 100,
      "executiontime": 3
    }
  ]
}
```

### 🎯 Промпт для анализа

Система использует детальный промпт, который включает:

1. **Роль**: "You are a database analyst and optimization expert"
2. **Задача**: Анализ логической модели данных и SQL запросов
3. **Входные данные**: DDL, запросы с статистикой, JDBC URL
4. **Требования к ответу**: JSON с ddl, migrations, queries
5. **Критерии качества**: Корректность, полнота, практичность

### 🔍 Логирование и мониторинг

#### Детальное логирование:
```
📤 ЗАПРОС К LLM (модель: qwen/qwen-2.5-7b-instruct):
System prompt: You are a database analyst and optimization expert...
User input: {"url": "jdbc:trino://example.com:8080?catalog=test"...

📥 ОТВЕТ ОТ LLM:
Полный ответ: {"ddl": [{"statement": "ALTER TABLE users ADD COLUMN email VARCHAR(255)"}]}

📤 ЗАПРОС К МОДЕЛИ ОЦЕНКИ (модель: google/gemini-2.5-flash-preview-09-2025):
Промпт для оценки: You are an evaluator of LLM responses...

📥 ОТВЕТ ОТ МОДЕЛИ ОЦЕНКИ:
Сырой ответ: '8'

✅ Итоговая оценка качества: 8/10
```

### ⚠️ Обработка ошибок

#### Fallback механизм:
1. **Ошибка API** → Возврат базового результата
2. **Ошибка парсинга JSON** → Создание структурированного ответа
3. **Ошибка оценки** → Оценка по умолчанию (5/10)

#### Типы ошибок:
- `ValueError` - отсутствие API ключа
- `json.JSONDecodeError` - некорректный JSON ответ
- `Exception` - общие ошибки API

### 🚀 Интеграция с системой

#### В task_manager.py:
```python
# Инициализация
if self.use_llm:
    self.llm_analyzer = LLMAnalyzer()

# Использование
if self.use_llm and self.llm_analyzer:
    llm_result = self.llm_analyzer.analyze_database(request_data)
    result = self._create_result_from_llm(llm_result, task.request)
    quality_score = self.llm_analyzer.evaluate_response(task_input_str, output_str)
```

#### В api.py:
```python
# Статистика
if task_manager.llm_analyzer:
    llm_info.update({
        "analysis_model": task_manager.llm_analyzer.analysis_model,
        "evaluation_model": task_manager.llm_analyzer.evaluation_model
    })
```

### 📊 Статистика использования

Через endpoint `/stats` доступна информация:
- Модель анализа: `qwen/qwen-2.5-7b-instruct`
- Модель оценки: `google/gemini-2.5-flash-preview-09-2025`
- Статус LLM: включен/выключен
- Количество обработанных задач

### 🔧 Настройка

#### Переменные окружения:
```bash
# .env файл
OPEN_ROUTER=your_openrouter_api_key_here
```

#### Кастомизация:
```python
# Создание с кастомными параметрами
analyzer = LLMAnalyzer(
    api_key="custom_key",
    base_url="custom_url"
)
```

## 📝 Система ротации логов

Автоматическое сохранение логов в файлы с ротацией каждый час:

### Возможности:
- **⏰ Автоматическая ротация** - новый файл каждый час
- **🗑️ Очистка старых файлов** - хранится максимум 24 файла
- **📊 Мониторинг через API** - информация о логах в `/stats`
- **🔍 Удобный поиск** - структурированные файлы для анализа

### Использование:
```bash
# Логи сохраняются в директории logs/
ls -la logs/

# Просмотр последнего лог файла
tail -f logs/sql_agent_*.log

# Поиск в логах
grep "LLM" logs/sql_agent_*.log
```

### Формат файлов:
- `sql_agent_YYYYMMDD_HHMMSS.log`
- Кодировка UTF-8
- Подробная документация в `LOG_ROTATION.md`

## 📝 Примеры использования

### Создание задачи оптимизации

```bash
curl -X POST "http://localhost:8001/new" \
  -H "Content-Type: application/json" \
  -d '{
    "url": "jdbc:trino://localhost:8080?catalog=flights",
    "ddl": [
      {"statement": "CREATE TABLE flights.public.flights (id INTEGER, name VARCHAR)"}
    ],
    "queries": [
      {
        "queryid": "test-query-1",
        "query": "SELECT * FROM flights.public.flights WHERE id = 1",
        "runquantity": 100
      }
    ]
  }'
```

### Проверка статуса задачи

```bash
curl "http://localhost:8001/status?task_id=your-task-id"
```

### Получение результата

```bash
curl "http://localhost:8001/getresult?task_id=your-task-id"
```

### Получение статистики

```bash
curl "http://localhost:8001/stats" | jq '.llm_info'
```

## ⚠️ Известные ограничения

1. **Нет реального подключения к БД** - только анализ структуры без выполнения запросов
2. **Зависимость от API ключей** - требует настройки OPEN_ROUTER для работы LLM
3. **Ограниченная обработка ошибок** в некоторых компонентах
4. **Таймауты LLM** - возможны задержки при обращении к внешним API
5. **Парсинг JSON ответов** - иногда LLM возвращает некорректный JSON, используется fallback

## 🔧 Технические детали

- **Язык**: Python 3.12+
- **Фреймворк**: FastAPI + Uvicorn
- **LLM**: qwen/qwen-2.5-7b-instruct (анализ) + google/gemini-2.5-flash-preview-09-2025 (оценка)
- **Валидация**: Pydantic
- **Асинхронность**: asyncio
- **Тестирование**: Встроенная система с конкурентными задачами
- **Максимум воркеров**: 4
- **Таймаут задач**: 15 минут
- **API**: OpenRouter для доступа к LLM моделям
- **Ротация логов**: schedule для автоматического сохранения логов

## 🎯 Готовность к конкурсу

SQL-agent полностью готов к участию в конкурсе ЛЦТ 2025:
- ✅ Все требования ТЗ выполнены
- ✅ Система протестирована и работает стабильно
- ✅ Код понятен для разработчиков
- ✅ Проект готов к развертыванию

## 🔄 Последние обновления

### v2.0 - Система ротации логов
- ✅ **Автоматическая ротация логов** - сохранение в файлы каждый час
- ✅ **Улучшенное логирование LLM** - детальные запросы и ответы
- ✅ **Мониторинг через API** - информация о логах в `/stats`
- ✅ **Graceful shutdown** - корректное завершение работы
- ✅ **Очистка старых файлов** - автоматическое управление дисковым пространством

### v1.0 - Базовая функциональность
- ✅ **Интеграция с LLM** - qwen/qwen-2.5-7b-instruct для анализа
- ✅ **Автоматическая оценка качества** - gemini-2.5-flash для оценки
- ✅ **Асинхронная обработка** - до 4 одновременных задач
- ✅ **REST API** - полный набор endpoints
- ✅ **Тестирование** - встроенная система тестов

**Проект готов к отправке организаторам конкурса!** 🚀