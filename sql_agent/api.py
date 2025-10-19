"""
REST API для SQL-agent.

Предоставляет HTTP endpoints для создания задач оптимизации базы данных,
получения статуса выполнения и результатов анализа.
"""

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.middleware.cors import CORSMiddleware
import os
import logging
import json
import time

from .models import (
    OptimizationRequest,
    TaskCreateResponse,
    TaskStatusResponse,
    TaskResultResponse,
    TaskStatus
)
from .task_manager import SimpleTaskManager

# Настройка логирования с ротацией
try:
    from .log_rotator import setup_logging
    log_rotator = setup_logging()
except ImportError:
    # Fallback если ротатор недоступен
    logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

# Создание FastAPI приложения
app = FastAPI(
    title="SQL-agent",
    description="REST API для анализа и оптимизации структуры базы данных",
    version="1.0.0"
)

# CORS middleware - разрешаем запросы с любых источников
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Время запуска сервиса для подсчета uptime
startup_time = time.time()


# Обработчик ошибок валидации JSON (возвращает 400 вместо 422)
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """
    Обработчик ошибок валидации Pydantic.
    Возвращает 400 Bad Request вместо стандартного 422.
    Распознает попытки отправить логи вместо запросов.
    """
    logger.warning(f"Ошибка валидации для {request.url.path}: {exc.errors()}")
    
    # Проверяем, не пытается ли клиент отправить лог вместо запроса
    try:
        body = await request.body()
        data = json.loads(body)
        
        # Если есть поля task_id, timestamp, input, output - это лог!
        if all(k in data for k in ['task_id', 'timestamp', 'input', 'output']):
            return JSONResponse(
                status_code=400,
                content={
                    "error": "Bad Request",
                    "detail": "Вы отправили файл лога задачи вместо нового запроса",
                    "hint": "Для создания новой задачи отправьте JSON с полями: url, ddl, queries",
                    "example": {
                        "url": "jdbc:trino://host:port?user=username",
                        "ddl": [{"statement": "CREATE TABLE ..."}],
                        "queries": [{"queryid": "1", "query": "SELECT ...", "runquantity": 100}]
                    }
                }
            )
    except:
        pass
    
    return JSONResponse(
        status_code=400,
        content={
            "error": "Bad Request",
            "detail": "Невалидный JSON или неверная структура данных",
            "required_fields": {
                "url": "JDBC connection string (must start with 'jdbc:')",
                "ddl": "Array of DDL statements with 'statement' field",
                "queries": "Array of queries with 'queryid', 'query', 'runquantity' fields"
            },
            "validation_errors": exc.errors()
        }
    )


# Обработчик ошибок синтаксиса JSON
@app.exception_handler(json.JSONDecodeError)
async def json_decode_exception_handler(request: Request, exc: json.JSONDecodeError):
    """
    Обработчик ошибок синтаксиса JSON.
    Возвращает 400 Bad Request при невалидном синтаксисе JSON.
    """
    logger.warning(f"Ошибка синтаксиса JSON для {request.url.path}: {str(exc)}")
    return JSONResponse(
        status_code=400,
        content={
            "error": "Bad Request",
            "detail": "Невалидный синтаксис JSON",
            "message": str(exc)
        }
    )


# Глобальный обработчик всех необработанных исключений
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """
    Глобальный обработчик всех необработанных исключений.
    Предотвращает разрыв соединения и всегда возвращает JSON ответ.
    """
    # Логируем полный стек ошибки
    logger.exception(f"Необработанное исключение при обработке {request.method} {request.url.path}")
    
    # Возвращаем JSON вместо разрыва соединения
    return JSONResponse(
        status_code=500,
        content={
            "error": "internal-error",
            "detail": f"{exc.__class__.__name__}: {str(exc)}",
            "path": str(request.url.path)
        }
    )


# Монтируем статическую директорию
static_dir = os.path.join(os.path.dirname(__file__), "..", "static")
if os.path.exists(static_dir):
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

# Добавляем эндпоинты для презентаций
@app.get("/presentation")
async def get_presentation():
    """Детальная презентация архитектуры (прокручиваемая)"""
    static_path = os.path.join(os.path.dirname(__file__), "..", "static", "pipeline.html")
    if os.path.exists(static_path):
        return FileResponse(static_path)
    return {"error": "pipeline not found"}



# Создание менеджера задач с поддержкой LLM
task_manager = SimpleTaskManager(
    max_workers=6,              # Максимум 6 параллельных задач (оптимально для 4 CPU)
    task_timeout_minutes=15,    # Таймаут 15 минут на задачу (согласно ТЗ)
    use_llm=True,              # Использовать LLM анализатор
    max_queue_size=1000,        # Максимум задач в очереди (увеличено для тестирования)
    cleanup_after_hours=1      # Очистка завершенных задач через 1 час (для интенсивного тестирования)
)


@app.get("/health")
async def health_check():
    """
    Расширенная проверка состояния сервиса и зависимостей.

    Returns:
        dict: Детальный статус всех компонентов системы
    """
    stats = task_manager.get_stats()
    
    # Проверка LLM подключения
    llm_status = "unavailable"
    llm_error = None
    if task_manager.llm_analyzer is not None:
        try:
            # Простая проверка доступности API ключа
            if task_manager.llm_analyzer.api_key:
                llm_status = "configured"
            else:
                llm_status = "no_api_key"
                llm_error = "API ключ не настроен"
        except Exception as e:
            llm_status = "error"
            llm_error = str(e)
    
    # Проверка очереди задач
    queue_status = "healthy"
    if stats["total_tasks"] >= task_manager.max_queue_size * 0.9:
        queue_status = "nearly_full"
    elif stats["total_tasks"] >= task_manager.max_queue_size:
        queue_status = "full"
    
    # Общий статус системы
    overall_status = "healthy"
    if llm_status == "error" or llm_status == "no_api_key":
        overall_status = "degraded"
    if queue_status == "full":
        overall_status = "critical"
    
    return {
        "status": overall_status,
        "version": "1.2.0",
        "uptime_seconds": round(time.time() - startup_time, 2),
        "components": {
            "api": "healthy",
            "task_manager": "healthy",
            "llm": {
                "status": llm_status,
                "provider": "openrouter" if llm_status == "configured" else None,
                "model": "nvidia/nemotron-nano-9b-v2" if llm_status == "configured" else None,
                "error": llm_error
            },
            "queue": {
                "status": queue_status,
                "current_size": stats["total_tasks"],
                "max_size": task_manager.max_queue_size,
                "usage_percent": round(stats["total_tasks"] / task_manager.max_queue_size * 100, 1) if task_manager.max_queue_size > 0 else 0
            }
        },
        "tasks": {
            "total": stats["total_tasks"],
            "running": stats["running_tasks"],
            "completed": stats["completed_tasks"],
            "failed": stats["failed_tasks"]
        }
    }


@app.post("/new", response_model=TaskCreateResponse)
async def create_optimization_task(request: OptimizationRequest):
    """
    Создание новой задачи оптимизации базы данных.

    Args:
        request: Запрос с DDL, SQL запросами и JDBC URL

    Returns:
        TaskCreateResponse: ID созданной задачи

    Raises:
        HTTPException: При ошибке создания задачи
    """
    try:
        task_id = task_manager.create_task(request)
        logger.info(f"Создана новая задача: {task_id}")
        return TaskCreateResponse(taskid=task_id)
    except Exception as e:
        logger.error(f"Ошибка при создании задачи: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Ошибка при создании задачи: {str(e)}")


@app.get("/status", response_model=TaskStatusResponse)
async def get_task_status(task_id: str = Query(..., description="ID задачи")):
    """Получение статуса задачи"""
    status = task_manager.get_task_status(task_id)
    if status is None:
        raise HTTPException(status_code=404, detail="Задача не найдена")

    return TaskStatusResponse(status=status)


@app.get("/getresult")
async def get_task_result(task_id: str = Query(..., description="ID задачи")):
    """Получение результата задачи"""
    status = task_manager.get_task_status(task_id)
    if status is None:
        raise HTTPException(status_code=404, detail="Задача не найдена")

    if status == TaskStatus.RUNNING:
        return JSONResponse(
            status_code=202,
            content={"error": "Задача еще выполняется"}
        )

    if status == TaskStatus.FAILED:
        error = task_manager.get_task_error(task_id)
        raise HTTPException(status_code=500, detail=f"Задача завершилась с ошибкой: {error}")

    if status == TaskStatus.DONE:
        result = task_manager.get_task_result(task_id)
        if result:
            return TaskResultResponse.from_optimization_result(result)
        else:
            raise HTTPException(status_code=500, detail="Результат задачи недоступен")


@app.get("/stats")
async def get_stats():
    """Получение статистики сервиса"""
    stats = task_manager.get_stats()

    # Добавляем информацию о LLM
    llm_info = {
        "llm_enabled": task_manager.use_llm,
        "llm_analyzer_available": task_manager.llm_analyzer is not None
    }

    if task_manager.llm_analyzer:
        llm_info.update({
            "analysis_model": task_manager.llm_analyzer.analysis_model,
            "evaluation_model": task_manager.llm_analyzer.evaluation_model
        })

    stats["llm_info"] = llm_info

    # Добавляем информацию о логах
    try:
        from sql_agent.log_rotator import get_log_rotator
        log_rotator = get_log_rotator()
        stats["log_info"] = log_rotator.get_log_info()
    except Exception as e:
        stats["log_info"] = {"error": f"Не удалось получить информацию о логах: {e}"}

    return stats


@app.get("/metrics")
async def get_metrics():
    """
    Получение детальных метрик для мониторинга.
    
    Включает:
    - Uptime сервиса
    - Статистику задач
    - Счетчики ошибок по типам
    - Состояние здоровья сервиса
    """
    stats = task_manager.get_stats()
    
    # Вычисляем uptime
    uptime_seconds = time.time() - startup_time
    uptime_minutes = uptime_seconds / 60
    uptime_hours = uptime_minutes / 60
    
    # Определяем состояние здоровья
    error_rate = 0
    if stats["total_tasks"] > 0:
        error_rate = stats["failed_tasks"] / stats["total_tasks"]
    
    health_status = "healthy"
    if error_rate > 0.5:
        health_status = "critical"
    elif error_rate > 0.2:
        health_status = "degraded"
    elif stats.get("error_statistics", {}).get("database_errors", 0) > 10:
        health_status = "warning"  # Много ошибок БД
    
    return {
        "service": "sql-agent",
        "version": "1.1.0",
        "uptime": {
            "seconds": round(uptime_seconds, 2),
            "minutes": round(uptime_minutes, 2),
            "hours": round(uptime_hours, 2)
        },
        "health": health_status,
        "tasks": {
            "total": stats["total_tasks"],
            "running": stats["running_tasks"],
            "actually_processing": stats.get("actually_processing", 0),
            "queued": stats.get("queued_tasks", 0),
            "completed": stats["completed_tasks"],
            "failed": stats["failed_tasks"],
            "max_workers": stats["max_workers"],
            "error_rate": round(error_rate * 100, 2)
        },
        "queue": {
            "max_size": stats.get("max_queue_size", 0),
            "current_size": stats["total_tasks"],
            "usage_percent": stats.get("queue_usage_percent", 0),
            "available_slots": max(0, stats.get("max_queue_size", 0) - stats["total_tasks"])
        },
        "errors": stats.get("error_statistics", {}),
        "llm": {
            "enabled": task_manager.use_llm,
            "available": task_manager.llm_analyzer is not None
        }
    }


@app.on_event("startup")
async def startup_event():
    """Инициализация при запуске"""
    logger.info("SQL-agent запущен")
    
    # Запускаем фоновую задачу автоочистки
    task_manager.start_cleanup_task()


@app.on_event("shutdown")
async def shutdown_event():
    """Очистка при завершении"""
    logger.info("SQL-agent остановлен")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
