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
import os
import logging
import json

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


# Обработчик ошибок валидации JSON (возвращает 400 вместо 422)
@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """
    Обработчик ошибок валидации Pydantic.
    Возвращает 400 Bad Request вместо стандартного 422.
    """
    logger.warning(f"Ошибка валидации для {request.url.path}: {exc.errors()}")
    return JSONResponse(
        status_code=400,
        content={
            "error": "Bad Request",
            "detail": "Невалидный JSON или неверная структура данных",
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
task_manager = SimpleTaskManager(max_workers=4, task_timeout_minutes=20, use_llm=True)


@app.get("/health")
async def health_check():
    """
    Проверка состояния сервиса.

    Returns:
        dict: Статус сервиса, версия и статистика задач
    """
    stats = task_manager.get_stats()
    return {
        "status": "healthy",
        "version": "1.0.0",
        "stats": stats
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


@app.on_event("startup")
async def startup_event():
    """Инициализация при запуске"""
    logger.info("SQL-agent запущен")


@app.on_event("shutdown")
async def shutdown_event():
    """Очистка при завершении"""
    logger.info("SQL-agent остановлен")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
