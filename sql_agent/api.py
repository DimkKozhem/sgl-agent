"""
REST API для SQL-agent
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse
from typing import Optional
import logging

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
    import logging
    logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)

# Создание FastAPI приложения
app = FastAPI(
    title="SQL-agent",
    description="REST API для анализа и оптимизации структуры базы данных",
    version="1.0.0"
)

# Создание менеджера задач с поддержкой LLM
task_manager = SimpleTaskManager(max_workers=4, task_timeout_minutes=15, use_llm=True)


@app.get("/health")
async def health_check():
    """Проверка состояния сервиса"""
    stats = task_manager.get_stats()
    return {
        "status": "healthy",
        "version": "1.0.0",
        "stats": stats
    }


@app.post("/new", response_model=TaskCreateResponse)
async def create_optimization_task(request: OptimizationRequest):
    """Создание новой задачи оптимизации"""
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
