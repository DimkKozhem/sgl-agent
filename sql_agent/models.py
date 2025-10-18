"""
Модели данных для SQL-agent.

Определяет структуры данных для запросов, ответов и задач
оптимизации базы данных с валидацией через Pydantic.
"""

from pydantic import BaseModel, Field, validator
from typing import List, Dict, Any, Optional
from enum import Enum
import uuid


class TaskStatus(str, Enum):
    """
    Статусы выполнения задач оптимизации.

    RUNNING: Задача выполняется
    DONE: Задача успешно завершена
    FAILED: Задача завершилась с ошибкой
    """
    RUNNING = "RUNNING"
    DONE = "DONE"
    FAILED = "FAILED"


class OptimizationRequest(BaseModel):
    """
    Запрос на оптимизацию структуры базы данных.

    Содержит все необходимые данные для анализа и оптимизации:
    - JDBC URL для подключения к БД
    - DDL команды для создания таблиц
    - SQL запросы с метриками выполнения
    """
    url: str = Field(..., description="JDBC URL для подключения к БД")
    ddl: List[Dict[str, str]] = Field(..., description="DDL команды создания таблиц")
    queries: List[Dict[str, Any]] = Field(..., description="SQL запросы с метриками")

    @validator('url')
    def validate_url(cls, v):
        if not v.startswith('jdbc:'):
            raise ValueError('URL должен начинаться с jdbc:')
        return v

    @validator('ddl')
    def validate_ddl(cls, v):
        if not v:
            raise ValueError('DDL не может быть пустым')
        for item in v:
            if 'statement' not in item:
                raise ValueError('DDL должен содержать поле statement')
        return v

    @validator('queries')
    def validate_queries(cls, v):
        if not v:
            raise ValueError('Queries не может быть пустым')
        for item in v:
            required_fields = ['queryid', 'query', 'runquantity']
            for field in required_fields:
                if field not in item:
                    raise ValueError(f'Query должен содержать поле {field}')
        return v


class OptimizationResult(BaseModel):
    """Результат оптимизации"""
    ddl: List[Dict[str, str]] = Field(..., description="Новые DDL команды")
    migrations: List[Dict[str, str]] = Field(..., description="Команды миграции данных")
    queries: List[Dict[str, Any]] = Field(..., description="Оптимизированные запросы")
    quality_score: Optional[int] = Field(None, description="Оценка качества оптимизации (1-100)")


class Task(BaseModel):
    """Задача оптимизации"""
    task_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    status: TaskStatus = Field(default=TaskStatus.RUNNING)
    request: OptimizationRequest
    result: Optional[OptimizationResult] = None
    error: Optional[str] = None


class TaskCreateResponse(BaseModel):
    """Ответ на создание задачи"""
    taskid: str


class TaskStatusResponse(BaseModel):
    """Ответ на запрос статуса задачи"""
    status: TaskStatus


class TaskResultResponse(BaseModel):
    """Ответ на запрос результата задачи"""
    ddl: List[Dict[str, str]]
    migrations: List[Dict[str, str]]
    queries: List[Dict[str, Any]]

    @classmethod
    def from_optimization_result(cls, result: OptimizationResult):
        return cls(
            ddl=result.ddl,
            migrations=result.migrations,
            queries=result.queries
        )


class StatsResponse(BaseModel):
    """Статистика сервиса"""
    task_statistics: Dict[str, int]
    system_info: Dict[str, Any]
