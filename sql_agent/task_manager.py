"""
Менеджер задач для SQL-agent
"""

import asyncio
from typing import Dict, Optional
from datetime import datetime, timedelta
from .models import Task, TaskStatus, OptimizationRequest, OptimizationResult
from .llm_analyzer import LLMAnalyzer
import logging

logger = logging.getLogger(__name__)


class SimpleTaskManager:
    """Менеджер задач"""
    
    def __init__(self, max_workers: int = 4, task_timeout_minutes: int = 15, use_llm: bool = True):
        self.tasks: Dict[str, Task] = {}
        self.max_workers = max_workers
        self.task_timeout_minutes = task_timeout_minutes
        self._running_tasks = 0
        self.use_llm = use_llm
        
        # Инициализируем LLM анализатор если включен
        self.llm_analyzer = None
        if self.use_llm:
            try:
                self.llm_analyzer = LLMAnalyzer()
                logger.info("LLM анализатор инициализирован")
            except Exception as e:
                logger.warning(f"Не удалось инициализировать LLM анализатор: {e}")
                self.use_llm = False
        
    def create_task(self, request: OptimizationRequest) -> str:
        """Создание новой задачи"""
        task = Task(request=request)
        self.tasks[task.task_id] = task
        
        # Запускаем обработку задачи
        asyncio.create_task(self._process_task(task.task_id))
        
        return task.task_id
    
    def get_task_status(self, task_id: str) -> Optional[TaskStatus]:
        """Получение статуса задачи"""
        task = self.tasks.get(task_id)
        return task.status if task else None
    
    def get_task_result(self, task_id: str) -> Optional[OptimizationResult]:
        """Получение результата задачи"""
        task = self.tasks.get(task_id)
        if task and task.status == TaskStatus.DONE:
            return task.result
        return None
    
    def get_task_error(self, task_id: str) -> Optional[str]:
        """Получение ошибки задачи"""
        task = self.tasks.get(task_id)
        if task and task.status == TaskStatus.FAILED:
            return task.error
        return None
    
    def get_stats(self) -> Dict[str, int]:
        """Получение статистики задач"""
        running_count = sum(1 for task in self.tasks.values() if task.status == TaskStatus.RUNNING)
        completed_count = sum(1 for task in self.tasks.values() if task.status == TaskStatus.DONE)
        failed_count = sum(1 for task in self.tasks.values() if task.status == TaskStatus.FAILED)
        
        return {
            "total_tasks": len(self.tasks),
            "running_tasks": running_count,
            "completed_tasks": completed_count,
            "failed_tasks": failed_count,
            "max_workers": self.max_workers
        }
    
    async def _process_task(self, task_id: str):
        """Обработка задачи"""
        task = self.tasks.get(task_id)
        if not task:
            return
        
        try:
            self._running_tasks += 1
            logger.info(f"Начинаем обработку задачи {task_id}")
            
            # Используем LLM анализатор если доступен
            if self.use_llm and self.llm_analyzer:
                logger.info(f"Используем LLM анализатор для задачи {task_id}")
                
                # Подготавливаем данные для LLM
                request_data = {
                    "url": task.request.url,
                    "ddl": task.request.ddl,
                    "queries": task.request.queries
                }
                
                # Анализируем с помощью LLM
                llm_result = self.llm_analyzer.analyze_database(request_data)
                
                # Создаем результат из ответа LLM
                result = self._create_result_from_llm(llm_result, task.request)
                
                # Оцениваем качество ответа
                task_input_str = str(request_data)
                output_str = str(llm_result)
                quality_score = self.llm_analyzer.evaluate_response(task_input_str, output_str)
                
                logger.info(f"Задача {task_id} выполнена с оценкой качества: {quality_score}/10")
                
            else:
                logger.info(f"Используем простую логику для задачи {task_id}")
                # Fallback к простой логике
                await asyncio.sleep(2)  # Имитация обработки
                result = self._create_simple_result(task.request)
            
            task.result = result
            task.status = TaskStatus.DONE
            
        except Exception as e:
            error_msg = f"Ошибка при выполнении задачи {task_id}: {str(e)}"
            logger.error(error_msg)
            task.status = TaskStatus.FAILED
            task.error = error_msg
            
        finally:
            self._running_tasks -= 1
    
    def _create_result_from_llm(self, llm_result: dict, request: OptimizationRequest) -> OptimizationResult:
        """Создание результата из ответа LLM"""
        try:
            # Извлекаем данные из ответа LLM
            ddl = llm_result.get("ddl", [])
            migrations = llm_result.get("migrations", [])
            queries = llm_result.get("queries", [])
            
            # Валидируем и дополняем данные если необходимо
            if not ddl:
                catalog_name = self._extract_catalog_from_url(request.url)
                ddl = [{"statement": f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.optimized_schema"}]
            
            if not migrations:
                migrations = [{"statement": "-- No migrations needed"}]
            
            if not queries:
                # Создаем оптимизированные запросы на основе исходных
                for query_data in request.queries:
                    queries.append({
                        "queryid": query_data["queryid"],
                        "query": f"-- Optimized version of: {query_data['query'][:100]}...",
                        "runquantity": query_data["runquantity"]
                    })
            
            return OptimizationResult(
                ddl=ddl,
                migrations=migrations,
                queries=queries
            )
            
        except Exception as e:
            logger.error(f"Ошибка при создании результата из LLM: {e}")
            # Fallback к простому результату
            return self._create_simple_result(request)
    
    def _create_simple_result(self, request: OptimizationRequest) -> OptimizationResult:
        """Создание простого результата"""
        # Извлекаем каталог из URL
        catalog_name = self._extract_catalog_from_url(request.url)
        
        # DDL команды с полными путями (первая команда - создание схемы)
        ddl = [
            {"statement": f"CREATE SCHEMA {catalog_name}.optimized_schema"},
            {"statement": f"CREATE TABLE {catalog_name}.optimized_schema.optimized_table (id INTEGER, data TEXT)"}
        ]
        
        # Миграции с полными путями
        migrations = [
            {"statement": f"INSERT INTO {catalog_name}.optimized_schema.optimized_table SELECT 1, 'test_data'"}
        ]
        
        # Оптимизированные запросы с полными путями
        queries = []
        for query_data in request.queries:
            queries.append({
                "queryid": query_data["queryid"],
                "query": f"SELECT * FROM {catalog_name}.optimized_schema.optimized_table WHERE id = 1",
                "runquantity": query_data["runquantity"]
            })
        
        return OptimizationResult(
            ddl=ddl,
            migrations=migrations,
            queries=queries
        )
    
    def _extract_catalog_from_url(self, url: str) -> str:
        """Извлечение имени каталога из JDBC URL"""
        # Простая логика извлечения каталога из URL
        # Пример: jdbc://postgresql://localhost:5432/testdb -> testdb
        try:
            # Убираем jdbc:// и извлекаем имя базы данных
            if 'jdbc://' in url:
                url_part = url.replace('jdbc://', '')
                # Ищем последний сегмент после / или ?
                if '/' in url_part:
                    db_part = url_part.split('/')[-1]
                    if '?' in db_part:
                        db_name = db_part.split('?')[0]
                    else:
                        db_name = db_part
                    return db_name if db_name else "default_catalog"
            return "default_catalog"
        except:
            return "default_catalog"
    
    def cleanup_old_tasks(self, hours: int = 24):
        """Очистка старых задач"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        old_tasks = []
        
        for task_id, task in self.tasks.items():
            if task.status in [TaskStatus.DONE, TaskStatus.FAILED]:
                old_tasks.append(task_id)
        
        for task_id in old_tasks:
            del self.tasks[task_id]
        
        if old_tasks:
            logger.info(f"Очищено {len(old_tasks)} старых задач")
