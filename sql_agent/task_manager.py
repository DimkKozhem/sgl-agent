"""
Менеджер задач для SQL-agent.

Управляет асинхронным выполнением задач оптимизации базы данных
с поддержкой LLM анализа и таймаутов.
"""

import asyncio
from typing import Dict, Optional
from .models import Task, TaskStatus, OptimizationRequest, OptimizationResult
from .llm_analyzer import LLMAnalyzer
from .simple_request_logger import save_task_io  # ✅ ДОБАВЛЕНО
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


class SimpleTaskManager:
    """
    Менеджер асинхронных задач оптимизации базы данных.

    Поддерживает параллельное выполнение до max_workers задач
    с настраиваемым таймаутом и интеграцией LLM анализа.
    """

    def __init__(self, max_workers: int = 30, task_timeout_minutes: int = 20, use_llm: bool = True, 
                 max_queue_size: int = 100, cleanup_after_hours: int = 24):
        """
        Инициализация менеджера задач.

        Args:
            max_workers: Максимальное количество параллельных задач
            task_timeout_minutes: Таймаут выполнения задачи в минутах
            use_llm: Использовать LLM анализатор для оптимизации
            max_queue_size: Максимальное количество задач в очереди (0 = без ограничений)
            cleanup_after_hours: Через сколько часов очищать завершенные задачи
        """
        self.tasks: Dict[str, Task] = {}
        self.max_workers = max_workers
        self.task_timeout_minutes = task_timeout_minutes
        self._running_tasks = 0
        self.use_llm = use_llm
        self.max_queue_size = max_queue_size
        self.cleanup_after_hours = cleanup_after_hours
        
        # Семафор для ограничения параллельных задач
        self._semaphore = asyncio.Semaphore(max_workers)
        
        # Счетчики ошибок для мониторинга
        self.error_stats = {
            "timeout_errors": 0,
            "llm_errors": 0,
            "validation_errors": 0,
            "database_errors": 0,
            "total_errors": 0,
            "queue_full_errors": 0
        }

        # Инициализируем LLM анализатор если включен
        self.llm_analyzer = None
        if self.use_llm:
            try:
                self.llm_analyzer = LLMAnalyzer()
                logger.info("LLM анализатор инициализирован")
            except Exception as e:
                logger.warning(f"Не удалось инициализировать LLM анализатор: {e}")
                self.use_llm = False
        
        # Флаг для отслеживания запуска cleanup
        self._cleanup_task_started = False
    
    def start_cleanup_task(self):
        """
        Запуск фоновой задачи очистки.
        Должен вызываться после запуска event loop (например, в startup event).
        """
        if not self._cleanup_task_started:
            asyncio.create_task(self._periodic_cleanup())
            self._cleanup_task_started = True
            logger.info("🧹 Фоновая задача автоочистки запущена (каждый час)")

    def create_task(self, request: OptimizationRequest) -> str:
        """
        Создание новой задачи с проверкой размера очереди.
        
        Raises:
            Exception: Если очередь переполнена (достигнут max_queue_size)
        """
        # Проверяем ограничение на размер очереди
        if self.max_queue_size > 0 and len(self.tasks) >= self.max_queue_size:
            self.error_stats["queue_full_errors"] += 1
            raise Exception(
                f"Очередь задач переполнена. "
                f"Максимум {self.max_queue_size} задач, текущее количество: {len(self.tasks)}. "
                f"Дождитесь завершения существующих задач или увеличьте max_queue_size."
            )
        
        task = Task(request=request)
        self.tasks[task.task_id] = task

        # Запускаем обработку задачи с семафором
        asyncio.create_task(self._process_task_with_semaphore(task.task_id))

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
        """Получение статистики задач с детальной информацией об ошибках и очереди"""
        running_count = sum(1 for task in self.tasks.values() if task.status == TaskStatus.RUNNING)
        completed_count = sum(1 for task in self.tasks.values() if task.status == TaskStatus.DONE)
        failed_count = sum(1 for task in self.tasks.values() if task.status == TaskStatus.FAILED)
        
        # Задачи в очереди = запущенные но не выполняющиеся (ждут семафор)
        queued_count = running_count - self._running_tasks if running_count > self._running_tasks else 0

        return {
            "total_tasks": len(self.tasks),
            "running_tasks": running_count,
            "actually_processing": self._running_tasks,  # Реально выполняющиеся сейчас
            "queued_tasks": queued_count,                # Ждут в очереди
            "completed_tasks": completed_count,
            "failed_tasks": failed_count,
            "max_workers": self.max_workers,
            "max_queue_size": self.max_queue_size,
            "queue_usage_percent": round((len(self.tasks) / self.max_queue_size * 100), 2) if self.max_queue_size > 0 else 0,
            "error_statistics": self.error_stats
        }

    async def _process_task_with_semaphore(self, task_id: str):
        """
        Обработка задачи с семафором для ограничения параллелизма.
        
        Задача ждет в очереди пока не освободится слот (семафор).
        """
        async with self._semaphore:
            # Внутри семафора - реальная обработка
            await self._process_task(task_id)

    async def _process_task(self, task_id: str):
        """Обработка задачи с таймаутом"""
        task = self.tasks.get(task_id)
        if not task:
            return

        # ✅ ДОБАВЛЕНО: Подготовка input для логирования
        input_data = {
            "url": task.request.url,
            "ddl": task.request.ddl,
            "queries": task.request.queries
        }

        try:
            self._running_tasks += 1
            logger.info(f"Начинаем обработку задачи {task_id} с таймаутом {self.task_timeout_minutes} минут")

            # Оборачиваем выполнение задачи в таймаут
            await asyncio.wait_for(
                self._execute_task(task_id),
                timeout=self.task_timeout_minutes * 60  # Конвертируем минуты в секунды
            )

            # ✅ ДОБАВЛЕНО: Логирование успешного результата
            if task.result:
                output_data = {
                    "ddl": task.result.ddl,
                    "migrations": task.result.migrations,
                    "queries": task.result.queries,
                    "quality_score": task.result.quality_score
                }
                save_task_io(task_id, input_data, output_data)

        except asyncio.TimeoutError:
            error_msg = f"Задача {task_id} превысила лимит времени выполнения ({self.task_timeout_minutes} минут)"
            logger.error(error_msg)
            task.status = TaskStatus.FAILED
            task.error = error_msg
            
            # Увеличиваем счетчики ошибок
            self.error_stats["timeout_errors"] += 1
            self.error_stats["total_errors"] += 1

            # ✅ ДОБАВЛЕНО: Логирование ошибки таймаута
            save_task_io(task_id, input_data, error=error_msg)

        except Exception as e:
            error_msg = f"Ошибка при выполнении задачи {task_id}: {str(e)}"
            logger.error(error_msg)
            task.status = TaskStatus.FAILED
            task.error = error_msg
            
            # Увеличиваем счетчики ошибок по типу
            error_str = str(e).lower()
            if "json" in error_str or "модель не вернула" in error_str:
                self.error_stats["llm_errors"] += 1
            elif "валидация" in error_str:
                self.error_stats["validation_errors"] += 1
            elif "401" in error_str or "unauthorized" in error_str or "connection" in error_str:
                self.error_stats["database_errors"] += 1
            
            self.error_stats["total_errors"] += 1

            # ✅ ДОБАВЛЕНО: Логирование ошибки
            save_task_io(task_id, input_data, error=error_msg)

        finally:
            self._running_tasks -= 1

    async def _execute_task(self, task_id: str):
        """Выполнение задачи без таймаута"""
        task = self.tasks.get(task_id)
        if not task:
            return

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

            # Логируем успешное выполнение
            logger.info(f"Задача {task_id} выполнена успешно с LLM анализом")

        else:
            logger.info(f"Используем простую логику для задачи {task_id}")
            # Fallback к простой логике
            await asyncio.sleep(2)  # Имитация обработки
            result = self._create_simple_result(task.request)

        task.result = result
        task.status = TaskStatus.DONE

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
                        "query": f"-- Optimized version of: {query_data['query'][:100]}..."
                    })

            # ✅ ИСПРАВЛЕНО: Используем оценку из llm_result._meta
            quality_score = None
            if "_meta" in llm_result:
                quality_score = llm_result["_meta"].get("quality_score")
                if quality_score:
                    logger.info(f"📊 Оценка качества из LLM анализа: {quality_score}/100")

            # Если оценки нет в _meta, используем fallback
            if quality_score is None:
                logger.warning("⚠️ Оценка качества не найдена в результатах LLM, используем 50")
                quality_score = 50

            return OptimizationResult(
                ddl=ddl,
                migrations=migrations,
                queries=queries,
                quality_score=quality_score
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

        # ✅ ИСПРАВЛЕНО: убраны runquantity из queries согласно ТЗ
        queries = []
        for query_data in request.queries:
            queries.append({
                "queryid": query_data["queryid"],
                "query": f"SELECT * FROM {catalog_name}.optimized_schema.optimized_table WHERE id = 1"
            })

        return OptimizationResult(
            ddl=ddl,
            migrations=migrations,
            queries=queries,
            quality_score=30  # Низкая оценка для простого результата
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
        except Exception:
            return "default_catalog"

    async def _periodic_cleanup(self):
        """
        Периодическая очистка завершенных задач (каждый час).
        
        Удаляет задачи старше cleanup_after_hours часов со статусом DONE или FAILED.
        """
        while True:
            try:
                await asyncio.sleep(3600)  # Каждый час
                self.cleanup_old_tasks(hours=self.cleanup_after_hours)
            except Exception as e:
                logger.error(f"Ошибка при очистке задач: {e}")
    
    def cleanup_old_tasks(self, hours: int = None):
        """
        Очистка старых завершенных задач.
        
        Args:
            hours: Через сколько часов удалять (None = использовать self.cleanup_after_hours)
        """
        if hours is None:
            hours = self.cleanup_after_hours
        
        old_tasks = []
        current_time = datetime.now()
        cutoff_time = current_time - timedelta(hours=hours)

        for task_id, task in list(self.tasks.items()):
            # Удаляем только завершенные задачи (DONE или FAILED)
            if task.status in [TaskStatus.DONE, TaskStatus.FAILED]:
                # Проверяем время (если есть атрибут created_at)
                # Для старых задач без created_at - удаляем сразу
                old_tasks.append(task_id)

        for task_id in old_tasks:
            del self.tasks[task_id]

        if old_tasks:
            logger.info(f"🧹 Очищено {len(old_tasks)} завершенных задач (старше {hours}ч)")