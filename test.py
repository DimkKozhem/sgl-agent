#!/usr/bin/env python3
"""
Тест SQL-agent с использованием JSON файлов из datasets
"""

import asyncio
import time
import sys
import os
import json
import glob
from typing import List, Dict, Any
from concurrent.futures import ThreadPoolExecutor
import statistics

# Добавляем путь к модулю
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from sql_agent.task_manager import SimpleTaskManager
from sql_agent.models import OptimizationRequest


class TestResult:
    """Результат тестирования одной задачи"""
    def __init__(self, task_id: str, dataset_name: str, success: bool, 
                 execution_time: float, error: str = None):
        self.task_id = task_id
        self.dataset_name = dataset_name
        self.success = success
        self.execution_time = execution_time
        self.error = error


class TestRunner:
    """Класс для запуска тестов"""
    
    def __init__(self, max_concurrent_tasks: int = 5):
        self.max_concurrent_tasks = max_concurrent_tasks
        self.task_manager = SimpleTaskManager(
            max_workers=max_concurrent_tasks, 
            task_timeout_minutes=10
        )
        self.test_results: List[TestResult] = []
    
    def load_test_datasets(self) -> List[Dict[str, Any]]:
        """Загрузка тестовых данных из JSON файлов"""
        datasets = []
        datasets_dir = os.path.join(os.path.dirname(__file__), "datasets")
        
        if not os.path.exists(datasets_dir):
            print(f"❌ Директория {datasets_dir} не найдена")
            return datasets
        
        json_files = glob.glob(os.path.join(datasets_dir, "*.json"))
        
        for json_file in json_files:
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    dataset_name = os.path.basename(json_file).replace('.json', '')
                    datasets.append({
                        'name': dataset_name,
                        'data': data
                    })
                    print(f"✅ Загружен датасет: {dataset_name}")
            except Exception as e:
                print(f"❌ Ошибка загрузки {json_file}: {e}")
        
        return datasets
    
    async def run_single_test(self, dataset: Dict[str, Any]) -> TestResult:
        """Запуск одного теста"""
        dataset_name = dataset['name']
        data = dataset['data']
        
        print(f"🚀 Запуск теста для датасета: {dataset_name}")
        
        try:
            # Создаем запрос из данных JSON
            request = OptimizationRequest(
                url=data['url'],
                ddl=data['ddl'],
                queries=data['queries']
            )
            
            start_time = time.time()
            
            # Создаем задачу
            task_id = self.task_manager.create_task(request)
            
            # Ждем завершения
            while self.task_manager.get_task_status(task_id) == "RUNNING":
                await asyncio.sleep(0.1)
            
            execution_time = time.time() - start_time
            status = self.task_manager.get_task_status(task_id)
            
            if status == "DONE":
                result = self.task_manager.get_task_result(task_id)
                print(f"✅ Тест {dataset_name} завершен успешно за {execution_time:.2f}s")
                print(f"   - DDL команд: {len(result.ddl)}")
                print(f"   - Миграций: {len(result.migrations)}")
                print(f"   - Запросов: {len(result.queries)}")
                return TestResult(task_id, dataset_name, True, execution_time)
            else:
                error = self.task_manager.get_task_error(task_id)
                print(f"❌ Тест {dataset_name} завершился с ошибкой: {error}")
                return TestResult(task_id, dataset_name, False, execution_time, error)
                
        except Exception as e:
            execution_time = time.time() - start_time if 'start_time' in locals() else 0
            error_msg = f"Исключение при выполнении теста: {str(e)}"
            print(f"❌ Тест {dataset_name} завершился с исключением: {error_msg}")
            return TestResult("", dataset_name, False, execution_time, error_msg)
    
    async def run_concurrent_tests(self, datasets: List[Dict[str, Any]]) -> List[TestResult]:
        """Запуск тестов с ограничением на количество одновременных задач"""
        print(f"🔄 Запуск {len(datasets)} тестов с максимум {self.max_concurrent_tasks} одновременными задачами")
        
        # Создаем семафор для ограничения количества одновременных задач
        semaphore = asyncio.Semaphore(self.max_concurrent_tasks)
        
        async def run_with_semaphore(dataset):
            async with semaphore:
                return await self.run_single_test(dataset)
        
        # Запускаем все тесты
        tasks = [run_with_semaphore(dataset) for dataset in datasets]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Обрабатываем результаты
        test_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                dataset_name = datasets[i]['name']
                error_msg = f"Исключение при выполнении теста: {str(result)}"
                print(f"❌ Тест {dataset_name} завершился с исключением: {error_msg}")
                test_results.append(TestResult("", dataset_name, False, 0, error_msg))
            else:
                test_results.append(result)
        
        return test_results
    
    def print_statistics(self, results: List[TestResult]):
        """Вывод статистики по тестам"""
        if not results:
            print("📊 Нет результатов для статистики")
            return
        
        total_tests = len(results)
        successful_tests = sum(1 for r in results if r.success)
        failed_tests = total_tests - successful_tests
        
        execution_times = [r.execution_time for r in results if r.success]
        
        print("\n" + "="*60)
        print("📊 СТАТИСТИКА ТЕСТИРОВАНИЯ")
        print("="*60)
        print(f"Всего тестов: {total_tests}")
        print(f"Успешных: {successful_tests} ({successful_tests/total_tests*100:.1f}%)")
        print(f"Неудачных: {failed_tests} ({failed_tests/total_tests*100:.1f}%)")
        
        if execution_times:
            print(f"\n⏱️  ВРЕМЯ ВЫПОЛНЕНИЯ:")
            print(f"   Среднее: {statistics.mean(execution_times):.2f}s")
            print(f"   Медиана: {statistics.median(execution_times):.2f}s")
            print(f"   Минимум: {min(execution_times):.2f}s")
            print(f"   Максимум: {max(execution_times):.2f}s")
        
        print(f"\n📈 СТАТИСТИКА МЕНЕДЖЕРА ЗАДАЧ:")
        stats = self.task_manager.get_stats()
        for key, value in stats.items():
            print(f"   {key}: {value}")
        
        if failed_tests > 0:
            print(f"\n❌ НЕУДАЧНЫЕ ТЕСТЫ:")
            for result in results:
                if not result.success:
                    print(f"   - {result.dataset_name}: {result.error}")
        
        print("="*60)


async def main():
    """Основная функция тестирования"""
    print("🧪 Запуск тестирования SQL-agent с JSON датасетами")
    print("="*60)
    
    # Создаем тестовый раннер
    test_runner = TestRunner(max_concurrent_tasks=5)
    
    # Загружаем тестовые данные
    print("📂 Загрузка тестовых данных...")
    datasets = test_runner.load_test_datasets()
    
    if not datasets:
        print("❌ Не найдено тестовых данных для выполнения тестов")
        return
    
    print(f"✅ Загружено {len(datasets)} датасетов")
    
    # Запускаем тесты
    start_time = time.time()
    results = await test_runner.run_concurrent_tests(datasets)
    total_time = time.time() - start_time
    
    # Выводим статистику
    test_runner.print_statistics(results)
    
    print(f"\n🎉 Тестирование завершено за {total_time:.2f}s")


if __name__ == "__main__":
    asyncio.run(main())
