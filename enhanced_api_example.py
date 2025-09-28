#!/usr/bin/env python3
"""
Пример модификации API для возврата оценки качества
"""

import requests
import json
import time
from typing import Dict, Any

class EnhancedAPIClient:
    def __init__(self, base_url: str = "http://localhost:8001"):
        self.base_url = base_url
    
    def create_task_with_quality_tracking(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Создает задачу и отслеживает оценку качества"""
        
        # Создаем задачу
        response = requests.post(f"{self.base_url}/new", json=data)
        task_info = response.json()
        task_id = task_info['taskid']
        
        print(f"🚀 Создана задача: {task_id}")
        
        # Ждем завершения
        while True:
            status_response = requests.get(f"{self.base_url}/status?task_id={task_id}")
            status = status_response.json()['status']
            
            if status == 'DONE':
                break
            elif status == 'FAILED':
                return {'error': 'Task failed'}
            
            time.sleep(1)
        
        # Получаем результат
        result_response = requests.get(f"{self.base_url}/getresult?task_id={task_id}")
        result = result_response.json()
        
        # В реальной системе здесь был бы парсинг логов для получения оценки
        # Для демонстрации используем примерную оценку
        quality_score = self._estimate_quality_from_result(result)
        
        return {
            'task_id': task_id,
            'result': result,
            'quality_score': quality_score,
            'quality_metrics': {
                'ddl_count': len(result.get('ddl', [])),
                'migrations_count': len(result.get('migrations', [])),
                'queries_count': len(result.get('queries', [])),
                'has_optimizations': len(result.get('ddl', [])) > 0 or len(result.get('migrations', [])) > 0
            }
        }
    
    def _estimate_quality_from_result(self, result: Dict[str, Any]) -> int:
        """Оценивает качество на основе результата (примерная логика)"""
        ddl_count = len(result.get('ddl', []))
        migrations_count = len(result.get('migrations', []))
        queries_count = len(result.get('queries', []))
        
        # Простая эвристика для оценки качества
        score = 5  # Базовая оценка
        
        if ddl_count > 0:
            score += 2
        if migrations_count > 0:
            score += 1
        if queries_count > 0:
            score += 1
        if ddl_count > 2:
            score += 1
        
        return min(score, 10)  # Максимум 10
    
    def demonstrate_enhanced_api(self):
        """Демонстрирует работу с улучшенным API"""
        print("🔍 ДЕМОНСТРАЦИЯ РАБОТЫ С ОЦЕНКОЙ КАЧЕСТВА")
        print("=" * 60)
        
        # Тестовые данные
        test_data = {
            "url": "jdbc:trino://test.example.com:8080?catalog=test",
            "ddl": [
                {
                    "statement": "CREATE TABLE test.users (id INTEGER, name VARCHAR(255), email VARCHAR(255), created_at TIMESTAMP)"
                }
            ],
            "queries": [
                {
                    "queryid": "test-query-1",
                    "query": "SELECT * FROM test.users WHERE id = 1",
                    "runquantity": 100,
                    "executiontime": 5
                }
            ]
        }
        
        # Обрабатываем задачу
        result = self.create_task_with_quality_tracking(test_data)
        
        if 'error' in result:
            print(f"❌ Ошибка: {result['error']}")
            return
        
        print(f"\n📊 РЕЗУЛЬТАТ С ОЦЕНКОЙ КАЧЕСТВА:")
        print(f"   Task ID: {result['task_id']}")
        print(f"   Оценка качества: {result['quality_score']}/10")
        print(f"   DDL команд: {result['quality_metrics']['ddl_count']}")
        print(f"   Миграций: {result['quality_metrics']['migrations_count']}")
        print(f"   Запросов: {result['quality_metrics']['queries_count']}")
        print(f"   Есть оптимизации: {'Да' if result['quality_metrics']['has_optimizations'] else 'Нет'}")
        
        print(f"\n📝 РЕКОМЕНДАЦИИ:")
        if result['quality_score'] >= 8:
            print("   ✅ Отличное качество рекомендаций")
        elif result['quality_score'] >= 6:
            print("   ✅ Хорошее качество рекомендаций")
        elif result['quality_score'] >= 4:
            print("   ⚠️  Среднее качество рекомендаций")
        else:
            print("   ❌ Низкое качество рекомендаций")
        
        return result

def main():
    """Основная функция"""
    client = EnhancedAPIClient()
    
    try:
        # Проверяем доступность сервера
        health_response = requests.get("http://localhost:8001/health")
        if health_response.status_code != 200:
            print("❌ Сервер недоступен. Запустите сервер командой: python main.py")
            return
        
        print("✅ Сервер доступен")
        
        # Запускаем демонстрацию
        client.demonstrate_enhanced_api()
        
    except requests.exceptions.ConnectionError:
        print("❌ Не удается подключиться к серверу")
        print("💡 Убедитесь, что сервер запущен: python main.py")
    except Exception as e:
        print(f"❌ Ошибка: {e}")

if __name__ == "__main__":
    main()
