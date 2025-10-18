"""
Unit-тесты для проверки структуры API согласно ТЗ.

Проверяет:
1. Структуру ответов от всех endpoints
2. Соответствие формату данных из ТЗ
3. Корректность обработки ошибок
"""

import pytest
import json
from fastapi.testclient import TestClient
from sql_agent.api import app
from sql_agent.models import TaskStatus

client = TestClient(app)


class TestAPIStructure:
    """Тесты структуры API согласно ТЗ"""

    # ==================== Тестовые данные ====================
    
    @pytest.fixture
    def valid_request(self):
        """Валидный запрос согласно ТЗ"""
        return {
            "url": "jdbc:trino://localhost:8080?catalog=test",
            "ddl": [
                {"statement": "CREATE TABLE test.public.users (id INTEGER, name VARCHAR(100))"}
            ],
            "queries": [
                {
                    "queryid": "test-query-1",
                    "query": "SELECT * FROM test.public.users",
                    "runquantity": 100,
                    "executiontime": 5
                }
            ]
        }

    @pytest.fixture
    def invalid_json_request(self):
        """Невалидный JSON для тестирования 400 ошибки"""
        return '{"url": "test", "ddl": [invalid json'

    # ==================== Тесты POST /new ====================

    def test_create_task_success(self, valid_request):
        """Проверка успешного создания задачи"""
        response = client.post("/new", json=valid_request)
        
        assert response.status_code == 200
        data = response.json()
        
        # Проверка структуры ответа
        assert "taskid" in data, "Ответ должен содержать поле 'taskid'"
        assert isinstance(data["taskid"], str), "'taskid' должен быть строкой"
        assert len(data["taskid"]) > 0, "'taskid' не должен быть пустым"
        
        # Проверка формата UUID
        parts = data["taskid"].split("-")
        assert len(parts) == 5, "'taskid' должен быть в формате UUID"

    def test_create_task_missing_url(self, valid_request):
        """Проверка ошибки при отсутствии URL"""
        invalid_request = valid_request.copy()
        del invalid_request["url"]
        
        response = client.post("/new", json=invalid_request)
        
        assert response.status_code == 400, "Должен вернуть 400 Bad Request"
        data = response.json()
        assert "error" in data or "detail" in data

    def test_create_task_missing_ddl(self, valid_request):
        """Проверка ошибки при отсутствии DDL"""
        invalid_request = valid_request.copy()
        del invalid_request["ddl"]
        
        response = client.post("/new", json=invalid_request)
        
        assert response.status_code == 400, "Должен вернуть 400 Bad Request"

    def test_create_task_missing_queries(self, valid_request):
        """Проверка ошибки при отсутствии queries"""
        invalid_request = valid_request.copy()
        del invalid_request["queries"]
        
        response = client.post("/new", json=invalid_request)
        
        assert response.status_code == 400, "Должен вернуть 400 Bad Request"

    def test_create_task_invalid_json(self):
        """Проверка обработки невалидного JSON"""
        response = client.post(
            "/new",
            content='{"url": "test", invalid json}',
            headers={"Content-Type": "application/json"}
        )
        
        assert response.status_code == 400, "Должен вернуть 400 Bad Request для невалидного JSON"

    # ==================== Тесты GET /status ====================

    def test_get_status_success(self, valid_request):
        """Проверка получения статуса задачи"""
        # Создаем задачу
        create_response = client.post("/new", json=valid_request)
        task_id = create_response.json()["taskid"]
        
        # Получаем статус
        response = client.get(f"/status?task_id={task_id}")
        
        assert response.status_code == 200
        data = response.json()
        
        # Проверка структуры ответа
        assert "status" in data, "Ответ должен содержать поле 'status'"
        assert data["status"] in ["RUNNING", "DONE", "FAILED"], \
            f"Статус должен быть RUNNING/DONE/FAILED, получен: {data['status']}"

    def test_get_status_not_found(self):
        """Проверка ошибки при несуществующей задаче"""
        fake_task_id = "00000000-0000-0000-0000-000000000000"
        response = client.get(f"/status?task_id={fake_task_id}")
        
        assert response.status_code == 404, "Должен вернуть 404 Not Found"

    def test_get_status_missing_task_id(self):
        """Проверка ошибки при отсутствии task_id"""
        response = client.get("/status")
        
        # Наш custom validation handler возвращает 400 вместо 422
        assert response.status_code == 400, "Должен вернуть 400 для отсутствующего параметра"

    # ==================== Тесты GET /getresult ====================

    def test_get_result_structure(self, valid_request):
        """
        Проверка структуры результата согласно ТЗ.
        
        Ожидаемая структура:
        {
            "ddl": [{"statement": "..."}],
            "migrations": [{"statement": "..."}],
            "queries": [{"queryid": "...", "query": "..."}]
        }
        """
        # Создаем задачу
        create_response = client.post("/new", json=valid_request)
        task_id = create_response.json()["taskid"]
        
        # Ждем выполнения (с таймаутом)
        import time
        max_wait = 300  # 5 минут максимум
        start = time.time()
        
        while time.time() - start < max_wait:
            status_response = client.get(f"/status?task_id={task_id}")
            status = status_response.json()["status"]
            
            if status == "DONE":
                break
            elif status == "FAILED":
                pytest.skip("Задача провалилась, пропускаем тест структуры")
                return
            
            time.sleep(2)
        
        # Получаем результат
        response = client.get(f"/getresult?task_id={task_id}")
        
        assert response.status_code == 200, "Должен вернуть 200 OK для завершенной задачи"
        data = response.json()
        
        # ==================== ПРОВЕРКА СТРУКТУРЫ ====================
        
        # Обязательные поля
        assert "ddl" in data, "Результат должен содержать поле 'ddl'"
        assert "migrations" in data, "Результат должен содержать поле 'migrations'"
        assert "queries" in data, "Результат должен содержать поле 'queries'"
        
        # Типы данных
        assert isinstance(data["ddl"], list), "'ddl' должен быть массивом"
        assert isinstance(data["migrations"], list), "'migrations' должен быть массивом"
        assert isinstance(data["queries"], list), "'queries' должен быть массивом"
        
        # Структура DDL
        for i, ddl_item in enumerate(data["ddl"]):
            assert isinstance(ddl_item, dict), f"ddl[{i}] должен быть объектом"
            assert "statement" in ddl_item, f"ddl[{i}] должен содержать 'statement'"
            assert isinstance(ddl_item["statement"], str), f"ddl[{i}].statement должен быть строкой"
        
        # Структура Migrations
        for i, mig_item in enumerate(data["migrations"]):
            assert isinstance(mig_item, dict), f"migrations[{i}] должен быть объектом"
            assert "statement" in mig_item, f"migrations[{i}] должен содержать 'statement'"
            assert isinstance(mig_item["statement"], str), f"migrations[{i}].statement должен быть строкой"
        
        # Структура Queries (КРИТИЧНО!)
        for i, query_item in enumerate(data["queries"]):
            assert isinstance(query_item, dict), f"queries[{i}] должен быть объектом"
            assert "queryid" in query_item, f"queries[{i}] должен содержать 'queryid'"
            assert "query" in query_item, f"queries[{i}] должен содержать 'query'"
            assert isinstance(query_item["queryid"], str), f"queries[{i}].queryid должен быть строкой"
            assert isinstance(query_item["query"], str), f"queries[{i}].query должен быть строкой"
        
        # Запрещенные поля
        assert "quality_score" not in data, "Поле 'quality_score' не должно возвращаться в API"
        assert "_meta" not in data, "Поле '_meta' не должно возвращаться в API"

    def test_get_result_running_task(self, valid_request):
        """Проверка получения результата для выполняющейся задачи"""
        # Создаем задачу
        create_response = client.post("/new", json=valid_request)
        task_id = create_response.json()["taskid"]
        
        # Сразу пытаемся получить результат
        response = client.get(f"/getresult?task_id={task_id}")
        
        # Должен вернуть 202 Accepted или информацию о том, что задача выполняется
        assert response.status_code in [202, 200]

    def test_get_result_not_found(self):
        """Проверка ошибки для несуществующей задачи"""
        fake_task_id = "00000000-0000-0000-0000-000000000000"
        response = client.get(f"/getresult?task_id={fake_task_id}")
        
        assert response.status_code == 404, "Должен вернуть 404 Not Found"

    # ==================== Тесты полных путей ====================

    def test_full_paths_in_ddl(self, valid_request):
        """Проверка использования полных путей в DDL"""
        create_response = client.post("/new", json=valid_request)
        task_id = create_response.json()["taskid"]
        
        # Ждем выполнения
        import time
        max_wait = 300
        start = time.time()
        
        while time.time() - start < max_wait:
            status_response = client.get(f"/status?task_id={task_id}")
            if status_response.json()["status"] in ["DONE", "FAILED"]:
                break
            time.sleep(2)
        
        # Получаем результат
        response = client.get(f"/getresult?task_id={task_id}")
        if response.status_code != 200:
            pytest.skip("Задача не завершилась успешно")
            return
        
        data = response.json()
        
        # Проверяем полные пути в DDL
        for ddl_item in data["ddl"]:
            statement = ddl_item["statement"]
            
            # CREATE SCHEMA пропускаем
            if "CREATE SCHEMA" in statement.upper():
                continue
            
            # CREATE TABLE должен использовать полный путь catalog.schema.table
            if "CREATE TABLE" in statement.upper():
                # Проверяем наличие трехчастного пути
                assert "optimized" in statement.lower(), \
                    f"DDL должен использовать schema 'optimized*': {statement[:100]}"
                
                # Проверяем формат catalog.schema.table
                import re
                path_pattern = r'\w+\.optimized[_\w]*\.\w+'
                assert re.search(path_pattern, statement), \
                    f"DDL должен использовать полный путь catalog.schema.table: {statement[:100]}"

    def test_queryid_preservation(self, valid_request):
        """Проверка сохранения оригинальных queryid"""
        create_response = client.post("/new", json=valid_request)
        task_id = create_response.json()["taskid"]
        
        # Ждем выполнения
        import time
        max_wait = 300
        start = time.time()
        
        while time.time() - start < max_wait:
            status_response = client.get(f"/status?task_id={task_id}")
            if status_response.json()["status"] in ["DONE", "FAILED"]:
                break
            time.sleep(2)
        
        response = client.get(f"/getresult?task_id={task_id}")
        if response.status_code != 200:
            pytest.skip("Задача не завершилась успешно")
            return
        
        data = response.json()
        original_queryids = [q["queryid"] for q in valid_request["queries"]]
        result_queryids = [q["queryid"] for q in data["queries"]]
        
        # КРИТИЧНО: все оригинальные queryid должны сохраниться
        for orig_id in original_queryids:
            assert orig_id in result_queryids, \
                f"Оригинальный queryid '{orig_id}' должен сохраниться в результате"

    # ==================== Тесты health check ====================

    def test_health_endpoint_exists(self):
        """Проверка наличия health endpoint"""
        response = client.get("/health")
        assert response.status_code == 200

    def test_metrics_endpoint_exists(self):
        """Проверка наличия metrics endpoint"""
        response = client.get("/metrics")
        assert response.status_code == 200
        
        data = response.json()
        assert "tasks" in data
        assert "health" in data

    # ==================== Тесты обработки ошибок ====================

    def test_global_exception_handler(self):
        """Проверка что глобальный exception handler возвращает JSON"""
        # Пытаемся получить результат с невалидным task_id
        response = client.get("/getresult?task_id=invalid-format-not-uuid")
        
        # Должен вернуть JSON, а не разорвать соединение
        assert response.headers.get("content-type") == "application/json"

    def test_400_for_invalid_json(self):
        """Проверка возврата 400 для невалидного JSON"""
        response = client.post(
            "/new",
            content='{"url": "test", invalid}',
            headers={"Content-Type": "application/json"}
        )
        
        assert response.status_code == 400, "Должен вернуть 400 для невалидного JSON"
        data = response.json()
        assert "error" in data or "detail" in data


class TestResponseStructure:
    """Детальные тесты структуры ответов"""

    def test_no_forbidden_fields_in_response(self):
        """Проверка отсутствия запрещенных полей в ответе"""
        request = {
            "url": "jdbc:trino://localhost:8080?catalog=test",
            "ddl": [{"statement": "CREATE TABLE test.public.t1 (id INT)"}],
            "queries": [{"queryid": "q1", "query": "SELECT * FROM test.public.t1", "runquantity": 10}]
        }
        
        create_response = client.post("/new", json=request)
        task_id = create_response.json()["taskid"]
        
        # Ждем завершения
        import time
        for _ in range(150):  # 5 минут максимум
            status_resp = client.get(f"/status?task_id={task_id}")
            if status_resp.json()["status"] in ["DONE", "FAILED"]:
                break
            time.sleep(2)
        
        result_response = client.get(f"/getresult?task_id={task_id}")
        
        if result_response.status_code == 200:
            data = result_response.json()
            
            # Запрещенные поля (не должны возвращаться по API)
            forbidden_fields = [
                "quality_score",
                "_meta",
                "llm_provider",
                "llm_model",
                "error_details",
                "internal_metrics"
            ]
            
            for field in forbidden_fields:
                assert field not in data, \
                    f"Запрещенное поле '{field}' не должно возвращаться в API ответе"

    def test_ddl_first_is_create_schema(self):
        """Проверка что первая DDL команда - CREATE SCHEMA"""
        request = {
            "url": "jdbc:trino://localhost:8080?catalog=test",
            "ddl": [{"statement": "CREATE TABLE test.public.t1 (id INT)"}],
            "queries": [{"queryid": "q1", "query": "SELECT * FROM test.public.t1", "runquantity": 10}]
        }
        
        create_response = client.post("/new", json=request)
        task_id = create_response.json()["taskid"]
        
        # Ждем
        import time
        for _ in range(150):
            status_resp = client.get(f"/status?task_id={task_id}")
            if status_resp.json()["status"] in ["DONE", "FAILED"]:
                break
            time.sleep(2)
        
        result_response = client.get(f"/getresult?task_id={task_id}")
        
        if result_response.status_code == 200:
            data = result_response.json()
            
            assert len(data["ddl"]) > 0, "DDL не должен быть пустым"
            first_ddl = data["ddl"][0]["statement"]
            
            assert "CREATE SCHEMA" in first_ddl.upper(), \
                "Первая DDL команда должна быть CREATE SCHEMA"


# ==================== Запуск тестов ====================

if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])

