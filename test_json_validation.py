#!/usr/bin/env python3
"""
Тест проверки обработки ошибок JSON с кодом 400 Bad Request
"""

import requests
import json

# URL вашего сервера
BASE_URL = "http://localhost:8001"


def test_invalid_json_syntax():
    """Тест 1: Невалидный синтаксис JSON (отсутствует закрывающая скобка)"""
    print("=" * 60)
    print("ТЕСТ 1: Невалидный синтаксис JSON")
    print("=" * 60)
    
    invalid_json = '{"url": "jdbc:postgresql://localhost:5432/db", "ddl": ['
    
    response = requests.post(
        f"{BASE_URL}/new",
        data=invalid_json,
        headers={"Content-Type": "application/json"}
    )
    
    print(f"Статус код: {response.status_code}")
    print(f"Ответ: {json.dumps(response.json(), indent=2, ensure_ascii=False)}")
    print(f"✅ УСПЕХ: Код 400" if response.status_code == 400 else f"❌ ОШИБКА: Ожидался код 400, получен {response.status_code}")
    print()


def test_missing_required_fields():
    """Тест 2: Отсутствуют обязательные поля"""
    print("=" * 60)
    print("ТЕСТ 2: Отсутствуют обязательные поля")
    print("=" * 60)
    
    invalid_data = {
        "url": "jdbc:postgresql://localhost:5432/db"
        # Отсутствуют ddl и queries
    }
    
    response = requests.post(
        f"{BASE_URL}/new",
        json=invalid_data
    )
    
    print(f"Статус код: {response.status_code}")
    print(f"Ответ: {json.dumps(response.json(), indent=2, ensure_ascii=False)}")
    print(f"✅ УСПЕХ: Код 400" if response.status_code == 400 else f"❌ ОШИБКА: Ожидался код 400, получен {response.status_code}")
    print()


def test_invalid_url_format():
    """Тест 3: Невалидный формат URL (не начинается с jdbc:)"""
    print("=" * 60)
    print("ТЕСТ 3: Невалидный формат URL")
    print("=" * 60)
    
    invalid_data = {
        "url": "postgresql://localhost:5432/db",  # Неправильный формат
        "ddl": [{"statement": "CREATE TABLE test (id INT)"}],
        "queries": [{"queryid": "1", "query": "SELECT * FROM test", "runquantity": 100}]
    }
    
    response = requests.post(
        f"{BASE_URL}/new",
        json=invalid_data
    )
    
    print(f"Статус код: {response.status_code}")
    print(f"Ответ: {json.dumps(response.json(), indent=2, ensure_ascii=False)}")
    print(f"✅ УСПЕХ: Код 400" if response.status_code == 400 else f"❌ ОШИБКА: Ожидался код 400, получен {response.status_code}")
    print()


def test_invalid_ddl_structure():
    """Тест 4: Невалидная структура DDL (отсутствует поле statement)"""
    print("=" * 60)
    print("ТЕСТ 4: Невалидная структура DDL")
    print("=" * 60)
    
    invalid_data = {
        "url": "jdbc:postgresql://localhost:5432/db",
        "ddl": [{"invalid_field": "CREATE TABLE test (id INT)"}],  # Неправильное поле
        "queries": [{"queryid": "1", "query": "SELECT * FROM test", "runquantity": 100}]
    }
    
    response = requests.post(
        f"{BASE_URL}/new",
        json=invalid_data
    )
    
    print(f"Статус код: {response.status_code}")
    print(f"Ответ: {json.dumps(response.json(), indent=2, ensure_ascii=False)}")
    print(f"✅ УСПЕХ: Код 400" if response.status_code == 400 else f"❌ ОШИБКА: Ожидался код 400, получен {response.status_code}")
    print()


def test_invalid_queries_structure():
    """Тест 5: Невалидная структура queries (отсутствуют обязательные поля)"""
    print("=" * 60)
    print("ТЕСТ 5: Невалидная структура queries")
    print("=" * 60)
    
    invalid_data = {
        "url": "jdbc:postgresql://localhost:5432/db",
        "ddl": [{"statement": "CREATE TABLE test (id INT)"}],
        "queries": [{"queryid": "1"}]  # Отсутствуют query и runquantity
    }
    
    response = requests.post(
        f"{BASE_URL}/new",
        json=invalid_data
    )
    
    print(f"Статус код: {response.status_code}")
    print(f"Ответ: {json.dumps(response.json(), indent=2, ensure_ascii=False)}")
    print(f"✅ УСПЕХ: Код 400" if response.status_code == 400 else f"❌ ОШИБКА: Ожидался код 400, получен {response.status_code}")
    print()


def test_malformed_json_literal():
    """Тест 6: Ошибка в литерале JSON (незакрытая кавычка)"""
    print("=" * 60)
    print("ТЕСТ 6: Ошибка в литерале JSON")
    print("=" * 60)
    
    # JSON с незакрытой кавычкой в строке
    invalid_json = '{"url": "jdbc:postgresql://localhost:5432/db'
    
    response = requests.post(
        f"{BASE_URL}/new",
        data=invalid_json,
        headers={"Content-Type": "application/json"}
    )
    
    print(f"Статус код: {response.status_code}")
    print(f"Ответ: {json.dumps(response.json(), indent=2, ensure_ascii=False)}")
    print(f"✅ УСПЕХ: Код 400" if response.status_code == 400 else f"❌ ОШИБКА: Ожидался код 400, получен {response.status_code}")
    print()


def main():
    """Запуск всех тестов"""
    print("\n" + "=" * 60)
    print("ТЕСТИРОВАНИЕ ОБРАБОТКИ ОШИБОК JSON (Код 400)")
    print("=" * 60)
    print(f"Сервер: {BASE_URL}")
    print()
    
    try:
        # Проверяем, что сервер доступен
        response = requests.get(f"{BASE_URL}/health", timeout=5)
        if response.status_code != 200:
            print(f"❌ Сервер недоступен по адресу {BASE_URL}")
            print("   Запустите сервер командой: python main.py")
            return
    except requests.exceptions.RequestException as e:
        print(f"❌ Не удается подключиться к серверу: {e}")
        print(f"   Убедитесь, что сервер запущен на {BASE_URL}")
        return
    
    print("✅ Сервер доступен, начинаем тестирование...\n")
    
    # Запускаем все тесты
    test_invalid_json_syntax()
    test_missing_required_fields()
    test_invalid_url_format()
    test_invalid_ddl_structure()
    test_invalid_queries_structure()
    test_malformed_json_literal()
    
    print("=" * 60)
    print("ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")
    print("=" * 60)


if __name__ == "__main__":
    main()

