"""
Простой логгер для сохранения входов и выходов задач.
Можно легко удалить без последствий для основного кода.
"""

import json
import os
from datetime import datetime


def save_task_io(task_id: str, input_data: dict, output_data: dict = None, error: str = None):
    """
    Сохраняет вход и выход задачи в JSON файл.

    Args:
        task_id: ID задачи
        input_data: Входные данные
        output_data: Выходные данные (если есть)
        error: Ошибка (если есть)
    """
    try:
        # Создаем папку если нет
        os.makedirs("task_logs", exist_ok=True)

        # Формируем данные
        log_data = {
            "task_id": task_id,
            "timestamp": datetime.now().isoformat(),
            "input": input_data,
            "output": output_data,
            "error": error
        }

        # Сохраняем в файл
        filename = f"task_logs/{task_id}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(log_data, f, indent=2, ensure_ascii=False)

        print(f"✅ Лог сохранен: {filename}")

    except Exception as e:
        print(f"⚠️ Ошибка сохранения лога: {e}")