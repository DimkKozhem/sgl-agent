#!/usr/bin/env python3
"""
Главный модуль SQL-agent с ротацией логов.

Этот модуль запускает REST API сервер для анализа и оптимизации структуры базы данных.
Включает в себя систему ротации логов и корректное завершение работы.
"""

import uvicorn
import logging
import os
import atexit
import signal
import sys
from sql_agent.api import app
from sql_agent.log_rotator import setup_logging, stop_logging

# Настройка ротации логов
log_rotator = setup_logging()
logger = logging.getLogger(__name__)


def signal_handler(signum, frame):
    """
    Обработчик сигналов для корректного завершения работы сервера.

    Args:
        signum: Номер сигнала (SIGINT, SIGTERM)
        frame: Текущий стек вызовов
    """
    logger.info(f"🛑 Получен сигнал {signum}, завершаем работу...")
    stop_logging()
    sys.exit(0)


def run_server():
    """
    Запуск REST API сервера с настройкой ротации логов.

    Настраивает обработчики сигналов, регистрирует функции очистки
    и запускает uvicorn сервер на указанном порту.
    """
    port = int(os.getenv("PORT", 8001))  # Используем порт 8001 по умолчанию

    # Регистрируем обработчики сигналов
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Регистрируем функцию очистки при завершении
    atexit.register(stop_logging)

    # Получаем информацию о логах
    log_info = log_rotator.get_log_info()
    logger.info("📝 Логирование настроено:")
    logger.info(f"   Директория: {log_info['log_directory']}")
    logger.info(f"   Текущий файл: {log_info['current_log_file']}")
    logger.info(f"   Максимум файлов: {log_info['max_files']}")

    logger.info(f"🚀 Запуск SQL-agent на 0.0.0.0:{port}")
    logger.info("⏰ Ротация логов каждый час")

    try:
        uvicorn.run(
            app,
            host="0.0.0.0",
            port=port,
            workers=1,
            log_level="info"
        )
    except KeyboardInterrupt:
        logger.info("🛑 Получен сигнал прерывания")
    finally:
        stop_logging()


if __name__ == "__main__":
    run_server()
