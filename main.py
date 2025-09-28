#!/usr/bin/env python3
"""
Главный модуль SQL-agent
"""

import uvicorn
import logging
import os
from sql_agent.api import app

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def run_server():
    """Запуск сервера"""
    port = int(os.getenv("PORT", 8001))  # Используем порт 8001 по умолчанию
    logger.info(f"Запуск SQL-agent на 0.0.0.0:{port}")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        workers=1,
        log_level="info"
    )


if __name__ == "__main__":
    run_server()
