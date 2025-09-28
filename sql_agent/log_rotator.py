#!/usr/bin/env python3
"""
Модуль для ротации логов по расписанию
Сохраняет логи в файлы каждый час с автоматической очисткой старых файлов
"""

import os
import logging
import logging.handlers
import threading
import time
from datetime import datetime
from typing import Optional
import schedule


class LogRotator:
    """Класс для управления ротацией логов"""
    
    def __init__(self,
                 log_dir: str = "logs",
                 max_files: int = 24,  # Хранить логи за 24 часа
                 log_format: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'):
        """
        Инициализация ротатора логов
        
        Args:
            log_dir: Директория для хранения логов
            max_files: Максимальное количество файлов логов
            log_format: Формат логов
        """
        self.log_dir = log_dir
        self.max_files = max_files
        self.log_format = log_format
        self.current_log_file = None
        self.scheduler_thread = None
        self.running = False
        
        # Создаем директорию для логов
        os.makedirs(log_dir, exist_ok=True)
        
        # Настраиваем логирование
        self._setup_logging()
        
        # Запускаем планировщик
        self._start_scheduler()
    
    def _setup_logging(self):
        """Настройка системы логирования"""
        # Создаем основной логгер
        self.logger = logging.getLogger('sql_agent')
        self.logger.setLevel(logging.INFO)
        
        # Очищаем существующие обработчики
        self.logger.handlers.clear()
        
        # Создаем обработчик для консоли
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(self.log_format)
        console_handler.setFormatter(console_formatter)
        self.logger.addHandler(console_handler)
        
        # Создаем обработчик для файла
        self._create_file_handler()
        
        # Настраиваем логирование для других модулей
        self._setup_module_loggers()
    
    def _create_file_handler(self):
        """Создает обработчик для записи в файл"""
        # Генерируем имя файла с текущим временем
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"sql_agent_{timestamp}.log"
        self.current_log_file = os.path.join(self.log_dir, log_filename)
        
        # Создаем обработчик файла
        file_handler = logging.FileHandler(self.current_log_file, encoding='utf-8')
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter(self.log_format)
        file_handler.setFormatter(file_formatter)
        
        # Добавляем обработчик к основному логгеру
        self.logger.addHandler(file_handler)
        
        self.logger.info(f"📝 Логирование начато в файл: {self.current_log_file}")
    
    def _setup_module_loggers(self):
        """Настраивает логирование для всех модулей проекта"""
        # Настраиваем логирование для всех модулей sql_agent
        module_loggers = [
            'sql_agent.api',
            'sql_agent.task_manager', 
            'sql_agent.llm_analyzer',
            'sql_agent.models'
        ]
        
        for module_name in module_loggers:
            module_logger = logging.getLogger(module_name)
            module_logger.setLevel(logging.INFO)
            # Модули будут использовать обработчики основного логгера
    
    def rotate_logs(self):
        """Ротация логов - создание нового файла"""
        try:
            self.logger.info("🔄 Начинаем ротацию логов...")
            
            # Удаляем старый файловый обработчик
            for handler in self.logger.handlers[:]:
                if isinstance(handler, logging.FileHandler):
                    handler.close()
                    self.logger.removeHandler(handler)
            
            # Создаем новый файловый обработчик
            self._create_file_handler()
            
            # Очищаем старые файлы
            self._cleanup_old_logs()
            
            self.logger.info("✅ Ротация логов завершена")
            
        except Exception as e:
            print(f"❌ Ошибка при ротации логов: {e}")
    
    def _cleanup_old_logs(self):
        """Удаляет старые файлы логов"""
        try:
            # Получаем список всех файлов логов
            log_files = []
            for filename in os.listdir(self.log_dir):
                if filename.startswith("sql_agent_") and filename.endswith(".log"):
                    filepath = os.path.join(self.log_dir, filename)
                    # Получаем время создания файла
                    mtime = os.path.getmtime(filepath)
                    log_files.append((mtime, filepath))
            
            # Сортируем по времени создания (новые первыми)
            log_files.sort(reverse=True)
            
            # Удаляем лишние файлы
            if len(log_files) > self.max_files:
                files_to_delete = log_files[self.max_files:]
                for _, filepath in files_to_delete:
                    try:
                        os.remove(filepath)
                        self.logger.info(f"🗑️ Удален старый лог файл: {os.path.basename(filepath)}")
                    except Exception as e:
                        self.logger.warning(f"⚠️ Не удалось удалить файл {filepath}: {e}")
            
            self.logger.info(f"📊 Всего файлов логов: {len(log_files)}")
            
        except Exception as e:
            self.logger.error(f"❌ Ошибка при очистке старых логов: {e}")
    
    def _start_scheduler(self):
        """Запускает планировщик для ротации логов"""
        # Планируем ротацию каждый час
        schedule.every().hour.do(self.rotate_logs)
        
        # Запускаем планировщик в отдельном потоке
        self.running = True
        self.scheduler_thread = threading.Thread(target=self._run_scheduler, daemon=True)
        self.scheduler_thread.start()
        
        self.logger.info("⏰ Планировщик ротации логов запущен (каждый час)")
    
    def _run_scheduler(self):
        """Запускает планировщик в отдельном потоке"""
        while self.running:
            try:
                schedule.run_pending()
                time.sleep(60)  # Проверяем каждую минуту
            except Exception as e:
                print(f"❌ Ошибка в планировщике: {e}")
                time.sleep(60)
    
    def stop(self):
        """Останавливает планировщик"""
        self.running = False
        if self.scheduler_thread and self.scheduler_thread.is_alive():
            self.scheduler_thread.join(timeout=5)
        
        # Закрываем все обработчики
        for handler in self.logger.handlers[:]:
            handler.close()
            self.logger.removeHandler(handler)
        
        self.logger.info("🛑 Ротатор логов остановлен")
    
    def get_log_info(self) -> dict:
        """Возвращает информацию о текущем состоянии логирования"""
        log_files = []
        total_size = 0
        
        try:
            for filename in os.listdir(self.log_dir):
                if filename.startswith("sql_agent_") and filename.endswith(".log"):
                    filepath = os.path.join(self.log_dir, filename)
                    size = os.path.getsize(filepath)
                    mtime = os.path.getmtime(filepath)
                    log_files.append({
                        "filename": filename,
                        "size": size,
                        "modified": datetime.fromtimestamp(mtime).isoformat()
                    })
                    total_size += size
        except Exception as e:
            self.logger.error(f"❌ Ошибка при получении информации о логах: {e}")
        
        return {
            "log_directory": self.log_dir,
            "current_log_file": self.current_log_file,
            "total_files": len(log_files),
            "total_size_bytes": total_size,
            "max_files": self.max_files,
            "files": sorted(log_files, key=lambda x: x["modified"], reverse=True)
        }

# Глобальный экземпляр ротатора
_log_rotator: Optional[LogRotator] = None

def get_log_rotator() -> LogRotator:
    """Получает глобальный экземпляр ротатора логов"""
    global _log_rotator
    if _log_rotator is None:
        _log_rotator = LogRotator()
    return _log_rotator

def setup_logging():
    """Настраивает систему логирования с ротацией"""
    return get_log_rotator()

def stop_logging():
    """Останавливает систему логирования"""
    global _log_rotator
    if _log_rotator:
        _log_rotator.stop()
        _log_rotator = None
