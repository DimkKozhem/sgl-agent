"""
Модуль для анализа и оптимизации БД с использованием LLM
"""

import json
import logging
from typing import Dict, Any, Optional
from openai import OpenAI
import os
from dotenv import load_dotenv

# Загружаем переменные окружения
load_dotenv()

logger = logging.getLogger(__name__)


class LLMAnalyzer:
    """Анализатор БД с использованием LLM"""
    
    def __init__(self, api_key: Optional[str] = None, base_url: Optional[str] = None):
        """
        Инициализация LLM анализатора
        
        Args:
            api_key: API ключ для OpenRouter (если не указан, берется из OPEN_ROUTER)
            base_url: Базовый URL для API (по умолчанию OpenRouter)
        """
        self.api_key = api_key or os.getenv("OPEN_ROUTER")
        self.base_url = base_url or "https://openrouter.ai/api/v1"
        
        if not self.api_key:
            raise ValueError("API ключ не найден. Установите OPEN_ROUTER в .env файле")
        
        # Инициализируем клиент
        self.client = OpenAI(
            base_url=self.base_url,
            api_key=self.api_key,
        )
        
        # Модель для анализа
        self.analysis_model = "qwen/qwen-2.5-7b-instruct"
        
        # Модель для оценки (используем Gemini как в оригинальном коде)
        self.evaluation_model = "google/gemini-2.5-flash-preview-09-2025"
        
        logger.info(f"LLM Analyzer инициализирован с моделью: {self.analysis_model}")
    
    def analyze_database(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Анализ структуры БД и генерация рекомендаций по оптимизации
        
        Args:
            request_data: Данные запроса с DDL, queries и URL
            
        Returns:
            Словарь с рекомендациями по оптимизации
        """
        try:
            # Формируем промпт для анализа
            system_prompt = self._get_analysis_prompt()
            user_input = json.dumps(request_data, ensure_ascii=False, indent=2)
            
            logger.info("Отправляем запрос к LLM для анализа БД")
            
            # Отправляем запрос к LLM
            response = self.client.chat.completions.create(
                model=self.analysis_model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_input}
                ],
                temperature=0.1,  # Низкая температура для более стабильных результатов
                max_tokens=4000,
                extra_headers={
                    "HTTP-Referer": "http://localhost",
                    "X-Title": "SQL Agent Analysis"
                }
            )
            
            llm_output = response.choices[0].message.content
            logger.info("Получен ответ от LLM")
            
            # Парсим JSON ответ
            try:
                result = json.loads(llm_output)
                logger.info("JSON ответ успешно распарсен")
                return result
            except json.JSONDecodeError as e:
                logger.warning(f"Ошибка парсинга JSON: {e}. Возвращаем сырой ответ")
                # Если JSON не парсится, возвращаем структурированный ответ
                return self._create_fallback_result(llm_output, request_data)
                
        except Exception as e:
            logger.error(f"Ошибка при анализе БД: {str(e)}")
            # Возвращаем базовый результат в случае ошибки
            return self._create_fallback_result("", request_data)
    
    def evaluate_response(self, task_input: str, output: str) -> int:
        """
        Оценка качества ответа LLM по 10-балльной шкале
        
        Args:
            task_input: Исходные данные задачи
            output: Ответ LLM
            
        Returns:
            Оценка от 1 до 10
        """
        try:
            prompt = f"""You are an evaluator of LLM responses. 
            Evaluate the response strictly on a 10-point scale (1 = worst, 10 = best), based on correctness and completeness.
            
            The LLM receives the following input:
            - DDL statements for creating tables. The script is split into individual queries for each table.
            - A set of queries with statistics on how many times each query was executed.
            - A JDBC connection string with login and password to evaluate the actual data.
            
            The LLM output includes:
            - A new set of DDL queries to modify the table structure.
            - A set of queries for data migration.
            - A set of queries with identifiers that use the new table structure.
            
            Your task: **return only a single integer from 1 to 10** that represents the overall quality of the LLM response. Do not include any explanation, text, or punctuation.
            
            Input: {task_input}
            LLM Response: {output}
            """
            
            response = self.client.chat.completions.create(
                model=self.evaluation_model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
                max_tokens=10,
                extra_headers={
                    "HTTP-Referer": "http://localhost",
                    "X-Title": "SQL Agent Evaluation"
                }
            )
            
            score_text = response.choices[0].message.content.strip()
            
            # Извлекаем числовую оценку
            try:
                score = int(score_text)
                if 1 <= score <= 10:
                    return score
                else:
                    logger.warning(f"Оценка вне диапазона 1-10: {score}")
                    return 5  # Средняя оценка по умолчанию
            except ValueError:
                logger.warning(f"Не удалось извлечь числовую оценку: {score_text}")
                return 5  # Средняя оценка по умолчанию
                
        except Exception as e:
            logger.error(f"Ошибка при оценке ответа: {str(e)}")
            return 5  # Средняя оценка по умолчанию
    
    def _get_analysis_prompt(self) -> str:
        """Получение системного промпта для анализа БД"""
        return """You are a database analyst and optimization expert. 
Your task is to analyze the logical data model, data statistics, 
as well as the structure and statistics of SQL queries.
Provide recommendations for modifying the data structure and queries 
to optimize performance.

Analyze the provided data and respond with a JSON object containing the following fields:
- ddl: a new set of DDL statements to modify the table structure (array of objects with "statement" field)
- migrations: a set of queries for migrating data (array of objects with "statement" field)  
- queries: a set of queries with their identifiers that use the new table structure (array of objects with "queryid", "query", "runquantity" fields)

Focus on:
1. Index optimization based on query patterns
2. Table partitioning strategies
3. Data type optimization
4. Query rewriting for better performance
5. Schema normalization/denormalization where appropriate

Return only valid JSON without any additional text or explanations."""

    def _create_fallback_result(self, llm_output: str, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Создание резервного результата в случае ошибки парсинга
        
        Args:
            llm_output: Сырой вывод LLM
            request_data: Исходные данные запроса
            
        Returns:
            Структурированный результат
        """
        # Извлекаем каталог из URL
        url = request_data.get("url", "jdbc:trino://localhost:8080")
        catalog_name = self._extract_catalog_from_url(url)
        
        # Базовые рекомендации
        ddl = [
            {"statement": f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.optimized_schema"},
            {"statement": f"CREATE TABLE {catalog_name}.optimized_schema.optimized_table (id INTEGER, data TEXT)"}
        ]
        
        migrations = [
            {"statement": f"INSERT INTO {catalog_name}.optimized_schema.optimized_table SELECT 1, 'optimized_data'"}
        ]
        
        queries = []
        for query_data in request_data.get("queries", []):
            queries.append({
                "queryid": query_data.get("queryid", "optimized_query"),
                "query": f"SELECT * FROM {catalog_name}.optimized_schema.optimized_table WHERE id = 1",
                "runquantity": query_data.get("runquantity", 1)
            })
        
        return {
            "ddl": ddl,
            "migrations": migrations,
            "queries": queries,
            "note": "Fallback result due to parsing error",
            "raw_output": llm_output[:500] if llm_output else ""
        }
    
    def _extract_catalog_from_url(self, url: str) -> str:
        """Извлечение имени каталога из JDBC URL"""
        try:
            if 'jdbc://' in url:
                url_part = url.replace('jdbc://', '')
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
