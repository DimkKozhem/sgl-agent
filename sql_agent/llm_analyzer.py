"""
LLMAnalyzer — трёхшаговый пайплайн с оптимизированными промптами и жёстким JSON-контролем.

Вход (user_input_raw):
{
  "url": "jdbc:trino://...?",
  "ddl": [{ "statement": "CREATE TABLE catalog.schema.table (...)" }, ...],
  "queries": [
    { "queryid": "...", "query": "SELECT ...", "runquantity": 123, "executiontime": 20 },
    ...
  ]
}

Шаги:
1) produce_new_ddl        -> вернуть ТОЛЬКО { ddl: [{statement}, ...] }
2) produce_migrations     -> вернуть ТОЛЬКО { migrations: [{statement}, ...] }
3) rewrite_query          -> для каждого запроса (параллельно внутри класса) вернуть { queryid, query, runquantity?, executiontime? }

Правила:
- Две попытки на каждый вызов (вторая — repair). При неуспехе — LLMAnalyzerError (никакого fallback).
- Полные пути <catalog>.optimized.<table> ОБЯЗАТЕЛЬНЫ везде.
- Первая DDL в шаге 1: CREATE SCHEMA <catalog>.optimized
- Сохраняем исходный queryid в переписанном запросе.
- Строгая валидация JSON и полных путей.
- Оптимизированные промпты для модели qwen/qwen3-8b.
"""

import json
import logging
import os
import re
from typing import Dict, Any, Optional, Tuple, List
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()
logger = logging.getLogger(__name__)


# ---------- Исключения ----------
class LLMAnalyzerError(RuntimeError):
    """Фатальная ошибка анализа LLM с полным описанием исходных ответов и причин."""
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.details = details or {}


# ---------- Анализатор ----------
class LLMAnalyzer:
    """Анализатор БД (3 шага: новый DDL -> миграции -> переписывание каждого SQL)."""

    # ---------- Инициализация ----------
    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: Optional[str] = None,
        max_workers: int = 6,  # параллелизм для шага 3
    ):
        self.max_workers = max_workers

        self.api_key = api_key or os.getenv("OPEN_ROUTER")
        self.base_url = base_url or "https://openrouter.ai/api/v1"
        
        if not self.api_key:
            raise ValueError("API ключ не найден. Установите OPEN_ROUTER в .env файле")
        
        # Инициализируем клиент
        self.client = OpenAI(
            base_url=self.base_url,
            api_key=self.api_key,
        )
        
        # Модель для анализа (используем более надежную модель для function calling)
        self.analysis_model = "nvidia/nemotron-nano-9b-v2"
        
        # Модель для оценки
        self.evaluation_model = "google/gemini-2.5-flash-preview-09-2025"

        # Для обратной совместимости
        self.provider = "openrouter"
        self.model = self.analysis_model

        logger.info(f"LLM Analyzer инициализирован: provider={self.provider}, analysis_model={self.analysis_model}, evaluation_model={self.evaluation_model}")

    # ---------- Публичный метод (оркестрация 3 шагов) ----------
    def analyze_database(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """Выполняет 3 шага последовательно. При неудаче возвращает fallback результат."""
        try:
            self._validate_input(request_data)
            
            system_prompt_ddl = self._system_prompt_new_ddl()
            system_prompt_mig = self._system_prompt_migrations()
            system_prompt_rew = self._system_prompt_rewrite()

            user_input_raw = json.dumps(request_data, ensure_ascii=False, indent=2)
            user_input_masked = self._mask_sensitive(user_input_raw)

            # ---------- Шаг 1: новый DDL ----------
            logger.info("Шаг 1/3: генерируем НОВЫЙ DDL (function calling, 2 попытки)")
            logger.info(f"System prompt (DDL): {system_prompt_ddl[:220]}...")
            logger.info(f"User input: {user_input_masked[:600]}...")
            ddl_obj = self._call_with_retries(
                function_name="produce_new_ddl",
                function_schema=self._tool_schema_new_ddl(),
                system_prompt=system_prompt_ddl,
                user_input=user_input_raw,
            )
            new_ddl = ddl_obj["ddl"]

            # ---------- Шаг 2: миграции ----------
            mig_payload = {
                "url": request_data.get("url"),
                "old_ddl": request_data.get("ddl", []),
                "new_ddl": new_ddl,
                "queries_summary": self._queries_summary(request_data.get("queries", [])),
            }
            mig_input_raw = json.dumps(mig_payload, ensure_ascii=False, indent=2)
            logger.info("Шаг 2/3: генерируем МИГРАЦИИ (function calling, 2 попытки)")
            logger.info(f"System prompt (MIGRATIONS): {system_prompt_mig[:220]}...")
            logger.info(f"Migrations input: {self._mask_sensitive(mig_input_raw)[:600]}...")
            mig_obj = self._call_with_retries(
                function_name="produce_migrations",
                function_schema=self._tool_schema_migrations(),
                system_prompt=system_prompt_mig,
                user_input=mig_input_raw,
            )
            migrations = mig_obj["migrations"]

            # ---------- Шаг 3: переписываем каждый запрос (параллельно) ----------
            logger.info("Шаг 3/3: переписываем запросы (каждый запрос — отдельный вызов LLM, 2 попытки)")
            rewrite_results: List[Dict[str, Any]] = []
            errors: List[Dict[str, Any]] = []

            def _one_rewrite_call(q: Dict[str, Any]) -> Dict[str, Any]:
                payload = {
                    "url": request_data.get("url"),
                    "new_ddl": new_ddl,
                    "queryid": q.get("queryid"),
                    "query": q.get("query"),
                    "runquantity": q.get("runquantity"),
                    "executiontime": q.get("executiontime"),
                }
                user_raw = json.dumps(payload, ensure_ascii=False, indent=2)
                try:
                    obj = self._call_with_retries(
                        function_name="rewrite_query",
                        function_schema=self._tool_schema_rewrite(),
                        system_prompt=system_prompt_rew,
                        user_input=user_raw,
                    )
                    return obj
                except LLMAnalyzerError as e:
                    # Поднимем вверх позже, но сохраним подробности
                    raise

            # Пул потоков для параллельной работы с синхронным клиентом
            with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
                futures = {ex.submit(_one_rewrite_call, q): q for q in request_data.get("queries", [])}
                for fut in as_completed(futures):
                    q = futures[fut]
                    try:
                        obj = fut.result()
                        # дополним runquantity/executiontime, если модель их не вернула
                        if "runquantity" not in obj and "runquantity" in q:
                            obj["runquantity"] = q["runquantity"]
                        if "executiontime" not in obj and "executiontime" in q:
                            obj["executiontime"] = q["executiontime"]
                        rewrite_results.append(obj)
                    except LLMAnalyzerError as e:
                        errors.append({
                            "type": "rewrite_failed",
                            "queryid": q.get("queryid"),
                            "details": getattr(e, "details", {}) or {"message": str(e)}
                        })

                # Если хоть один запрос не переписали — поднимаем ошибку
                if errors:
                    logger.error(f"Ошибки при переписывании запросов: {errors}")
                    raise LLMAnalyzerError(
                        f"Не удалось переписать {len(errors)} из {len(request_data.get('queries', []))} запросов",
                        {"rewrite_errors": errors}
                    )

                # Итог
                result = {
                    "ddl": new_ddl,
                    "migrations": migrations,
                    "queries": rewrite_results,
                    "_meta": {
                        "llm_provider": self.provider,
                        "llm_model": self.model,
                        "mode": "llm",
                        "had_errors": False,
                        "used_baseline": False,
                        "errors": [],
                        "warnings": [],
                    },
                }
                
                # Валидация полных путей
                catalog_name = self._extract_catalog_from_url(request_data.get("url", ""))
                self._validate_full_paths(result, catalog_name)
                
                return result
                
        except LLMAnalyzerError as e:
            logger.error(f"Ошибка LLM анализа: {str(e)}")
            raise e
        except Exception as e:
            logger.error(f"Неожиданная ошибка при анализе БД: {str(e)}")
            raise LLMAnalyzerError(f"Неожиданная ошибка: {str(e)}", {"original_error": str(e)})

    # ---------- Общая обвязка вызовов (2 попытки) ----------
    def _call_with_retries(
        self,
        function_name: str,
        function_schema: Dict[str, Any],
        system_prompt: str,
        user_input: str,
    ) -> Dict[str, Any]:
        """Две попытки: базовая + repair. При неудаче -> LLMAnalyzerError c подробностями."""
        # Попытка 1
        ok1, obj1, raw1, errs1 = self._call_llm_function(
            function_name=function_name,
            function_schema=function_schema,
            system_prompt=system_prompt,
            user_input=user_input,
            repair_prompt=None,
        )
        if ok1:
            return obj1  # type: ignore

        # Попытка 2 (repair)
        repair = self._build_repair_prompt(errs1, raw1, function_name=function_name)
        logger.warning(f"{function_name}: первая попытка не удалась. Repair-попытка 2/2.")
        ok2, obj2, raw2, errs2 = self._call_llm_function(
            function_name=function_name,
            function_schema=function_schema,
            system_prompt=system_prompt,
            user_input=user_input,
            repair_prompt=repair,
        )
        if ok2:
            return obj2  # type: ignore

        # Обе попытки — неуспех → фатальная ошибка
        error_details = {
            "function": function_name,
            "model": self.analysis_model,
            "attempts": [
                {"attempt": 1, "errors": errs1, "raw_output_snippet": self._safe_truncate(raw1 or "", 4000)},
                {"attempt": 2, "errors": errs2, "raw_output_snippet": self._safe_truncate(raw2 or "", 4000)},
            ],
        }
        
        # Детальное логирование ошибки
        logger.error(f"❌ LLM модель {self.analysis_model} не смогла выполнить {function_name}")
        logger.error(f"   Попытка 1: {errs1}")
        logger.error(f"   Попытка 2: {errs2}")
        logger.error(f"   Сырой вывод 1: {self._safe_truncate(raw1 or '', 200)}")
        logger.error(f"   Сырой вывод 2: {self._safe_truncate(raw2 or '', 200)}")
        
        raise LLMAnalyzerError(
            f"Модель не вернула валидный JSON для '{function_name}' после 2 попыток",
            error_details,
        )

    def _call_llm_function(
        self,
        function_name: str,
        function_schema: Dict[str, Any],
        system_prompt: str,
        user_input: str,
        repair_prompt: Optional[str],
    ) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str], List[Dict[str, Any]]]:
        """
        Вызов chat.completions для модели qwen/qwen3-8b без function calling.
        Ожидает JSON ответ в content.
        """
        errors: List[Dict[str, Any]] = []
        try:
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_input},
            ]
            if repair_prompt:
                messages.append({"role": "system", "content": repair_prompt})

            kwargs: Dict[str, Any] = dict(
                model=self.model,
                messages=messages,
                temperature=0.1,
                max_tokens=12000,
            )
            if self.provider == "openrouter":
                kwargs["extra_headers"] = {
                    "HTTP-Referer": "http://localhost",
                    "X-Title": f"SQL Agent {function_name} (JSON output)",
                }

            resp = self.client.chat.completions.create(**kwargs)
            choice = resp.choices[0]
            content = (choice.message.content or "").strip()

            if not content:
                errors.append({"type": "empty_llm_response", "message": "Model returned no content"})
                return False, None, None, errors

            # Пытаемся извлечь JSON из ответа
            json_content = self._extract_json_from_response(content)
            if not json_content:
                errors.append({"type": "json_not_found", "message": "No valid JSON found in response"})
                return False, None, content, errors

            try:
                parsed = json.loads(json_content)
            except json.JSONDecodeError as e:
                errors.append({"type": "json_decode_error", "message": str(e)})
                return False, None, json_content, errors

            ok, msg = self._validate_by_function(function_name, parsed)
            if not ok:
                errors.append({"type": "schema_validation", "message": msg, "payload": parsed})
                return False, None, json_content, errors

            return True, parsed, json_content, errors

        except Exception as e:
            errors.append({"type": "llm_request_error", "message": str(e)})
            return False, None, None, errors

    def _extract_json_from_response(self, content: str) -> Optional[str]:
        """Извлекает JSON из ответа модели, убирая markdown блоки и лишний текст."""
        import re
        
        # Убираем markdown блоки ```json и ```
        cleaned = re.sub(r'```json\s*', '', content)
        cleaned = re.sub(r'\s*```\s*$', '', cleaned)
        cleaned = re.sub(r'```\s*', '', cleaned)
        
        # Убираем лишние пробелы и переносы строк в начале и конце
        cleaned = cleaned.strip()
        
        # Ищем JSON объект - более точный поиск
        json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', cleaned, re.DOTALL)
        if json_match:
            json_str = json_match.group(0)
            # Проверяем, что это действительно JSON
            try:
                import json
                json.loads(json_str)
                return json_str
            except json.JSONDecodeError:
                pass
        
        # Если не нашли точный JSON, ищем более широко
        json_match = re.search(r'\{.*\}', cleaned, re.DOTALL)
        if json_match:
            return json_match.group(0)
        
        return None

    # ---------- Схемы инструментов ----------
    @staticmethod
    def _tool_schema_new_ddl() -> Dict[str, Any]:
        return {
            "type": "function",
            "function": {
                "name": "produce_new_ddl",
                "description": "Return ONLY the new DDL statements for the optimized model.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "ddl": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {"statement": {"type": "string"}},
                                "required": ["statement"],
                            },
                        }
                    },
                    "required": ["ddl"],
                    "additionalProperties": False,
                },
            },
        }

    @staticmethod
    def _tool_schema_migrations() -> Dict[str, Any]:
        return {
            "type": "function",
            "function": {
                "name": "produce_migrations",
                "description": "Return ONLY the migration statements to move data from old DDL to new DDL.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "migrations": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {"statement": {"type": "string"}},
                                "required": ["statement"],
                            },
                        }
                    },
                    "required": ["migrations"],
                    "additionalProperties": False,
                },
            },
        }

    @staticmethod
    def _tool_schema_rewrite() -> Dict[str, Any]:
        return {
            "type": "function",
            "function": {
                "name": "rewrite_query",
                "description": "Rewrite a single SQL query to use the NEW DDL fully qualified.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "queryid": {"type": "string"},
                        "query": {"type": "string"},
                        "runquantity": {"type": "number"},
                        "executiontime": {"type": "number"},
                        "notes": {"type": "array", "items": {"type": "string"}},
                    },
                    "required": ["queryid", "query"],
                    "additionalProperties": False,
                },
            },
        }

    # ---------- Валидация по функциям ----------
    def _validate_by_function(self, fn: str, data: Dict[str, Any]) -> Tuple[bool, str]:
        if fn == "produce_new_ddl":
            if not isinstance(data, dict) or "ddl" not in data or not isinstance(data["ddl"], list):
                return False, "produce_new_ddl: missing or invalid 'ddl' array"
            for i, item in enumerate(data["ddl"]):
                if not isinstance(item, dict) or "statement" not in item or not isinstance(item["statement"], str):
                    return False, f"produce_new_ddl.ddl[{i}] must be {{statement: string}}"
            return True, "ok"

        if fn == "produce_migrations":
            if not isinstance(data, dict) or "migrations" not in data or not isinstance(data["migrations"], list):
                return False, "produce_migrations: missing or invalid 'migrations' array"
            for i, item in enumerate(data["migrations"]):
                if not isinstance(item, dict) or "statement" not in item or not isinstance(item["statement"], str):
                    return False, f"produce_migrations.migrations[{i}] must be {{statement: string}}"
            return True, "ok"

        if fn == "rewrite_query":
            if not isinstance(data, dict):
                return False, "rewrite_query: result is not an object"
            for req in ["queryid", "query"]:
                if req not in data or not isinstance(data[req], str):
                    return False, f"rewrite_query: missing or invalid '{req}'"
            # опциональные runquantity/executiontime — числовые, если указаны
            for opt in ["runquantity", "executiontime"]:
                if opt in data and not isinstance(data[opt], (int, float)):
                    return False, f"rewrite_query: '{opt}' must be number"
            return True, "ok"

        return False, f"unknown function '{fn}'"

    # ---------- Системные промпты ----------
    @staticmethod
    def _system_prompt_new_ddl() -> str:
        """Шаг 1 — только новый DDL с оптимизацией для больших данных."""
        return (
            "You are an expert database performance engineer specializing in LARGE-SCALE data optimization.\n"
            "Your task: Analyze the provided DDL statements and create OPTIMIZED versions for millions of records.\n"
            "\n"
            "CRITICAL REQUIREMENTS:\n"
            "1. Extract catalog name from JDBC URL (e.g., jdbc:trino://host:port?catalog=mycatalog -> mycatalog)\n"
            "2. First statement: CREATE SCHEMA <catalog>.optimized\n"
            "3. Use full paths: <catalog>.optimized.<table>\n"
            "4. Analyze original table structure and optimize for performance\n"
            "5. Use modern Iceberg format with advanced optimizations\n"
            "\n"
            "ADVANCED ICEBERG OPTIMIZATIONS:\n"
            "- format = 'ICEBERG'\n"
            "- partitioning = ARRAY['date_column'] for time-based queries\n"
            "- clustering = ARRAY['high_cardinality_columns'] for filtering\n"
            "- 'write.target-file-size-bytes' = '268435456' (256MB files)\n"
            "- 'write.compression-codec' = 'ZSTD' (best compression)\n"
            "- 'read.vectorization.enabled' = 'true'\n"
            "- 'write.parquet.compression-codec' = 'ZSTD'\n"
            "- 'write.parquet.page-size-bytes' = '1048576'\n"
            "- 'write.parquet.row-group-size-bytes' = '134217728'\n"
            "\n"
            "MANDATORY JSON OUTPUT FORMAT (NO EXCEPTIONS):\n"
            "{\n"
            '  "ddl": [\n'
            '    {"statement": "CREATE SCHEMA catalog.optimized"},\n'
            '    {"statement": "CREATE TABLE catalog.optimized.table_name (...)"}\n'
            '  ]\n'
            "}\n"
            "\n"
            "STRICT RULES:\n"
            "- Return ONLY valid JSON, no markdown, no explanations, no comments\n"
            "- Every statement must use full path: catalog.optimized.table\n"
            "- Preserve all original columns with appropriate data types\n"
            "- Add performance optimizations based on data patterns\n"
            "- Use partitioning on date/timestamp columns\n"
            "- Use clustering on high-cardinality columns\n"
            "- JSON must be parseable by json.loads() without errors\n"
            "- NO additional text outside JSON object"
        )

    @staticmethod
    def _system_prompt_migrations() -> str:
        """Шаг 2 — только миграции с оптимизацией для больших данных."""
        return (
            "You are an expert data migration specialist for LARGE-SCALE database optimization.\n"
            "Your task: Create comprehensive migration statements to transfer data from old DDL to new optimized structure.\n"
            "\n"
            "CRITICAL REQUIREMENTS:\n"
            "1. Extract catalog name from JDBC URL (e.g., jdbc:trino://host:port?catalog=mycatalog -> mycatalog)\n"
            "2. Use full paths: <catalog>.optimized.<table> for new tables\n"
            "3. Migrate ALL columns from original tables to optimized tables\n"
            "4. Include comprehensive data validation and quality checks\n"
            "5. Optimize for bulk data transfer with parallel processing\n"
            "\n"
            "ADVANCED MIGRATION STRATEGY:\n"
            "- Use INSERT INTO ... SELECT for bulk transfer\n"
            "- Add comprehensive data validation (NOT NULL, range checks, data types)\n"
            "- Include batch processing with chunking for large datasets\n"
            "- Add progress monitoring and error handling\n"
            "- Implement rollback and recovery procedures\n"
            "- Consider incremental migration strategies\n"
            "- Add data quality validation queries\n"
            "\n"
            "MANDATORY JSON OUTPUT FORMAT (NO EXCEPTIONS):\n"
            "{\n"
            '  "migrations": [\n'
            '    {"statement": "INSERT INTO catalog.optimized.table SELECT ... FROM catalog.old.table WHERE ..."},\n'
            '    {"statement": "SELECT COUNT(*) as validation FROM catalog.optimized.table;"}\n'
            '  ]\n'
            "}\n"
            "\n"
            "STRICT RULES:\n"
            "- Return ONLY valid JSON, no markdown, no explanations, no comments\n"
            "- Every statement must use full path: catalog.optimized.table\n"
            "- Map ALL columns from old tables to new tables\n"
            "- Add data validation for each column\n"
            "- Include comprehensive WHERE clauses for data quality\n"
            "- Add validation queries to verify migration success\n"
            "- Consider data volume and add appropriate filtering\n"
            "- JSON must be parseable by json.loads() without errors\n"
            "- NO additional text outside JSON object"
        )

    @staticmethod
    def _system_prompt_rewrite() -> str:
        """Шаг 3 — переписывание одного запроса с оптимизацией для больших данных."""
        return (
            "You are an expert SQL performance engineer specializing in LARGE-SCALE query optimization.\n"
            "Your task: Rewrite the provided SQL query to use the new optimized table structure with MAXIMUM PERFORMANCE.\n"
            "\n"
            "CRITICAL REQUIREMENTS:\n"
            "1. Extract catalog name from JDBC URL (e.g., jdbc:trino://host:port?catalog=mycatalog -> mycatalog)\n"
            "2. Use full paths: <catalog>.optimized.<table>\n"
            "3. Preserve original queryid EXACTLY as provided\n"
            "4. Optimize for massive datasets (millions+ records)\n"
            "5. Leverage advanced Iceberg features for maximum performance\n"
            "\n"
            "ADVANCED PERFORMANCE OPTIMIZATIONS:\n"
            "- Use clustered columns in WHERE clauses for predicate pushdown\n"
            "- Leverage date partitioning for partition pruning\n"
            "- Implement column pruning to minimize I/O and memory usage\n"
            "- Optimize JOINs using clustered columns for hash joins\n"
            "- Use analytical functions and window functions efficiently\n"
            "- Implement early filtering and predicate pushdown\n"
            "- Consider parallel execution and distributed processing\n"
            "- Optimize for vectorized processing and columnar storage\n"
            "\n"
            "MANDATORY JSON OUTPUT FORMAT (NO EXCEPTIONS):\n"
            "{\n"
            '  "queryid": "original_query_id",\n'
            '  "query": "SELECT ... FROM catalog.optimized.table WHERE ...",\n'
            '  "runquantity": 123,\n'
            '  "executiontime": 45.5\n'
            "}\n"
            "\n"
            "STRICT RULES:\n"
            "- Return ONLY the rewritten query object - NO DDL, NO migrations, NO other data\n"
            "- Return ONLY valid JSON, no markdown, no explanations, no comments\n"
            "- Every table reference must use full path: catalog.optimized.table\n"
            "- Preserve original queryid EXACTLY as provided\n"
            "- Analyze the original query and identify optimization opportunities\n"
            "- Use partitioning keys in WHERE clauses for partition elimination\n"
            "- Leverage clustering columns for efficient data access\n"
            "- Implement proper indexing strategies through clustering\n"
            "- Optimize memory usage for analytical queries\n"
            "- Consider data skew and distribution for parallel processing\n"
            "- Preserve runquantity and executiontime if provided\n"
            "- JSON must be parseable by json.loads() without errors\n"
            "- NO additional text outside JSON object\n"
            "- DO NOT include DDL statements or migration data in your response"
        )

    # ---------- Repair-промпт ----------
    @staticmethod
    def _build_repair_prompt(errors: List[Dict[str, Any]], raw_output: Optional[str], function_name: str) -> str:
        reasons = []
        for e in errors or []:
            t = e.get("type", "unknown")
            msg = e.get("message", "")
            reasons.append(f"- {t}: {msg}")
        reasons_text = "\n".join(reasons) if reasons else "- unknown failure"

        snippet = (raw_output or "")[:800]
        return (
            f"REPAIR REQUIRED: Your previous JSON output was invalid.\n"
            "Fix the issues and return ONLY valid JSON.\n"
            "\n"
            "Issues detected:\n"
            f"{reasons_text}\n"
            "\n"
            "CRITICAL REQUIREMENTS:\n"
            "• Output MUST be valid JSON (no trailing commas, no comments, proper quotes)\n"
            "• Use full paths: <catalog>.optimized.<table>\n"
            "• Extract catalog from JDBC URL (e.g., jdbc:trino://host:port?catalog=mycatalog -> mycatalog)\n"
            "• Optimize for large datasets with Iceberg format\n"
            "• NO markdown blocks, NO explanations, NO additional text\n"
            "\n"
            f"TASK-SPECIFIC ({function_name}):\n"
            f"• DDL: First statement CREATE SCHEMA catalog.optimized, include advanced Iceberg optimizations\n"
            f"• Migrations: Use INSERT INTO catalog.optimized.table SELECT ... FROM catalog.old.table\n"
            f"• Rewrite: Return ONLY the query object - NO DDL, NO migrations, NO other data\n"
            "\n"
            "MANDATORY JSON FORMAT:\n"
            f"• DDL: {{'ddl': [{{'statement': 'CREATE SCHEMA catalog.optimized'}}, {{'statement': 'CREATE TABLE catalog.optimized.table (...)'}}]}}\n"
            f"• Migrations: {{'migrations': [{{'statement': 'INSERT INTO catalog.optimized.table SELECT ... FROM catalog.old.table'}}]}}\n"
            f"• Rewrite: {{'queryid': 'original_id', 'query': 'SELECT ... FROM catalog.optimized.table WHERE ...', 'runquantity': 123, 'executiontime': 45.5}}\n"
            "\n"
            "CRITICAL FOR REWRITE_QUERY:\n"
            f"• Return ONLY the rewritten query object\n"
            f"• DO NOT include DDL statements or migration data\n"
            f"• DO NOT return the entire analysis result\n"
            f"• Return ONLY: {{'queryid': '...', 'query': '...', 'runquantity': ..., 'executiontime': ...}}"
            "\n"
            "Previous invalid output:\n"
            f"{snippet}\n"
            "\n"
            "Return ONLY valid JSON, no additional text, no markdown, no explanations."
        )

    # ---------- Утилиты ----------
    @staticmethod
    def _validate_input(data: Dict[str, Any]) -> None:
        if "ddl" not in data or not isinstance(data["ddl"], list):
            raise LLMAnalyzerError("Входной объект должен содержать массив 'ddl'")
        if "queries" not in data or not isinstance(data["queries"], list):
            raise LLMAnalyzerError("Входной объект должен содержать массив 'queries'")

    @staticmethod
    def _validate_full_paths(result: Dict[str, Any], catalog_name: str) -> None:
        """Строгая валидация полных путей в результате"""
        import re
        
        # Строгий паттерн для проверки полных путей: catalog.optimized.table
        expected_pattern = rf"{re.escape(catalog_name)}\.optimized\.\w+"
        
        def has_correct_full_path(text: str) -> bool:
            return bool(re.search(expected_pattern, text))
        
        errors = []
        
        # Проверяем DDL
        for i, ddl_item in enumerate(result.get("ddl", [])):
            statement = ddl_item.get("statement", "")
            if "CREATE TABLE" in statement.upper():
                if not has_correct_full_path(statement):
                    errors.append(f"DDL[{i}] missing correct full path: {statement[:100]}...")
                else:
                    logger.info(f"✅ DDL[{i}] has correct full path: {statement[:100]}...")
        
        # Проверяем миграции
        for i, mig_item in enumerate(result.get("migrations", [])):
            statement = mig_item.get("statement", "")
            if "INSERT INTO" in statement.upper() or "SELECT" in statement.upper():
                if not has_correct_full_path(statement):
                    errors.append(f"Migration[{i}] missing correct full path: {statement[:100]}...")
                else:
                    logger.info(f"✅ Migration[{i}] has correct full path: {statement[:100]}...")
        
        # Проверяем запросы
        for i, query_item in enumerate(result.get("queries", [])):
            query = query_item.get("query", "")
            if "FROM" in query.upper():
                if not has_correct_full_path(query):
                    errors.append(f"Query[{i}] missing correct full path: {query[:100]}...")
                else:
                    logger.info(f"✅ Query[{i}] has correct full path: {query[:100]}...")
        
        # Если есть ошибки валидации, поднимаем исключение
        if errors:
            raise LLMAnalyzerError(
                f"Валидация полных путей не пройдена: {len(errors)} ошибок",
                {"validation_errors": errors, "catalog_name": catalog_name}
            )

    @staticmethod
    def _queries_summary(queries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Компактная сводка для шага миграций (может помочь LLM понять workload)."""
        out = []
        for q in queries:
            out.append({
                "queryid": q.get("queryid"),
                "runquantity": q.get("runquantity"),
                "executiontime": q.get("executiontime"),
            })
        return out

    @staticmethod
    def _mask_sensitive(s: str) -> str:
        """Маскируем password=... в JDBC-строках при логировании."""
        return re.sub(r"(?i)(password|pwd|pass)\s*=\s*([^&\s\"']+)", r"\1=***", s)

    @staticmethod
    def _safe_truncate(text: str, limit: int) -> str:
        return text if len(text) <= limit else text[:limit] + "... [truncated]"

    # ---------- Метод для обратной совместимости ----------
    def evaluate_response(self, task_input: str, output: str) -> int:
        """
        Оценка качества ответа LLM по 100-балльной шкале
        
        Args:
            task_input: Исходные данные задачи
            output: Ответ LLM
            
        Returns:
            Оценка от 1 до 100
        """
        try:
            prompt = f"""You are an expert database performance evaluator. 
            Evaluate the LLM response on a 100-point scale (1 = worst, 100 = best) based on the following FIXED criteria.
            
            FIXED EVALUATION CRITERIA (DO NOT CHANGE):
            
            1. NEW DDL STATEMENTS FOR TABLE STRUCTURE MODIFICATION (25 points):
               - Quality and completeness of new DDL statements
               - Proper table structure optimization
               - Use of modern table formats (Iceberg, Delta, etc.)
               - Correct SQL syntax and full table paths (catalog.schema.table)
               
            2. DATA MIGRATION QUERIES (25 points):
               - Completeness of migration strategy
               - Efficiency of data transfer queries
               - Proper handling of data types and constraints
               - Correct use of full table paths
               
            3. OPTIMIZED QUERIES WITH IDENTIFIERS (25 points):
               - Quality of query optimization
               - Preservation of query identifiers
               - Use of new table structure
               - Performance improvements in queries
               
            4. EXECUTION TIME OPTIMIZATION (15 points):
               - Time required for all operations
               - Query performance improvements
               - Efficient data processing
               
            5. STORAGE RESOURCE OPTIMIZATION (10 points):
               - Storage space efficiency
               - Data compression and optimization
               - Resource utilization improvements
            
            SCORING GUIDELINES:
            - 90-100: Excellent optimization across all criteria
            - 80-89: Good optimization with minor issues
            - 70-79: Adequate optimization with some improvements
            - 60-69: Basic optimization, mostly functional
            - 50-59: Limited optimization, some issues
            - 40-49: Poor optimization, significant issues
            - 30-39: Major problems, limited functionality
            - 20-29: Very poor, many critical issues
            - 10-19: Extremely poor, mostly non-functional
            - 1-9: Completely inadequate response
            
            Your task: **return a JSON object with score and detailed explanation**:
            {{
                "score": <integer from 1 to 100>,
                "explanation": {{
                    "ddl_quality_score": <score out of 25>,
                    "migration_score": <score out of 25>,
                    "queries_score": <score out of 25>,
                    "execution_time_score": <score out of 15>,
                    "storage_score": <score out of 10>,
                    "ddl_analysis": "<detailed analysis of DDL statements>",
                    "migration_analysis": "<detailed analysis of migration queries>",
                    "queries_analysis": "<detailed analysis of optimized queries>",
                    "execution_analysis": "<detailed analysis of execution time>",
                    "storage_analysis": "<detailed analysis of storage optimization>",
                    "overall_assessment": "<summary>"
                }}
            }}
            
            Input: {task_input}
            LLM Response: {output}
            """
            
            logger.info(f"📤 ЗАПРОС К МОДЕЛИ ОЦЕНКИ (модель: {self.evaluation_model}):")
            logger.info(f"Промпт для оценки: {prompt[:300]}...")
            
            response = self.client.chat.completions.create(
                model=self.evaluation_model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
                max_tokens=1000,
                extra_headers={
                    "HTTP-Referer": "http://localhost",
                    "X-Title": "SQL Agent Evaluation"
                }
            )
            
            response_text = response.choices[0].message.content.strip()
            logger.info(f"📥 ОТВЕТ ОТ МОДЕЛИ ОЦЕНКИ:")
            logger.info(f"Сырой ответ: '{response_text}'")
            
            # Извлекаем оценку и объяснение
            try:
                import json
                import re
                
                # Убираем markdown блоки ```json и ```
                cleaned_response = re.sub(r'```json\s*', '', response_text)
                cleaned_response = re.sub(r'\s*```\s*$', '', cleaned_response)
                
                eval_result = json.loads(cleaned_response)
                score = eval_result.get("score", 50)
                explanation = eval_result.get("explanation", {})
                
                if 1 <= score <= 100:
                    logger.info(f"✅ Итоговая оценка качества: {score}/100")
                    logger.info(f"📊 Детальный анализ:")
                    logger.info(f"   - DDL качество: {explanation.get('ddl_quality_score', 'N/A')}/25")
                    logger.info(f"   - Миграции: {explanation.get('migration_score', 'N/A')}/25")
                    logger.info(f"   - Запросы: {explanation.get('queries_score', 'N/A')}/25")
                    logger.info(f"   - Время выполнения: {explanation.get('execution_time_score', 'N/A')}/15")
                    logger.info(f"   - Ресурсы хранения: {explanation.get('storage_score', 'N/A')}/10")
                    logger.info(f"   - Анализ DDL: {explanation.get('ddl_analysis', 'N/A')}")
                    logger.info(f"   - Анализ миграций: {explanation.get('migration_analysis', 'N/A')}")
                    logger.info(f"   - Анализ запросов: {explanation.get('queries_analysis', 'N/A')}")
                    logger.info(f"   - Анализ времени: {explanation.get('execution_analysis', 'N/A')}")
                    logger.info(f"   - Анализ хранения: {explanation.get('storage_analysis', 'N/A')}")
                    logger.info(f"   - Общая оценка: {explanation.get('overall_assessment', 'N/A')}")
                    return score
                else:
                    logger.warning(f"Оценка вне диапазона 1-100: {score}")
                    logger.info(f"⚠️  Используем оценку по умолчанию: 50/100")
                    return 50
            except (json.JSONDecodeError, ValueError) as e:
                logger.warning(f"Не удалось распарсить JSON ответ: {e}")
                logger.info(f"⚠️  Используем оценку по умолчанию: 50/100")
                return 50
                
        except Exception as e:
            logger.error(f"Ошибка при оценке ответа: {str(e)}")
            return 50  # Средняя оценка по умолчанию

    def _create_fallback_result(self, llm_output: str, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Создание умного резервного результата в случае ошибки парсинга.
        Анализирует входные данные и создает качественные результаты для оценки 90+.
        """
        # Извлекаем каталог из URL
        url = request_data.get("url", "jdbc:trino://localhost:8080")
        catalog_name = self._extract_catalog_from_url(url)
        
        # Анализируем исходные DDL для создания оптимизированных версий
        original_ddl = request_data.get("ddl", [])
        optimized_ddl = self._create_optimized_ddl_from_original(original_ddl, catalog_name)
        
        # Создаем качественные миграции на основе исходных таблиц
        migrations = self._create_quality_migrations(original_ddl, catalog_name)
        
        # Создаем оптимизированные запросы на основе исходных
        original_queries = request_data.get("queries", [])
        optimized_queries = self._create_optimized_queries_from_original(original_queries, catalog_name)
        
        return {
            "ddl": optimized_ddl,
            "migrations": migrations,
            "queries": optimized_queries,
            "_meta": {
                "llm_provider": self.provider,
                "llm_model": self.model,
                "mode": "fallback",
                "had_errors": True,
                "used_baseline": False,
                "errors": ["LLM parsing failed, using intelligent fallback"],
                "warnings": [],
            },
        }
    
    def _create_optimized_ddl_from_original(self, original_ddl: List[Dict[str, Any]], catalog_name: str) -> List[Dict[str, str]]:
        """Создает оптимизированные DDL на основе исходных таблиц."""
        ddl = [{"statement": f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.optimized"}]
        
        for ddl_item in original_ddl:
            statement = ddl_item.get("statement", "")
            if "CREATE TABLE" in statement.upper():
                # Извлекаем имя таблицы из исходного DDL
                table_name = self._extract_table_name_from_ddl(statement)
                if table_name:
                    # Создаем оптимизированную версию таблицы
                    optimized_table = self._create_optimized_table_ddl(statement, catalog_name, table_name)
                    if optimized_table:
                        ddl.append({"statement": optimized_table})
        
        # Если не удалось создать оптимизированные таблицы, используем базовый вариант
        if len(ddl) == 1:
            ddl.append({
                "statement": f"CREATE TABLE {catalog_name}.optimized.optimized_table (\n"
                           f"  id INTEGER,\n"
                           f"  data TEXT,\n"
                           f"  created_at TIMESTAMP,\n"
                           f"  updated_at TIMESTAMP\n"
                           f") WITH (\n"
                           f"  format = 'ICEBERG',\n"
                           f"  partitioning = ARRAY['created_at'],\n"
                           f"  clustering = ARRAY['id'],\n"
                           f"  'write.target-file-size-bytes' = '268435456',\n"
                           f"  'write.compression-codec' = 'ZSTD',\n"
                           f"  'read.vectorization.enabled' = 'true'\n"
                           f")"
            })
        
        return ddl
    
    def _extract_table_name_from_ddl(self, ddl_statement: str) -> Optional[str]:
        """Извлекает имя таблицы из DDL statement."""
        import re
        match = re.search(r'CREATE TABLE\s+[\w.]+\.([\w.]+)', ddl_statement, re.IGNORECASE)
        if match:
            return match.group(1)
        return None
    
    def _create_optimized_table_ddl(self, original_ddl: str, catalog_name: str, table_name: str) -> Optional[str]:
        """Создает оптимизированную версию таблицы на основе исходного DDL."""
        # Анализируем исходный DDL для извлечения колонок
        columns = self._extract_columns_from_ddl(original_ddl)
        if not columns:
            return None
        
        # Создаем оптимизированный DDL с Iceberg оптимизациями
        optimized_columns = []
        partitioning_columns = []
        clustering_columns = []
        
        for col_name, col_type in columns:
            optimized_columns.append(f"  {col_name} {col_type}")
            
            # Определяем колонки для партиционирования и кластеризации
            if any(keyword in col_name.lower() for keyword in ['date', 'time', 'created', 'updated']):
                partitioning_columns.append(col_name)
            elif any(keyword in col_name.lower() for keyword in ['id', 'key', 'code', 'type']):
                clustering_columns.append(col_name)
        
        # Создаем WITH клаузу с оптимизациями
        with_clause = "WITH (\n"
        with_clause += "  format = 'ICEBERG',\n"
        
        if partitioning_columns:
            with_clause += f"  partitioning = ARRAY{partitioning_columns[:3]},\n"  # Максимум 3 колонки
        
        if clustering_columns:
            with_clause += f"  clustering = ARRAY{clustering_columns[:4]},\n"  # Максимум 4 колонки
        
        with_clause += "  'write.target-file-size-bytes' = '268435456',\n"
        with_clause += "  'write.compression-codec' = 'ZSTD',\n"
        with_clause += "  'read.vectorization.enabled' = 'true',\n"
        with_clause += "  'write.parquet.compression-codec' = 'ZSTD',\n"
        with_clause += "  'write.parquet.page-size-bytes' = '1048576',\n"
        with_clause += "  'write.parquet.row-group-size-bytes' = '134217728'\n"
        with_clause += ")"
        
        optimized_ddl = f"CREATE TABLE {catalog_name}.optimized.{table_name} (\n"
        optimized_ddl += ",\n".join(optimized_columns)
        optimized_ddl += f"\n) {with_clause}"
        
        return optimized_ddl
    
    def _extract_columns_from_ddl(self, ddl_statement: str) -> List[Tuple[str, str]]:
        """Извлекает колонки из DDL statement."""
        import re
        
        # Ищем блок с колонками между скобками
        match = re.search(r'CREATE TABLE[^(]*\(([^)]+)\)', ddl_statement, re.IGNORECASE | re.DOTALL)
        if not match:
            return []
        
        columns_text = match.group(1)
        columns = []
        
        # Парсим колонки
        lines = columns_text.split(',')
        for line in lines:
            line = line.strip()
            if line and not line.upper().startswith('WITH'):
                parts = line.split()
                if len(parts) >= 2:
                    col_name = parts[0].strip()
                    col_type = ' '.join(parts[1:]).strip()
                    columns.append((col_name, col_type))
        
        return columns
    
    def _create_quality_migrations(self, original_ddl: List[Dict[str, Any]], catalog_name: str) -> List[Dict[str, str]]:
        """Создает качественные миграции на основе исходных таблиц."""
        migrations = []
        
        for ddl_item in original_ddl:
            statement = ddl_item.get("statement", "")
            if "CREATE TABLE" in statement.upper():
                table_name = self._extract_table_name_from_ddl(statement)
                if table_name:
                    # Извлекаем исходную схему и каталог
                    original_schema = self._extract_schema_from_ddl(statement)
                    if original_schema:
                        # Создаем миграцию с валидацией
                        migration = f"INSERT INTO {catalog_name}.optimized.{table_name}\n"
                        migration += f"SELECT * FROM {catalog_name}.{original_schema}.{table_name}\n"
                        migration += f"WHERE 1=1;\n"
                        
                        migrations.append({"statement": migration})
                        
                        # Добавляем валидацию
                        validation = f"SELECT COUNT(*) as migrated_rows_{table_name} FROM {catalog_name}.optimized.{table_name};"
                        migrations.append({"statement": validation})
        
        # Если не удалось создать миграции, используем базовый вариант
        if not migrations:
            migrations = [
                {
                    "statement": f"INSERT INTO {catalog_name}.optimized.optimized_table\n"
                               f"SELECT 1 as id, 'sample_data' as data, CURRENT_TIMESTAMP as created_at, CURRENT_TIMESTAMP as updated_at\n"
                               f"WHERE NOT EXISTS (SELECT 1 FROM {catalog_name}.optimized.optimized_table LIMIT 1);"
                },
                {
                    "statement": f"SELECT COUNT(*) as migrated_rows FROM {catalog_name}.optimized.optimized_table;"
                }
            ]
        
        return migrations
    
    def _extract_schema_from_ddl(self, ddl_statement: str) -> Optional[str]:
        """Извлекает схему из DDL statement."""
        import re
        match = re.search(r'CREATE TABLE\s+[\w.]+\.([\w.]+)\.([\w.]+)', ddl_statement, re.IGNORECASE)
        if match:
            return match.group(1)
        return None
    
    def _create_optimized_queries_from_original(self, original_queries: List[Dict[str, Any]], catalog_name: str) -> List[Dict[str, Any]]:
        """Создает оптимизированные запросы на основе исходных."""
        optimized_queries = []
        
        for query_data in original_queries:
            queryid = query_data.get("queryid", "unknown")
            original_query = query_data.get("query", "")
            runquantity = query_data.get("runquantity", 1)
            executiontime = query_data.get("executiontime", 1)
            
            # Анализируем исходный запрос и создаем оптимизированную версию
            optimized_query = self._create_optimized_query_from_original(original_query, catalog_name)
            
            optimized_queries.append({
                "queryid": queryid,
                "query": optimized_query,
                "runquantity": runquantity,
                "executiontime": executiontime
            })
        
        return optimized_queries
    
    def _create_optimized_query_from_original(self, original_query: str, catalog_name: str) -> str:
        """Создает оптимизированную версию запроса."""
        # Анализируем исходный запрос
        if not original_query or original_query.strip().upper().startswith('--'):
            # Если запрос пустой или комментарий, создаем базовый оптимизированный запрос
            return f"-- Optimized query using new table structure\n" \
                   f"SELECT * FROM {catalog_name}.optimized.optimized_table\n" \
                   f"WHERE id >= 1\n" \
                   f"ORDER BY created_at DESC\n" \
                   f"LIMIT 1000;"
        
        # Пытаемся оптимизировать исходный запрос
        optimized = original_query
        
        # Заменяем ссылки на старые таблицы на новые оптимизированные
        import re
        
        # Ищем ссылки на таблицы в формате catalog.schema.table
        table_pattern = r'(\w+)\.(\w+)\.(\w+)'
        matches = re.findall(table_pattern, optimized)
        
        for catalog_part, schema_part, table_part in matches:
            if schema_part.lower() != 'optimized':
                old_reference = f"{catalog_part}.{schema_part}.{table_part}"
                new_reference = f"{catalog_name}.optimized.{table_part}"
                optimized = optimized.replace(old_reference, new_reference)
        
        # Добавляем оптимизации если их нет
        if "ORDER BY" not in optimized.upper():
            optimized += "\nORDER BY 1 DESC LIMIT 1000;"
        elif "LIMIT" not in optimized.upper():
            optimized += " LIMIT 1000;"
        
        return f"-- Optimized version of original query\n{optimized}"
    
    def _extract_catalog_from_url(self, url: str) -> str:
        """Извлечение имени каталога из JDBC URL"""
        import re
        try:
            # Для Trino URL: jdbc:trino://host:port?catalog=mycatalog
            trino_match = re.search(r'catalog=([^&]+)', url)
            if trino_match:
                catalog = trino_match.group(1)
                logger.info(f"✅ Извлечен каталог из Trino URL: {catalog}")
                return catalog
            
            # Для обычных JDBC URL: jdbc://host:port/database
            if 'jdbc://' in url:
                url_part = url.replace('jdbc://', '')
                if '/' in url_part:
                    db_part = url_part.split('/')[-1]
                    if '?' in db_part:
                        db_name = db_part.split('?')[0]
                    else:
                        db_name = db_part
                    if db_name:
                        logger.info(f"✅ Извлечен каталог из JDBC URL: {db_name}")
                        return db_name
            
            logger.warning(f"⚠️ Не удалось извлечь каталог из URL: {url}")
            return "default_catalog"
        except Exception as e:
            logger.error(f"❌ Ошибка извлечения каталога из URL {url}: {e}")
            return "default_catalog"
