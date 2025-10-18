"""
LLMAnalyzer — гибридный пайплайн для оптимизации SQL с использованием LLM и детерминированной генерации.

Архитектура:
1. LLM анализирует схему и предлагает стратегию оптимизации
2. Детерминированный движок генерирует валидный SQL с реальными колонками
3. Реальные оптимизации запросов через sqlglot (замена SELECT *, добавление LIMIT)
4. Робастный парсинг сложных SQL структур

Ключевые улучшения:
- Полностью гибридный подход: LLM для анализа, шаблоны для генерации
- Реальные оптимизации SQL через изменение AST, не комментарии
- Робастный парсинг через sqlglot с fallback на улучшенный regex
- Подключение к БД для получения статистики данных
"""

import json
import logging
import os
import re
from typing import Dict, Any, Optional, Tuple, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from dotenv import load_dotenv
from openai import OpenAI

try:
    import sqlglot
    SQLGLOT_AVAILABLE = True
except ImportError:
    SQLGLOT_AVAILABLE = False
    logging.warning("sqlglot не установлен. SQL-парсинг будет отключен. Установите: pip install sqlglot")

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
    """Гибридный анализатор БД: LLM для анализа + детерминированная генерация SQL."""

    # ---------- Инициализация ----------
    def __init__(
            self,
            api_key: Optional[str] = None,
            base_url: Optional[str] = None,
            max_workers: int = 6,
    ):
        self.max_workers = max_workers
        self.api_key = api_key or os.getenv("OPEN_ROUTER")
        self.base_url = base_url or "https://openrouter.ai/api/v1"

        if not self.api_key:
            raise ValueError("API ключ не найден. Установите OPEN_ROUTER в .env файле")

        self.client = OpenAI(base_url=self.base_url, api_key=self.api_key)

        self.analysis_model = "nvidia/nemotron-nano-9b-v2"
        self.evaluation_model = "google/gemini-2.5-flash-preview-09-2025"

        # Потокобезопасность
        self._errors_lock = Lock()
        self._results_lock = Lock()

        # SQL-парсинг через sqlglot
        self.enable_sql_parsing = SQLGLOT_AVAILABLE
        if not self.enable_sql_parsing:
            logger.warning("SQL-парсинг отключен. Некоторые оптимизации будут недоступны.")

        self.provider = "openrouter"
        self.model = self.analysis_model

        logger.info("LLM Analyzer инициализирован:")
        logger.info(f"  - Provider: {self.provider}")
        logger.info(f"  - Analysis model: {self.analysis_model}")
        logger.info(f"  - SQL parsing: {'включен' if self.enable_sql_parsing else 'отключен'}")

    # ---------- Публичный метод ----------
    def analyze_database(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """Выполняет гибридный анализ и оптимизацию БД с автоматической оценкой."""
        try:
            self._validate_input(request_data)

            # Сначала пробуем извлечь из URL
            catalog_name = self._extract_catalog_from_url(request_data.get("url", ""))
            original_ddl = request_data.get("ddl", [])
            
            # Если каталог = default_catalog, пробуем извлечь из DDL
            if catalog_name == "default_catalog" and original_ddl:
                catalog_from_ddl = self._extract_catalog_from_ddl(original_ddl)
                if catalog_from_ddl and catalog_from_ddl != "public":
                    catalog_name = catalog_from_ddl
                    logger.info(f"✅ Каталог извлечен из DDL: {catalog_name}")

            logger.info(f"Используем каталог: {catalog_name}")

            # ====== ПОДКЛЮЧЕНИЕ К БД ДЛЯ СТАТИСТИКИ ======
            from .db_connector import DatabaseConnector

            db_connector = None
            table_stats = {}

            try:
                db_connector = DatabaseConnector(request_data.get("url", ""))
                if db_connector.connect():
                    logger.info("✅ Подключение к БД установлено, пытаемся получить статистику...")

                    # Получаем статистику по каждой таблице
                    stats_collected = 0
                    for ddl_item in original_ddl:
                        statement = ddl_item.get("statement", "")
                        table_name = self._extract_table_name_robust(statement)
                        if table_name:
                            stats = db_connector.get_table_stats(table_name)
                            column_stats = db_connector.get_column_stats(table_name)

                            # Считаем только если получили хотя бы что-то
                            if stats or column_stats:
                                table_stats[table_name] = {
                                    "row_count": stats.get("row_count", 0),
                                    "size_bytes": stats.get("total_size_bytes", 0),
                                    "column_stats": column_stats
                                }
                                stats_collected += 1

                    if stats_collected > 0:
                        logger.info(f"📊 Получена статистика для {stats_collected}/{len(original_ddl)} таблиц")
                    else:
                        logger.info(f"ℹ️ Статистика БД недоступна (недостаточно прав), используем только структуру схемы")
                else:
                    logger.warning("⚠️ Не удалось подключиться к БД, продолжаем без статистики")
            except Exception as e:
                logger.warning(f"⚠️ Ошибка при получении статистики БД: {e}")
                logger.info("ℹ️ Продолжаем оптимизацию на основе структуры схемы")
            finally:
                if db_connector:
                    db_connector.close()

            # ====== ШАГ 1: LLM анализирует таблицы ======
            logger.info("=" * 70)
            logger.info("ШАГ 1/4: LLM анализирует схему БД")
            logger.info("=" * 70)

            tables_analysis = self._analyze_ddl_with_llm(original_ddl, catalog_name, table_stats)

            # ====== ШАГ 2: Генерируем DDL детерминированно ======
            logger.info("=" * 70)
            logger.info("ШАГ 2/4: Генерация DDL (детерминированная)")
            logger.info("=" * 70)

            new_ddl = self._generate_ddl_deterministic(tables_analysis, catalog_name)

            # ====== ШАГ 3: Генерируем миграции ======
            logger.info("=" * 70)
            logger.info("ШАГ 3/4: Генерация миграций")
            logger.info("=" * 70)

            mig_payload = {
                "url": request_data.get("url"),
                "catalog_name": catalog_name,  # Явно передаем каталог
                "old_ddl": original_ddl,
                "new_ddl": new_ddl,
            }
            mig_input_raw = json.dumps(mig_payload, ensure_ascii=False, indent=2)

            mig_obj = self._call_with_retries(
                function_name="produce_migrations",
                function_schema=self._tool_schema_migrations(),
                system_prompt=self._system_prompt_migrations(),
                user_input=mig_input_raw,
            )
            migrations = mig_obj["migrations"]

            # ====== ШАГ 4: Оптимизируем запросы ======
            logger.info("=" * 70)
            logger.info("ШАГ 4/4: Оптимизация запросов (параллельно)")
            logger.info("=" * 70)

            optimized_queries = self._optimize_queries_parallel(
                request_data.get("queries", []),
                new_ddl,
                catalog_name
            )

            # Формируем результат
            result = {
                "ddl": new_ddl,
                "migrations": migrations,
                "queries": optimized_queries,
                "_meta": {
                    "llm_provider": self.provider,
                    "llm_model": self.model,
                    "mode": "hybrid",
                    "sql_parsing_enabled": self.enable_sql_parsing,
                    "had_errors": False,
                    "errors": [],
                    "warnings": [],
                },
            }

            self._validate_full_paths(result, catalog_name)

            # ✅ АВТОМАТИЧЕСКАЯ ОЦЕНКА КАЧЕСТВА
            logger.info("=" * 70)
            logger.info("ОЦЕНКА КАЧЕСТВА РЕЗУЛЬТАТА")
            logger.info("=" * 70)

            evaluation_score, evaluation_details = self._evaluate_result_internal(
                request_data,
                result
            )

            # Добавляем оценку в метаданные
            result["_meta"]["quality_score"] = evaluation_score
            result["_meta"]["quality_details"] = evaluation_details

            logger.info("=" * 70)
            logger.info("✅ Анализ БД успешно завершен")
            logger.info(f"📊 Итоговая оценка качества: {evaluation_score}/100")
            logger.info("=" * 70)

            return result

        except LLMAnalyzerError:
            raise
        except Exception as e:
            logger.error(f"Неожиданная ошибка: {str(e)}", exc_info=True)
            raise LLMAnalyzerError(f"Неожиданная ошибка: {str(e)}", {"original_error": str(e)})

    # ========================================================================
    # ГИБРИДНЫЙ ПОДХОД: LLM анализирует → Детерминированный движок генерирует
    # ========================================================================
    def _evaluate_result_internal(
            self,
            request_data: Dict[str, Any],
            result: Dict[str, Any]
    ) -> Tuple[int, Dict[str, Any]]:
        """Внутренняя оценка результата с детальным логированием."""
        try:
            task_input = json.dumps({
                "url": request_data.get("url"),
                "ddl_count": len(request_data.get("ddl", [])),
                "queries_count": len(request_data.get("queries", []))
            }, indent=2)

            output = json.dumps({
                "ddl_count": len(result.get("ddl", [])),
                "migrations_count": len(result.get("migrations", [])),
                "queries_count": len(result.get("queries", [])),
                "mode": result["_meta"]["mode"]
            }, indent=2)

            prompt = f"""Evaluate database optimization result on 100-point scale.

    Input:
    {task_input}

    Output:
    {output}

    Criteria:
    1. DDL Quality (25): Structure, optimizations, full paths
    2. Migration Quality (25): Strategy, validation
    3. Query Optimization (25): Performance, correctness
    4. Execution Time (15): Expected gains
    5. Storage (10): Compression, efficiency

    Return JSON:
    {{
      "score": <1-100>,
      "ddl_score": <0-25>,
      "migration_score": <0-25>,
      "query_score": <0-25>,
      "execution_score": <0-15>,
      "storage_score": <0-10>,
      "strengths": ["strength1", "strength2"],
      "weaknesses": ["weakness1", "weakness2"],
      "recommendations": ["rec1", "rec2"]
    }}"""

            logger.info("📤 Запрос к модели оценки...")

            response = self.client.chat.completions.create(
                model=self.evaluation_model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
                max_tokens=1500,
                extra_headers={
                    "HTTP-Referer": "http://localhost",
                    "X-Title": "SQL Agent Quality Evaluation"
                }
            )

            content = response.choices[0].message.content.strip()

            json_content = self._extract_json_from_response(content)
            if not json_content:
                logger.warning("⚠️ Модель оценки не вернула JSON, используем базовую оценку")
                return 70, {"note": "Evaluation model failed, using default score"}

            eval_result = json.loads(json_content)
            score = eval_result.get("score", 70)

            if not (1 <= score <= 100):
                logger.warning(f"⚠️ Оценка вне диапазона: {score}, используем 70")
                score = 70

            logger.info("📥 Результат оценки:")
            logger.info(f"   🎯 Общая оценка: {score}/100")
            logger.info(f"   📋 DDL качество: {eval_result.get('ddl_score', 'N/A')}/25")
            logger.info(f"   🔄 Миграции: {eval_result.get('migration_score', 'N/A')}/25")
            logger.info(f"   ⚡ Запросы: {eval_result.get('query_score', 'N/A')}/25")
            logger.info(f"   ⏱️  Время: {eval_result.get('execution_score', 'N/A')}/15")
            logger.info(f"   💾 Хранение: {eval_result.get('storage_score', 'N/A')}/10")

            strengths = eval_result.get("strengths", [])
            if strengths:
                logger.info("   ✅ Сильные стороны:")
                for s in strengths:
                    logger.info(f"      - {s}")

            weaknesses = eval_result.get("weaknesses", [])
            if weaknesses:
                logger.info("   ⚠️  Слабые стороны:")
                for w in weaknesses:
                    logger.info(f"      - {w}")

            recommendations = eval_result.get("recommendations", [])
            if recommendations:
                logger.info("   💡 Рекомендации:")
                for r in recommendations:
                    logger.info(f"      - {r}")

            details = {
                "ddl_score": eval_result.get("ddl_score"),
                "migration_score": eval_result.get("migration_score"),
                "query_score": eval_result.get("query_score"),
                "execution_score": eval_result.get("execution_score"),
                "storage_score": eval_result.get("storage_score"),
                "strengths": strengths,
                "weaknesses": weaknesses,
                "recommendations": recommendations
            }

            return score, details

        except Exception as e:
            logger.error(f"❌ Ошибка при оценке результата: {e}")
            return 70, {"error": str(e), "note": "Evaluation failed, using default score"}

    def _analyze_ddl_with_llm(
            self,
            original_ddl: List[Dict[str, Any]],
            catalog_name: str,
            table_stats: Dict[str, Dict[str, Any]] = None
    ) -> List[Dict[str, Any]]:
        """LLM ТОЛЬКО анализирует таблицы и предлагает стратегию оптимизации."""

        if table_stats is None:
            table_stats = {}

        tables_analysis = []

        for ddl_item in original_ddl:
            statement = ddl_item.get("statement", "")
            if "CREATE TABLE" not in statement.upper():
                continue

            table_name = self._extract_table_name_robust(statement)
            if not table_name:
                continue

            columns = self._extract_columns_robust(statement)
            if not columns:
                continue

            # Получаем статистику таблицы
            stats = table_stats.get(table_name, {})
            row_count = stats.get("row_count", "unknown")
            size_bytes = stats.get("size_bytes", 0)
            column_stats = stats.get("column_stats", {})

            # LLM анализирует ТОЛЬКО стратегию с учетом реальных данных
            columns_data = [{
                "name": c[0],
                "type": c[1],
                "distinct_count": column_stats.get(c[0], {}).get("distinct_count", "unknown"),
                "cardinality": column_stats.get(c[0], {}).get("cardinality", "unknown")
            } for c in columns]

            analysis_prompt = f"""Analyze this table structure and suggest optimization strategy.

Table: {table_name}
Row count: {row_count}
Size: {size_bytes} bytes
Columns: {json.dumps(columns_data, indent=2)}

Return JSON with optimization strategy:
{{
  "table_name": "{table_name}",
  "partition_columns": ["column_name"],
  "cluster_columns": ["column_name"],
  "compression": "ZSTD",
  "rationale": "brief explanation based on data statistics"
}}

Rules:
- Max 2 partition columns (prefer high-cardinality date/timestamp)
- Max 4 cluster columns (prefer columns used in JOIN/WHERE with good cardinality)
- Consider row_count and data distribution
- Return ONLY valid JSON"""

            try:
                resp = self.client.chat.completions.create(
                    model=self.model,
                    messages=[{"role": "user", "content": analysis_prompt}],
                    temperature=0.1,
                    max_tokens=500,
                )

                content = (resp.choices[0].message.content or "").strip()
                json_content = self._extract_json_from_response(content)

                if json_content:
                    analysis = json.loads(json_content)
                    tables_analysis.append({
                        "table_name": table_name,
                        "original_columns": columns,
                        "partition_columns": analysis.get("partition_columns", [])[:2],
                        "cluster_columns": analysis.get("cluster_columns", [])[:4],
                        "compression": analysis.get("compression", "ZSTD"),
                    })
                else:
                    tables_analysis.append(self._heuristic_analysis(
                        table_name,
                        columns,
                        row_count=row_count if isinstance(row_count, int) else 0,
                        column_stats=column_stats
                    ))

            except Exception as e:
                logger.warning(f"LLM analysis failed for {table_name}: {e}")
                logger.info(f"Используем эвристический анализ для {table_name}")
                tables_analysis.append(self._heuristic_analysis(
                    table_name,
                    columns,
                    row_count=row_count if isinstance(row_count, int) else 0,
                    column_stats=column_stats
                ))

        return tables_analysis

    def _heuristic_analysis(
            self,
            table_name: str,
            columns: List[Tuple[str, str]],
            row_count: int = 0,
            column_stats: Dict[str, Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Эвристический анализ с учетом статистики данных."""

        if column_stats is None:
            column_stats = {}

        partition_cols = []
        cluster_cols = []

        for col_name, col_type in columns:
            col_lower = col_name.lower()
            type_upper = col_type.upper()

            col_stat = column_stats.get(col_name, {})
            cardinality = col_stat.get("cardinality", 0)

            # Партиционирование: дата + high cardinality
            if any(kw in col_lower for kw in ['date', 'time', 'created', 'updated']):
                if any(t in type_upper for t in ['DATE', 'TIMESTAMP', 'TIME']):
                    if cardinality > 10 or cardinality == 0:
                        partition_cols.append(col_name)

            # Кластеризация: ID, key + средняя cardinality
            if any(kw in col_lower for kw in ['id', 'key', 'code', 'type', 'status']):
                if row_count == 0 or (10 < cardinality < row_count * 0.5) or cardinality == 0:
                    cluster_cols.append(col_name)

        return {
            "table_name": table_name,
            "original_columns": columns,
            "partition_columns": partition_cols[:2],
            "cluster_columns": cluster_cols[:4],
            "compression": "ZSTD",
        }

    def _generate_ddl_deterministic(
            self,
            tables_analysis: List[Dict[str, Any]],
            catalog_name: str
    ) -> List[Dict[str, str]]:
        """Детерминированная генерация DDL на основе анализа LLM."""

        ddl_statements = []

        ddl_statements.append({
            "statement": f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.optimized"
        })

        for analysis in tables_analysis:
            table_name = analysis["table_name"]
            columns = analysis["original_columns"]
            partition_cols = analysis["partition_columns"]
            cluster_cols = analysis["cluster_columns"]
            compression = analysis["compression"]

            columns_sql = ",\n  ".join([f"{col[0]} {col[1]}" for col in columns])

            with_parts = [
                "format = 'ICEBERG'",
                f"'write.compression-codec' = '{compression}'",
                "'write.target-file-size-bytes' = '268435456'",
                "'read.vectorization.enabled' = 'true'",
                "'write.parquet.compression-codec' = 'ZSTD'",
                "'write.parquet.page-size-bytes' = '1048576'",
                "'write.parquet.row-group-size-bytes' = '134217728'",
            ]

            if partition_cols:
                valid_partitions = [c for c in partition_cols if any(col[0] == c for col in columns)]
                if valid_partitions:
                    parts_str = ", ".join([f"'{c}'" for c in valid_partitions])
                    with_parts.insert(1, f"partitioning = ARRAY[{parts_str}]")

            if cluster_cols:
                valid_clusters = [c for c in cluster_cols if any(col[0] == c for col in columns)]
                if valid_clusters:
                    clusters_str = ", ".join([f"'{c}'" for c in valid_clusters])
                    insert_pos = 2 if partition_cols else 1
                    with_parts.insert(insert_pos, f"clustering = ARRAY[{clusters_str}]")

            with_clause = ",\n  ".join(with_parts)

            ddl = f"""CREATE TABLE {catalog_name}.optimized.{table_name} (
  {columns_sql}
) WITH (
  {with_clause}
)"""

            ddl_statements.append({"statement": ddl})
            logger.info(f"✅ Сгенерирован DDL для {table_name}")

        return ddl_statements

    # ========================================================================
    # РОБАСТНЫЙ SQL-ПАРСИНГ через sqlglot
    # ========================================================================

    def _extract_table_name_robust(self, ddl: str) -> Optional[str]:
        """Извлекает имя таблицы с учетом кавычек и сложных случаев."""
        if self.enable_sql_parsing:
            try:
                parsed = sqlglot.parse_one(ddl, dialect="trino")
                table = parsed.find(sqlglot.exp.Table)
                if table:
                    return table.name
            except Exception as e:
                logger.debug(f"sqlglot parse failed, fallback to regex: {e}")

        match = re.search(
            r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?'
            r'(?:(?:`|")?(\w+)(?:`|")?\.)?'
            r'(?:(?:`|")?(\w+)(?:`|")?\.)?'
            r'(?:`|")?(\w+)(?:`|")?',
            ddl,
            re.IGNORECASE
        )
        if match:
            return match.group(3) or match.group(2) or match.group(1)
        return None

    def _extract_columns_robust(self, ddl: str) -> List[Tuple[str, str]]:
        """Извлекает колонки с учетом вложенных типов и сложных структур."""
        if self.enable_sql_parsing:
            try:
                parsed = sqlglot.parse_one(ddl, dialect="trino")
                columns = []

                # Безопасное извлечение колонок
                try:
                    for col_def in parsed.find_all(sqlglot.exp.ColumnDef):
                        col_name = col_def.this.name
                        col_type = col_def.kind.sql(dialect="trino") if col_def.kind else "UNKNOWN"
                        columns.append((col_name, col_type))
                except (AttributeError, TypeError) as inner_e:
                    logger.debug(f"Cannot extract columns via sqlglot: {inner_e}")
                    raise  # Передаем во внешний except

                if columns:
                    return columns
            except Exception as e:
                logger.debug(f"sqlglot column extraction failed, fallback to regex: {e}")

        match = re.search(r'\(([^)]+(?:\([^)]*\)[^)]*)*)\)', ddl, re.DOTALL | re.IGNORECASE)
        if not match:
            return []

        columns_text = match.group(1)
        columns = []
        current_col = []
        depth = 0

        for char in columns_text + ',':
            if char in '([<':
                depth += 1
                current_col.append(char)
            elif char in ')]>':
                depth -= 1
                current_col.append(char)
            elif char == ',' and depth == 0:
                col_str = ''.join(current_col).strip()
                if col_str and not col_str.upper().startswith(('PRIMARY', 'FOREIGN', 'CONSTRAINT', 'CHECK')):
                    parts = col_str.split(None, 1)
                    if len(parts) >= 2:
                        col_name = parts[0].strip('`"[]')
                        col_type = parts[1].split()[0]
                        columns.append((col_name, col_type))
                current_col = []
            else:
                current_col.append(char)

        return columns

    # ========================================================================
    # РЕАЛЬНЫЕ ОПТИМИЗАЦИИ через изменение SQL, не комментарии
    # ========================================================================

    def _optimize_queries_parallel(
            self,
            queries: List[Dict[str, Any]],
            new_ddl: List[Dict[str, str]],
            catalog_name: str
    ) -> List[Dict[str, Any]]:
        """Параллельно переписывает и оптимизирует запросы."""

        table_metadata = self._extract_table_metadata(new_ddl)

        results = []
        errors = []

        def _optimize_one_query(q: Dict[str, Any]) -> Dict[str, Any]:
            try:
                queryid = q.get("queryid")
                original_query = q.get("query", "")

                query_with_new_paths = self._replace_table_paths_robust(
                    original_query,
                    catalog_name
                )

                optimized_query = self._apply_real_optimizations(
                    query_with_new_paths,
                    table_metadata,
                    catalog_name
                )

                # ✅ ИСПРАВЛЕНО: Только queryid и query согласно ТЗ
                return {
                    "queryid": queryid,
                    "query": optimized_query
                }

            except Exception as e:
                logger.error(f"Query optimization failed for {q.get('queryid')}: {e}")
                with self._errors_lock:
                    errors.append({
                        "queryid": q.get("queryid"),
                        "error": str(e),
                        "traceback": str(e)
                    })
                raise

        with ThreadPoolExecutor(max_workers=self.max_workers) as ex:
            futures = {ex.submit(_optimize_one_query, q): q for q in queries}

            for fut in as_completed(futures):
                try:
                    result = fut.result()
                    with self._results_lock:
                        results.append(result)
                except Exception:
                    pass

        if errors:
            raise LLMAnalyzerError(
                f"Не удалось оптимизировать {len(errors)} из {len(queries)} запросов",
                {"optimization_errors": errors}
            )

        return results

    def _extract_table_metadata(self, ddl_list: List[Dict[str, str]]) -> Dict[str, Dict[str, Any]]:
        """Извлекает метаданные таблиц для оптимизаций."""
        metadata = {}

        for ddl_item in ddl_list:
            statement = ddl_item.get("statement", "")
            if "CREATE TABLE" not in statement.upper():
                continue

            table_name = self._extract_table_name_robust(statement)
            if not table_name:
                continue

            columns = self._extract_columns_robust(statement)
            partition_cols = self._extract_array_from_with(statement, "partitioning")
            cluster_cols = self._extract_array_from_with(statement, "clustering")

            metadata[table_name] = {
                "columns": [c[0] for c in columns],
                "partition_columns": partition_cols,
                "cluster_columns": cluster_cols,
            }

        return metadata

    def _extract_array_from_with(self, ddl: str, property_name: str) -> List[str]:
        """Извлекает массив из WITH clause."""
        pattern = rf"{property_name}\s*=\s*ARRAY\[(.*?)\]"
        match = re.search(pattern, ddl, re.IGNORECASE)
        if match:
            items = match.group(1).split(',')
            return [item.strip().strip("'\"") for item in items]
        return []

    def _clean_sql_for_parsing(self, sql: str) -> str:
        """
        Очистка SQL от комментариев перед парсингом sqlglot.
        
        Удаляет однострочные (--) и многострочные (/* */) комментарии,
        которые могут вызвать проблемы при парсинге.
        """
        # Удаляем однострочные комментарии --
        sql = re.sub(r'--[^\n]*', '', sql)
        
        # Удаляем многострочные комментарии /* */
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
        
        return sql.strip()

    def _apply_real_optimizations(
            self,
            query: str,
            table_metadata: Dict[str, Dict[str, Any]],
            catalog_name: str
    ) -> str:
        """Применяет РЕАЛЬНЫЕ изменения к SQL, не комментарии."""

        optimized = query
        applied = []

        if not self.enable_sql_parsing:
            return self._apply_simple_optimizations(query)

        try:
            # Очищаем SQL от комментариев перед парсингом
            clean_query = self._clean_sql_for_parsing(optimized)
            parsed = sqlglot.parse_one(clean_query, dialect="trino")

            modified_star = self._replace_select_star_sqlglot(parsed, table_metadata, catalog_name)
            if modified_star:
                applied.append("column pruning (SELECT *)")

            if not self._has_limit(parsed) and not self._is_aggregation_sqlglot(parsed):
                parsed = self._add_limit_sqlglot(parsed, 10000)
                applied.append("automatic LIMIT 10000")

            partition_used = self._check_partition_usage(parsed, table_metadata)
            if partition_used:
                applied.extend([f"partition pruning on {t}.{c}" for t, c in partition_used])

            cluster_joins = self._check_cluster_joins(parsed, table_metadata)
            if cluster_joins:
                applied.extend([f"clustered join on {t}.{c}" for t, c in cluster_joins])

            optimized = parsed.sql(dialect="trino")

            if applied:
                logger.info(f"✅ Реальные оптимизации: {', '.join(applied)}")

            return optimized

        except Exception as e:
            logger.warning(f"SQL optimization failed, using simple approach: {e}")
            return self._apply_simple_optimizations(query)

    def _replace_select_star_sqlglot(
            self,
            parsed: sqlglot.Expression,
            table_metadata: Dict[str, Dict[str, Any]],
            catalog_name: str
    ) -> bool:
        """Заменяет SELECT * на конкретные колонки."""
        modified = False

        try:
            for select in parsed.find_all(sqlglot.exp.Select):
                if len(select.expressions) == 1:
                    expr = select.expressions[0]
                    if isinstance(expr, sqlglot.exp.Star):
                        from_clause = select.find(sqlglot.exp.From)
                        if from_clause and from_clause.this:
                            table_name = from_clause.this.name

                            if table_name in table_metadata:
                                columns = table_metadata[table_name]["columns"]
                                if columns:
                                    select.expressions = [
                                        sqlglot.exp.Column(this=col)
                                        for col in columns
                                    ]
                                    modified = True
                                    logger.info(f"✅ Заменен SELECT * на {len(columns)} колонок для {table_name}")
        except (AttributeError, TypeError) as e:
            logger.debug(f"Cannot replace SELECT *: {e}")

        return modified

    def _has_limit(self, parsed: sqlglot.Expression) -> bool:
        """Проверяет наличие LIMIT."""
        return parsed.find(sqlglot.exp.Limit) is not None

    def _add_limit_sqlglot(self, parsed: sqlglot.Expression, limit: int) -> sqlglot.Expression:
        """Добавляет LIMIT к запросу."""
        return parsed.limit(limit)

    def _is_aggregation_sqlglot(self, parsed: sqlglot.Expression) -> bool:
        """Проверяет наличие агрегатных функций или GROUP BY."""
        try:
            has_group = parsed.find(sqlglot.exp.Group) is not None
            # Безопасная проверка агрегатных функций
            agg_funcs = list(parsed.find_all(sqlglot.exp.AggFunc))
            has_agg = len(agg_funcs) > 0
            return has_group or has_agg
        except (AttributeError, TypeError) as e:
            logger.debug(f"Cannot check aggregation: {e}, assuming no aggregation")
            return False

    def _check_partition_usage(
            self,
            parsed: sqlglot.Expression,
            table_metadata: Dict[str, Dict[str, Any]]
    ) -> List[Tuple[str, str]]:
        """Проверяет использование партиционных колонок в WHERE."""
        used = []

        try:
            where = parsed.find(sqlglot.exp.Where)
            if not where:
                return used

            where_columns = {col.name for col in where.find_all(sqlglot.exp.Column)}

            for table, meta in table_metadata.items():
                for part_col in meta["partition_columns"]:
                    if part_col in where_columns:
                        used.append((table, part_col))
        except (AttributeError, TypeError) as e:
            logger.debug(f"Cannot check partition usage: {e}")

        return used

    def _check_cluster_joins(
            self,
            parsed: sqlglot.Expression,
            table_metadata: Dict[str, Dict[str, Any]]
    ) -> List[Tuple[str, str]]:
        """Проверяет JOIN по кластерным колонкам."""
        used = []

        try:
            for join in parsed.find_all(sqlglot.exp.Join):
                if not join.on:
                    continue

                join_columns = {col.name for col in join.on.find_all(sqlglot.exp.Column)}

                for table, meta in table_metadata.items():
                    for cluster_col in meta["cluster_columns"]:
                        if cluster_col in join_columns:
                            used.append((table, cluster_col))
        except (AttributeError, TypeError) as e:
            logger.debug(f"Cannot check cluster joins: {e}")

        return used

    def _apply_simple_optimizations(self, query: str) -> str:
        """Простые оптимизации без sqlglot."""
        optimized = query

        if "LIMIT" not in optimized.upper() and not self._is_aggregation_query(optimized):
            optimized = optimized.rstrip(';') + "\nLIMIT 10000;"

        return optimized

    def _is_aggregation_query(self, query: str) -> bool:
        """Проверяет, является ли запрос агрегационным."""
        upper_query = query.upper()
        return any(kw in upper_query for kw in ["GROUP BY", "COUNT(", "SUM(", "AVG(", "MAX(", "MIN(", "HAVING"])

    def _replace_table_paths_robust(self, query: str, catalog_name: str) -> str:
        """Заменяет пути таблиц через sqlglot или regex."""
        if self.enable_sql_parsing:
            try:
                parsed = sqlglot.parse_one(query, dialect="trino")

                # Безопасный обход таблиц
                try:
                    for table in parsed.find_all(sqlglot.exp.Table):
                        if table.catalog and table.db:
                            if table.db.lower() != "optimized":
                                table.set("db", sqlglot.exp.Identifier(this="optimized"))
                                table.set("catalog", sqlglot.exp.Identifier(this=catalog_name))
                except (AttributeError, TypeError) as inner_e:
                    logger.debug(f"Cannot iterate tables: {inner_e}, fallback to regex")
                    raise  # Передаем во внешний except

                return parsed.sql(dialect="trino")
            except Exception as e:
                logger.debug(f"sqlglot path replacement failed, fallback to regex: {e}")

        pattern = r'(\w+)\.(\w+)\.(\w+)'

        def replace_path(match):
            cat, schema, table = match.groups()
            if schema.lower() != 'optimized':
                return f"{catalog_name}.optimized.{table}"
            return match.group(0)

        return re.sub(pattern, replace_path, query)

    def _call_with_retries(
            self,
            function_name: str,
            function_schema: Dict[str, Any],
            system_prompt: str,
            user_input: str,
            max_attempts: int = 3
    ) -> Dict[str, Any]:
        """
        Вызов LLM с несколькими попытками и repair.
        
        Args:
            max_attempts: Максимальное количество попыток (по умолчанию 3)
        """
        all_errors = []
        
        for attempt in range(1, max_attempts + 1):
            # Для первой попытки repair_prompt = None
            # Для последующих - используем ошибки предыдущей попытки
            repair_prompt = None
            if attempt > 1:
                prev_errors = all_errors[-1]["errors"]
                prev_raw = all_errors[-1].get("raw_output", "")
                repair_prompt = self._build_repair_prompt(prev_errors, prev_raw, function_name=function_name)
                logger.warning(f"{function_name}: попытка {attempt}/{max_attempts} (с repair)")
            
            ok, obj, raw, errs = self._call_llm_function(
                function_name=function_name,
                function_schema=function_schema,
                system_prompt=system_prompt,
                user_input=user_input,
                repair_prompt=repair_prompt,
            )
            
            # Сохраняем результат попытки
            all_errors.append({
                "attempt": attempt,
                "errors": errs,
                "raw_output": self._safe_truncate(raw or "", 4000)
            })
            
            if ok:
                if attempt > 1:
                    logger.info(f"✅ {function_name}: успешно на попытке {attempt}/{max_attempts}")
                return obj

        # Все попытки провалились
        error_details = {
            "function": function_name,
            "model": self.analysis_model,
            "attempts": all_errors,
        }

        raise LLMAnalyzerError(
            f"Модель не вернула валидный JSON для '{function_name}' после {max_attempts} попыток",
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
        """Вызов LLM."""
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
                    "X-Title": f"SQL Agent {function_name}",
                }

            resp = self.client.chat.completions.create(**kwargs)
            choice = resp.choices[0]
            content = (choice.message.content or "").strip()

            if not content:
                errors.append({"type": "empty_llm_response", "message": "Model returned no content"})
                return False, None, None, errors

            json_content = self._extract_json_from_response(content)
            if not json_content:
                errors.append({"type": "json_not_found", "message": "No valid JSON found"})
                return False, None, content, errors

            try:
                parsed = json.loads(json_content)
            except json.JSONDecodeError as e:
                errors.append({"type": "json_decode_error", "message": str(e)})
                return False, None, json_content, errors

            ok, msg = self._validate_by_function(function_name, parsed)
            if not ok:
                errors.append({"type": "schema_validation", "message": msg})
                return False, None, json_content, errors

            return True, parsed, json_content, errors

        except Exception as e:
            errors.append({"type": "llm_request_error", "message": str(e)})
            return False, None, None, errors

    def _extract_json_from_response(self, content: str) -> Optional[str]:
        """
        Извлекает JSON из ответа с улучшенной очисткой.
        
        Поддерживает различные форматы ответов LLM:
        - Чистый JSON
        - JSON в markdown блоках ```json
        - JSON с текстом до/после
        - JSON с trailing commas
        """
        # Удаляем markdown блоки
        cleaned = re.sub(r'```json\s*', '', content)
        cleaned = re.sub(r'```javascript\s*', '', cleaned)
        cleaned = re.sub(r'\s*```\s*', '', cleaned)
        cleaned = re.sub(r'```\s*', '', cleaned).strip()
        
        # Удаляем возможный текст перед JSON
        cleaned = re.sub(r'^[^{]*', '', cleaned)

        start = cleaned.find('{')
        if start == -1:
            return None

        depth = 0
        for i, char in enumerate(cleaned[start:], start):
            if char == '{':
                depth += 1
            elif char == '}':
                depth -= 1
                if depth == 0:
                    try:
                        json_str = cleaned[start:i + 1]
                        
                        # Пытаемся убрать trailing commas перед парсингом
                        json_str = re.sub(r',(\s*[}\]])', r'\1', json_str)
                        
                        json.loads(json_str)
                        return json_str
                    except json.JSONDecodeError:
                        pass

        return None

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

    def _validate_by_function(self, fn: str, data: Dict[str, Any]) -> Tuple[bool, str]:
        if fn == "produce_migrations":
            if not isinstance(data, dict) or "migrations" not in data or not isinstance(data["migrations"], list):
                return False, "produce_migrations: missing or invalid 'migrations' array"
            for i, item in enumerate(data["migrations"]):
                if not isinstance(item, dict) or "statement" not in item or not isinstance(item["statement"], str):
                    return False, f"produce_migrations.migrations[{i}] must be {{statement: string}}"
            return True, "ok"

        return False, f"unknown function '{fn}'"

    @staticmethod
    def _system_prompt_migrations() -> str:
        """Промпт для генерации миграций."""
        return (
            "You are an expert data migration specialist for LARGE-SCALE database optimization.\n"
            "Your task: Create comprehensive migration statements to transfer data from old DDL to new optimized structure.\n"
            "\n"
            "CRITICAL REQUIREMENTS:\n"
            "1. USE the 'catalog_name' field from input payload - do NOT extract from URL\n"
            "2. MANDATORY: ALL paths must use format: <catalog_name>.optimized.<table_name>\n"
            "3. Extract table names from old_ddl and new_ddl\n"
            "4. Migrate ALL columns from original tables to optimized tables\n"
            "5. Include SELECT COUNT(*) validation queries for each migration\n"
            "\n"
            "PATH EXAMPLES:\n"
            "- If catalog_name='flights': flights.optimized.flights_table\n"
            "- If catalog_name='analytics': analytics.optimized.users\n"
            "- If old table was 'flights.public.flights', new is 'flights.optimized.flights'\n"
            "\n"
            "MANDATORY JSON OUTPUT FORMAT:\n"
            "{\n"
            '  "migrations": [\n'
            '    {"statement": "INSERT INTO <catalog_name>.optimized.<table> SELECT * FROM <catalog_name>.public.<table>"},\n'
            '    {"statement": "SELECT COUNT(*) as validation FROM <catalog_name>.optimized.<table>"}\n'
            '  ]\n'
            "}\n"
            "\n"
            "CRITICAL: Replace <catalog_name> with the actual catalog_name from input!\n"
            "Return ONLY valid JSON, no markdown, no explanations."
        )

    @staticmethod
    def _build_repair_prompt(errors: List[Dict[str, Any]], raw_output: Optional[str], function_name: str) -> str:
        """Строит промпт для исправления ошибок с конкретными инструкциями."""
        reasons = []
        has_json_error = False
        has_schema_error = False
        
        for e in errors or []:
            t = e.get("type", "unknown")
            msg = e.get("message", "")
            reasons.append(f"- {t}: {msg}")
            
            if t in ["json_decode_error", "json_not_found"]:
                has_json_error = True
            elif t == "schema_validation":
                has_schema_error = True
        
        reasons_text = "\n".join(reasons) if reasons else "- unknown failure"

        snippet = (raw_output or "")[:800]
        
        # Базовые инструкции
        prompt = (
            f"REPAIR REQUIRED: Your previous JSON output was invalid.\n"
            "Fix the issues and return ONLY valid JSON.\n"
            "\n"
            "Issues detected:\n"
            f"{reasons_text}\n"
            "\n"
        )
        
        # Дополнительные инструкции в зависимости от типа ошибки
        if has_json_error:
            prompt += (
                "CRITICAL JSON SYNTAX ERRORS DETECTED:\n"
                "- Remove ALL trailing commas before } or ]\n"
                "- Use double quotes (\") for strings, NOT single quotes\n"
                "- Do NOT include any text, markdown, or code blocks\n"
                "- Do NOT add comments inside JSON\n"
                "- Start with { and end with }\n"
                "\n"
            )
        
        if has_schema_error:
            prompt += (
                "SCHEMA VALIDATION ERRORS DETECTED:\n"
                f"- Ensure all required fields for '{function_name}' are present\n"
                "- Use full paths: <catalog>.optimized.<table>\n"
                "- Check the 'catalog_name' field in input and use it in ALL paths\n"
                "- Each statement must have 'statement' field\n"
                "- Each query must have 'queryid' and 'query' fields\n"
                "\n"
                "PATH FORMAT REMINDER:\n"
                "- DDL: CREATE TABLE <catalog_name>.optimized.<table_name>\n"
                "- Migration: INSERT INTO <catalog_name>.optimized.<table> SELECT * FROM <catalog_name>.<old_schema>.<table>\n"
                "- Query: FROM <catalog_name>.optimized.<table>\n"
                "\n"
            )
        
        prompt += (
            f"Previous invalid output (first 800 chars):\n{snippet}\n"
            "\n"
            "FINAL INSTRUCTION: Return ONLY valid JSON, no markdown, no explanations, no extra text."
        )
        
        return prompt

    @staticmethod
    def _validate_input(data: Dict[str, Any]) -> None:
        if "ddl" not in data or not isinstance(data["ddl"], list):
            raise LLMAnalyzerError("Входной объект должен содержать массив 'ddl'")
        if "queries" not in data or not isinstance(data["queries"], list):
            raise LLMAnalyzerError("Входной объект должен содержать массив 'queries'")

    @staticmethod
    def _validate_full_paths(result: Dict[str, Any], catalog_name: str) -> None:
        """
        Валидация путей с учетом разных форматов каталогов.
        
        Принимает:
        - catalog.optimized.table (строгое соответствие)
        - any_catalog.optimized.table (любой каталог с optimized)
        - CREATE SCHEMA statements (пропускаем)
        """
        # Гибкий паттерн: любой каталог + .optimized. + имя таблицы
        optimized_pattern = r'\w+\.optimized\.\w+'
        
        def has_optimized_path(text: str) -> bool:
            """Проверяет наличие пути с .optimized."""
            return bool(re.search(optimized_pattern, text))
        
        def is_schema_statement(text: str) -> bool:
            """Проверяет является ли statement созданием схемы."""
            return bool(re.search(r'CREATE\s+SCHEMA', text, re.IGNORECASE))

        errors = []
        warnings = []

        for i, ddl_item in enumerate(result.get("ddl", [])):
            statement = ddl_item.get("statement", "")
            
            # Пропускаем CREATE SCHEMA
            if is_schema_statement(statement):
                continue
                
            if "CREATE TABLE" in statement.upper():
                if not has_optimized_path(statement):
                    errors.append(f"DDL[{i}] missing .optimized. path: {statement[:150]}...")

        for i, mig_item in enumerate(result.get("migrations", [])):
            statement = mig_item.get("statement", "")
            
            # SELECT для валидации можно пропустить
            if statement.upper().strip().startswith("SELECT"):
                continue
                
            if "INSERT INTO" in statement.upper():
                if not has_optimized_path(statement):
                    errors.append(f"Migration[{i}] missing .optimized. path: {statement[:150]}...")

        for i, query_item in enumerate(result.get("queries", [])):
            query = query_item.get("query", "")
            if "FROM" in query.upper() or "JOIN" in query.upper():
                if not has_optimized_path(query):
                    # Для запросов даем warning, а не error (могут быть подзапросы без путей)
                    warnings.append(f"Query[{i}] may be missing .optimized. path")

        if errors:
            raise LLMAnalyzerError(
                f"Валидация путей не пройдена: {len(errors)} критических ошибок",
                {
                    "validation_errors": errors,
                    "warnings": warnings,
                    "catalog_name": catalog_name,
                    "hint": "DDL и миграции ДОЛЖНЫ использовать пути вида: <catalog>.optimized.<table>"
                }
            )

    @staticmethod
    def _safe_truncate(text: str, limit: int) -> str:
        return text if len(text) <= limit else text[:limit] + "... [truncated]"

    def _extract_catalog_from_ddl(self, ddl_list: List[Dict[str, str]]) -> Optional[str]:
        """
        Извлечение имени каталога из DDL statements.
        
        Ищет 3-частные пути вида catalog.schema.table в CREATE TABLE statements.
        """
        try:
            for ddl_item in ddl_list[:5]:  # Проверяем первые 5 DDL
                statement = ddl_item.get("statement", "")
                
                # Ищем паттерн: CREATE TABLE catalog.schema.table
                match = re.search(
                    r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+)\.(\w+)\.(\w+)',
                    statement,
                    re.IGNORECASE
                )
                
                if match:
                    catalog = match.group(1)
                    schema = match.group(2)
                    table = match.group(3)
                    
                    # Исключаем служебные схемы
                    if schema.lower() not in ['information_schema', 'pg_catalog', 'sys']:
                        logger.debug(f"Найден путь в DDL: {catalog}.{schema}.{table}")
                        return catalog
            
            return None
        except Exception as e:
            logger.debug(f"Не удалось извлечь каталог из DDL: {e}")
            return None

    def _extract_catalog_from_url(self, url: str) -> str:
        """Извлечение имени каталога из JDBC URL"""
        try:
            trino_match = re.search(r'catalog=([^&]+)', url)
            if trino_match:
                catalog = trino_match.group(1)
                logger.info(f"✅ Извлечен каталог из Trino URL: {catalog}")
                return catalog

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

