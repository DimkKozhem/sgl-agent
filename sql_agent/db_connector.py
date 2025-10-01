"""
Модуль для подключения к базе данных и получения статистики.

Поддерживает подключение к различным типам БД (Trino, PostgreSQL, MySQL)
и предоставляет методы для получения статистики по таблицам и колонкам.
"""

import logging
from typing import Dict, Any
from urllib.parse import urlparse, parse_qs

logger = logging.getLogger(__name__)


class DatabaseConnector:
    """
    Подключение к базе данных для анализа данных.

    Автоматически определяет тип БД по JDBC URL и устанавливает
    соответствующее подключение для получения статистики.
    """

    def __init__(self, jdbc_url: str):
        """
        Инициализация коннектора к базе данных.

        Args:
            jdbc_url: JDBC URL для подключения к БД
        """
        self.jdbc_url = jdbc_url
        self.connection = None
        self.db_type = self._detect_db_type(jdbc_url)

    def _detect_db_type(self, url: str) -> str:
        """
        Определяет тип базы данных из JDBC URL.

        Args:
            url: JDBC URL для анализа

        Returns:
            str: Тип БД (trino, postgresql, mysql, unknown)
        """
        if "trino://" in url or "presto://" in url:
            return "trino"
        elif "postgresql://" in url:
            return "postgresql"
        elif "mysql://" in url:
            return "mysql"
        return "unknown"

    def connect(self) -> bool:
        """Устанавливает подключение к БД"""
        try:
            if self.db_type == "trino":
                try:
                    import trino
                except ImportError:
                    logger.warning("⚠️ Модуль trino не установлен. Установите: pip install trino")
                    return False

                parsed = self._parse_jdbc_url(self.jdbc_url)
                self.connection = trino.dbapi.connect(
                    host=parsed['host'],
                    port=parsed['port'],
                    user=parsed['user'],
                    catalog=parsed['catalog'],
                    schema=parsed.get('schema', 'default')
                )
                logger.info(f"✅ Подключено к Trino: {parsed['host']}")
                return True

            elif self.db_type == "postgresql":
                try:
                    import psycopg2
                except ImportError:
                    logger.warning("⚠️ Модуль psycopg2 не установлен. Установите: pip install psycopg2-binary")
                    return False

                parsed = self._parse_jdbc_url(self.jdbc_url)
                self.connection = psycopg2.connect(
                    host=parsed['host'],
                    port=parsed['port'],
                    user=parsed['user'],
                    password=parsed.get('password', ''),
                    database=parsed['catalog']
                )
                logger.info(f"✅ Подключено к PostgreSQL: {parsed['host']}")
                return True

            else:
                logger.warning(f"⚠️ Неподдерживаемый тип БД: {self.db_type}")
                return False

        except Exception as e:
            logger.error(f"❌ Ошибка подключения к БД: {e}")
            return False

    def _parse_jdbc_url(self, jdbc_url: str) -> Dict[str, Any]:
        """Парсит JDBC URL"""
        url = jdbc_url.replace("jdbc:", "")
        parsed = urlparse(url)
        params = parse_qs(parsed.query)

        result = {
            'host': parsed.hostname or 'localhost',
            'port': parsed.port or 8080,
            'catalog': params.get('catalog', [parsed.path.strip('/')])[0] if params.get('catalog') else parsed.path.strip('/'),
            'user': params.get('user', [''])[0],
            'password': params.get('password', [''])[0],
        }

        if 'schema' in params:
            result['schema'] = params['schema'][0]

        return result

    def get_table_stats(self, table_name: str, schema: str = None) -> Dict[str, Any]:
        """Получает статистику по таблице"""
        if not self.connection:
            logger.warning("⚠️ Нет подключения к БД")
            return {}

        try:
            cursor = self.connection.cursor()

            full_table = f"{schema}.{table_name}" if schema else table_name

            cursor.execute(f"SELECT COUNT(*) FROM {full_table}")
            row_count = cursor.fetchone()[0]

            if self.db_type == "trino":
                try:
                    cursor.execute(f"""
                        SELECT
                            SUM(compressed_size) as total_size,
                            COUNT(DISTINCT partition_key) as partition_count
                        FROM "{schema}$partitions"
                        WHERE table_name = '{table_name}'
                    """)
                    size_info = cursor.fetchone()
                    total_size = size_info[0] if size_info else 0
                    partition_count = size_info[1] if size_info else 0
                except Exception:
                    total_size = 0
                    partition_count = 0
            else:
                total_size = 0
                partition_count = 0

            cursor.close()

            stats = {
                "row_count": row_count,
                "total_size_bytes": total_size,
                "partition_count": partition_count,
                "avg_row_size": total_size / row_count if row_count > 0 else 0
            }

            logger.info(f"📊 Статистика {full_table}: {row_count} строк")
            return stats

        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики для {table_name}: {e}")
            return {}

    def get_column_stats(self, table_name: str, schema: str = None) -> Dict[str, Dict[str, Any]]:
        """Получает статистику по колонкам"""
        if not self.connection:
            return {}

        try:
            cursor = self.connection.cursor()
            full_table = f"{schema}.{table_name}" if schema else table_name

            cursor.execute(f"DESCRIBE {full_table}")
            columns = [row[0] for row in cursor.fetchall()]

            stats = {}
            for column in columns:
                try:
                    cursor.execute(f"""
                        SELECT
                            COUNT(DISTINCT {column}) as distinct_count,
                            COUNT(*) - COUNT({column}) as null_count
                        FROM {full_table}
                    """)
                    result = cursor.fetchone()

                    stats[column] = {
                        "distinct_count": result[0],
                        "null_count": result[1],
                        "cardinality": result[0]
                    }
                except Exception as e:
                    logger.debug(f"Не удалось получить статистику для колонки {column}: {e}")
                    stats[column] = {
                        "distinct_count": 0,
                        "null_count": 0,
                        "cardinality": 0
                    }

            cursor.close()
            return stats

        except Exception as e:
            logger.error(f"❌ Ошибка получения статистики колонок: {e}")
            return {}

    def close(self):
        """Закрывает подключение"""
        if self.connection:
            try:
                self.connection.close()
                logger.info("🔌 Подключение к БД закрыто")
            except Exception as e:
                logger.warning(f"⚠️ Ошибка при закрытии подключения: {e}")
