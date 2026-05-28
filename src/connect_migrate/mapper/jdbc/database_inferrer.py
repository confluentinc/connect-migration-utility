"""Detects the database family of a JDBC connector config."""

import logging
from typing import Any, Dict, List, Optional


JDBC_DATABASE_TYPES: Dict[str, Dict[str, Any]] = {
    "mysql": {"url_patterns": ["mysql", "mariadb"], "default_port": "3306"},
    "oracle": {"url_patterns": ["oracle", "oracle:thin"], "default_port": "1521"},
    "sqlserver": {"url_patterns": ["sqlserver", "mssql"], "default_port": "1433"},
    "postgresql": {"url_patterns": ["postgresql", "postgres"], "default_port": "5432"},
    "snowflake": {"url_patterns": ["snowflake"], "default_port": "443"},
}


class DatabaseInferrer:
    def __init__(
        self,
        url_parser: "JdbcUrlParser",  # noqa: F821 — kept for backward-compatible signature
        logger: Optional[logging.Logger] = None,
    ):
        self._url_parser = url_parser
        self.logger = logger or logging.getLogger(__name__)

    def infer_database_type(self, config: Dict[str, Any]) -> str:
        """Return one of ``mysql|postgresql|oracle|sqlserver|snowflake|unknown``."""
        if "connection.url" in config and isinstance(config["connection.url"], str):
            url = config["connection.url"].lower()
            self.logger.info(f"Analyzing JDBC URL for database type: {url}")

            for db_type, info in JDBC_DATABASE_TYPES.items():
                for pattern in info["url_patterns"]:
                    if f"jdbc:{pattern}://" in url:
                        self.logger.info(
                            f"Detected database type '{db_type}' using precise pattern 'jdbc:{pattern}://'"
                        )
                        return db_type

            for db_type, info in JDBC_DATABASE_TYPES.items():
                if any(pattern in url for pattern in info["url_patterns"]):
                    self.logger.info(
                        f"Detected database type '{db_type}' using fallback pattern matching"
                    )
                    return db_type

            self.logger.warning(f"No database type detected for URL: {url}")

        if "database.type" in config:
            db_type = config["database.type"].lower()
            self.logger.info(f"Using database type from config: {db_type}")
            return db_type

        self.logger.warning("No database type detected, returning 'unknown'")
        return "unknown"
