"""Detects the database family of a JDBC connector config."""

import logging
from typing import Any, Dict, Optional

from connect_migrate.mapper.jdbc.url_parser import JdbcUrlParser


# Each entry: which substrings to look for in the JDBC URL (after ``jdbc:``)
# and the default port for that database family. ``property_mappings`` is
# preserved as an extension hook (currently always empty).
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
        url_parser: JdbcUrlParser,
        logger: Optional[logging.Logger] = None,
    ):
        self._url_parser = url_parser
        self.logger = logger or logging.getLogger(__name__)

    def infer_database_type(self, config: Dict[str, Any]) -> str:
        """Return one of ``mysql|postgresql|oracle|sqlserver|snowflake|unknown``."""
        if "connection.url" in config and isinstance(config["connection.url"], str):
            url = config["connection.url"].lower()
            self.logger.info(f"Analyzing JDBC URL for database type: {url}")

            # Precise pattern: jdbc:<db>://
            for db_type, info in JDBC_DATABASE_TYPES.items():
                for pattern in info["url_patterns"]:
                    if f"jdbc:{pattern}://" in url:
                        self.logger.info(
                            f"Detected database type '{db_type}' using precise pattern 'jdbc:{pattern}://'"
                        )
                        return db_type

            # Fallback: any pattern anywhere
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

    def map_jdbc_properties(
        self,
        config: Dict[str, Any],
        db_type: str,
    ) -> Dict[str, Any]:
        """Map JDBC URL components to database-specific config keys.

        The mapping is driven by the ``property_mappings`` table on the
        :data:`JDBC_DATABASE_TYPES` entry for ``db_type`` (currently empty —
        this returns ``{}`` until ``property_mappings`` is populated).
        """
        self.logger.debug(f"Mapping JDBC properties for config: {config}")

        db_info = JDBC_DATABASE_TYPES.get(db_type, {})
        property_mappings = db_info.get("property_mappings", {})
        self.logger.debug(f"Property mappings for {db_type}: {property_mappings}")

        url = config.get("connection.url")
        if not (isinstance(url, str) and url.startswith("jdbc:")):
            return {}

        connection_info = self._url_parser.parse_jdbc_url(url)

        mapped_config: Dict[str, Any] = {}
        for fm_prop, jdbc_prop in property_mappings.items():
            if jdbc_prop in connection_info:
                mapped_config[fm_prop] = connection_info[jdbc_prop]
                self.logger.debug(
                    f"Mapped {jdbc_prop} ({connection_info[jdbc_prop]}) to {fm_prop}"
                )
            else:
                self.logger.debug(f"JDBC property {jdbc_prop} not found in connection_info")

        self.logger.debug(f"Final JDBC mapped config: {mapped_config}")
        return mapped_config
