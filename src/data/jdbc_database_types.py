"""URL patterns and default ports for the JDBC database types recognized by the comparator."""

JDBC_DATABASE_TYPES = {
    "mysql": {
        "url_patterns": ["mysql", "mariadb"],
        "default_port": "3306",
    },
    "oracle": {
        "url_patterns": ["oracle", "oracle:thin"],
        "default_port": "1521",
    },
    "sqlserver": {
        "url_patterns": ["sqlserver", "mssql"],
        "default_port": "1433",
    },
    "postgresql": {
        "url_patterns": ["postgresql", "postgres"],
        "default_port": "5432",
    },
    "snowflake": {
        "url_patterns": ["snowflake"],
        "default_port": "443",
    },
}