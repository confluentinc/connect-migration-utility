"""Parses JDBC and MongoDB connection URLs into structured connection info."""

import logging
import re
from typing import Dict, Optional


class JdbcUrlParser:
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)

    def parse_jdbc_url(self, url: str) -> Dict[str, str]:
        """Parse a JDBC URL (including Oracle's complex DESCRIPTION format).

        Returns a dict with any of: ``host``, ``port``, ``db.name``, ``user``,
        ``password``, ``db.connection.type``, ``ssl.server.cert.dn``.
        """
        original_url = url
        url = url.lower()
        connection_info: Dict[str, str] = {}

        self.logger.debug(f"Parsing JDBC URL: {original_url}")

        if "@(DESCRIPTION=" in original_url:
            self.logger.debug("Detected Oracle complex format with DESCRIPTION block.")
            host_match = re.search(r"\(HOST=([^\)]+)\)", original_url, re.IGNORECASE)
            if host_match:
                connection_info["host"] = host_match.group(1)
                self.logger.debug(f"[ORACLE] Extracted host: {connection_info['host']}")
            port_match = re.search(r"\(PORT=([^\)]+)\)", original_url, re.IGNORECASE)
            if port_match:
                connection_info["port"] = port_match.group(1)
                self.logger.debug(f"[ORACLE] Extracted port: {connection_info['port']}")
            dbtype_dbname_match = re.search(
                r"\(CONNECT_DATA=\(([^=\)]+)=([^\)]+)\)\)",
                original_url,
                re.IGNORECASE,
            )
            if dbtype_dbname_match:
                connection_info["db.connection.type"] = dbtype_dbname_match.group(1)
                connection_info["db.name"] = dbtype_dbname_match.group(2)
                self.logger.debug(
                    f"[ORACLE] Extracted db.connection.type: {connection_info['db.connection.type']}"
                )
                self.logger.debug(f"[ORACLE] Extracted db.name: {connection_info['db.name']}")
            ssl_cert_dn_match = re.search(
                r'SSL_SERVER_CERT_DN=\\?"?([^\)\"]+)\\?"?\)',
                original_url,
                re.IGNORECASE,
            )
            if ssl_cert_dn_match:
                connection_info["ssl.server.cert.dn"] = ssl_cert_dn_match.group(1)
                self.logger.debug(
                    f"[ORACLE] Extracted ssl.server.cert.dn: {connection_info['ssl.server.cert.dn']}"
                )
            self.logger.debug(f"[ORACLE] Final connection_info: {connection_info}")
            return connection_info

        self.logger.debug("Using standard JDBC URL parsing logic.")

        host_match = re.search(r"jdbc:[^:]+://([^:/]+)", url)
        if host_match:
            connection_info["host"] = host_match.group(1)
            self.logger.debug(f"Extracted host: {connection_info['host']}")

        port_match = re.search(r"://[^:]+:(\d+)", url)
        if port_match:
            connection_info["port"] = port_match.group(1)
            self.logger.debug(f"Extracted port: {connection_info['port']}")

        db_match = re.search(r"://[^/]+/([^/?]+)", url)
        if db_match:
            connection_info["db.name"] = db_match.group(1)
            self.logger.debug(f"Extracted db_name: {connection_info['db.name']}")

        user_match = re.search(r"[?&]user=([^&]+)", url)
        if user_match:
            connection_info["user"] = user_match.group(1)
            self.logger.debug(f"Extracted user: {connection_info['user']}")

        password_match = re.search(r"[?&]password=([^&]+)", url)
        if password_match:
            connection_info["password"] = password_match.group(1)
            self.logger.debug(f"Extracted password: {connection_info['password']}")

        self.logger.debug(f"Final connection_info: {connection_info}")
        return connection_info

    def parse_mongodb_url(self, url: str) -> Dict[str, str]:
        """Parse a MongoDB connection string (``mongodb://`` or ``mongodb+srv://``)."""
        original_url = url
        url = url.lower()
        connection_info: Dict[str, str] = {}

        self.logger.debug(f"Parsing MongoDB connection string: {original_url}")

        if "mongodb+srv://" in url:
            srv_part = url.replace("mongodb+srv://", "")
            if "@" in srv_part:
                credentials_part, host_part = srv_part.split("@", 1)
                if ":" in credentials_part:
                    user, password = credentials_part.split(":", 1)
                    connection_info["user"] = user
                    connection_info["password"] = password
                    self.logger.debug(f"Extracted user: {connection_info['user']}")
                    self.logger.debug(f"Extracted password: {connection_info['password']}")
                host = host_part.split("/")[0].split("?")[0]
                connection_info["host"] = host
                self.logger.debug(f"Extracted host: {connection_info['host']}")
                if "/" in host_part:
                    db_part = host_part.split("/", 1)[1]
                    db_name = db_part.split("?")[0] if "?" in db_part else db_part
                    connection_info["database"] = db_name
                    self.logger.debug(f"Extracted database: {connection_info['database']}")
        elif "mongodb://" in url:
            mongo_part = url.replace("mongodb://", "")
            if "@" in mongo_part:
                credentials_part, host_part = mongo_part.split("@", 1)
                if ":" in credentials_part:
                    user, password = credentials_part.split(":", 1)
                    connection_info["user"] = user
                    connection_info["password"] = password
                    self.logger.debug(f"Extracted user: {connection_info['user']}")
                    self.logger.debug(f"Extracted password: {connection_info['password']}")
                host = host_part.split("/")[0].split("?")[0]
                connection_info["host"] = host
                self.logger.debug(f"Extracted host: {connection_info['host']}")
                if "/" in host_part:
                    db_part = host_part.split("/", 1)[1]
                    db_name = db_part.split("?")[0] if "?" in db_part else db_part
                    connection_info["database"] = db_name
                    self.logger.debug(f"Extracted database: {connection_info['database']}")
            else:
                host = mongo_part.split("/")[0].split("?")[0]
                connection_info["host"] = host
                self.logger.debug(f"Extracted host: {connection_info['host']}")
                if "/" in mongo_part:
                    db_part = mongo_part.split("/", 1)[1]
                    db_name = db_part.split("?")[0] if "?" in db_part else db_part
                    connection_info["database"] = db_name
                    self.logger.debug(f"Extracted database: {connection_info['database']}")

        self.logger.debug(f"Final MongoDB connection_info: {connection_info}")
        return connection_info
