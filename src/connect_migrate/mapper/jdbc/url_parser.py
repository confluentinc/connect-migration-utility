"""Parses JDBC and MongoDB connection URLs into structured connection info."""

import logging
import re
from typing import Dict, Optional


class JdbcUrlParser:
    def __init__(self, logger: Optional[logging.Logger] = None) -> None:
        self.logger = logger or logging.getLogger(__name__)

    def parse_jdbc_url(self, url: str) -> Dict[str, str]:
        """Parse a JDBC URL (including Oracle's complex DESCRIPTION format).

        Returns a dict with any of: ``host``, ``port``, ``db.name``, ``user``,
        ``password``, ``db.connection.type``, ``ssl.server.cert.dn``.

        The URL is parsed case-insensitively (via ``re.IGNORECASE``) without
        lowercasing the source string, so credential values keep their original
        case.
        """
        connection_info: Dict[str, str] = {}

        if "@(DESCRIPTION=" in url:
            host_match = re.search(r"\(HOST=([^\)]+)\)", url, re.IGNORECASE)
            if host_match:
                connection_info["host"] = host_match.group(1)
            port_match = re.search(r"\(PORT=([^\)]+)\)", url, re.IGNORECASE)
            if port_match:
                connection_info["port"] = port_match.group(1)
            dbtype_dbname_match = re.search(
                r"\(CONNECT_DATA=\(([^=\)]+)=([^\)]+)\)\)",
                url,
                re.IGNORECASE,
            )
            if dbtype_dbname_match:
                connection_info["db.connection.type"] = dbtype_dbname_match.group(1)
                connection_info["db.name"] = dbtype_dbname_match.group(2)
            ssl_cert_dn_match = re.search(
                r'SSL_SERVER_CERT_DN=\\?"?([^\)\"]+)\\?"?\)',
                url,
                re.IGNORECASE,
            )
            if ssl_cert_dn_match:
                connection_info["ssl.server.cert.dn"] = ssl_cert_dn_match.group(1)
            return connection_info

        host_match = re.search(r"jdbc:[^:]+://([^:/]+)", url, re.IGNORECASE)
        if host_match:
            connection_info["host"] = host_match.group(1)

        port_match = re.search(r"://[^:]+:(\d+)", url)
        if port_match:
            connection_info["port"] = port_match.group(1)

        db_match = re.search(r"://[^/]+/([^/?]+)", url)
        if db_match:
            connection_info["db.name"] = db_match.group(1)

        user_match = re.search(r"[?&]user=([^&]+)", url, re.IGNORECASE)
        if user_match:
            connection_info["user"] = user_match.group(1)

        password_match = re.search(r"[?&]password=([^&]+)", url, re.IGNORECASE)
        if password_match:
            connection_info["password"] = password_match.group(1)

        return connection_info

    def parse_mongodb_url(self, url: str) -> Dict[str, str]:
        """Parse a MongoDB connection string (``mongodb://`` or ``mongodb+srv://``).

        Preserves the original case of credentials. Scheme detection is
        case-insensitive.
        """
        connection_info: Dict[str, str] = {}
        url_lc = url.lower()

        if url_lc.startswith("mongodb+srv://"):
            body = url[len("mongodb+srv://"):]
        elif url_lc.startswith("mongodb://"):
            body = url[len("mongodb://"):]
        else:
            return connection_info

        if "@" in body:
            credentials_part, host_part = body.split("@", 1)
            if ":" in credentials_part:
                user, password = credentials_part.split(":", 1)
                connection_info["user"] = user
                connection_info["password"] = password
        else:
            host_part = body

        host = host_part.split("/")[0].split("?")[0]
        if host:
            connection_info["host"] = host
        if "/" in host_part:
            db_part = host_part.split("/", 1)[1]
            db_name = db_part.split("?")[0] if "?" in db_part else db_part
            if db_name:
                connection_info["database"] = db_name

        return connection_info
