"""
Database Utilities Module
Provides JDBC and MongoDB connection string parsing utilities.
"""

import re
import logging
from typing import Dict, Any, Optional


class DatabaseUtils:
    """Utilities for parsing database connection strings."""
    
    # Database type mappings
    JDBC_DATABASE_TYPES = {
        'mysql': {
            'url_patterns': ['mysql', 'mariadb'],
            'default_port': '3306',
        },
        'oracle': {
            'url_patterns': ['oracle', 'oracle:thin'],
            'default_port': '1521',
        },
        'sqlserver': {
            'url_patterns': ['sqlserver', 'mssql'],
            'default_port': '1433',
        },
        'postgresql': {
            'url_patterns': ['postgresql', 'postgres'],
            'default_port': '5432',
        },
        'snowflake': {
            'url_patterns': ['snowflake'],
            'default_port': '443',
        }
    }
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)
    
    def parse_jdbc_url(self, url: str) -> Dict[str, str]:
        """Parse JDBC URL to extract connection details from real JDBC URLs, including Oracle complex formats."""
        original_url = url
        url = url.lower()
        connection_info = {}

        self.logger.debug(f"Parsing JDBC URL: {original_url}")

        # Detect Oracle complex format by presence of '@(DESCRIPTION='
        if '@(DESCRIPTION=' in original_url:
            self.logger.debug("Detected Oracle complex format with DESCRIPTION block.")
            # Extract HOST (case-insensitive)
            host_match = re.search(r'\(HOST=([^\)]+)\)', original_url, re.IGNORECASE)
            if host_match:
                connection_info['host'] = host_match.group(1)
                self.logger.debug(f"[ORACLE] Extracted host: {connection_info['host']}")
            # Extract PORT (case-insensitive)
            port_match = re.search(r'\(PORT=([^\)]+)\)', original_url, re.IGNORECASE)
            if port_match:
                connection_info['port'] = port_match.group(1)
                self.logger.debug(f"[ORACLE] Extracted port: {connection_info['port']}")
            # Extract db.connection.type and db.name
            dbtype_dbname_match = re.search(r'\(CONNECT_DATA=\(([^=\)]+)=([^\)]+)\)\)', original_url, re.IGNORECASE)
            if dbtype_dbname_match:
                connection_info['db.connection.type'] = dbtype_dbname_match.group(1)
                connection_info['db.name'] = dbtype_dbname_match.group(2)
                self.logger.debug(f"[ORACLE] Extracted db.connection.type: {connection_info['db.connection.type']}")
                self.logger.debug(f"[ORACLE] Extracted db.name: {connection_info['db.name']}")
            # Extract ssl.server.cert.dn
            ssl_cert_dn_match = re.search(r'SSL_SERVER_CERT_DN=\\?"?([^\)\"]+)\\?"?\)', original_url, re.IGNORECASE)
            if ssl_cert_dn_match:
                connection_info['ssl.server.cert.dn'] = ssl_cert_dn_match.group(1)
                self.logger.debug(f"[ORACLE] Extracted ssl.server.cert.dn: {connection_info['ssl.server.cert.dn']}")
            self.logger.debug(f"[ORACLE] Final connection_info: {connection_info}")
            return connection_info
        else:
            self.logger.debug("Using standard JDBC URL parsing logic.")

        # Standard format: jdbc:postgresql://localhost:5432/dbname
        host_match = re.search(r'jdbc:[^:]+://([^:/]+)', url)
        if host_match:
            connection_info['host'] = host_match.group(1)
            self.logger.debug(f"Extracted host: {connection_info['host']}")

        # Extract port
        port_match = re.search(r'://[^:]+:(\d+)', url)
        if port_match:
            connection_info['port'] = port_match.group(1)
            self.logger.debug(f"Extracted port: {connection_info['port']}")

        # Extract database name
        db_match = re.search(r'://[^/]+/([^/?]+)', url)
        if db_match:
            connection_info['db.name'] = db_match.group(1)
            self.logger.debug(f"Extracted db_name: {connection_info['db.name']}")

        # Extract user from query parameters
        user_match = re.search(r'[?&]user=([^&]+)', url)
        if user_match:
            connection_info['user'] = user_match.group(1)
            self.logger.debug(f"Extracted user: {connection_info['user']}")

        # Extract password from query parameters
        password_match = re.search(r'[?&]password=([^&]+)', url)
        if password_match:
            connection_info['password'] = password_match.group(1)
            self.logger.debug(f"Extracted password: [REDACTED]")

        self.logger.debug(f"Final connection_info: {connection_info}")
        return connection_info

    def parse_mongodb_connection_string(self, url: str) -> Dict[str, str]:
        """Parse MongoDB connection string to extract connection details"""
        original_url = url
        url = url.lower()
        connection_info = {}

        self.logger.debug(f"Parsing MongoDB connection string: {original_url}")

        # Handle MongoDB Atlas connection strings (mongodb+srv://)
        if 'mongodb+srv://' in url:
            srv_part = url.replace('mongodb+srv://', '')

            if '@' in srv_part:
                credentials_part, host_part = srv_part.split('@', 1)

                if ':' in credentials_part:
                    user, password = credentials_part.split(':', 1)
                    connection_info['user'] = user
                    connection_info['password'] = password
                    self.logger.debug(f"Extracted user: {connection_info['user']}")

                host = host_part.split('/')[0].split('?')[0]
                connection_info['host'] = host
                self.logger.debug(f"Extracted host: {connection_info['host']}")

                if '/' in host_part:
                    db_part = host_part.split('/', 1)[1]
                    if '?' in db_part:
                        db_name = db_part.split('?')[0]
                    else:
                        db_name = db_part
                    connection_info['database'] = db_name
                    self.logger.debug(f"Extracted database: {connection_info['database']}")

        # Handle regular MongoDB connection strings (mongodb://)
        elif 'mongodb://' in url:
            mongo_part = url.replace('mongodb://', '')

            if '@' in mongo_part:
                credentials_part, host_part = mongo_part.split('@', 1)

                if ':' in credentials_part:
                    user, password = credentials_part.split(':', 1)
                    connection_info['user'] = user
                    connection_info['password'] = password
                    self.logger.debug(f"Extracted user: {connection_info['user']}")

                host = host_part.split('/')[0].split('?')[0]
                connection_info['host'] = host
                self.logger.debug(f"Extracted host: {connection_info['host']}")

                if '/' in host_part:
                    db_part = host_part.split('/', 1)[1]
                    if '?' in db_part:
                        db_name = db_part.split('?')[0]
                    else:
                        db_name = db_part
                    connection_info['database'] = db_name
                    self.logger.debug(f"Extracted database: {connection_info['database']}")
            else:
                host = mongo_part.split('/')[0].split('?')[0]
                connection_info['host'] = host
                self.logger.debug(f"Extracted host: {connection_info['host']}")

                if '/' in mongo_part:
                    db_part = mongo_part.split('/', 1)[1]
                    if '?' in db_part:
                        db_name = db_part.split('?')[0]
                    else:
                        db_name = db_part
                    connection_info['database'] = db_name
                    self.logger.debug(f"Extracted database: {connection_info['database']}")

        self.logger.debug(f"Final MongoDB connection_info: {connection_info}")
        return connection_info

    def get_database_type(self, config: Dict[str, Any]) -> str:
        """Detect database type from JDBC URL"""
        if 'connection.url' not in config:
            return 'unknown'

        url = config['connection.url'].lower()
        
        for db_type, info in self.JDBC_DATABASE_TYPES.items():
            for pattern in info['url_patterns']:
                if pattern in url:
                    self.logger.debug(f"Detected database type: {db_type}")
                    return db_type
        
        return 'unknown'

    def map_jdbc_properties(self, config: Dict[str, Any], db_type: str) -> Dict[str, Any]:
        """Map JDBC properties to database-specific properties"""
        self.logger.debug(f"Mapping JDBC properties for config: {config}")

        db_info = self.JDBC_DATABASE_TYPES.get(db_type, {})
        property_mappings = db_info.get('property_mappings', {})
        self.logger.debug(f"Property mappings for {db_type}: {property_mappings}")

        # Return the config with any additional mappings applied
        return config


# Singleton instance for convenience
_database_utils_instance = None


def get_database_utils(logger: Optional[logging.Logger] = None) -> DatabaseUtils:
    """Get or create a DatabaseUtils instance."""
    global _database_utils_instance
    if _database_utils_instance is None:
        _database_utils_instance = DatabaseUtils(logger)
    return _database_utils_instance

