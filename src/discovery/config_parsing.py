"""JDBC URL, MongoDB connection string, and database-type detection."""

from __future__ import annotations

import base64
import json
import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import requests
from requests.auth import HTTPBasicAuth
from translation.matching.semantic_matcher import Property, SemanticMatcher


class ConfigParsingMixin:

    def _get_database_type(self, config: Dict[str, Any]) -> str:
        """Determine database type from JDBC connector config"""
        # Check connection URL
        if 'connection.url' in config and isinstance(config['connection.url'], str):
            url = config['connection.url'].lower()
            self.logger.info(f"Analyzing JDBC URL for database type: {url}")

            # More precise pattern matching - look for jdbc:database_type:// pattern
            for db_type, info in self.jdbc_database_types.items():
                for pattern in info['url_patterns']:
                    # Look for the pattern in the JDBC protocol part specifically
                    if f'jdbc:{pattern}://' in url:
                        self.logger.info(f"Detected database type '{db_type}' using precise pattern 'jdbc:{pattern}://'")
                        return db_type

            # Fallback to the old method for backward compatibility
            for db_type, info in self.jdbc_database_types.items():
                if any(pattern in url for pattern in info['url_patterns']):
                    self.logger.info(f"Detected database type '{db_type}' using fallback pattern matching")
                    return db_type

            self.logger.warning(f"No database type detected for URL: {url}")

        # Check specific database type config if available
        if 'database.type' in config:
            db_type = config['database.type'].lower()
            self.logger.info(f"Using database type from config: {db_type}")
            return db_type

        self.logger.warning("No database type detected, returning 'unknown'")
        return 'unknown'


    def _parse_jdbc_url(self, url: str) -> Dict[str, str]:
        """Parse JDBC URL to extract connection details from real JDBC URLs, including Oracle complex formats."""

        original_url = url
        url = url.lower()
        connection_info = {}

        self.logger.debug(f"Parsing JDBC URL: {original_url}")

        # Detect Oracle complex format by presence of '@(DESCRIPTION='
        # "jdbc:oracle:thin:@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCPS)(HOST=<span>{connection.host})(PORT=</span>{connection.port}))(CONNECT_DATA=(<span>{db.connection.type}=</span>{db.name}))(SECURITY=(SSL_SERVER_CERT_DN="${ssl.server.cert.dn}")))"
        if '@(DESCRIPTION=' in original_url:
            self.logger.debug("Detected Oracle complex format with DESCRIPTION block.")
            # Extract HOST (case-insensitive to handle Host, HOST, host, etc.)
            host_match = re.search(r'\(HOST=([^\)]+)\)', original_url, re.IGNORECASE)
            if host_match:
                connection_info['host'] = host_match.group(1)
                self.logger.debug(f"[ORACLE] Extracted host: {connection_info['host']}")
            # Extract PORT (case-insensitive to handle Port, PORT, port, etc.)
            port_match = re.search(r'\(PORT=([^\)]+)\)', original_url, re.IGNORECASE)
            if port_match:
                connection_info['port'] = port_match.group(1)
                self.logger.debug(f"[ORACLE] Extracted port: {connection_info['port']}")
            # Extract db.connection.type and db.name (fix group assignments)
            # Handle both SERVICE_NAME and SID, case-insensitive
            dbtype_dbname_match = re.search(r'\(CONNECT_DATA=\(([^=\)]+)=([^\)]+)\)\)', original_url, re.IGNORECASE)
            if dbtype_dbname_match:
                connection_info['db.connection.type'] = dbtype_dbname_match.group(1)
                connection_info['db.name'] = dbtype_dbname_match.group(2)
                self.logger.debug(f"[ORACLE] Extracted db.connection.type: {connection_info['db.connection.type']}")
                self.logger.debug(f"[ORACLE] Extracted db.name: {connection_info['db.name']}")
            # Extract ssl.server.cert.dn (case-insensitive to handle ssl_server_cert_dn, SSL_SERVER_CERT_DN, etc.)
            ssl_cert_dn_match = re.search(r'SSL_SERVER_CERT_DN=\\?"?([^\)\"]+)\\?"?\)', original_url, re.IGNORECASE)
            if ssl_cert_dn_match:
                connection_info['ssl.server.cert.dn'] = ssl_cert_dn_match.group(1)
                self.logger.debug(f"[ORACLE] Extracted ssl.server.cert.dn: {connection_info['ssl.server.cert.dn']}")
            self.logger.debug(f"[ORACLE] Final connection_info: {connection_info}")
            return connection_info
        else:
            self.logger.debug("Using standard JDBC URL parsing logic.")

        # Existing logic for standard format
        # Extract host - look for pattern like jdbc:postgresql://localhost:5432/dbname
        host_match = re.search(r'jdbc:[^:]+://([^:/]+)', url)
        if host_match:
            host = host_match.group(1)
            connection_info['host'] = host
            self.logger.debug(f"Extracted host: {connection_info['host']}")

        # Extract port - look for pattern like :5432/
        port_match = re.search(r'://[^:]+:(\d+)', url)
        if port_match:
            port = port_match.group(1)
            connection_info['port'] = port
            self.logger.debug(f"Extracted port: {connection_info['port']}")

        # Extract database name - look for pattern like /dbname? or /dbname
        db_match = re.search(r'://[^/]+/([^/?]+)', url)
        if db_match:
            db_name = db_match.group(1)
            connection_info['db.name'] = db_name
            self.logger.debug(f"Extracted db_name: {connection_info['db.name']}")

        # Extract user from query parameters
        user_match = re.search(r'[?&]user=([^&]+)', url)
        if user_match:
            user = user_match.group(1)
            connection_info['user'] = user
            self.logger.debug(f"Extracted user: {connection_info['user']}")

        # Extract password from query parameters
        password_match = re.search(r'[?&]password=([^&]+)', url)
        if password_match:
            password = password_match.group(1)
            connection_info['password'] = password
            self.logger.debug(f"Extracted password: {connection_info['password']}")

        self.logger.debug(f"Final connection_info: {connection_info}")
        return connection_info


    def _parse_mongodb_connection_string(self, url: str) -> Dict[str, str]:
        """Parse MongoDB connection string to extract connection details"""
        original_url = url
        url = url.lower()
        connection_info = {}

        self.logger.debug(f"Parsing MongoDB connection string: {original_url}")

        # Handle MongoDB Atlas connection strings (mongodb+srv://)
        # Format: mongodb+srv://username:password@cluster.mongodb.net/database?options
        if 'mongodb+srv://' in url:
            # Extract the part after mongodb+srv://
            srv_part = url.replace('mongodb+srv://', '')

            # Split by @ to separate credentials from host
            if '@' in srv_part:
                credentials_part, host_part = srv_part.split('@', 1)

                # Extract username and password from credentials
                if ':' in credentials_part:
                    user, password = credentials_part.split(':', 1)
                    connection_info['user'] = user
                    connection_info['password'] = password
                    self.logger.debug(f"Extracted user: {connection_info['user']}")
                    self.logger.debug(f"Extracted password: {connection_info['password']}")

                # Extract host from host part
                host = host_part.split('/')[0].split('?')[0]
                connection_info['host'] = host
                self.logger.debug(f"Extracted host: {connection_info['host']}")

                # Extract database if present
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
            # Extract the part after mongodb://
            mongo_part = url.replace('mongodb://', '')

            # Split by @ to separate credentials from host
            if '@' in mongo_part:
                credentials_part, host_part = mongo_part.split('@', 1)

                # Extract username and password from credentials
                if ':' in credentials_part:
                    user, password = credentials_part.split(':', 1)
                    connection_info['user'] = user
                    connection_info['password'] = password
                    self.logger.debug(f"Extracted user: {connection_info['user']}")
                    self.logger.debug(f"Extracted password: {connection_info['password']}")

                # Extract host from host part
                host = host_part.split('/')[0].split('?')[0]
                connection_info['host'] = host
                self.logger.debug(f"Extracted host: {connection_info['host']}")

                # Extract database if present
                if '/' in host_part:
                    db_part = host_part.split('/', 1)[1]
                    if '?' in db_part:
                        db_name = db_part.split('?')[0]
                    else:
                        db_name = db_part
                    connection_info['database'] = db_name
                    self.logger.debug(f"Extracted database: {connection_info['database']}")
            else:
                # No credentials, just host
                host = mongo_part.split('/')[0].split('?')[0]
                connection_info['host'] = host
                self.logger.debug(f"Extracted host: {connection_info['host']}")

                # Extract database if present
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


    def _map_jdbc_properties(self, config: Dict[str, Any], db_type: str) -> Dict[str, Any]:
        """Map JDBC properties to database-specific properties"""
        self.logger.debug(f"Mapping JDBC properties for config: {config}")

        # Get database-specific property mappings
        db_info = self.jdbc_database_types.get(db_type, {})
        property_mappings = db_info.get('property_mappings', {})
        self.logger.debug(f"Property mappings for {db_type}: {property_mappings}")

        # Parse JDBC URL and map properties
        if 'connection.url' in config and isinstance(config['connection.url'], str) and config['connection.url'].startswith('jdbc:'):
            connection_info = self._parse_jdbc_url(config['connection.url'])

            # Map connection details to database-specific properties
            mapped_config = {}
            for fm_prop, jdbc_prop in property_mappings.items():
                if jdbc_prop in connection_info:
                    mapped_config[fm_prop] = connection_info[jdbc_prop]
                    self.logger.debug(f"Mapped {jdbc_prop} ({connection_info[jdbc_prop]}) to {fm_prop}")
                else:
                    self.logger.debug(f"JDBC property {jdbc_prop} not found in connection_info")

            self.logger.debug(f"Final JDBC mapped config: {mapped_config}")
            return mapped_config

        return {}

