"""
Debezium V1 to V2 Translator

This module handles the translation of Debezium CDC connector configurations
from v1 format to v2 format for SQL Server, MySQL, and PostgreSQL connectors.

Based on the official Migration Guides for each connector type.
"""

import logging
from typing import Any, Dict, List, Tuple, Optional


class DebeziumV1ToV2Translator:
    """
    Translates Debezium CDC connector configurations from v1 to v2 format.
    
    Supports:
    - SQL Server CDC
    - MySQL CDC
    - PostgreSQL CDC
    """
    
    # V1 connector class mappings
    V1_SQLSERVER_CONNECTOR = "io.debezium.connector.sqlserver.SqlServerConnector"
    V1_MYSQL_CONNECTOR = "io.debezium.connector.mysql.MySqlConnector"
    V1_POSTGRESQL_CONNECTOR = "io.debezium.connector.postgresql.PostgresConnector"
    
    # V2 FM template IDs
    V2_SQLSERVER_TEMPLATE_ID = "SqlServerCdcSourceV2"
    V2_MYSQL_TEMPLATE_ID = "MySqlCdcSourceV2"
    V2_POSTGRESQL_TEMPLATE_ID = "PostgresCdcSourceV2"
    
    def __init__(self, logger: logging.Logger = None):
        """
        Initialize the translator.
        
        Args:
            logger: Optional logger instance. If not provided, creates a default one.
        """
        self.logger = logger or logging.getLogger(__name__)
    
    # ==================== Detection Methods ====================
    
    def is_debezium_sqlserver_v1(self, connector_class: str) -> bool:
        """
        Check if the connector class is a Debezium SQL Server v1 connector.
        
        Args:
            connector_class: The connector class string to check
            
        Returns:
            True if it's a v1 SQL Server connector, False otherwise
        """
        if not connector_class:
            return False
        return connector_class == self.V1_SQLSERVER_CONNECTOR

    def is_debezium_mysql_v1(self, connector_class: str) -> bool:
        """
        Check if the connector class is a Debezium MySQL v1 connector.
        
        Args:
            connector_class: The connector class string to check
            
        Returns:
            True if it's a v1 MySQL connector, False otherwise
        """
        if not connector_class:
            return False
        return connector_class == self.V1_MYSQL_CONNECTOR

    def is_debezium_postgresql_v1(self, connector_class: str) -> bool:
        """
        Check if the connector class is a Debezium PostgreSQL v1 connector.
        
        Args:
            connector_class: The connector class string to check
            
        Returns:
            True if it's a v1 PostgreSQL connector, False otherwise
        """
        if not connector_class:
            return False
        return connector_class == self.V1_POSTGRESQL_CONNECTOR
    
    def is_debezium_v1(self, connector_class: str) -> bool:
        """
        Check if the connector class is any Debezium v1 connector.
        
        Args:
            connector_class: The connector class string to check
            
        Returns:
            True if it's any v1 Debezium CDC connector, False otherwise
        """
        return (self.is_debezium_sqlserver_v1(connector_class) or
                self.is_debezium_mysql_v1(connector_class) or
                self.is_debezium_postgresql_v1(connector_class))
    
    # ==================== Translation Methods ====================
    
    def translate_v1_to_v2(self, connector_class: str, config: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str], List[str]]:
        """
        Translate any Debezium v1 config to v2 format based on connector type.
        
        Args:
            connector_class: The original v1 connector class
            config: The v1 connector configuration dictionary
            
        Returns:
            Tuple of (translated_config, warnings_list, errors_list)
        """
        if self.is_debezium_sqlserver_v1(connector_class):
            translated, warnings = self.translate_sqlserver_v1_to_v2(config)
            return translated, warnings, []
        elif self.is_debezium_mysql_v1(connector_class):
            translated, warnings = self.translate_mysql_v1_to_v2(config)
            return translated, warnings, []
        elif self.is_debezium_postgresql_v1(connector_class):
            return self.translate_postgresql_v1_to_v2(config)
        else:
            return config, [], [f"Unknown connector class: {connector_class}"]

    def translate_sqlserver_v1_to_v2(self, config: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
        """
        Translate Debezium SQL Server v1 connector configuration to v2 format.
        
        Based on the Migration Guide: Debezium SQL Server CDC Source Connector v1 to v2
        
        Key translations:
        - connector.class: SqlServerConnector → SqlServerCdcSourceV2
        - database.hostname, database.port, database.dbname → database.url (JDBC URL format)
        - database.server.name → topic.prefix
        - database.history.* → schema.history.internal.*
        - database.applicationIntent → driver.applicationIntent
        - Adds schema.history.internal.kafka.topic if not present
        
        Args:
            config: The v1 connector configuration dictionary
            
        Returns:
            Tuple of (translated_config, warnings_list)
        """
        translated = {}
        warnings = []
        
        # Extract key values needed for translation
        hostname = config.get('database.hostname', '')
        port = config.get('database.port', '1433')  # Default SQL Server port
        dbname = config.get('database.dbname', '')
        server_name = config.get('database.server.name', '')
        
        # Track which keys have been processed/translated
        processed_keys = set()
        
        # 1. Translate connector.class from v1 to v2 template ID
        # FM uses template_id as connector.class value, not the Java class name
        if 'connector.class' in config:
            translated['connector.class'] = self.V2_SQLSERVER_TEMPLATE_ID
            processed_keys.add('connector.class')
            warnings.append(f"connector.class changed to FM template ID '{self.V2_SQLSERVER_TEMPLATE_ID}' (v2)")
        
        # 2. Build database.url from hostname, port, and dbname (BREAKING CHANGE)
        if hostname:
            # Build JDBC URL: jdbc:sqlserver://hostname:port;databaseName=dbname
            jdbc_url_parts = [f"jdbc:sqlserver://{hostname}"]
            if port:
                jdbc_url_parts[0] += f":{port}"
            if dbname:
                jdbc_url_parts.append(f"databaseName={dbname}")
            
            translated['database.url'] = ";".join(jdbc_url_parts)
            processed_keys.add('database.hostname')
            processed_keys.add('database.port')
            warnings.append(f"database.url constructed from database.hostname, database.port, database.dbname: {translated['database.url']}")
        
        # 3. Translate database.server.name → topic.prefix (CRITICAL RENAME)
        if server_name:
            translated['topic.prefix'] = server_name
            processed_keys.add('database.server.name')
            warnings.append(f"database.server.name renamed to topic.prefix: {server_name}")
        
        # 4. Translate database.dbname → database.names (kept separately for v2)
        if dbname:
            translated['database.names'] = dbname
            processed_keys.add('database.dbname')
            warnings.append(f"database.dbname also mapped to database.names: {dbname}")
        
        # 5. Translate database.applicationIntent → driver.applicationIntent
        if 'database.applicationIntent' in config:
            translated['driver.applicationIntent'] = config['database.applicationIntent']
            processed_keys.add('database.applicationIntent')
            warnings.append(f"database.applicationIntent renamed to driver.applicationIntent: {config['database.applicationIntent']}")
        
        # 6. Translate database.history.* → schema.history.internal.*
        history_prefix_old = 'database.history.'
        history_prefix_new = 'schema.history.internal.'
        for key, value in config.items():
            if key.startswith(history_prefix_old):
                new_key = history_prefix_new + key[len(history_prefix_old):]
                translated[new_key] = value
                processed_keys.add(key)
                warnings.append(f"Renamed {key} to {new_key}")
        
        # 7. Ensure schema.history.internal.kafka.topic is set (CRITICAL for migration)
        # If not already set from database.history.kafka.topic, use database.server.name
        if 'schema.history.internal.kafka.topic' not in translated:
            if server_name:
                translated['schema.history.internal.kafka.topic'] = server_name
                warnings.append(f"Added schema.history.internal.kafka.topic using database.server.name value: {server_name}")
        
        # 8. Handle after.state.only deprecation (Feature Parity note)
        # The after.state.only property has been deprecated in v2
        # If set to false, user must add ExtractNewRecordState SMT
        if 'after.state.only' in config:
            after_state_only_value = config['after.state.only'].lower()
            processed_keys.add('after.state.only')
            if after_state_only_value == 'false':
                warnings.append("DEPRECATED: 'after.state.only' is deprecated in v2. Since it was set to 'false', you must add the ExtractNewRecordState SMT to achieve the same full change event envelope behavior.")
            else:
                warnings.append("DEPRECATED: 'after.state.only' is deprecated in v2 and will not be included in the translated config.")
        
        # 9. Note about tombstones.on.delete default change
        # v1 default: false, v2 default: true
        if 'tombstones.on.delete' not in config:
            warnings.append("BEHAVIOR CHANGE: tombstones.on.delete default changed from 'false' (v1) to 'true' (v2). If you relied on the previous default and do not want tombstone records on deletes, explicitly set 'tombstones.on.delete': 'false'")
        
        # 10. Copy over all other configs that weren't processed
        for key, value in config.items():
            if key not in processed_keys and key not in translated:
                translated[key] = value
        
        self.logger.info(f"Translated {len(processed_keys)} SQL Server configs from v1 to v2 format")
        self.logger.debug(f"Translated config keys: {list(translated.keys())}")
        
        return translated, warnings

    def translate_mysql_v1_to_v2(self, config: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
        """
        Translate Debezium MySQL v1 connector configuration to v2 format.
        
        Based on the Migration Guide: Debezium MySQL CDC Source Connector v1 to v2
        
        Key translations:
        - connector.class: MySqlCdcSource → MySqlCdcSourceV2
        - database.server.name → topic.prefix (CRITICAL)
        - database.history.kafka.topic → schema.history.internal.kafka.topic (CRITICAL)
        - database.history.* → schema.history.internal.*
        - binlog.filename.override and binlog.row.in.binlog.file are deprecated (GTID preferred)
        - after.state.only is deprecated
        - include.schema.changes default changed from false to true
        
        Args:
            config: The v1 connector configuration dictionary
            
        Returns:
            Tuple of (translated_config, warnings_list)
        """
        translated = {}
        warnings = []
        
        # Extract key values needed for translation
        server_name = config.get('database.server.name', '')
        
        # Track which keys have been processed/translated
        processed_keys = set()
        
        # 1. Translate connector.class from v1 to v2 template ID
        # FM uses template_id as connector.class value, not the Java class name
        if 'connector.class' in config:
            translated['connector.class'] = self.V2_MYSQL_TEMPLATE_ID
            processed_keys.add('connector.class')
            warnings.append(f"connector.class changed to FM template ID '{self.V2_MYSQL_TEMPLATE_ID}' (v2)")
        
        # 2. Translate database.server.name → topic.prefix (CRITICAL RENAME)
        if server_name:
            translated['topic.prefix'] = server_name
            processed_keys.add('database.server.name')
            warnings.append(f"database.server.name renamed to topic.prefix: {server_name}")
        
        # 3. Translate database.history.kafka.topic → schema.history.internal.kafka.topic (CRITICAL)
        if 'database.history.kafka.topic' in config:
            translated['schema.history.internal.kafka.topic'] = config['database.history.kafka.topic']
            processed_keys.add('database.history.kafka.topic')
        
        # 4. Translate database.history.* → schema.history.internal.*
        history_prefix_old = 'database.history.'
        history_prefix_new = 'schema.history.internal.'
        for key, value in config.items():
            if key.startswith(history_prefix_old) and key not in processed_keys:
                new_key = history_prefix_new + key[len(history_prefix_old):]
                translated[new_key] = value
                processed_keys.add(key)
                warnings.append(f"Renamed {key} to {new_key}")
        
        # 5. Handle deprecated binlog properties (GTID is now preferred)
        deprecated_binlog_props = ['binlog.filename.override', 'binlog.row.in.binlog.file']
        for prop in deprecated_binlog_props:
            if prop in config:
                processed_keys.add(prop)
                warnings.append(f"DEPRECATED: '{prop}' is deprecated in v2. GTID-based replication is now preferred. Ensure your MySQL server is configured with gtid-mode=ON and enforce-gtid-consistency=ON.")
        
        # 6. Handle after.state.only deprecation
        if 'after.state.only' in config:
            after_state_only_value = config['after.state.only'].lower()
            processed_keys.add('after.state.only')
            if after_state_only_value == 'false':
                warnings.append("DEPRECATED: 'after.state.only' is deprecated in v2. Since it was set to 'false', you must add the ExtractNewRecordState SMT to achieve the same full change event envelope behavior.")
            else:
                warnings.append("DEPRECATED: 'after.state.only' is deprecated in v2 and will not be included in the translated config.")
        
        # 7. Note about include.schema.changes default change
        # v1 default: false, v2 default: true
        if 'include.schema.changes' not in config:
            warnings.append("BEHAVIOR CHANGE: include.schema.changes default changed from 'false' (v1) to 'true' (v2). The connector will now automatically create and publish to a schema history topic. If you do not want this behavior, explicitly set 'include.schema.changes': 'false'")
        
        # 8. Copy over all other configs that weren't processed
        for key, value in config.items():
            if key not in processed_keys and key not in translated:
                translated[key] = value
        
        self.logger.info(f"Translated {len(processed_keys)} MySQL configs from v1 to v2 format")
        self.logger.debug(f"Translated config keys: {list(translated.keys())}")
        
        return translated, warnings

    def translate_postgresql_v1_to_v2(self, config: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str], List[str]]:
        """
        Translate Debezium PostgreSQL v1 connector configuration to v2 format.
        
        Based on the Migration Guide: Debezium PostgreSQL CDC Source Connector v1 to v2
        
        Key translations:
        - connector.class: PostgresCdcSource → PostgresCdcSourceV2
        - database.server.name → topic.prefix (CRITICAL)
        - plugin.name must be pgoutput (v2 default)
        - publication.name is now required when using pgoutput
        - after.state.only is deprecated
        - No schema history topic needed (simpler than SQL Server/MySQL)
        
        Args:
            config: The v1 connector configuration dictionary
            
        Returns:
            Tuple of (translated_config, warnings_list, errors_list)
        """
        translated = {}
        warnings = []
        translation_errors = []
        
        # Extract key values needed for translation
        server_name = config.get('database.server.name', '')
        plugin_name = config.get('plugin.name', '')
        
        # Track which keys have been processed/translated
        processed_keys = set()
        
        # 1. Translate connector.class from v1 to v2 template ID
        # FM uses template_id as connector.class value, not the Java class name
        if 'connector.class' in config:
            translated['connector.class'] = self.V2_POSTGRESQL_TEMPLATE_ID
            processed_keys.add('connector.class')
            warnings.append(f"connector.class changed to FM template ID '{self.V2_POSTGRESQL_TEMPLATE_ID}' (v2)")
        
        # 2. Translate database.server.name → topic.prefix (CRITICAL RENAME)
        if server_name:
            translated['topic.prefix'] = server_name
            processed_keys.add('database.server.name')
            warnings.append(f"database.server.name renamed to topic.prefix: {server_name}")
        
        # 3. Handle plugin.name - v2 defaults to pgoutput
        # If using decoderbufs, warn about migration to pgoutput
        if plugin_name:
            if plugin_name.lower() == 'decoderbufs':
                translated['plugin.name'] = 'pgoutput'
                processed_keys.add('plugin.name')
                warnings.append("BREAKING CHANGE: plugin.name changed from 'decoderbufs' to 'pgoutput'. The v2 connector exclusively supports pgoutput. Ensure your PostgreSQL database has a PUBLICATION created.")
            elif plugin_name.lower() == 'pgoutput':
                translated['plugin.name'] = 'pgoutput'
                processed_keys.add('plugin.name')
            else:
                warnings.append(f"WARNING: Unrecognized plugin.name '{plugin_name}'. V2 connector exclusively supports 'pgoutput'. Please verify compatibility.")
        else:
            # No plugin specified, v2 defaults to pgoutput
            translated['plugin.name'] = 'pgoutput'
            warnings.append("plugin.name set to 'pgoutput' (v2 default). Ensure your PostgreSQL database has a PUBLICATION created.")
        
        # 4. Check for publication.name - required when using pgoutput
        if 'publication.name' not in config:
            translation_errors.append("REQUIRED: 'publication.name' is mandatory in v2 when using pgoutput. You must create a publication on your PostgreSQL database (e.g., CREATE PUBLICATION confluent_publication FOR ALL TABLES;) and set this property.")
        
        # 5. Handle after.state.only deprecation
        if 'after.state.only' in config:
            after_state_only_value = config['after.state.only'].lower()
            processed_keys.add('after.state.only')
            if after_state_only_value == 'false':
                warnings.append("DEPRECATED: 'after.state.only' is deprecated in v2. Since it was set to 'false', you must add the ExtractNewRecordState SMT to achieve the same full change event envelope behavior.")
            else:
                warnings.append("DEPRECATED: 'after.state.only' is deprecated in v2 and will not be included in the translated config.")
        
        # 6. Note: PostgreSQL doesn't use schema history topic
        # This is simpler than SQL Server/MySQL - only connector offsets need to be preserved
        warnings.append("NOTE: PostgreSQL CDC does not use a schema history topic. Schema information is embedded within the WAL stream. Only connector offsets need to be preserved for migration.")
        
        # 7. Copy over all other configs that weren't processed
        for key, value in config.items():
            if key not in processed_keys and key not in translated:
                translated[key] = value
        
        self.logger.info(f"Translated {len(processed_keys)} PostgreSQL configs from v1 to v2 format")
        self.logger.debug(f"Translated config keys: {list(translated.keys())}")
        
        return translated, warnings, translation_errors

