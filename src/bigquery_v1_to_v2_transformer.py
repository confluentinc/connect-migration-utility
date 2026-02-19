"""
BigQuery V1 to V2 Transformer

This module handles the transformation of BigQuery Sink connector configurations
from V1 format (BigQuerySinkConnector / Legacy InsertAll API) to V2 format 
(BigQueryStorageSink / Storage Write API).

Based on the official Confluent migration tool:
https://github.com/confluentinc/confluent-connector-migration-tool/blob/master/bigquery-v2-sink/migrate-to-bq-v2-sink.py
"""

import logging
from typing import Any, Dict, List, Tuple, Optional


class BigQueryV1ToV2Transformer:
    """
    Transforms BigQuery Sink connector configurations from V1 (Legacy InsertAll API) 
    to V2 (Storage Write API) format.
    
    V1 Connector: com.wepay.kafka.connect.bigquery.BigQuerySinkConnector (Legacy InsertAll API)
    V2 Connector: BigQueryStorageSink (Storage Write API)
    
    Key changes in V2:
    - connector.class: BigQuerySinkConnector → BigQueryStorageSink
    - table.name.format → topic2table.map
    - New required fields: ingestion.mode, authentication.method
    - Some configs not supported in V2
    - Breaking API changes (TIMESTAMP, DATE, DATETIME handling)
    """
    
    # V1 connector class (Legacy BigQuery connector)
    V1_BIGQUERY_CONNECTOR = "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector"
    
    # V2 template ID (Storage Write API)
    V2_BIGQUERY_TEMPLATE_ID = "BigQueryStorageSink"
    
    # Configuration mappings from V1 (Legacy) to V2 (Storage Write API)
    # Based on: https://github.com/confluentinc/confluent-connector-migration-tool
    V1_TO_V2_MAPPING = {
        "keyfile": "keyfile",
        "project": "project",
        "datasets": "datasets",
        "topics": "topics",
        "tasks.max": "tasks.max",
        "sanitize.topics": "sanitize.topics",
        "sanitize.field.names": "sanitize.field.names",
        "auto.update.schemas": "auto.update.schemas",
        "input.data.format": "input.data.format",
        "input.key.format": "input.key.format",
        "table.name.format": "topic2table.map",
        "topic2table.map": "topic2table.map",
        "sanitize.field.names.in.array": "sanitize.field.names.in.array",
        "bigquery.retry.count": "bigQueryRetry",
        "bigquery.thread.pool.size": "threadPoolSize",
        "buffer.count.records": "queueSize",
    }
    
    # Required configurations that must be present in V1 config
    REQUIRED_CONFIGS = [
        "project",
        "datasets", 
        "topics",
    ]
    
    # Common configs that should be preserved
    COMMON_CONFIGS = [
        "kafka.api.key",
        "kafka.api.secret",
        "kafka.service.account.id",
        "kafka.auth.mode",
        "kafka.endpoint",
        "kafka.region",
        "schema.registry.url",
        "schema.registry.basic.auth.user.info",
        "max.poll.interval.ms",
        "max.poll.records",
        "cloud.environment",
        "cloud.provider",
        "errors.tolerance",
        "errors.deadletterqueue.topic.name",
    ]
    
    # Default values for V2 Storage Write API connector
    V2_DEFAULTS = {
        "authentication.method": "Google cloud service account",
        "sanitize.field.names": "false",
        "sanitize.field.names.in.array": "false",
        "sanitize.topics": "true",
        "auto.update.schemas": "DISABLED",
        "input.key.format": "BYTES",
    }
    
    # User-configurable options from the official migration tool
    # These configs require user review - we set defaults and add warnings
    # Based on: https://github.com/confluentinc/confluent-connector-migration-tool/blob/master/bigquery-v2-sink/migrate-to-bq-v2-sink.py
    USER_CONFIGURABLE_DEFAULTS = {
        "ingestion.mode": {
            "default": "STREAMING",
            "options": ["STREAMING", "BATCH LOADING", "UPSERT", "UPSERT_DELETE"],
            "description": "Ingestion mode. STREAMING: Lower latency, higher cost. BATCH LOADING: Higher latency, lower cost. UPSERT/UPSERT_DELETE: For upsert operations.",
        },
        "use.integer.for.int8.int16": {
            "default": "false",
            "options": ["true", "false"],
            "description": "INT8 and INT16 fields are cast to FLOAT by default. Set to 'true' to cast to INTEGER instead.",
        },
        "use.date.time.formatter": {
            "default": "false",
            "options": ["true", "false"],
            "description": "Use DateTimeFormatter for parsing DATE, TIME, DATETIME, TIMESTAMP fields. Set to 'true' for more flexible parsing.",
        },
        "auto.create.tables": {
            "default": "DISABLED",
            "options": ["DISABLED", "PARTITION", "NO_PARTITION"],
            "description": "Auto-create BigQuery tables. DISABLED: Tables must exist. PARTITION: Create partitioned tables. NO_PARTITION: Create non-partitioned tables.",
        },
        "partitioning.type": {
            "default": "DAY",
            "options": ["DAY", "HOUR", "MONTH", "YEAR", "NONE"],
            "description": "Partitioning granularity when auto.create.tables is PARTITION. Only applies if auto.create.tables != DISABLED.",
        },
    }
    
    # Breaking changes from Legacy InsertAll API to Storage Write API
    # Based on: https://github.com/confluentinc/confluent-connector-migration-tool
    BREAKING_CHANGES = {
        "TIMESTAMP": "TIMESTAMP values are now interpreted as microseconds since epoch instead of seconds. This may cause data to be written to incorrect time periods.",
        "DATE": "DATE fields now support INT values in range -719162 to 2932896, which was not supported in Legacy API.",
        "DATETIME_FORMAT": "DATE, TIME, DATETIME, TIMESTAMP fields now support only a subset of the datetime canonical format that was supported in Legacy API.",
        "DATA_TYPES": "Storage Write API has different data type support compared to Legacy InsertAll API. Some data types may not be compatible.",
        "INT8_INT16": "INT8 and INT16 fields are now cast to FLOAT type in BigQuery by default. Set 'use.integer.for.int8.int16' to 'true' to cast to INTEGER instead.",
    }
    
    # Configs not supported in V2 Storage Write API connector
    # Based on: https://github.com/confluentinc/confluent-connector-migration-tool
    UNSUPPORTED_CONFIGS = {
        "allow.schema.unionization": "Schema unionization is not supported in V2 connector. This functionality is now part of the auto.update.schemas property, which handles schema evolution for both primitive and complex types (structs and arrays).",
        "all.bq.fields.nullable": "All BigQuery fields nullable setting is not supported in V2 connector. This controlled whether all fields were made nullable.",
        "convert.double.special.values": "Double special values conversion is not supported in V2 connector. This handled +Infinity, -Infinity, and NaN conversions.",
        "allow.bigquery.required.field.relaxation": "BigQuery required field relaxation is not supported in V2 connector. This allowed relaxing required field constraints.",
    }
    
    def __init__(self, logger: logging.Logger = None):
        """
        Initialize the transformer.
        
        Args:
            logger: Optional logger instance. If not provided, creates a default one.
        """
        self.logger = logger or logging.getLogger(__name__)
    
    # ==================== Detection Methods ====================
    
    def is_bigquery_v1(self, connector_class: str) -> bool:
        """
        Check if the connector class is a BigQuery V1 (Legacy) sink connector.
        
        Args:
            connector_class: The connector class string to check
            
        Returns:
            True if it's a V1 BigQuery sink connector, False otherwise
        """
        if not connector_class:
            return False
        return connector_class == self.V1_BIGQUERY_CONNECTOR
    
    def is_bigquery_v1_config(self, config: Dict[str, Any]) -> bool:
        """
        Check if the configuration is for a BigQuery V1 (Legacy) sink connector.
        
        Args:
            config: Connector configuration dictionary
            
        Returns:
            True if it's a V1 BigQuery sink connector config, False otherwise
        """
        connector_class = config.get("connector.class", "")
        return self.is_bigquery_v1(connector_class)
    
    def is_bigquery_v2(self, connector_class: str) -> bool:
        """
        Check if the connector class is a BigQuery V2 (Storage Write API) sink connector.
        
        Args:
            connector_class: The connector class string to check
            
        Returns:
            True if it's a V2 BigQuery sink connector, False otherwise
        """
        if not connector_class:
            return False
        return connector_class == self.V2_BIGQUERY_TEMPLATE_ID
    
    def is_bigquery_v2_config(self, config: Dict[str, Any]) -> bool:
        """
        Check if the configuration is for a BigQuery V2 (Storage Write API) sink connector.
        
        Args:
            config: Connector configuration dictionary
            
        Returns:
            True if it's a V2 BigQuery sink connector config, False otherwise
        """
        connector_class = config.get("connector.class", "")
        return self.is_bigquery_v2(connector_class)
    
    # ==================== Translation Methods ====================
    
    def translate_v1_to_v2(self, config: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str], List[str]]:
        """
        Translate BigQuery V1 (Legacy InsertAll API) configuration to V2 (Storage Write API) format.
        
        Based on the official Confluent migration tool:
        https://github.com/confluentinc/confluent-connector-migration-tool/blob/master/bigquery-v2-sink/migrate-to-bq-v2-sink.py
        
        Args:
            config: The V1 connector configuration dictionary
            
        Returns:
            Tuple of (translated_config, warnings_list, errors_list)
        """
        translated = {}
        warnings = []
        errors = []
        processed_keys = set()
        
        # 1. Validate required configs - add errors if missing
        for required_key in self.REQUIRED_CONFIGS:
            if required_key not in config or not config[required_key]:
                errors.append(f"Missing required configuration: '{required_key}'")
        
        # 2. Set connector.class to V2 template ID
        if 'connector.class' in config:
            translated['connector.class'] = self.V2_BIGQUERY_TEMPLATE_ID
            processed_keys.add('connector.class')
        
        # 3. Check for unsupported configs and add warnings
        for unsupported_key, message in self.UNSUPPORTED_CONFIGS.items():
            if unsupported_key in config:
                warnings.append(f"UNSUPPORTED: '{unsupported_key}' - {message}")
                processed_keys.add(unsupported_key)
        
        # 4. Add breaking changes warnings
        for change_type, description in self.BREAKING_CHANGES.items():
            warnings.append(f"BREAKING CHANGE ({change_type}): {description}")
        
        # 5. Apply V1 to V2 config mappings
        for v1_key, v2_key in self.V1_TO_V2_MAPPING.items():
            if v1_key in config:
                translated[v2_key] = config[v1_key]
                processed_keys.add(v1_key)
        
        # 6. Copy common configs
        for common_key in self.COMMON_CONFIGS:
            if common_key in config and common_key not in translated:
                translated[common_key] = config[common_key]
                processed_keys.add(common_key)
        
        # 7. Handle name
        if 'name' in config:
            translated['name'] = config['name']
            processed_keys.add('name')
        
        # 8. Handle tasks.max
        if 'tasks.max' in config:
            translated['tasks.max'] = config['tasks.max']
            processed_keys.add('tasks.max')
        else:
            translated['tasks.max'] = 1
        
        # 9. Apply V2 defaults for missing configurations
        for default_key, default_value in self.V2_DEFAULTS.items():
            if default_key not in translated:
                translated[default_key] = default_value
        
        # 10. Apply user-configurable defaults and add warnings
        # Based on: https://github.com/confluentinc/confluent-connector-migration-tool/blob/master/bigquery-v2-sink/migrate-to-bq-v2-sink.py
        for config_key, config_info in self.USER_CONFIGURABLE_DEFAULTS.items():
            if config_key not in translated:
                translated[config_key] = config_info["default"]
                warnings.append(
                    f"USER CONFIG REQUIRED: '{config_key}' set to default '{config_info['default']}'. "
                    f"{config_info['description']} Options: {', '.join(config_info['options'])}"
                )
        
        # 11. Set sanitize.field.names.in.array based on sanitize.field.names
        if 'sanitize.field.names' in translated:
            sanitize_val = str(translated['sanitize.field.names']).lower() == 'true'
            translated['sanitize.field.names.in.array'] = str(sanitize_val).lower()
        
        # 12. Handle keyfile - if present, set authentication method
        if 'keyfile' in config:
            translated['authentication.method'] = "Google cloud service account"
            translated['keyfile'] = config['keyfile']
            processed_keys.add('keyfile')
        
        # 13. Handle transforms (SMTs)
        transform_keys = [k for k in config.keys() if k.startswith('transforms')]
        for transform_key in transform_keys:
            translated[transform_key] = config[transform_key]
            processed_keys.add(transform_key)
        
        # 14. Copy any remaining unprocessed configs (excluding unsupported ones)
        for key, value in config.items():
            if key not in processed_keys and key not in translated:
                if key not in self.UNSUPPORTED_CONFIGS:
                    translated[key] = value
        
        self.logger.info(f"Translated {len(processed_keys)} BigQuery V1 configs to V2 format")
        self.logger.debug(f"Translated config keys: {list(translated.keys())}")
        
        return translated, warnings, errors


# ==================== Module-level Functions (for backward compatibility) ====================

_transformer = None

def _get_transformer() -> BigQueryV1ToV2Transformer:
    """Get or create the global transformer instance."""
    global _transformer
    if _transformer is None:
        _transformer = BigQueryV1ToV2Transformer()
    return _transformer


def is_bigquery_v1_connector(config: Dict[str, Any]) -> bool:
    """
    Check if the connector is a BigQuery V1 (Legacy) sink connector.
    
    Args:
        config: Connector configuration dictionary
        
    Returns:
        True if the connector is a BigQuery V1 sink connector, False otherwise
    """
    return _get_transformer().is_bigquery_v1_config(config)


def is_bigquery_v2_connector(config: Dict[str, Any]) -> bool:
    """
    Check if the connector is a BigQuery V2 (Storage Write API) sink connector.
    
    Args:
        config: Connector configuration dictionary
        
    Returns:
        True if the connector is a BigQuery V2 sink connector, False otherwise
    """
    return _get_transformer().is_bigquery_v2_config(config)


def check_unsupported_configs(v1_config: Dict[str, Any]) -> list:
    """
    Check for configurations that are not supported in V2 connector.
    
    Args:
        v1_config: V1 BigQuery Legacy connector configuration
        
    Returns:
        List of unsupported config keys found in the V1 config
    """
    transformer = _get_transformer()
    found_unsupported = []
    for config_key in transformer.UNSUPPORTED_CONFIGS.keys():
        if config_key in v1_config:
            found_unsupported.append(config_key)
    return found_unsupported


def transform_v1_to_v2(v1_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform a V1 BigQuery Legacy configuration to a V2 Storage Write API configuration.
    
    If the config is already V2, it is returned as-is without transformation.
    
    Args:
        v1_config: V1 BigQuery Legacy connector configuration
        
    Returns:
        V2 BigQuery Storage Write API connector configuration
        
    Raises:
        Exception: If transformation fails or required configs are missing
    """
    transformer = _get_transformer()
    
    # If already V2, return as-is
    if transformer.is_bigquery_v2_config(v1_config):
        transformer.logger.info("Config is already BigQuery V2, keeping as-is")
        return v1_config.copy()
    
    try:
        translated, warnings, errors = transformer.translate_v1_to_v2(v1_config)
        
        # Log warnings
        for warning in warnings:
            transformer.logger.debug(f"BigQuery V1→V2 Warning: {warning}")
        
        # If there are critical errors, raise exception
        if errors:
            error_msg = "; ".join(errors)
            raise Exception(f"BigQuery V1 to V2 transformation failed: {error_msg}")
        
        return translated
    except Exception as e:
        raise Exception(f"Error transforming BigQuery V1 to V2 configuration: {e}") from e


def transform_v1_to_v2_full(v1_config: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str], List[str]]:
    """
    Transform a V1 BigQuery Legacy configuration to a V2 Storage Write API configuration.
    
    If the config is already V2, it is returned as-is without transformation.
    
    Returns full results including warnings and errors.
    
    Args:
        v1_config: V1 BigQuery Legacy connector configuration
        
    Returns:
        Tuple of (v2_config, warnings, errors)
    """
    transformer = _get_transformer()
    
    # If already V2, return as-is
    if transformer.is_bigquery_v2_config(v1_config):
        transformer.logger.info("Config is already BigQuery V2, keeping as-is")
        return v1_config.copy(), [], []
    
    return transformer.translate_v1_to_v2(v1_config)
