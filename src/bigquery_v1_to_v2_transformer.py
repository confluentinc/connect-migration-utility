"""
Apache Connect Migration Utility
Copyright 2024-2025 The Apache Software Foundation

This product includes software developed at The Apache Software Foundation.
"""

from typing import Dict, Any


# Define the mapping between BigQuery Legacy and Storage Write API configurations
LEGACY_TO_STORAGE_MAPPING = {
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
    "bigquery.retry.count": "bigQueryRetry",
    "bigquery.thread.pool.size": "threadPoolSize",
    "buffer.count.records": "queueSize"
}

# Default values for Storage Write API connector configurations
STORAGE_DEFAULTS = {
    "sanitize.field.names": "false",
    "sanitize.field.names.in.array": "false",
    "auto.update.schemas": "DISABLED"
}

# Common configurations that should be preserved
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
    "cloud.provider"
]

# Configurations not supported in V2 Storage Write API connector
UNSUPPORTED_CONFIGS = {
    "allow.schema.unionization": "Schema unionization is not supported in V2 connector",
    "all.bq.fields.nullable": "All BigQuery fields nullable setting is not supported in V2 connector",
    "convert.double.special.values": "Double special values conversion is not supported in V2 connector",
    "allow.bigquery.required.field.relaxation": "BigQuery required field relaxation is not supported in V2 connector"
}


def is_bigquery_v1_connector(config: Dict[str, Any]) -> bool:
    """
    Check if the connector is a BigQuery V1 (Legacy) sink connector.
    
    Args:
        config: Connector configuration dictionary
        
    Returns:
        True if the connector is a BigQuery V1 sink connector, False otherwise
    """
    connector_class = config.get("connector.class", "")
    return connector_class == "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector"


def check_unsupported_configs(v1_config: Dict[str, Any]) -> list:
    """
    Check for configurations that are not supported in V2 connector.
    
    Args:
        v1_config: V1 BigQuery Legacy connector configuration
        
    Returns:
        List of unsupported config keys found in the V1 config
    """
    found_unsupported = []
    for config_key in UNSUPPORTED_CONFIGS.keys():
        if config_key in v1_config:
            found_unsupported.append(config_key)
    return found_unsupported


def transform_v1_to_v2(v1_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform a V1 BigQuery Legacy configuration to a V2 Storage Write API configuration.
    Uses sensible defaults for user-configurable options.
    
    Args:
        v1_config: V1 BigQuery Legacy connector configuration
        
    Returns:
        V2 BigQuery Storage Write API connector configuration
        
    Raises:
        Exception: If transformation fails
    """
    try:
        # Initialize the Storage Write API configuration
        v2_config = {
            "connector.class": "BigQueryStorageSink",
            "tasks.max": v1_config.get("tasks.max", 1),
        }

        # Legacy connector only supports service account authentication via keyfile
        if "keyfile" in v1_config:
            v2_config["authentication.method"] = "Google cloud service account"
        else:
            v2_config["authentication.method"] = "Google cloud service account"

        # Map legacy configurations to Storage Write API configurations
        config_mapping = {
            "topics": "topics",
            "project": "project",
            "datasets": "datasets",
            "keyfile": "keyfile",
            "input.data.format": "input.data.format",
            "input.key.format": "input.key.format",
            "sanitize.topics": "sanitize.topics",
            "sanitize.field.names": "sanitize.field.names",
            "auto.update.schemas": "auto.update.schemas",
            "topic2table.map": "topic2table.map",
            "sanitize.field.names.in.array": "sanitize.field.names.in.array"
        }

        # Copy mapped configurations
        for legacy_key, storage_key in config_mapping.items():
            if legacy_key in v1_config:
                v2_config[storage_key] = v1_config[legacy_key]

        # Handle table.name.format -> topic2table.map mapping
        if "table.name.format" in v1_config and "topic2table.map" not in v2_config:
            v2_config["topic2table.map"] = v1_config["table.name.format"]

        # Copy common configurations
        for config_key in COMMON_CONFIGS:
            if config_key in v1_config:
                v2_config[config_key] = v1_config[config_key]

        # Copy additional configurations (excluding unsupported ones)
        for config_key, config_value in v1_config.items():
            if (config_key not in config_mapping and
                config_key not in COMMON_CONFIGS and
                config_key not in UNSUPPORTED_CONFIGS and
                config_key not in ["name", "connector.class", "tasks.max", "authentication.method"]):
                v2_config[config_key] = config_value

        # Apply storage defaults for missing configurations
        for config_key, default_value in STORAGE_DEFAULTS.items():
            if config_key not in v2_config:
                v2_config[config_key] = default_value

        # Apply default values from Storage Write API connector template
        if 'input.key.format' not in v2_config:
            v2_config['input.key.format'] = 'BYTES'
        if 'sanitize.topics' not in v2_config:
            v2_config['sanitize.topics'] = 'true'
        if 'sanitize.field.names' not in v2_config:
            v2_config['sanitize.field.names'] = 'false'
        if 'auto.update.schemas' not in v2_config:
            v2_config['auto.update.schemas'] = 'DISABLED'
        if 'topic2table.map' not in v2_config:
            v2_config['topic2table.map'] = ''
        if 'topic2clustering.fields.map' not in v2_config:
            v2_config['topic2clustering.fields.map'] = ''

        # Set sanitize.field.names.in.array based on sanitize.field.names
        if 'sanitize.field.names' in v2_config:
            sanitize_field_names = v2_config['sanitize.field.names'].lower() == 'true'
            v2_config['sanitize.field.names.in.array'] = str(sanitize_field_names).lower()

        # Set default ingestion mode and other required V2 fields
        v2_config['ingestion.mode'] = 'STREAMING'  # Default to STREAMING
        v2_config['use.integer.for.int8.int16'] = 'false'  # Default to FLOAT casting
        v2_config['use.date.time.formatter'] = 'false'  # Default to SimpleDateFormat
        v2_config['auto.create.tables'] = 'DISABLED'  # Default to disabled
        v2_config['partitioning.type'] = 'DAY'  # Default partitioning type

        # Update name if present
        if 'name' in v1_config:
            v2_config['name'] = v1_config['name'] + '_v2'

        return v2_config
    except Exception as e:
        raise Exception(f"Error transforming BigQuery V1 to V2 configuration: {e}") from e

