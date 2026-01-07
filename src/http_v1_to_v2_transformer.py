"""
Apache Connect Migration Utility
Copyright 2024-2025 The Apache Software Foundation

This product includes software developed at The Apache Software Foundation.
"""

from typing import Dict, Any


# Define the mapping between V1 and V2 configurations
V1_TO_V2_MAPPING = {
    "auth.type": "auth.type",
    "batch.json.as.array": "api1.batch.json.as.array",
    "batch.key.pattern": "api1.batch.key.pattern",
    "batch.max.size": "api1.max.batch.size",
    "batch.prefix": "api1.batch.prefix",
    "batch.separator": "api1.batch.separator",
    "batch.suffix": "api1.batch.suffix",
    "behavior.on.error": "behavior.on.error",
    "behavior.on.null.values": "api1.behavior.on.null.values",
    "connection.password": "connection.password",
    "connection.user": "connection.user",
    "header.separator": "api1.http.request.headers.separator",
    "headers": "api1.http.request.headers",
    "http.connect.timeout.ms": "api1.http.connect.timeout.ms",
    "http.request.timeout.ms": "api1.http.request.timeout.ms",
    "https.host.verifier.enabled": "https.host.verifier.enabled",
    "https.ssl.key.password": "https.ssl.key.password",
    "https.ssl.keystore.password": "https.ssl.keystore.password",
    "https.ssl.keystorefile": "https.ssl.keystorefile",
    "https.ssl.protocol": "https.ssl.protocol",
    "https.ssl.truststore.password": "https.ssl.truststore.password",
    "https.ssl.truststorefile": "https.ssl.truststorefile",
    "max.retries": "api1.max.retries",
    "oauth2.client.auth.mode": "oauth2.client.auth.mode",
    "oauth2.client.header.separator": "oauth2.client.header.separator",
    "oauth2.client.headers": "oauth2.client.headers",
    "oauth2.client.id": "oauth2.client.id",
    "oauth2.client.scope": "oauth2.client.scope",
    "oauth2.client.secret": "oauth2.client.secret",
    "oauth2.jwt.claimset": "oauth2.jwt.claimset",
    "oauth2.jwt.enabled": "oauth2.jwt.enabled",
    "oauth2.jwt.keystore.password": "oauth2.jwt.keystore.password",
    "oauth2.jwt.keystore.path": "oauth2.jwt.keystore.path",
    "oauth2.jwt.keystore.type": "oauth2.jwt.keystore.type",
    "oauth2.token.property": "oauth2.token.property",
    "oauth2.token.url": "oauth2.token.url",
    "regex.patterns": "api1.regex.patterns",
    "regex.replacements": "api1.regex.replacements",
    "regex.separator": "api1.regex.separator",
    "report.errors.as": "report.errors.as",
    "request.body.format": "api1.request.body.format",
    "request.method": "api1.http.request.method",
    "retry.backoff.ms": "api1.retry.backoff.ms",
    "retry.backoff.policy": "api1.retry.backoff.policy",
    "retry.on.status.codes": "api1.retry.on.status.codes",
    "sensitive.headers": "api1.http.request.sensitive.headers",
    "topics": "api1.topics"
}

# Common configurations to be copied as is
COMMON_CONFIGS = [
    "input.data.format",
    "kafka.api.key",
    "kafka.api.secret",
    "kafka.auth.mode",
    "kafka.service.account.id",
    "max.poll.interval.ms",
    "max.poll.records",
    "name",
    "schema.context.name",
    "tasks.max",
    "topics"
]


def is_http_v1_connector(config: Dict[str, Any]) -> bool:
    """
    Check if the connector is an HTTP V1 sink connector.
    
    Args:
        config: Connector configuration dictionary
        
    Returns:
        True if the connector is an HTTP V1 sink connector, False otherwise
    """
    connector_class = config.get("connector.class", "")
    return connector_class == "io.confluent.connect.http.HttpSinkConnector"


def transform_v1_to_v2(v1_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform a V1 HTTP sink connector configuration to a V2 configuration.
    
    Args:
        v1_config: V1 HTTP sink connector configuration
        
    Returns:
        V2 HTTP sink connector configuration
        
    Raises:
        Exception: If required fields are missing or transformation fails
    """
    try:
        # Extract base URL and path from http.api.url
        http_api_url = v1_config.get("http.api.url", "")
        if not http_api_url:
            raise Exception("Missing 'http.api.url' in V1 configuration.")

        parts = http_api_url.split("/")
        base_url = "/".join(parts[:3])
        api_path = "/" + "/".join(parts[3:])
        name = v1_config.get("name", "") + "_v2"

        # Initialize the V2 configuration
        v2_config = {
            "connector.class": "HttpSinkV2",
            "tasks.max": v1_config.get("tasks.max", 1),
            "apis.num": "1",
            "api1.http.api.path": api_path,
        }

        # Copy configurations using the mapping
        for v1_key, v2_key in V1_TO_V2_MAPPING.items():
            if v1_key in v1_config:
                v2_config[v2_key] = v1_config[v1_key]

        # Copy common configurations as is
        for common_key in COMMON_CONFIGS:
            if common_key in v1_config:
                v2_config[common_key] = v1_config[common_key]

        # Ensure http.api.base.url is set after copying configurations
        v2_config["http.api.base.url"] = base_url
        v2_config["name"] = name

        return v2_config
    except Exception as e:
        raise Exception(f"Error transforming V1 to V2 configuration: {e}") from e

