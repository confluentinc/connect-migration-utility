"""
HTTP Sink V1 to V2 Transformer

This module handles the transformation of HTTP Sink connector configurations
from V1 format (HttpSinkConnector) to V2 format (GenericHttpSinkConnector / HttpSinkV2).

Based on the Confluent HTTP Sink V2 connector specification.
"""

import logging
from typing import Any, Dict, List, Tuple, Optional
from urllib.parse import urlparse


class HttpV1ToV2Transformer:
    """
    Transforms HTTP Sink connector configurations from V1 to V2 format.
    
    V1 Connector: io.confluent.connect.http.HttpSinkConnector (HttpSink)
    V2 Connector: io.confluent.connect.http.sink.GenericHttpSinkConnector (HttpSinkV2)
    
    Key changes in V2:
    - Single http.api.url → http.api.base.url + api{N}.http.api.path
    - Support for multiple APIs (apis.num)
    - Many configs prefixed with api1. for API-specific settings
    - Some configs renamed (e.g., request.method → api1.http.request.method)
    - Some value format changes (e.g., behavior.on.error values)
    """
    
    # V1 connector class
    V1_HTTP_SINK_CONNECTOR = "io.confluent.connect.http.HttpSinkConnector"
    
    # V2 template ID
    V2_HTTP_SINK_TEMPLATE_ID = "HttpSinkV2"
    
    # V2 connector class
    V2_HTTP_SINK_CONNECTOR = "io.confluent.connect.http.sink.GenericHttpSinkConnector"
    
    # Configuration mappings from V1 to V2
    # Format: {v1_key: v2_key}
    V1_TO_V2_MAPPING = {
        # API-specific configs (prefixed with api1. in V2)
        "batch.json.as.array": "api1.batch.json.as.array",
        "batch.key.pattern": "api1.batch.key.pattern",
        "batch.max.size": "api1.max.batch.size",
        "batch.prefix": "api1.batch.prefix",
        "batch.separator": "api1.batch.separator",
        "batch.suffix": "api1.batch.suffix",
        "behavior.on.null.values": "api1.behavior.on.null.values",
        "header.separator": "api1.http.request.headers.separator",
        "headers": "api1.http.request.headers",
        "http.connect.timeout.ms": "api1.http.connect.timeout.ms",
        "http.request.timeout.ms": "api1.http.request.timeout.ms",
        "max.retries": "api1.max.retries",
        "regex.patterns": "api1.regex.patterns",
        "regex.replacements": "api1.regex.replacements",
        "regex.separator": "api1.regex.separator",
        "request.body.format": "api1.request.body.format",
        "request.method": "api1.http.request.method",
        "retry.backoff.ms": "api1.retry.backoff.ms",
        "retry.backoff.policy": "api1.retry.backoff.policy",
        "retry.on.status.codes": "api1.retry.on.status.codes",
        "sensitive.headers": "api1.http.request.sensitive.headers",
        "topics": "api1.topics",
        
        # Global/shared configs (same name in V2)
        "auth.type": "auth.type",
        "behavior.on.error": "behavior.on.error",
        "connection.password": "connection.password",
        "connection.user": "connection.user",
        "https.host.verifier.enabled": "https.host.verifier.enabled",
        "https.ssl.key.password": "https.ssl.key.password",
        "https.ssl.keystore.password": "https.ssl.keystore.password",
        "https.ssl.keystorefile": "https.ssl.keystorefile",
        "https.ssl.protocol": "https.ssl.protocol",
        "https.ssl.truststore.password": "https.ssl.truststore.password",
        "https.ssl.truststorefile": "https.ssl.truststorefile",
        "report.errors.as": "report.errors.as",
        
        # OAuth2 configs (same name in V2)
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
        
        # HTTP Proxy configs
        "http.proxy.host": "http.proxy.host",
        "http.proxy.port": "http.proxy.port",
        "http.proxy.user": "http.proxy.user",
        "http.proxy.password": "http.proxy.password",
    }
    
    # Configs that should be copied as-is (common across V1 and V2)
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
        "topics",  # Keep at root level as well as api1.topics
        "errors.tolerance",
        "errors.deadletterqueue.topic.name",
        "errors.deadletterqueue.topic.replication.factor",
    ]
    
    # Value transformations for specific configs
    # Format: {config_key: {v1_value: v2_value}}
    VALUE_TRANSFORMATIONS = {
        "behavior.on.error": {
            "ignore": "IGNORE",
            "fail": "FAIL",
            "log": "IGNORE",  # V1 'log' maps to V2 'IGNORE' with logging
        },
        "behavior.on.null.values": {
            "ignore": "IGNORE",
            "delete": "DELETE",
            "fail": "FAIL",
        },
        "request.body.format": {
            "string": "STRING",
            "json": "JSON",
        },
        "report.errors.as": {
            "error_string": "Error string",
            "http_response": "Http response",
        },
    }
    
    # Configs that are deprecated or not supported in V2
    DEPRECATED_CONFIGS = [
        "confluent.topic.bootstrap.servers",
        "confluent.topic.sasl.jaas.config",
        "confluent.topic.sasl.mechanism",
        "confluent.topic.security.protocol",
        "reporter.error.topic.name",
        "reporter.result.topic.name",
    ]
    
    def __init__(self, logger: logging.Logger = None):
        """
        Initialize the transformer.
        
        Args:
            logger: Optional logger instance. If not provided, creates a default one.
        """
        self.logger = logger or logging.getLogger(__name__)
    
    # ==================== Detection Methods ====================
    
    def is_http_v1(self, connector_class: str) -> bool:
        """
        Check if the connector class is an HTTP V1 sink connector.
        
        Args:
            connector_class: The connector class string to check
            
        Returns:
            True if it's a V1 HTTP sink connector, False otherwise
        """
        if not connector_class:
            return False
        return connector_class == self.V1_HTTP_SINK_CONNECTOR
    
    def is_http_v1_config(self, config: Dict[str, Any]) -> bool:
        """
        Check if the configuration is for an HTTP V1 sink connector.
        
        Args:
            config: Connector configuration dictionary
            
        Returns:
            True if it's a V1 HTTP sink connector config, False otherwise
        """
        connector_class = config.get("connector.class", "")
        return self.is_http_v1(connector_class)
    
    def is_http_v2(self, connector_class: str) -> bool:
        """
        Check if the connector class is an HTTP V2 sink connector.
        
        Args:
            connector_class: The connector class string to check
            
        Returns:
            True if it's a V2 HTTP sink connector, False otherwise
        """
        if not connector_class:
            return False
        return connector_class in (self.V2_HTTP_SINK_CONNECTOR, self.V2_HTTP_SINK_TEMPLATE_ID)
    
    def is_http_v2_config(self, config: Dict[str, Any]) -> bool:
        """
        Check if the configuration is for an HTTP V2 sink connector.
        
        Args:
            config: Connector configuration dictionary
            
        Returns:
            True if it's a V2 HTTP sink connector config, False otherwise
        """
        connector_class = config.get("connector.class", "")
        return self.is_http_v2(connector_class)
    
    # ==================== URL Parsing Methods ====================
    
    def parse_http_api_url(self, http_api_url: str) -> Tuple[str, str]:
        """
        Parse http.api.url into base URL and API path.
        
        Args:
            http_api_url: The full HTTP API URL from V1 config
            
        Returns:
            Tuple of (base_url, api_path)
        """
        if not http_api_url:
            return "", ""
        
        try:
            parsed = urlparse(http_api_url)
            # Base URL: scheme://netloc
            base_url = f"{parsed.scheme}://{parsed.netloc}"
            # API path: path (ensure it starts with /)
            api_path = parsed.path
            if not api_path:
                api_path = "/"
            elif not api_path.startswith("/"):
                api_path = "/" + api_path
            
            # Include query string in path if present
            if parsed.query:
                api_path += f"?{parsed.query}"
            
            return base_url, api_path
        except Exception as e:
            self.logger.warning(f"Failed to parse http.api.url '{http_api_url}': {e}")
            # Fallback: split by first 3 slashes
            parts = http_api_url.split("/")
            if len(parts) >= 3:
                base_url = "/".join(parts[:3])
                api_path = "/" + "/".join(parts[3:]) if len(parts) > 3 else "/"
                return base_url, api_path
            return http_api_url, "/"
    
    # ==================== Value Transformation Methods ====================
    
    def transform_value(self, key: str, value: Any) -> Tuple[Any, Optional[str]]:
        """
        Transform a configuration value from V1 to V2 format.
        
        Args:
            key: The configuration key (V1 format)
            value: The configuration value
            
        Returns:
            Tuple of (transformed_value, warning_message or None)
        """
        if key in self.VALUE_TRANSFORMATIONS and value is not None:
            value_str = str(value).lower()
            transformations = self.VALUE_TRANSFORMATIONS[key]
            if value_str in transformations:
                new_value = transformations[value_str]
                if new_value != value:
                    return new_value, f"Value for '{key}' transformed from '{value}' to '{new_value}'"
        return value, None
    
    # ==================== Translation Methods ====================
    
    def translate_v1_to_v2(self, config: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str], List[str]]:
        """
        Translate HTTP Sink V1 configuration to V2 format.
        
        Args:
            config: The V1 connector configuration dictionary
            
        Returns:
            Tuple of (translated_config, warnings_list, errors_list)
        """
        translated = {}
        warnings = []
        errors = []
        processed_keys = set()
        
        # 1. Set connector.class to V2 template ID
        if 'connector.class' in config:
            translated['connector.class'] = self.V2_HTTP_SINK_TEMPLATE_ID
            processed_keys.add('connector.class')
        
        # 2. Handle http.api.url → http.api.base.url + api1.http.api.path
        http_api_url = config.get('http.api.url', '')
        if http_api_url:
            base_url, api_path = self.parse_http_api_url(http_api_url)
            translated['http.api.base.url'] = base_url
            translated['api1.http.api.path'] = api_path
            processed_keys.add('http.api.url')
        else:
            errors.append("Missing required 'http.api.url' in V1 configuration")
        
        # 3. Set apis.num to 1 (single API in V1)
        translated['apis.num'] = "1"
        
        # 4. Apply V1 to V2 config mappings
        for v1_key, v2_key in self.V1_TO_V2_MAPPING.items():
            if v1_key in config:
                value = config[v1_key]
                # Transform value if needed
                transformed_value, warning = self.transform_value(v1_key, value)
                translated[v2_key] = transformed_value
                processed_keys.add(v1_key)
                if warning:
                    warnings.append(warning)
        
        # 5. Copy common configs
        for common_key in self.COMMON_CONFIGS:
            if common_key in config and common_key not in translated:
                translated[common_key] = config[common_key]
                processed_keys.add(common_key)
        
        # 6. Handle name (append _v2 suffix)
        if 'name' in config:
            original_name = config['name']
            translated['name'] = original_name 
            processed_keys.add('name')
        
        # 7. Handle tasks.max
        if 'tasks.max' in config:
            translated['tasks.max'] = config['tasks.max']
            processed_keys.add('tasks.max')
        else:
            translated['tasks.max'] = 1
            warnings.append("tasks.max set to default value of 1")
        
        # 8. Handle deprecated configs
        for deprecated_key in self.DEPRECATED_CONFIGS:
            if deprecated_key in config:
                processed_keys.add(deprecated_key)
                warnings.append(f"DEPRECATED: '{deprecated_key}' is not used in V2 and has been removed")
    
        
        # 10. Handle transforms (SMTs)
        transform_keys = [k for k in config.keys() if k.startswith('transforms')]
        for transform_key in transform_keys:
            translated[transform_key] = config[transform_key]
            processed_keys.add(transform_key)
        
        # 11. Copy any remaining unprocessed configs
        for key, value in config.items():
            if key not in processed_keys and key not in translated:
                # Don't copy if it's a known deprecated config
                if key not in self.DEPRECATED_CONFIGS:
                    translated[key] = value
                    warnings.append(f"Copied unrecognized config '{key}' as-is (please verify compatibility)")
        
        # 12. Add behavior change notes
        if 'behavior.on.error' not in config:
            warnings.append("BEHAVIOR CHANGE: Default behavior.on.error in V2 is 'FAIL' (V1 default was 'ignore'). Explicitly set if needed.")
        
        if 'request.body.format' not in config:
            warnings.append("BEHAVIOR CHANGE: Default request.body.format in V2 is 'JSON' (V1 default was 'string'). Explicitly set if needed.")
        
        self.logger.info(f"Translated {len(processed_keys)} HTTP V1 configs to V2 format")
        self.logger.debug(f"Translated config keys: {list(translated.keys())}")
        
        return translated, warnings, errors


# ==================== Module-level Functions (for backward compatibility) ====================

# Global transformer instance
_transformer = None

def _get_transformer() -> HttpV1ToV2Transformer:
    """Get or create the global transformer instance."""
    global _transformer
    if _transformer is None:
        _transformer = HttpV1ToV2Transformer()
    return _transformer


def is_http_v1_connector(config: Dict[str, Any]) -> bool:
    """
    Check if the connector is an HTTP V1 sink connector.
    
    Args:
        config: Connector configuration dictionary
        
    Returns:
        True if the connector is an HTTP V1 sink connector, False otherwise
    """
    return _get_transformer().is_http_v1_config(config)


def is_http_v2_connector(config: Dict[str, Any]) -> bool:
    """
    Check if the connector is an HTTP V2 sink connector.
    
    Args:
        config: Connector configuration dictionary
        
    Returns:
        True if the connector is an HTTP V2 sink connector, False otherwise
    """
    return _get_transformer().is_http_v2_config(config)


def transform_v1_to_v2(v1_config: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform a V1 HTTP sink connector configuration to a V2 configuration.
    
    If the config is already V2, it is returned as-is without transformation.
    
    This is a backward-compatible wrapper that returns only the transformed config.
    For full results including warnings and errors, use the class method directly.
    
    Args:
        v1_config: V1 HTTP sink connector configuration
        
    Returns:
        V2 HTTP sink connector configuration
        
    Raises:
        Exception: If required fields are missing or transformation fails
    """
    transformer = _get_transformer()
    
    # If already V2, return as-is
    if transformer.is_http_v2_config(v1_config):
        transformer.logger.info("Config is already HTTP V2, keeping as-is")
        return v1_config.copy()
    
    try:
        translated, warnings, errors = transformer.translate_v1_to_v2(v1_config)
        
        # Log warnings
        for warning in warnings:
            transformer.logger.debug(f"HTTP V1→V2 Warning: {warning}")
        
        # If there are critical errors, raise exception
        if errors:
            error_msg = "; ".join(errors)
            raise Exception(f"HTTP V1 to V2 transformation failed: {error_msg}")
        
        return translated
    except Exception as e:
        raise Exception(f"Error transforming V1 to V2 configuration: {e}") from e


def transform_v1_to_v2_full(v1_config: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str], List[str]]:
    """
    Transform a V1 HTTP sink connector configuration to a V2 configuration.
    
    If the config is already V2, it is returned as-is without transformation.
    
    Returns full results including warnings and errors.
    
    Args:
        v1_config: V1 HTTP sink connector configuration
        
    Returns:
        Tuple of (v2_config, warnings, errors)
    """
    transformer = _get_transformer()
    
    # If already V2, return as-is
    if transformer.is_http_v2_config(v1_config):
        transformer.logger.info("Config is already HTTP V2, keeping as-is")
        return v1_config.copy(), [], []
    
    return transformer.translate_v1_to_v2(v1_config)
