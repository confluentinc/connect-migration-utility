"""
Apache Connect Migration Utility
Copyright 2024-2025 The Apache Software Foundation

This product includes software developed at The Apache Software Foundation.
"""

import json
import logging
import requests
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import re

class ConnectValidator:
    """Validates Kafka Connect configurations for migration compatibility."""
    
    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)
        
    
    def validate_connector_config(self, config: Dict[str, Any], connector_name: str) -> Dict[str, Any]:
        """
        Validate a single connector configuration.
        
        Args:
            config: Connector configuration dictionary
            connector_name: Name of the connector
            
        Returns:
            Dictionary containing validation results
        """
        validation_result = {
            'connector_name': connector_name,
            'is_valid': True,
            'errors': [],
            'warnings': [],
            'recommendations': [],
            'migration_complexity': 'low'
        }
        
        try:
            # Check for required connector.class
            if 'connector.class' not in config:
                validation_result['errors'].append("Missing required 'connector.class' property")
                validation_result['is_valid'] = False
                return validation_result
            
            connector_class = config['connector.class']
            
            # Validate required properties
            self._validate_required_properties(config, connector_class, validation_result)
            
            # Check for deprecated properties
            self._check_deprecated_properties(config, validation_result)
            
            # Check for unsupported properties
            self._check_unsupported_properties(config, validation_result)
            
            # Validate transforms
            self._validate_transforms(config, validation_result)
            
            # Validate predicates
            self._validate_predicates(config, validation_result)
            
            # Check JDBC URL format if applicable
            if 'connection.url' in config:
                self._validate_jdbc_url(config['connection.url'], validation_result)
            
            # Determine migration complexity
            validation_result['migration_complexity'] = self._assess_migration_complexity(validation_result)
            
        except Exception as e:
            validation_result['errors'].append(f"Validation error: {str(e)}")
            validation_result['is_valid'] = False
            self.logger.error(f"Error validating connector {connector_name}: {e}")
        
        return validation_result
    
    def validate_with_confluent_cloud(self, config: Dict[str, Any], connector_name: str,
                                    environment_id: str, kafka_cluster_id: str,
                                    bearer_token: str, disable_ssl_verify: bool = False) -> Dict[str, Any]:
        """
        Validate connector configuration against Confluent Cloud API.
        
        Args:
            config: Connector configuration dictionary
            connector_name: Name of the connector
            environment_id: Confluent Cloud environment ID
            kafka_cluster_id: Confluent Cloud LKC cluster ID
            bearer_token: Bearer token for authentication
            disable_ssl_verify: Whether to disable SSL verification
            
        Returns:
            Dictionary containing validation results from Confluent Cloud
        """
        validation_result = {
            'connector_name': connector_name,
            'validation_source': 'confluent_cloud',
            'is_valid': False,
            'errors': [],
            'warnings': [],
            'api_response': None,
            'status_code': None
        }
        
        try:
            # Extract connector class to determine the validation endpoint
            connector_class = config.get('connector.class', '')
            if not connector_class:
                validation_result['errors'].append("Missing 'connector.class' property")
                return validation_result
            
            # Extract connector type from class name (e.g., MongoDbAtlasSource from io.confluent.connect.mongodb.MongoDbAtlasSource)
            connector_type = connector_class.split('.')[-1]
            
            # Build validation URL
            base_url = "https://api.confluent.cloud/internal/environments"
            validation_url = f"{base_url}/{environment_id}/clusters/{kafka_cluster_id}/connector-plugins/{connector_type}/config/async-validate"
            
            # Prepare headers
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {bearer_token}"
            }
            
            # Prepare request body with config
            request_body = {
                "config": config
            }
            
            self.logger.info(f"Validating connector '{connector_name}' against Confluent Cloud API: {validation_url}")
            
            # Make API request
            response = requests.post(
                validation_url,
                json=request_body,
                headers=headers,
                verify=not disable_ssl_verify,
                timeout=3  # 3 second timeout for initial validation request
            )
            
            validation_result['status_code'] = response.status_code
            validation_result['api_response'] = response.text
            
            if response.status_code == 200:
                try:
                    response_data = response.json()
                    
                    # Handle asynchronous validation response
                    if 'id' in response_data and 'status' in response_data:
                        validation_id = response_data['id']
                        status_phase = response_data['status'].get('phase', 'UNKNOWN')
                        
                        validation_result['validation_id'] = validation_id
                        validation_result['status_phase'] = status_phase
                        validation_result['api_response'] = response_data
                        
                        if status_phase == 'VALIDATING':
                            validation_result['is_valid'] = None  # Still validating
                            validation_result['warnings'].append(f"Validation is in progress (ID: {validation_id}). Getting validation status...")
                            
                            # Extract connector type from config for polling
                            connector_type = config.get('connector.class', '').split('.')[-1] if config.get('connector.class') else 'Unknown'
                            
                            # Automatically get validation status instead of just warning
                            try:
                                poll_result = self.poll_validation_status(
                                    validation_id=validation_id,
                                    connector_type=connector_type,
                                    environment_id=environment_id,
                                    kafka_cluster_id=kafka_cluster_id,
                                    bearer_token=bearer_token,
                                    disable_ssl_verify=disable_ssl_verify,
                                    max_wait_time=60
                                )
                                
                                # Update validation result with polling results
                                validation_result.update(poll_result)
                                validation_result['auto_polled'] = True
                                
                            except Exception as e:
                                validation_result['warnings'].append(f"Auto-polling failed: {str(e)}. Manual polling may be required.")
                                
                        elif status_phase == 'COMPLETED':
                            validation_result['is_valid'] = True
                            # Parse final validation results if available
                            self._parse_validation_results(response_data, validation_result)
                        elif status_phase == 'FAILED':
                            validation_result['is_valid'] = False
                            validation_result['errors'].append(f"Validation failed (ID: {validation_id})")
                        else:
                            validation_result['warnings'].append(f"Unknown validation status: {status_phase}")
                    else:
                        # Handle direct validation results (non-async response)
                        validation_result['is_valid'] = True
                        self._parse_validation_results(response_data, validation_result)
                        
                except json.JSONDecodeError:
                    validation_result['errors'].append("Invalid JSON response from Confluent Cloud API")
                    validation_result['is_valid'] = False
                    
            elif response.status_code == 401:
                validation_result['errors'].append("Authentication failed. Check your bearer token.")
            elif response.status_code == 403:
                validation_result['errors'].append("Access denied. Check your permissions.")
            elif response.status_code == 404:
                validation_result['errors'].append(f"Connector type '{connector_type}' not found or not supported.")
            else:
                validation_result['errors'].append(f"API request failed with status {response.status_code}: {response.text}")
                
        except requests.exceptions.RequestException as e:
            validation_result['errors'].append(f"Network error: {str(e)}")
        except Exception as e:
            validation_result['errors'].append(f"Validation error: {str(e)}")
            self.logger.error(f"Error validating connector {connector_name} with Confluent Cloud: {e}")
        
        return validation_result
    
    def _parse_validation_results(self, response_data: Dict[str, Any], validation_result: Dict[str, Any]) -> None:
        """Parse validation results from Confluent Cloud API response."""
        # Parse validation results from response
        if 'configs' in response_data:
            for config_item in response_data['configs']:
                if 'errors' in config_item and config_item['errors']:
                    validation_result['errors'].extend(config_item['errors'])
                    validation_result['is_valid'] = False
                
                if 'warnings' in config_item and config_item['warnings']:
                    validation_result['warnings'].extend(config_item['warnings'])
        
        # Check for top-level errors
        if 'errors' in response_data and response_data['errors']:
            validation_result['errors'].extend(response_data['errors'])
            validation_result['is_valid'] = False
    
    def poll_validation_status(self, validation_id: str, connector_type: str, environment_id: str, 
                             kafka_cluster_id: str, bearer_token: str,
                             disable_ssl_verify: bool = False, max_wait_time: int = 60, 
                             start_time: float = None, poll_attempts: int = 0) -> Dict[str, Any]:
        """
        Recursively poll for validation status until completion or timeout.
        
        Args:
            validation_id: Validation ID returned from async validation
            connector_type: Type of connector (e.g., MongoDbAtlasSource)
            environment_id: Confluent Cloud environment ID
            kafka_cluster_id: Confluent Cloud LKC cluster ID
            bearer_token: Bearer token for authentication
            disable_ssl_verify: Whether to disable SSL verification
            max_wait_time: Maximum time to wait in seconds
            start_time: Internal parameter for tracking timeout (auto-set on first call)
            poll_attempts: Internal parameter for tracking attempts (auto-set on first call)
            
        Returns:
            Dictionary containing final validation results
        """
        import time
        
        # Initialize start_time and poll_attempts on first call
        if start_time is None:
            start_time = time.time()
            poll_attempts = 0
        
        # Check timeout
        if (time.time() - start_time) >= max_wait_time:
            return {
                'validation_id': validation_id,
                'connector_type': connector_type,
                'final_status': 'TIMEOUT',
                'is_valid': False,
                'errors': [],
                'warnings': [f"Validation polling timed out after {max_wait_time} seconds"],
                'api_response': None,
                'poll_attempts': poll_attempts
            }
        
        try:
            # Build status check URL using the specific format you mentioned
            base_url = "https://api.confluent.cloud/internal/environments"
            status_url = f"{base_url}/{environment_id}/clusters/{kafka_cluster_id}/connector-plugins/{connector_type}/config/async-validate/{validation_id}"
            
            headers = {
                "Authorization": f"Bearer {bearer_token}"
            }
            
            self.logger.info(f"Polling validation status for ID {validation_id} (attempt {poll_attempts + 1}): {status_url}")
            
            response = requests.get(
                status_url,
                headers=headers,
                verify=not disable_ssl_verify,
                timeout=3  # 3 second timeout for each individual GET request
            )
            
            poll_attempts += 1
            
            if response.status_code == 200:
                response_data = response.json()
                
                if 'status' in response_data:
                    status_phase = response_data['status'].get('phase', 'UNKNOWN')
                    
                    if status_phase == 'COMPLETED':
                        self.logger.info(f"Validation completed for ID {validation_id}")
                        return {
                            'validation_id': validation_id,
                            'connector_type': connector_type,
                            'final_status': status_phase,
                            'is_valid': True,
                            'errors': [],
                            'warnings': [],
                            'api_response': response_data,
                            'poll_attempts': poll_attempts
                        }
                        
                    elif status_phase == 'FAILED':
                        self.logger.info(f"Validation failed for ID {validation_id}")
                        return {
                            'validation_id': validation_id,
                            'connector_type': connector_type,
                            'final_status': status_phase,
                            'is_valid': False,
                            'errors': [f"Validation failed (ID: {validation_id})"],
                            'warnings': [],
                            'api_response': response_data,
                            'poll_attempts': poll_attempts
                        }
                        
                    elif status_phase == 'VALIDATING':
                        self.logger.info(f"Validation still in progress for ID {validation_id}, phase: {status_phase}. Recursively polling again in 3 seconds...")
                        
                        # Wait 3 seconds before recursive call
                        time.sleep(3)
                        
                        # Recursive call - if status_phase == 'VALIDATING', recur the GET call
                        return self.poll_validation_status(
                            validation_id=validation_id,
                            connector_type=connector_type,
                            environment_id=environment_id,
                            kafka_cluster_id=kafka_cluster_id,
                            bearer_token=bearer_token,
                            disable_ssl_verify=disable_ssl_verify,
                            max_wait_time=max_wait_time,
                            start_time=start_time,
                            poll_attempts=poll_attempts
                        )
                        
                    else:
                        self.logger.warning(f"Unknown status phase: {status_phase}")
                        return {
                            'validation_id': validation_id,
                            'connector_type': connector_type,
                            'final_status': status_phase,
                            'is_valid': False,
                            'errors': [],
                            'warnings': [f"Unknown status phase: {status_phase}"],
                            'api_response': response_data,
                            'poll_attempts': poll_attempts
                        }
                else:
                    return {
                        'validation_id': validation_id,
                        'connector_type': connector_type,
                        'final_status': 'UNKNOWN',
                        'is_valid': False,
                        'errors': ["No status information in response"],
                        'warnings': [],
                        'api_response': response_data,
                        'poll_attempts': poll_attempts
                    }
                    
            elif response.status_code == 404:
                return {
                    'validation_id': validation_id,
                    'connector_type': connector_type,
                    'final_status': 'NOT_FOUND',
                    'is_valid': False,
                    'errors': [f"Validation ID {validation_id} not found"],
                    'warnings': [],
                    'api_response': None,
                    'poll_attempts': poll_attempts
                }
            else:
                return {
                    'validation_id': validation_id,
                    'connector_type': connector_type,
                    'final_status': 'API_ERROR',
                    'is_valid': False,
                    'errors': [f"Status check failed with status {response.status_code}: {response.text}"],
                    'warnings': [],
                    'api_response': None,
                    'poll_attempts': poll_attempts
                }
                
        except requests.exceptions.RequestException as e:
            return {
                'validation_id': validation_id,
                'connector_type': connector_type,
                'final_status': 'NETWORK_ERROR',
                'is_valid': False,
                'errors': [f"Network error during polling: {str(e)}"],
                'warnings': [],
                'api_response': None,
                'poll_attempts': poll_attempts
            }
        except Exception as e:
            return {
                'validation_id': validation_id,
                'connector_type': connector_type,
                'final_status': 'ERROR',
                'is_valid': False,
                'errors': [f"Error during polling: {str(e)}"],
                'warnings': [],
                'api_response': None,
                'poll_attempts': poll_attempts
            }
    
    def _validate_required_properties(self, config: Dict[str, Any], connector_class: str,
                                    validation_result: Dict[str, Any]) -> None:
        """Validate that required properties are present."""
        if connector_class in self.required_properties:
            required_props = self.required_properties[connector_class]
            for prop in required_props:
                if prop not in config:
                    validation_result['errors'].append(f"Missing required property: {prop}")
                    validation_result['is_valid'] = False
    
    def _check_deprecated_properties(self, config: Dict[str, Any], 
                                   validation_result: Dict[str, Any]) -> None:
        """Check for deprecated properties and provide migration guidance."""
        for deprecated_prop in self.deprecated_properties:
            if deprecated_prop in config:
                validation_result['warnings'].append(
                    f"Deprecated property '{deprecated_prop}': {self.deprecated_properties[deprecated_prop]}"
                )
    
    def _check_unsupported_properties(self, config: Dict[str, Any], 
                                    validation_result: Dict[str, Any]) -> None:
        """Check for properties not supported in Fully Managed Kafka Connect."""
        for unsupported_prop in self.unsupported_properties:
            if unsupported_prop in config:
                validation_result['errors'].append(
                    f"Property '{unsupported_prop}' is not supported in Fully Managed Kafka Connect"
                )
                validation_result['is_valid'] = False
    
    def _validate_transforms(self, config: Dict[str, Any], 
                           validation_result: Dict[str, Any]) -> None:
        """Validate transforms configuration."""
        transforms = config.get('transforms', '')
        if transforms:
            transform_list = [t.strip() for t in transforms.split(',') if t.strip()]
            
            for transform in transform_list:
                transform_type = config.get(f'transforms.{transform}.type', '')
                if transform_type in self.unsupported_transforms:
                    validation_result['errors'].append(
                        f"Transform '{transform_type}' is not supported in Fully Managed Kafka Connect"
                    )
                    validation_result['is_valid'] = False
                elif not transform_type:
                    validation_result['warnings'].append(
                        f"Transform '{transform}' is missing type configuration"
                    )
    
    def _validate_predicates(self, config: Dict[str, Any], 
                           validation_result: Dict[str, Any]) -> None:
        """Validate predicates configuration."""
        predicates = config.get('predicates', '')
        if predicates:
            predicate_list = [p.strip() for p in predicates.split(',') if p.strip()]
            
            for predicate in predicate_list:
                predicate_type = config.get(f'predicates.{predicate}.type', '')
                if predicate_type in self.unsupported_predicates:
                    validation_result['errors'].append(
                        f"Predicate '{predicate_type}' is not supported in Fully Managed Kafka Connect"
                    )
                    validation_result['is_valid'] = False
                elif not predicate_type:
                    validation_result['warnings'].append(
                        f"Predicate '{predicate}' is missing type configuration"
                    )
    
    def _validate_jdbc_url(self, jdbc_url: str, validation_result: Dict[str, Any]) -> None:
        """Validate JDBC URL format."""
        if not jdbc_url.startswith('jdbc:'):
            validation_result['errors'].append("Invalid JDBC URL format: must start with 'jdbc:'")
            validation_result['is_valid'] = False
            return
        
        # Check for common JDBC drivers
        supported_drivers = ['mysql', 'postgresql', 'oracle', 'sqlserver', 'snowflake']
        driver_found = any(driver in jdbc_url.lower() for driver in supported_drivers)
        
        if not driver_found:
            validation_result['warnings'].append(
                "JDBC URL may contain unsupported driver. Supported drivers: " + 
                ", ".join(supported_drivers)
            )
    
    def _assess_migration_complexity(self, validation_result: Dict[str, Any]) -> str:
        """Assess the complexity of migration based on validation results."""
        error_count = len(validation_result['errors'])
        warning_count = len(validation_result['warnings'])
        
        if error_count == 0 and warning_count == 0:
            return 'low'
        elif error_count <= 2 and warning_count <= 3:
            return 'medium'
        else:
            return 'high'
    
    def validate_connector_file(self, file_path: Path) -> Dict[str, Any]:
        """
        Validate a connector configuration file.
        
        Args:
            file_path: Path to the connector configuration file
            
        Returns:
            Dictionary containing validation results
        """
        try:
            with open(file_path, 'r') as f:
                config_data = json.load(f)
            
            # Extract connector name and config
            if 'config' in config_data:
                config = config_data['config']
                connector_name = config.get('name', file_path.stem)
            else:
                config = config_data
                connector_name = config.get('name', file_path.stem)
            
            return self.validate_connector_config(config, connector_name)
            
        except json.JSONDecodeError as e:
            return {
                'connector_name': file_path.stem,
                'is_valid': False,
                'errors': [f"Invalid JSON format: {str(e)}"],
                'warnings': [],
                'recommendations': [],
                'migration_complexity': 'unknown'
            }
        except Exception as e:
            return {
                'connector_name': file_path.stem,
                'is_valid': False,
                'errors': [f"File reading error: {str(e)}"],
                'warnings': [],
                'recommendations': [],
                'migration_complexity': 'unknown'
            }
    
    def validate_directory(self, directory_path: Path) -> Dict[str, Any]:
        """
        Validate all connector configuration files in a directory.
        
        Args:
            directory_path: Path to the directory containing connector configurations
            
        Returns:
            Dictionary containing validation summary and individual results
        """
        validation_summary = {
            'total_connectors': 0,
            'valid_connectors': 0,
            'invalid_connectors': 0,
            'total_errors': 0,
            'total_warnings': 0,
            'complexity_distribution': {'low': 0, 'medium': 0, 'high': 0},
            'connector_results': []
        }
        
        try:
            for file_path in directory_path.glob("*.json"):
                if file_path.is_file():
                    validation_summary['total_connectors'] += 1
                    result = self.validate_connector_file(file_path)
                    validation_summary['connector_results'].append(result)
                    
                    if result['is_valid']:
                        validation_summary['valid_connectors'] += 1
                    else:
                        validation_summary['invalid_connectors'] += 1
                    
                    validation_summary['total_errors'] += len(result['errors'])
                    validation_summary['total_warnings'] += len(result['warnings'])
                    
                    complexity = result.get('migration_complexity', 'unknown')
                    if complexity in validation_summary['complexity_distribution']:
                        validation_summary['complexity_distribution'][complexity] += 1
            
        except Exception as e:
            self.logger.error(f"Error validating directory {directory_path}: {e}")
        
        return validation_summary
    
    def generate_validation_report(self, validation_summary: Dict[str, Any], 
                                 output_dir: Path) -> Path:
        """
        Generate a validation report and save it to a file.
        
        Args:
            validation_summary: Validation summary dictionary
            output_dir: Directory to save the report
            
        Returns:
            Path to the generated report file
        """
        report_lines = []
        report_lines.append("=" * 80)
        report_lines.append("KAFKA CONNECT VALIDATION REPORT")
        report_lines.append("=" * 80)
        report_lines.append("")
        
        # Summary statistics
        report_lines.append("SUMMARY STATISTICS")
        report_lines.append("-" * 40)
        report_lines.append(f"Total Connectors: {validation_summary['total_connectors']}")
        report_lines.append(f"Valid Connectors: {validation_summary['valid_connectors']}")
        report_lines.append(f"Invalid Connectors: {validation_summary['invalid_connectors']}")
        report_lines.append(f"Total Errors: {validation_summary['total_errors']}")
        report_lines.append(f"Total Warnings: {validation_summary['total_warnings']}")
        report_lines.append("")
        
        # Complexity distribution
        report_lines.append("MIGRATION COMPLEXITY DISTRIBUTION")
        report_lines.append("-" * 40)
        for complexity, count in validation_summary['complexity_distribution'].items():
            report_lines.append(f"{complexity.title()}: {count}")
        report_lines.append("")
        
        # Individual connector results
        report_lines.append("INDIVIDUAL CONNECTOR RESULTS")
        report_lines.append("-" * 40)
        
        for result in validation_summary['connector_results']:
            report_lines.append(f"Connector: {result['connector_name']}")
            report_lines.append(f"Status: {'✓ VALID' if result['is_valid'] else '✗ INVALID'}")
            report_lines.append(f"Migration Complexity: {result['migration_complexity'].title()}")
            
            if result['errors']:
                report_lines.append("Errors:")
                for error in result['errors']:
                    report_lines.append(f"  - {error}")
            
            if result['warnings']:
                report_lines.append("Warnings:")
                for warning in result['warnings']:
                    report_lines.append(f"  - {warning}")
            
            if result['recommendations']:
                report_lines.append("Recommendations:")
                for rec in result['recommendations']:
                    report_lines.append(f"  - {rec}")
            
            report_lines.append("")
        
        # Save report to file
        report_file = output_dir / "connect_validation_report.txt"
        try:
            with open(report_file, 'w') as f:
                f.write('\n'.join(report_lines))
            self.logger.info(f"Validation report saved to: {report_file}")
        except Exception as e:
            self.logger.error(f"Failed to save validation report: {e}")
            raise
        
        return report_file


def validate_connector_config(config: Dict[str, Any], connector_name: str) -> Dict[str, Any]:
    """Convenience function to validate a single connector configuration."""
    validator = ConnectValidator()
    return validator.validate_connector_config(config, connector_name)


def validate_connector_file(file_path: Path) -> Dict[str, Any]:
    """Convenience function to validate a connector configuration file."""
    validator = ConnectValidator()
    return validator.validate_connector_file(file_path)


def validate_directory(directory_path: Path) -> Dict[str, Any]:
    """Convenience function to validate all connectors in a directory."""
    validator = ConnectValidator()
    return validator.validate_directory(directory_path)


def validate_with_confluent_cloud(config: Dict[str, Any], connector_name: str,
                                environment_id: str, kafka_cluster_id: str,
                                bearer_token: str, disable_ssl_verify: bool = False) -> Dict[str, Any]:
    """Convenience function to validate connector configuration against Confluent Cloud API."""
    validator = ConnectValidator()
    return validator.validate_with_confluent_cloud(
        config, connector_name, environment_id, kafka_cluster_id, 
        bearer_token, disable_ssl_verify
    )


def poll_validation_status(validation_id: str, connector_type: str, environment_id: str, 
                          kafka_cluster_id: str, bearer_token: str,
                          disable_ssl_verify: bool = False, max_wait_time: int = 60) -> Dict[str, Any]:
    """Convenience function to poll validation status from Confluent Cloud API."""
    validator = ConnectValidator()
    return validator.poll_validation_status(
        validation_id, connector_type, environment_id, kafka_cluster_id, 
        bearer_token, disable_ssl_verify, max_wait_time
    )


def validate_with_confluent_cloud_and_poll(config: Dict[str, Any], connector_name: str,
                                         environment_id: str, kafka_cluster_id: str,
                                         bearer_token: str, disable_ssl_verify: bool = False,
                                         max_wait_time: int = 60) -> Dict[str, Any]:
    """
    Convenience function to validate connector configuration and poll for results.
    This combines both the initial validation request and status polling.
    Note: The main validate_with_confluent_cloud method now automatically polls when needed.
    """
    validator = ConnectValidator()
    
    # Initial validation request (now includes automatic polling)
    result = validator.validate_with_confluent_cloud(
        config, connector_name, environment_id, kafka_cluster_id, 
        bearer_token, disable_ssl_verify
    )
    
    # Check if auto-polling was performed
    if result.get('auto_polled'):
        result['polled_for_results'] = True
        print(f"Auto-polling completed for {connector_name}")
    
    return result
