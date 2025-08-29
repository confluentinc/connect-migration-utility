"""
Apache Connect Migration Utility
Copyright 2024-2025 The Apache Software Foundation

This product includes software developed at The Apache Software Foundation.
"""

#!/usr/bin/env python3
import argparse
import logging
import sys
from pathlib import Path
from typing import Any, Dict
from config_discovery import ConfigDiscovery
from connector_comparator import ConnectorComparator
from summary import generate_migration_summary
import json

from create_connector import ConnectorCreator
from offset_manager import OffsetManager

FM_CONFIGS_DIR = "fm_configs"
SM_CONFIGS_DIR = "sm_configs_compiled"


def setup_logging(output_dir: Path):
    """Setup logging configuration"""
    log_file = output_dir / 'migration.log'

    # Clear any existing handlers to avoid conflicts
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Create file handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)

    # Create console handler (explicitly for stdout)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.INFO)

    # Configure root logger
    root_logger.setLevel(logging.INFO)
    root_logger.addHandler(file_handler)
    root_logger.addHandler(console_handler)

def write_fm_configs_to_file(fm_configs: Dict[str, Any], output_dir: Path, logger: logging.Logger):
    """Write FM configs to file"""
    # This function writes FM configs to the specified output directory as JSON files.
    for connector_name, fm_config in fm_configs.items():
        errors = fm_config.get('errors', [])
        if errors:
            # There are mapping errors, save to unsuccessful_configs_with_errors
            complete_dir = output_dir / FM_CONFIGS_DIR / ConnectorComparator.UNSUCCESSFUL_CONFIGS_DIR
            complete_dir.mkdir(exist_ok=True)
            config_file = complete_dir / f"{connector_name}.json"
        else:
            # There are no mapping errors, save to successful_configs
            complete_dir = output_dir / FM_CONFIGS_DIR / ConnectorComparator.SUCCESSFUL_CONFIGS_DIR
            complete_dir.mkdir(exist_ok=True)
            config_file = complete_dir / f"{connector_name}.json"
        with open(config_file, 'w') as f:
            json.dump(fm_config, f, indent=2)

    logger.info(f"Saved {len(fm_configs)} FM configurations to {output_dir}")

    # Save all FM configs
    all_configs_file = output_dir / 'all_fm_configs.json'
    with open(all_configs_file, 'w') as f:
        json.dump(fm_configs, f, indent=2)

    logger.info(f"Saved {len(fm_configs)} FM configurations to {all_configs_file}")


def main():
    parser = argparse.ArgumentParser(description="Connector Migration Tool")
    parser.add_argument('--worker-urls', type=str, help='Comma-separated list of worker URLs')
    parser.add_argument('--worker-urls-file', type=str, help='Path to file containing worker URLs')
    parser.add_argument('--config-file', type=str, help='Path to JSON file containing connector configurations')
    parser.add_argument('--config-dir', type=str, help='Path to directory containing connector config JSON files')
    parser.add_argument('--redact', action='store_true', help='Redact sensitive configurations')
    parser.add_argument('--sensitive-file', type=str, help='Path to file containing sensitive config keys')
    parser.add_argument('--worker-config-file', type=str, help='Path to file containing additional worker configs (key=value)')
    parser.add_argument('--output-dir', type=str, default='output', help='Output directory for all files')

    # Confluent Cloud credentials for FM transforms (optional)
    parser.add_argument('--create-connector', type=bool, default=False, help='Whether to create connectors or not')
    parser.add_argument('--environment', type=str, choices=['prod', 'stag', 'devel'], help='Environment to create connectors in (choose from prod, stag, devel)')
    parser.add_argument('--offsets-required', type=bool, default=False, help='Whether to create connectors with offsets')
    parser.add_argument('--env-id', type=str, help='Confluent Cloud environment ID')
    parser.add_argument('--lkc-id', type=str, help='Confluent Cloud LKC cluster ID')
    parser.add_argument('--stop-connector', type=bool, default=False, help='Whether to stop connectors or not')
    parser.add_argument('--kafka-api-key', type=str, help='Kafka API key for LKC cluster')
    parser.add_argument('--kafka-api-secret', type=str, help='Kafka API secret for LKC cluster')
    parser.add_argument('--bearer-token', type=str, help='Confluent Cloud bearer token (api_key:api_secret) (or use --prompt-bearer-token for secure input)')
    parser.add_argument('--prompt-bearer-token', action='store_true', help='Prompt for bearer token securely (recommended)')
    parser.add_argument('--disable-ssl-verify', action='store_true', help='Disable SSL certificate verification for HTTPS requests')
    parser.add_argument('--semantic-cache-folder', type=str, help='Cache folder for sentence transformer models (default: auto-detected from pip installation)')


    args = parser.parse_args()

    # Create output directory if it doesn't exist
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / FM_CONFIGS_DIR).mkdir(exist_ok=True)

    # Setup logging
    setup_logging(output_dir)
    logger = logging.getLogger(__name__)

    # Initialize offset manager
    offset_manager = OffsetManager.get_instance(logger)

    # Handle bearer token input
    bearer_token = args.bearer_token
    if args.prompt_bearer_token:
        import getpass
        bearer_token = getpass.getpass("Enter Confluent Cloud bearer token: ")
        if not bearer_token:
            logger.error("Bearer token is required when using --prompt-bearer-token")
            sys.exit(1)

    try:
        discovery = None
        if getattr(args, 'worker_urls', None) or getattr(args, 'worker_urls_file', None):
            discovery = ConfigDiscovery(
                worker_urls=getattr(args, 'worker_urls', None),
                worker_urls_file=getattr(args, 'worker_urls_file', None),
                redact=getattr(args, 'redact', None),
                output_dir=output_dir,
                sensitive_file=getattr(args, 'sensitive_file', None),
                worker_config_file=getattr(args, 'worker_config_file', None),
                disable_ssl_verify=getattr(args, 'disable_ssl_verify', None)
            )

        # Step 1: Get Connector Configs (either from discovery, file, or directory)
        if getattr(args, 'config_file', None) and getattr(args, 'config_dir', None):
            logger.error("Cannot specify both --config-file and --config-dir. Please use only one.")
            sys.exit(1)
        elif getattr(args, 'config_file', None):
            logger.info(f"Reading connector configurations from file: {args.config_file}")
            connectors_json = Path(args.config_file)
            if not connectors_json.exists():
                raise FileNotFoundError(f"Config file not found: {args.config_file}")
            logger.info(f"Using config file: {connectors_json}")
        elif getattr(args, 'config_dir', None):
            logger.info(f"Reading connector configurations from directory: {args.config_dir}")
            config_dir = Path(args.config_dir)
            if not config_dir.exists() or not config_dir.is_dir():
                raise FileNotFoundError(f"Config directory not found or is not a directory: {args.config_dir}")
            merged_dir = output_dir / 'sm_configs_compiled'
            merged_dir.mkdir(exist_ok=True)
            all_connectors_dict = {}  # Accumulate all connectors here
            for file in sorted(config_dir.iterdir()):
                ConnectorComparator.parse_connector_file(file, all_connectors_dict, logger)
            # Validation: ensure at least one connector was found
            if not all_connectors_dict:
                logger.error(f"No valid connector configs found in directory: {args.config_dir}")
                sys.exit(1)
            # Write all connectors to a single file
            all_connectors_path = merged_dir / 'combined_connectors_sm_configs.json'
            with open(all_connectors_path, 'w') as all_f:
                json.dump({"connectors": all_connectors_dict}, all_f, indent=2)
            logger.info(f"Wrote all connectors to {all_connectors_path}")
            connectors_json = all_connectors_path
        elif discovery:
            logger.info("Starting connector config discovery...")
            connectors_json = discovery.discover_and_save()
            logger.info(f"Connector config discovery completed. Configs saved to {connectors_json}")
        else:
            logger.error("No connector configs found. Please provide either --config-file, --config-dir, or --worker-urls/--worker-urls-file.")
            sys.exit(1)

        # Step 2: Process Each Connector
        logger.info("Starting connector processing...")

        # Parse worker URLs for the comparator
        worker_urls_list = []
        if getattr(args, 'worker_urls', None):
            worker_urls_list = [url.strip() for url in args.worker_urls.split(',')]
        elif hasattr(args, 'worker_urls_file') and getattr(args, 'worker_urls_file', None):
            with open(args.worker_urls_file, 'r') as f:
                worker_urls_list = [line.strip() for line in f if line.strip()]

        comparator = ConnectorComparator(
            input_file=connectors_json,
            output_dir=output_dir,
            worker_urls=worker_urls_list,
            env_id=getattr(args, 'env_id', None),
            lkc_id=getattr(args, 'lkc_id', None),
            bearer_token=bearer_token,
            disable_ssl_verify=getattr(args, 'disable_ssl_verify', None)
        )
        fm_configs = comparator.process_connectors()
        logger.info("Connector processing completed successfully")

        # If offsets are required, fetch them from the workers and add them to the FM configs
        if getattr(args, 'offsets_required', None) and discovery:
            logger.info("Fetching offsets for connectors")
            configs_with_offsets = offset_manager.get_connector_configs_offsets(worker_urls_list)
            for config in configs_with_offsets:
                if config.get('offsets') and config.get('name') in fm_configs:
                    fm_configs[config['name']]['offsets'] = config['offsets']
            logger.info(f"FM configs with offsets: {fm_configs}")

        # Write FM configs to file
        write_fm_configs_to_file(fm_configs, output_dir, logger)

        # stop connector working in SM based on user input

        if not getattr(args, 'create_connector', False):
            logger.info("Skipping connector creation")
            sys.exit(0)

        # Parse JSON files in output_dir/SUCCESSFUL_CONFIGS_DIR and call create_connector for each
        successful_configs_dir = output_dir / FM_CONFIGS_DIR / ConnectorComparator.SUCCESSFUL_CONFIGS_DIR
        if not successful_configs_dir.exists() or not successful_configs_dir.is_dir():
            logger.warning(f"SUCCESSFUL_CONFIGS_DIR '{successful_configs_dir}' does not exist or is not a directory. Skipping connector creation.")
        else:
            # For each JSON file in the directory, call create_connector
            create_results_dir = output_dir / "create_connector_results"
            create_results_dir.mkdir(exist_ok=True)
            connector_creator = ConnectorCreator(
                environment=getattr(args, 'environment', None), # Use 'stag' for staging environment
            )
            for json_file in sorted(successful_configs_dir.glob("*.json")):
                logger.info(f"Creating connector(s) from {json_file}")
                try:
                    # Only call create_connector if env_id, lkc_id, and bearer_token are provided
                    if getattr(args, 'env_id', None) and getattr(args, 'lkc_id', None) and bearer_token:
                        results = connector_creator.create_connector_from_json_file(
                            environment_id=getattr(args, 'env_id', None),
                            kafka_cluster_id=getattr(args, 'lkc_id', None),
                            kafka_api_key=getattr(args, 'kafka_api_key', None),
                            kafka_api_secret=getattr(args, 'kafka_api_secret', None),
                            json_file_path=str(json_file),
                            bearer_token=bearer_token,
                            disable_ssl_verify=getattr(args, 'disable_ssl_verify', None)
                        )
                        logger.info(f"Successfully created connector(s) from {json_file}: {results}")
                        # Store each result as a separate JSON file
                        for idx, result in enumerate(results):
                            connector_name = result.get("name") or f"connector_{idx}"
                            out_path = create_results_dir / f"{connector_name}.json"
                            # Ensure unique file name if duplicate names
                            if out_path.exists():
                                out_path = create_results_dir / f"{connector_name}_{idx}.json"
                            with open(out_path, "w") as f:
                                json.dump(result, f, indent=2)
                            logger.info(f"Stored create_connector result to {out_path}")
                    else:
                        logger.warning("Skipping connector creation: env_id, lkc_id, or bearer_token not provided.")
                        break
                except Exception as e:
                    logger.error(f"Failed to create connector(s) from {json_file}: {e}")
        # Generate migration summary automatically
        logger.info("Generating migration summary...")
        try:
            summary_report = generate_migration_summary(output_dir)
            logger.info("Migration summary generated successfully")
            logger.info(f"Summary: {summary_report['total_successful_files']} successful, {summary_report['total_unsuccessful_files']} failed")
        except Exception as e:
            logger.warning(f"Failed to generate migration summary: {e}")
            logger.info("Continuing without summary generation...")

    except Exception as e:
        logger.error(f"Migration failed: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
