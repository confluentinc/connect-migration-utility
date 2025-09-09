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
    """Write FM configs to file in the discovered_configs structure"""
    # Directory structure
    successful_dir = output_dir / ConnectorComparator.DISCOVERED_CONFIGS_DIR / ConnectorComparator.SUCCESSFUL_CONFIGS_SUBDIR
    unsuccessful_dir = output_dir / ConnectorComparator.DISCOVERED_CONFIGS_DIR / ConnectorComparator.UNSUCCESSFUL_CONFIGS_SUBDIR
    successful_fm_dir = successful_dir / ConfigDiscovery.FM_CONFIGS_DIR
    unsuccessful_fm_dir = unsuccessful_dir / ConfigDiscovery.FM_CONFIGS_DIR

    # Create directories if they don't exist
    successful_fm_dir.mkdir(parents=True, exist_ok=True)
    unsuccessful_fm_dir.mkdir(parents=True, exist_ok=True)
    successful_dir.mkdir(parents=True, exist_ok=True)
    unsuccessful_dir.mkdir(parents=True, exist_ok=True)

    for connector_name, fm_config in fm_configs.items():
        mapping_errors = fm_config.get('mapping_errors', [])
        minimal_fm = {
            "name": connector_name,
            "config": fm_config.get("config", {})
        }
        # Consider config unsuccessful if it has either errors or mapping_errors
        if mapping_errors:
            # Save full config in unsuccessful_configs
            full_config_file = unsuccessful_dir / f"{connector_name}.json"
            with open(full_config_file, 'w') as f:
                json.dump(fm_config, f, indent=2)
            # Save minimal fm_config in fm_configs
            fm_file = unsuccessful_fm_dir / f"fm_config_{connector_name}.json"
            with open(fm_file, 'w') as f:
                json.dump(minimal_fm, f, indent=2)
        else:
            # Save full config in successful_configs
            full_config_file = successful_dir / f"{connector_name}.json"
            with open(full_config_file, 'w') as f:
                json.dump(fm_config, f, indent=2)
            # Save minimal fm_config in fm_configs
            fm_file = successful_fm_dir / f"fm_config_{connector_name}.json"
            with open(fm_file, 'w') as f:
                json.dump(minimal_fm, f, indent=2)

    logger.info(f"Saved {len(fm_configs)} FM configurations to {output_dir / ConnectorComparator.DISCOVERED_CONFIGS_DIR}")

    # Save all FM configs (full) in discovered_configs
    all_configs_file = output_dir / ConnectorComparator.DISCOVERED_CONFIGS_DIR / 'compiled_output_fm_configs.json'
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
    parser.add_argument('--environment-id', type=str, help='Confluent Cloud environment ID')
    parser.add_argument('--cluster-id', type=str, help='Confluent Cloud LKC cluster ID')
    parser.add_argument('--bearer-token', type=str, help='Confluent Cloud bearer token (api_key:api_secret) (or use --prompt-bearer-token for secure input)')
    parser.add_argument('--prompt-bearer-token', action='store_true', help='Prompt for bearer token securely (recommended)')
    parser.add_argument('--disable-ssl-verify', action='store_true', help='Disable SSL certificate verification for HTTPS requests')
    parser.add_argument('--semantic-cache-folder', type=str, help='Cache folder for sentence transformer models (default: auto-detected from pip installation)')


    args = parser.parse_args()

    # Create output directory if it doesn't exist
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Setup logging
    setup_logging(output_dir)
    logger = logging.getLogger(__name__)

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
            all_connectors_dict = {}  # Accumulate all connectors here
            for file in sorted(config_dir.iterdir()):
                ConnectorComparator.parse_connector_file(file, all_connectors_dict, logger)
            # Validation: ensure at least one connector was found
            if not all_connectors_dict:
                logger.error(f"No valid connector configs found in directory: {args.config_dir}")
                sys.exit(1)
            # Write all connectors to a single file
            all_connectors_path = output_dir / 'compiled_input_sm_configs.json'
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
            env_id=getattr(args, 'environment_id', None),
            lkc_id=getattr(args, 'cluster_id', None),
            bearer_token=bearer_token,
            disable_ssl_verify=getattr(args, 'disable_ssl_verify', None)
        )
        fm_configs = comparator.process_connectors()
        logger.info("Connector processing completed successfully")

        # Write FM configs to file
        write_fm_configs_to_file(fm_configs, output_dir, logger)

        # TCO information
        tco_info = comparator.process_tco_information()

        # Generate migration summary automatically
        logger.info("Generating migration summary...")
        try:
            summary_report = generate_migration_summary(output_dir, tco_info)
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
