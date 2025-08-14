#!/usr/bin/env python3
import argparse
import logging
import sys
from pathlib import Path
from config_discovery import ConfigDiscovery
from connector_comparator import ConnectorComparator

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

def main():
    parser = argparse.ArgumentParser(description="Connector Migration Tool")
    parser.add_argument('--worker-urls', type=str, help='Comma-separated list of worker URLs')
    parser.add_argument('--worker-urls-file', type=str, help='Path to file containing worker URLs')
    parser.add_argument('--config-file', type=str, help='Path to JSON file containing connector configurations')
    parser.add_argument('--redact', action='store_true', help='Redact sensitive configurations')
    parser.add_argument('--sensitive-file', type=str, help='Path to file containing sensitive config keys')
    parser.add_argument('--worker-config-file', type=str, help='Path to file containing additional worker configs (key=value)')
    parser.add_argument('--output-dir', type=str, default='output', help='Output directory for all files')
    
    # Confluent Cloud credentials for FM transforms (optional)
    parser.add_argument('--env-id', type=str, help='Confluent Cloud environment ID')
    parser.add_argument('--lkc-id', type=str, help='Confluent Cloud LKC cluster ID')
    parser.add_argument('--bearer-token', type=str, help='Confluent Cloud bearer token (api_key:api_secret) (or use --prompt-bearer-token for secure input)')
    parser.add_argument('--prompt-bearer-token', action='store_true', help='Prompt for bearer token securely (recommended)')
    parser.add_argument('--disable-ssl-verify', action='store_true', help='Disable SSL certificate verification for HTTPS requests')
    parser.add_argument('--semantic-cache-folder', type=str, help='Cache folder for sentence transformer models (default: auto-detected from pip installation)')

    
    args = parser.parse_args()

    # Create output directory if it doesn't exist
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / 'fm_configs').mkdir(exist_ok=True)

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
        # Step 1: Get Connector Configs (either from discovery or file)
        if args.config_file:
            logger.info(f"Reading connector configurations from file: {args.config_file}")
            connectors_json = Path(args.config_file)
            if not connectors_json.exists():
                raise FileNotFoundError(f"Config file not found: {args.config_file}")
            logger.info(f"Using config file: {connectors_json}")
        else:
            logger.info("Starting connector discovery...")
            discovery = ConfigDiscovery(
                worker_urls=args.worker_urls,
                worker_urls_file=getattr(args, 'worker_urls_file', None),
                redact=args.redact,
                output_dir=output_dir,
                sensitive_file=args.sensitive_file,
                worker_config_file=args.worker_config_file,
                disable_ssl_verify=args.disable_ssl_verify
            )
            connectors_json = discovery.discover_and_save()
            logger.info(f"Connector discovery completed. Configs saved to {connectors_json}")

        # Step 2: Process Each Connector
        logger.info("Starting connector processing...")
        
        # Parse worker URLs for the comparator
        worker_urls_list = []
        if args.worker_urls:
            worker_urls_list = [url.strip() for url in args.worker_urls.split(',')]
        elif hasattr(args, 'worker_urls_file') and args.worker_urls_file:
            with open(args.worker_urls_file, 'r') as f:
                worker_urls_list = [line.strip() for line in f if line.strip()]
        
        comparator = ConnectorComparator(
            input_file=connectors_json,
            output_dir=output_dir,
            worker_urls=worker_urls_list,
            env_id=args.env_id,
            lkc_id=args.lkc_id,
            bearer_token=bearer_token,
            disable_ssl_verify=args.disable_ssl_verify
        )
        comparator.process_connectors()
        logger.info("Connector processing completed successfully")

    except Exception as e:
        logger.error(f"Migration failed: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main() 