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
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )

def main():
    parser = argparse.ArgumentParser(description="Connector Migration Tool")
    parser.add_argument('--worker-urls', type=str, help='Comma-separated list of worker URLs')
    parser.add_argument('--worker-urls-file', type=str, help='Path to file containing worker URLs')
    parser.add_argument('--redact', action='store_true', help='Redact sensitive configurations')
    parser.add_argument('--output-dir', type=str, default='output', help='Output directory for all files')
    args = parser.parse_args()

    # Create output directory if it doesn't exist
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / 'fm_configs').mkdir(exist_ok=True)

    # Setup logging
    setup_logging(output_dir)
    logger = logging.getLogger(__name__)

    try:
        # Step 1: Discover Connector Configs
        logger.info("Starting connector discovery...")
        discovery = ConfigDiscovery(
            worker_urls=args.worker_urls,
            worker_urls_file=args.worker_urls_file,
            redact=args.redact,
            output_dir=output_dir
        )
        connectors_json = discovery.discover_and_save()
        logger.info(f"Connector discovery completed. Configs saved to {connectors_json}")

        # Step 2: Process Each Connector
        logger.info("Starting connector processing...")
        comparator = ConnectorComparator(
            input_file=connectors_json,
            output_dir=output_dir,
            worker_urls=discovery.worker_urls
        )
        comparator.process_connectors()
        logger.info("Connector processing completed successfully")

    except Exception as e:
        logger.error(f"Migration failed: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main() 