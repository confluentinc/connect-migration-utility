import argparse
import getpass
import json
import logging
import shutil
from pathlib import Path

from requests.auth import HTTPBasicAuth

from connect_migrate.discovery.config_discovery import ConfigDiscovery
from connect_migrate.mapper.connector_mapper import ConnectorMapper
from connect_migrate.migration.connector_creator import ConnectorCreator, KafkaAuth
from connect_migrate.migration.offset_manager import OffsetManager
from connect_migrate.utils.logging_setup import setup_logging


def _prepare_output_dir(path: Path, clean: bool) -> None:
    """Create ``path`` (mkdir -p). If ``clean`` is set, rmtree it first, but refuse
    to operate on dangerous paths ($HOME, cwd, or any ancestor of cwd)."""
    if clean and path.exists():
        resolved = path.resolve()
        cwd = Path.cwd().resolve()
        home = Path.home().resolve()
        if resolved == home:
            raise SystemExit(f"Refusing to --clean home directory: {resolved}")
        if resolved == cwd:
            raise SystemExit(f"Refusing to --clean current working directory: {resolved}")
        try:
            cwd.relative_to(resolved)
        except ValueError:
            pass
        else:
            raise SystemExit(f"Refusing to --clean an ancestor of cwd: {resolved}")
        if resolved == resolved.parent:
            raise SystemExit(f"Refusing to --clean filesystem root: {resolved}")
        shutil.rmtree(resolved)
    path.mkdir(parents=True, exist_ok=True)


def main():
    parser = argparse.ArgumentParser(description="Create Confluent Cloud connectors from JSON file")
    parser.add_argument('--fm-config-dir', type=str, required=True,
                        help='Directory containing FM connector JSON config files')
    parser.add_argument('--migration-output-dir', type=str, default='migration_output')
    token_group = parser.add_mutually_exclusive_group(required=True)
    token_group.add_argument('--bearer-token', type=str,
                             help='Confluent Cloud bearer token (api_key:api_secret). Mutually exclusive with --prompt-bearer-token.')
    token_group.add_argument('--prompt-bearer-token', action='store_true',
                             help='Prompt for the bearer token interactively (recommended; the input is not echoed).')
    parser.add_argument('--kafka-api-key', type=str, help='Kafka API key for LKC cluster')
    parser.add_argument('--kafka-api-secret', type=str, help='Kafka API secret for LKC cluster')
    parser.add_argument('--kafka-service-account-id', type=str, help='Confluent Cloud service account (optional, alternative to api_key/api_secret)')
    parser.add_argument('--kafka-auth-mode', type=str, choices = ['SERVICE_ACCOUNT','KAFKA_API_KEY'], default='KAFKA_API_KEY', help='Kafka authentication mode (default: KAFKA_API_KEY)')

    parser.add_argument('--environment-id', type=str, required=True, help='Confluent Cloud environment ID')
    parser.add_argument('--cluster-id', type=str, required=True, help='Confluent Cloud LKC cluster ID')
    parser.add_argument('--worker-urls', type=str, help='Comma-separated list of worker URLs to fetch latest offsets from')
    parser.add_argument('--worker-username', type=str, help='Username for basic authentication with Connect worker REST API')
    parser.add_argument('--worker-password', type=str, help='Password for basic authentication with Connect worker REST API')
    parser.add_argument('--migration-mode', type=str, choices=['stop_create_latest_offset', 'create', 'create_latest_offset'], required=True)
    parser.add_argument('--disable-ssl-verify', action='store_true', help='Disable SSL certificate verification for HTTPS requests')



    parser.add_argument('--semantic-cache-folder', type=str,
                        help='Cache folder for sentence transformer models (default: auto-detected from pip installation)')
    parser.add_argument('--clean', action='store_true',
                        help='Delete migration-output-dir before writing. Refuses to operate on $HOME, cwd, or any ancestor of cwd.')

    args = parser.parse_args()

    fm_config_dir = Path(args.fm_config_dir)
    migration_output_dir = Path(args.migration_output_dir or "migration_output")
    _prepare_output_dir(migration_output_dir, clean=args.clean)

    # Setup logging
    setup_logging(migration_output_dir)
    logger = logging.getLogger(__name__)


    if args.prompt_bearer_token:
        bearer_token = getpass.getpass("Confluent Cloud bearer token (api_key:api_secret): ")
        if not bearer_token:
            parser.error("--prompt-bearer-token requires a non-empty token")
    else:
        bearer_token = args.bearer_token

    kafka_auth = KafkaAuth(
        api_key=getattr(args, 'kafka_api_key', None),
        api_secret=getattr(args, 'kafka_api_secret', None),
        service_account_id=getattr(args, 'kafka_service_account_id', None),
        auth_mode=getattr(args, 'kafka_auth_mode', 'KAFKA_API_KEY')
    )
    kafka_auth.verify_kafka_auth()

    env_id = args.environment_id
    lkc_id = args.cluster_id
    environment = "prod"
    worker_urls = getattr(args, 'worker_urls', None)
    if worker_urls:
        worker_urls = worker_urls.split(',')
    else:
        worker_urls = []

    disable_ssl_verify = getattr(args, 'disable_ssl_verify', False)
    migration_mode = args.migration_mode

    # Setup basic auth for Connect worker API if credentials provided
    worker_username = getattr(args, 'worker_username', None)
    worker_password = getattr(args, 'worker_password', None)
    worker_auth = None
    if worker_username and worker_password:
        worker_auth = HTTPBasicAuth(worker_username, worker_password)
        logger.info("Basic authentication enabled for Connect worker API")
    elif worker_username or worker_password:
        logger.warning("Basic auth username or password provided but not both - authentication disabled")

    creator = ConnectorCreator(environment)

    logger.info("Starting connector creation process")
    successes = []
    failures = []
    if migration_mode=='create':
        for json_file in fm_config_dir.glob("*.json"):

            try:
                print(f"Creating connector(s) from file: {json_file}")
                created_connectors = creator.create_connector_from_json_file(
                    environment_id=env_id,
                    kafka_cluster_id=lkc_id,
                    kafka_auth=kafka_auth,
                    json_file_path=json_file,
                    bearer_token=bearer_token
                )
                for conn in created_connectors:
                    # Heuristic: error_code or status >= 400 means failure
                    if 'error_code' in conn or (isinstance(conn.get('status'), int) and conn['status'] >= 400):
                        failures.append(conn)
                    else:
                        successes.append(conn)
                    print(f"Created connector: {conn.get('name')}")
            except Exception as e:
                failures.append({"file": str(json_file), "error": str(e)})
                print(f"Failed to create connectors from {json_file}: {str(e)}")
                continue
    elif migration_mode in  ['stop_create_latest_offset', 'create_latest_offset']:
        if not getattr(args, 'worker_urls', None):
            parser.error(f"--worker-urls is required to fetch offsets for migration mode '{migration_mode}'")

        offset_manager = OffsetManager.get_instance(logger)

        connector_fm_configs = {}
        for json_file in fm_config_dir.glob("*.json"):
            try:
                ConnectorMapper.parse_connector_file(json_file, connector_fm_configs, logger)
            except Exception as e:
                logger.error(f"Failed to extract connectors from {json_file}: {str(e)}")
                continue

        connector_configs_from_worker = []
        for worker_url in worker_urls:
            logger.info(f"Getting connector configs for worker: {worker_url}")
            connector_configs_from_worker.extend(ConfigDiscovery.get_connector_configs_from_worker(worker_url, disable_ssl_verify, logger, auth=worker_auth))

        for connector_name in connector_fm_configs:
            fm_entry = connector_fm_configs[connector_name]
            fm_name = fm_entry['name']
            matching_worker_sm_config = next((wc for wc in connector_configs_from_worker if wc['name'] == fm_name), None)
            if not matching_worker_sm_config:
                logger.warning(f"No matching SM connector configs found in given worker URLs to fetch offsets for FM connector '{fm_name}'")
                continue

            try:
                # stop connector
                if migration_mode == 'stop_create_latest_offset':
                    logger.info("Stopping CP connector after validation as per migration mode")
                    creator.stop_cp_connector(matching_worker_sm_config.get('worker', None), fm_name, disable_ssl_verify, auth=worker_auth)

                offsets = offset_manager.get_offsets_of_connector(matching_worker_sm_config, disable_ssl_verify, auth=worker_auth)
                if not offsets:
                    logger.info(f"No offsets found on workers for connector '{fm_name}'")
                else:
                    logger.info(f"Found offsets on workers for connector '{fm_name}': {offsets}")
                    fm_entry['offsets'] = offsets

                # create connector
                logger.info(f"Creating connector '{fm_name}' with offsets from workers")
                created_connector = creator.create_connector_from_config(
                    environment_id=env_id,
                    kafka_cluster_id=lkc_id,
                    kafka_auth=kafka_auth,
                    fm_config=fm_entry,
                    bearer_token=bearer_token
                )
                if created_connector is not None and not ('error_code' in created_connector or (isinstance(created_connector.get('status'), int) and created_connector['status'] >= 400)):
                    successes.append(created_connector)
                    logger.info(f"Created connector: {created_connector.get('name') if created_connector else fm_name}")
                else:
                    failures.append(created_connector)
                    logger.info(f"Failed to create connector '{fm_name}': {created_connector}")

            except Exception as e:
                failures.append({"connector": fm_name, "error": str(e)})
                logger.info(f"Failed to create connector '{fm_name}': {str(e)}")
                continue

    # Write results to files
    with open(migration_output_dir / "successful_migration.json", "w") as f:
        json.dump(successes, f, indent=2)
    with open(migration_output_dir / "unsuccessful_migration.json", "w") as f:
        json.dump(failures, f, indent=2)


if __name__ == "__main__":
    main()
