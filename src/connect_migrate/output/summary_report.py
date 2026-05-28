"""
Apache Connect Migration Utility
Copyright 2024-2025 The Apache Software Foundation

This product includes software developed at The Apache Software Foundation.
"""

import logging
import os
import re
from collections import defaultdict
from typing import Any, Dict, Iterator, Tuple, Union

from connect_migrate.mapper.connector_mapper import ConnectorMapper
from connect_migrate.utils.json_files import iter_json_files, read_json


logger = logging.getLogger(__name__)


def count_files(path):
    if not os.path.exists(path):
        return 0
    return sum(1 for _ in iter_json_files(path))


def _iter_connector_data(config_path) -> Iterator[Tuple[str, Any]]:
    """Yield ``(file_path, parsed_json)`` for every connector JSON under ``config_path``.

    Files that fail to read or parse are logged at warning and skipped, so the caller
    can stay focused on shape-handling.
    """
    for entry in iter_json_files(config_path):
        try:
            yield str(entry), read_json(entry)
        except (OSError, ValueError) as e:
            logger.warning(f"Failed to read {entry}: {e}")


def get_connector_type_counts(config_path):
    counts: Dict[str, int] = defaultdict(int)
    for _, data in _iter_connector_data(config_path):
        if "config" in data:
            connector_class = data["config"].get("connector.class")
        elif "sm_config" in data and isinstance(data["sm_config"], list) and data["sm_config"]:
            connector_class = data["sm_config"][0].get("connector.class")
        else:
            connector_class = None
        if connector_class:
            counts[connector_class] += 1
    return counts

def extract_config_name(data):
    if "config" in data and "name" in data["config"]:
        return data["config"]["name"]
    elif "sm_config" in data and isinstance(data["sm_config"], list) and data["sm_config"]:
        return data["sm_config"][0].get("name", "UNKNOWN_CONFIG")
    else:
        return "UNKNOWN_CONFIG"

def extract_transform_name(error_str):
    match = re.search(r"Transform\s+'([^']+)'", error_str)
    if match:
        return match.group(1)
    return "UNKNOWN_TRANSFORM"

def collect_mapping_errors_with_details(config_path):
    error_summary = defaultdict(lambda: {
        "count": 0,
        "occurrences": []  # list of (config_name, transform_name)
    })
    for _, data in _iter_connector_data(config_path):
        config_name = extract_config_name(data)
        errors = []
        if "mapping_errors" in data:
            errors = data["mapping_errors"]
        elif "config" in data and "mapping_errors" in data["config"]:
            errors = data["config"]["mapping_errors"]
        if not isinstance(errors, list):
            continue
        for error in errors:
            error = error.strip()
            transform = extract_transform_name(error)
            error_summary[error]["count"] += 1
            error_summary[error]["occurrences"].append((config_name, transform))
    return error_summary

def summarize_output(base_dir):
    summary = {
        "fm_configs_found": 0,
        "total_successful_files": 0,
        "total_unsuccessful_files": 0,
        "details": {},
        "global_successful_types": defaultdict(int),
        "global_unsuccessful_types": defaultdict(int),
        "global_mapping_errors": {},  # now with details
    }

    for root, dirs, files in os.walk(base_dir):
        if os.path.basename(root) == str(ConnectorMapper.DISCOVERED_CONFIGS_DIR):
            summary["fm_configs_found"] += 1
            parent_folder = os.path.relpath(os.path.dirname(root), base_dir)

            success_path = os.path.join(root, str(ConnectorMapper.SUCCESSFUL_CONFIGS_SUBDIR))
            fail_path = os.path.join(root, str(ConnectorMapper.UNSUCCESSFUL_CONFIGS_SUBDIR))

            successful_files = count_files(success_path)
            unsuccessful_files = count_files(fail_path)
            total_files = successful_files + unsuccessful_files

            success_types = get_connector_type_counts(success_path)
            fail_types = get_connector_type_counts(fail_path)
            mapping_errors = collect_mapping_errors_with_details(fail_path)

            for k, v in success_types.items():
                summary["global_successful_types"][k] += v
            for k, v in fail_types.items():
                summary["global_unsuccessful_types"][k] += v
            for err, details in mapping_errors.items():
                if err not in summary["global_mapping_errors"]:
                    summary["global_mapping_errors"][err] = {
                        "count": 0,
                        "occurrences": []
                    }
                summary["global_mapping_errors"][err]["count"] += details["count"]
                summary["global_mapping_errors"][err]["occurrences"].extend(details["occurrences"])

            summary["total_successful_files"] += successful_files
            summary["total_unsuccessful_files"] += unsuccessful_files

            summary["details"][parent_folder] = {
                "total_files_in_fm_configs": total_files,
                "successful_files": successful_files,
                "unsuccessful_files": unsuccessful_files,
                "successful_connector_types": dict(success_types),
                "unsuccessful_connector_types": dict(fail_types),
                "mapping_errors": mapping_errors
            }

    return summary

def generate_tco_information_output(tco_info: Dict[str, Union[int, Dict[str, Any]]], output_dir: str):
    """Generate TCO information output file with formatted text summary."""
    if not tco_info:
        logger.info("No TCO information provided.")
        return

    summary_file_path = os.path.join(output_dir, "tco_information.txt")

    # Create visually pleasing summary
    try:
        # Calculate pack info
        premium_connectors = tco_info.get('premium_pack_connectors', {})
        premium_connector_count = 0
        for key, value in premium_connectors.items():
            premium_connector_count += value.get('connector_count', 0)

        commercial_connectors = tco_info.get('commercial_pack_connectors', {})
        commercial_connector_count = 0
        for key, value in commercial_connectors.items():
            commercial_connector_count += value.get('connector_count', 0)

        premium_pack_count = premium_connector_count

        commercial_pack_count = (commercial_connector_count // 5) + (1 if commercial_connector_count % 5 > 0 else 0)

        unknown_names = tco_info.get('unknown_pack_connectors', [])

        lines = ["=" * 80, " TCO Information Summary", "=" * 80,
                 f"Total Connectors: {tco_info.get('total_connectors', 0)}",
                 f"Total Tasks: {tco_info.get('total_tasks', 0)}",
                 f"Total Worker Nodes: {tco_info.get('worker_node_count', 0)}"]

        lines.append("=" * 80)
        # Connector Pack Information
        lines.append("TCO Information:")
        lines.append(f"  - Premium Packs required: {premium_pack_count}")
        lines.append(f"    - Premium pack Connectors: {premium_connectors}")

        lines.append(f"  - Commercial Packs required: {commercial_pack_count}")
        lines.append(f"    - Commercial pack Connectors: {commercial_connectors}")

        lines.append(f"  - Non-Commercial pack Connectors: {tco_info.get('non_commercial_pack_connectors', 0)}")
        if unknown_names:
            lines.append(f"  - Unknown Pack Connector Names: {', '.join(unknown_names)}")

        lines.append("=" * 80)

        worker_map = tco_info.get('worker_node_task_map', {})
        lines.append("Worker Node Task Distribution:")
        for worker, details in worker_map.items():
            lines.append(f"  - Worker Node: {worker}")
            lines.append(f"    - Task Count: {details.get('task_count', 0)}")
            task_list = details.get('task_list', [])
            if task_list:
                lines.append(f"    - Tasks:")
                for task in task_list:
                    lines.append(f"      - {task}")
            else:
                lines.append(f"    - Tasks: None")

        lines.append("=" * 80)
        with open(summary_file_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))
        logger.info(f"TCO summary saved to: {summary_file_path}")
    except OSError as e:
        logger.warning(f"Failed to save TCO summary to file: {e}")

def generate_migration_summary(output_dir):
    """Generate migration summary for the given output directory."""
    report = summarize_output(output_dir)
    total_files_overall = report['total_successful_files'] + report['total_unsuccessful_files']

    # Generate summary text
    summary_lines = []

    summary_lines.append("================================================================================")
    summary_lines.append(" Overall Summary")
    summary_lines.append("================================================================================")
    summary_lines.append(f"Number of Connector clusters scanned: {report['fm_configs_found']}")
    summary_lines.append(f"Total Connector configurations scanned: {total_files_overall}")
    summary_lines.append(f"Total Connectors that can be successfully migrated: {report['total_successful_files']}")
    summary_lines.append(f"Total Connectors that have errors in migration: {report['total_unsuccessful_files']}")

    summary_lines.append("")
    summary_lines.append("================================================================================")
    summary_lines.append("Summary By Connector Type")
    summary_lines.append("================================================================================")
    summary_lines.append("✅ Connector types (successful across all clusters):")
    for k, v in sorted(report["global_successful_types"].items(), key=lambda item: item[1], reverse=True):
        summary_lines.append(f"  - {k}: {v}")

    summary_lines.append("")
    summary_lines.append("❌ Connector types (with errors across all clusters):")
    for k, v in sorted(report["global_unsuccessful_types"].items(), key=lambda item: item[1], reverse=True):
        summary_lines.append(f"  - {k}: {v}")

    summary_lines.append("")
    summary_lines.append("================================================================================")
    summary_lines.append(" Per-Cluster Summary (sorted by successful configurations for migration)")
    summary_lines.append("================================================================================")
    sorted_folders = sorted(
        report["details"].items(),
        key=lambda item: item[1]["successful_files"],
        reverse=True
    )
    for folder, stats in sorted_folders:
        summary_lines.append("")
        summary_lines.append(f"Cluster Details: {folder}")
        summary_lines.append(f"  Total Connector configurations scanned: {stats['total_files_in_fm_configs']}")
        summary_lines.append(f"  Total Connectors that can be successfully migrated: {stats['successful_files']}")
        if stats['successful_connector_types']:
            summary_lines.append(f"    ✅ Connector types (successful):")
            for conn_type, count in stats['successful_connector_types'].items():
                summary_lines.append(f"      - {conn_type}: {count}")
        summary_lines.append(f"  Total Connectors that have errors in migration: {stats['unsuccessful_files']}")
        if stats['unsuccessful_connector_types']:
            summary_lines.append(f"    ❌ Connector types (with errors):")
            for conn_type, count in stats['unsuccessful_connector_types'].items():
                summary_lines.append(f"      - {conn_type}: {count}")
        if stats['mapping_errors']:
            summary_lines.append(f"    ⚠️ Mapping errors:")
            for err_msg, detail in sorted(stats['mapping_errors'].items(), key=lambda x: x[1]["count"], reverse=True):
                summary_lines.append(f"      - '{err_msg}': found in {detail['count']} file(s)")

    summary_lines.append("")
    summary_lines.append("================================================================================")
    summary_lines.append(" Connector Mapping Errors (all unsuccessful configs)")
    summary_lines.append("================================================================================")
    for error, details in sorted(report["global_mapping_errors"].items(), key=lambda x: x[1]["count"], reverse=True):
        summary_lines.append("")
        summary_lines.append(f"❌ '{error}'")
        summary_lines.append(f"   ↳ Found in {details['count']} occurrences")

    # Save summary to text file
    summary_file_path = os.path.join(output_dir, "summary.txt")
    try:
        with open(summary_file_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(summary_lines))
        logger.info(f"Migration summary saved to: {summary_file_path}")
    except OSError as e:
        logger.warning(f"Failed to save summary to file: {e}")

    # Also print to console/logs
    for line in summary_lines:
        logger.info(line)

    return report
