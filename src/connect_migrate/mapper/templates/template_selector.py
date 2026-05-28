"""Selects the right FM template (and plugin name) for a given SM connector.

The selection rules baked in here, from highest to lowest priority:

1. **Debezium v1 <-> v2 connector-class mapping** — if the user asked for
   one Debezium version but the SM config references the other, the
   lookup is retargeted to the matching FM template family.
2. **Single template found** — used directly.
3. **Multiple templates, CDC family** — filtered by ``debezium_version``.
4. **Multiple templates, JDBC family** — auto-selected by database type
   parsed from the connection URL (via the injected
   ``database_type_resolver`` callable).
5. **Multiple templates, anything else** — interactive user prompt.
"""

import json
import logging
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional


DEBEZIUM_V1_TO_V2_CLASS_MAPPING: Dict[str, str] = {
    "io.debezium.connector.mysql.MySqlConnector": "io.debezium.connector.v2.mysql.MySqlConnectorV2",
    "io.debezium.connector.postgresql.PostgresConnector": "io.debezium.connector.v2.postgresql.PostgresConnectorV2",
    "io.debezium.connector.sqlserver.SqlServerConnector": "io.debezium.connector.v2.sqlserver.SqlServerConnectorV2",
    "io.debezium.connector.mariadb.MariaDbConnector": "io.debezium.connector.v2.mariadb.MariaDbConnector",
}

DEBEZIUM_V2_TO_V1_CLASS_MAPPING: Dict[str, str] = {
    v: k for k, v in DEBEZIUM_V1_TO_V2_CLASS_MAPPING.items()
}

JDBC_CONNECTOR_CLASSES = (
    "io.confluent.connect.jdbc.JdbcSourceConnector",
    "io.confluent.connect.jdbc.JdbcSinkConnector",
)

# Database type -> (source plugin, sink plugin)
_DB_TO_PLUGIN_NAME: Dict[str, "tuple[str, str]"] = {
    "mysql": ("MySqlSource", "MySqlSink"),
    "postgresql": ("PostgresSource", "PostgresSink"),
    "oracle": ("OracleDatabaseSource", "OracleDatabaseSink"),
    "sqlserver": ("MicrosoftSqlServerSource", "MicrosoftSqlServerSink"),
    "snowflake": ("SnowflakeSource", "SnowflakeSink"),
}

# Database type -> list of template_id values to look for
_DB_TO_TEMPLATE_IDS: Dict[str, List[str]] = {
    "mysql": ["MySqlSource", "MySqlSink"],
    "postgresql": ["PostgresSource", "PostgresSink"],
    "oracle": ["OracleDatabaseSource", "OracleDatabaseSink"],
    "sqlserver": ["MicrosoftSqlServerSource", "MicrosoftSqlServerSink"],
    "snowflake": ["SnowflakeSource"],
}


class TemplateSelector:
    def __init__(
        self,
        fm_template_dir: Path,
        debezium_version: str,
        database_type_resolver: Callable[[Dict[str, Any]], str],
        logger: Optional[logging.Logger] = None,
    ):
        self.fm_template_dir = fm_template_dir
        self.debezium_version = debezium_version
        self._resolve_database_type = database_type_resolver
        self.logger = logger or logging.getLogger(__name__)

    # ------------------------------------------------------------------ plugin name

    def get_plugin_name(
        self,
        connector_class: str,
        config_dict: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """Return the FM plugin name (``template_id``) for a connector class."""
        if connector_class in JDBC_CONNECTOR_CLASSES:
            return self._get_jdbc_plugin_name(connector_class, config_dict)
        return self._get_plugin_name_from_template(connector_class)

    def _get_jdbc_plugin_name(
        self,
        connector_class: str,
        config_dict: Optional[Dict[str, Any]],
    ) -> Optional[str]:
        if not config_dict:
            self.logger.warning("Cannot determine JDBC plugin name - no config provided")
            return None

        db_type = self._resolve_database_type(config_dict)
        if not db_type:
            self.logger.warning(
                "Cannot determine JDBC plugin name - could not detect database type from connection URL"
            )
            return None

        self.logger.info(f"Detected database type for JDBC connector: {db_type}")
        is_source = connector_class == "io.confluent.connect.jdbc.JdbcSourceConnector"

        if db_type in _DB_TO_PLUGIN_NAME:
            source_plugin, sink_plugin = _DB_TO_PLUGIN_NAME[db_type]
            plugin_name = source_plugin if is_source else sink_plugin
            self.logger.info(f"Determined JDBC plugin name: {plugin_name}")
            return plugin_name

        self.logger.warning(f"No plugin mapping found for database type: {db_type}")
        return None

    def _get_plugin_name_from_template(self, connector_class: str) -> Optional[str]:
        if not self.fm_template_dir or not self.fm_template_dir.exists():
            self.logger.warning(f"FM template directory does not exist: {self.fm_template_dir}")
            return None

        target_connector_class = self._apply_debezium_class_mapping(connector_class)

        for template_file in self.fm_template_dir.glob("*.json"):
            try:
                with open(template_file, "r") as f:
                    template_data = json.load(f)

                found_match = False
                if template_data.get("connector.class") == target_connector_class:
                    found_match = True
                elif "templates" in template_data:
                    for template in template_data["templates"]:
                        if template.get("connector.class") == target_connector_class:
                            found_match = True
                            break

                if found_match:
                    template_id = None
                    if template_data.get("template_id"):
                        template_id = template_data.get("template_id")
                    elif "templates" in template_data and len(template_data["templates"]) > 0:
                        template_id = template_data["templates"][0].get("template_id")

                    if template_id:
                        self.logger.info(
                            f"Found plugin name '{template_id}' for connector class '{connector_class}'"
                        )
                        return template_id
                    self.logger.warning(
                        f"Template found for {connector_class} but no template_id present"
                    )
            except Exception as e:
                self.logger.debug(f"Error reading template {template_file}: {str(e)}")
                continue

        self.logger.warning(f"No FM template found for connector class: {connector_class}")
        return None

    # ------------------------------------------------------------------ template path

    def find_template_path(
        self,
        connector_class: str,
        connector_name: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """Return the path to the FM template file that matches a connector class."""
        if not self.fm_template_dir or not self.fm_template_dir.exists():
            self.logger.error(f"FM template directory does not exist: {self.fm_template_dir}")
            return None

        target_connector_class = self._apply_debezium_class_mapping(
            connector_class, log_at_info=True
        )

        matching, template_info = self._scan_for_connector_class(target_connector_class)

        if not matching and target_connector_class != connector_class:
            self.logger.warning(
                f"No FM templates found for migrated connector.class: {target_connector_class}, "
                f"trying original: {connector_class}"
            )
            matching, template_info = self._scan_for_connector_class(connector_class)

        if not matching:
            self.logger.warning(f"No FM templates found for connector.class: {connector_class}")
            return None

        if len(matching) == 1:
            self.logger.info(f"Using single FM template: {matching[0]}")
            return matching[0]

        # Multiple matches: filter CDC by version first
        filtered = self._filter_cdc_templates_by_version(connector_class, template_info)
        if len(filtered) == 1:
            self.logger.info(
                f"Using filtered CDC template (debezium_version={self.debezium_version}): "
                f"{filtered[0]['path']}"
            )
            return filtered[0]["path"]
        elif len(filtered) < len(template_info):
            template_info = filtered
            self.logger.info(
                f"Filtered to {len(filtered)} CDC templates based on debezium_version={self.debezium_version}"
            )

        if connector_class in JDBC_CONNECTOR_CLASSES and config:
            return self._auto_select_jdbc_template(
                connector_class, template_info, config, connector_name
            )
        return self._prompt_user_for_template(connector_class, template_info, connector_name)

    # ------------------------------------------------------------------ internals

    def _apply_debezium_class_mapping(
        self,
        connector_class: str,
        log_at_info: bool = False,
    ) -> str:
        """Translate the connector class through the Debezium v1<->v2 mapping if applicable."""
        log = self.logger.info if log_at_info else self.logger.debug
        if (
            self.debezium_version == "v2"
            and connector_class in DEBEZIUM_V1_TO_V2_CLASS_MAPPING
        ):
            mapped = DEBEZIUM_V1_TO_V2_CLASS_MAPPING[connector_class]
            log(f"Migrating from v1 to v2: {connector_class} -> {mapped}")
            return mapped
        if (
            self.debezium_version == "v1"
            and connector_class in DEBEZIUM_V2_TO_V1_CLASS_MAPPING
        ):
            mapped = DEBEZIUM_V2_TO_V1_CLASS_MAPPING[connector_class]
            log(f"Migrating from v2 to v1: {connector_class} -> {mapped}")
            return mapped
        return connector_class

    def _scan_for_connector_class(
        self,
        connector_class: str,
    ) -> "tuple[List[str], List[Dict[str, str]]]":
        matching: List[str] = []
        template_info: List[Dict[str, str]] = []

        for template_file in self.fm_template_dir.glob("*.json"):
            try:
                with open(template_file, "r") as f:
                    template_data = json.load(f)

                found = None
                if template_data.get("connector.class") == connector_class:
                    found = template_data.get("connector.class")
                elif "templates" in template_data:
                    for tmpl in template_data["templates"]:
                        if tmpl.get("connector.class") == connector_class:
                            found = tmpl.get("connector.class")
                            break

                if found:
                    template_path = str(template_file)
                    template_id = "Unknown"
                    if template_data.get("template_id"):
                        template_id = template_data.get("template_id")
                    elif (
                        "templates" in template_data
                        and len(template_data["templates"]) > 0
                    ):
                        template_id = template_data["templates"][0].get(
                            "template_id", "Unknown"
                        )

                    matching.append(template_path)
                    template_info.append(
                        {
                            "path": template_path,
                            "template_id": template_id,
                            "filename": template_file.name,
                        }
                    )
                    self.logger.debug(
                        f"Found matching FM template: {template_file} (template_id: {template_id})"
                    )
            except Exception as e:
                self.logger.warning(f"Error reading template {template_file}: {str(e)}")
                continue

        return matching, template_info

    def _filter_cdc_templates_by_version(
        self,
        connector_class: str,
        template_info: List[Dict[str, str]],
    ) -> List[Dict[str, str]]:
        is_debezium = (
            "debezium" in connector_class.lower()
            or connector_class.startswith("io.debezium.connector")
        )
        if not is_debezium:
            return template_info

        v2_templates: List[Dict[str, str]] = []
        v1_templates: List[Dict[str, str]] = []

        for template in template_info:
            template_id = template.get("template_id", "").lower()
            filename = template.get("filename", "").lower()
            is_v2 = "v2" in template_id or "v2" in filename

            try:
                with open(template["path"], "r") as f:
                    template_data = json.load(f)
                connector_class_in_template = None
                if template_data.get("connector.class"):
                    connector_class_in_template = template_data.get("connector.class")
                elif (
                    "templates" in template_data
                    and len(template_data["templates"]) > 0
                ):
                    connector_class_in_template = template_data["templates"][0].get(
                        "connector.class"
                    )

                if connector_class_in_template:
                    if (
                        ".v2." in connector_class_in_template
                        or "ConnectorV2" in connector_class_in_template
                    ):
                        is_v2 = True
                    elif (
                        ".v2." not in connector_class_in_template
                        and "ConnectorV2" not in connector_class_in_template
                    ):
                        is_v2 = False
            except Exception as e:
                self.logger.debug(
                    f"Could not read template file {template['path']}: {e}"
                )

            (v2_templates if is_v2 else v1_templates).append(template)

        if self.debezium_version == "v2":
            if v2_templates:
                self.logger.info(
                    f"Filtering to {len(v2_templates)} v2 CDC templates (debezium_version=v2)"
                )
                return v2_templates
            self.logger.info(
                f"No v2 templates found, using {len(v1_templates)} v1 templates"
            )
            return v1_templates

        # debezium_version == 'v1'
        if v1_templates:
            self.logger.info(
                f"Filtering to {len(v1_templates)} v1 CDC templates (debezium_version=v1)"
            )
            return v1_templates
        self.logger.info(
            f"No v1 templates found, using {len(v2_templates)} v2 templates"
        )
        return v2_templates

    def _auto_select_jdbc_template(
        self,
        connector_class: str,
        template_info: List[Dict[str, str]],
        config: Dict[str, Any],
        connector_name: Optional[str] = None,
    ) -> Optional[str]:
        connector_display = (
            f"{connector_name} ({connector_class})" if connector_name else connector_class
        )

        db_type = self._resolve_database_type(config)
        self.logger.info(f"Detected database type: {db_type} for connector: {connector_display}")

        # Special-case Snowflake: rescan for Snowflake-specific connector classes
        if db_type == "snowflake":
            self.logger.info(
                f"Detected Snowflake database for {connector_class}, looking for Snowflake-specific templates"
            )
            if connector_class == "io.confluent.connect.jdbc.JdbcSourceConnector":
                target = "io.confluent.connect.snowflake.jdbc.SnowflakeSourceConnector"
            else:
                target = "io.confluent.connect.snowflake.jdbc.SnowflakeSinkConnector"
            self.logger.info(f"Looking for Snowflake template with connector class: {target}")

            _, snowflake_template_info = self._scan_for_connector_class(target)
            for tmpl in snowflake_template_info:
                self.logger.info(
                    f"Found Snowflake template: {tmpl['template_id']} (File: {tmpl['filename']})"
                )
            if snowflake_template_info:
                selected = snowflake_template_info[0]
                self.logger.info(
                    f"Auto-selected Snowflake template for {connector_display}: "
                    f"{selected['template_id']} (File: {selected['filename']})"
                )
                return selected["path"]

        expected_templates = _DB_TO_TEMPLATE_IDS.get(db_type, [])
        self.logger.info(f"Expected templates for {db_type}: {expected_templates}")
        self.logger.info(f"Available templates: {[t['template_id'] for t in template_info]}")
        self.logger.info(f"Connector class: {connector_class}")

        for template in template_info:
            template_id = template["template_id"]
            self.logger.info(f"Checking template: {template_id}")
            if template_id in expected_templates:
                self.logger.info(f"Template {template_id} is in expected templates")
                if (
                    connector_class == "io.confluent.connect.jdbc.JdbcSourceConnector"
                    and "Source" in template_id
                ):
                    self.logger.info(
                        f"Auto-selected JDBC Source template for {connector_display}: "
                        f"{template_id} (File: {template['filename']})"
                    )
                    return template["path"]
                if (
                    connector_class == "io.confluent.connect.jdbc.JdbcSinkConnector"
                    and "Sink" in template_id
                ):
                    self.logger.info(
                        f"Auto-selected JDBC Sink template for {connector_display}: "
                        f"{template_id} (File: {template['filename']})"
                    )
                    return template["path"]
                if (
                    connector_class == "io.confluent.connect.jdbc.JdbcSourceConnector"
                    and "Sink" not in template_id
                ):
                    self.logger.info(
                        f"Auto-selected JDBC template (fallback) for {connector_display}: "
                        f"{template_id} (File: {template['filename']})"
                    )
                    return template["path"]
                if (
                    connector_class == "io.confluent.connect.jdbc.JdbcSinkConnector"
                    and "Source" not in template_id
                ):
                    self.logger.info(
                        f"Auto-selected JDBC template (fallback) for {connector_display}: "
                        f"{template_id} (File: {template['filename']})"
                    )
                    return template["path"]
            else:
                self.logger.info(f"Template {template_id} is NOT in expected templates")

        # Partial-match fallback
        for template in template_info:
            template_id_lower = template["template_id"].lower()
            if db_type in template_id_lower:
                self.logger.info(
                    f"Auto-selected JDBC template (partial match) for {connector_display}: "
                    f"{template['template_id']} (File: {template['filename']})"
                )
                return template["path"]

        self.logger.warning(
            f"Could not auto-select JDBC template for {connector_display} with database type {db_type}"
        )
        return self._prompt_user_for_template(connector_class, template_info, connector_name)

    def _prompt_user_for_template(
        self,
        connector_class: str,
        template_info: List[Dict[str, str]],
        connector_name: Optional[str] = None,
    ) -> Optional[str]:
        connector_display = (
            f"{connector_name} ({connector_class})" if connector_name else connector_class
        )
        print(f"\nMultiple FM templates found for connector: {connector_display}")
        print("Available templates:")
        for i, info in enumerate(template_info, 1):
            print(f"{i}. Template ID: {info['template_id']} (File: {info['filename']})")

        while True:
            try:
                choice = int(
                    input(
                        f"\nPlease select an FM template for '{connector_display}' "
                        f"(1-{len(template_info)}): "
                    )
                )
                if 1 <= choice <= len(template_info):
                    selected = template_info[choice - 1]
                    self.logger.info(
                        f"User selected FM template for {connector_display}: {selected['path']}"
                    )
                    return selected["path"]
                print(f"Please enter a number between 1 and {len(template_info)}")
            except ValueError:
                print("Please enter a valid number")
