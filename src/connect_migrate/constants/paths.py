"""Default paths used throughout the utility.

Lifted out of class constants / hardcoded strings so the values live in
one place. Paths are relative to the process's working directory — the
utility is invoked from the repo root.
"""

from pathlib import Path


# FM templates shipped with the repo
DEFAULT_FM_TEMPLATE_DIR: Path = Path("templates/fm")

# Fallback list of FM-supported SMTs when the CC internal validate
# endpoint isn't reachable.
FM_TRANSFORMS_FALLBACK_FILE: Path = Path("fm_transforms_list.json")

# Directory layout for discovery output (created under --output-dir)
DISCOVERED_CONFIGS_DIR: Path = Path("discovered_configs")
SUCCESSFUL_CONFIGS_SUBDIR: Path = Path("successful_configs")
UNSUCCESSFUL_CONFIGS_SUBDIR: Path = Path("unsuccessful_configs_with_errors")

# Subdir of each successful/unsuccessful folder holding the minimal FM configs
FM_CONFIGS_SUBDIR: str = "fm_configs"

# Compiled / aggregate output filenames
COMPILED_INPUT_SM_CONFIGS_FILE: str = "compiled_input_sm_configs.json"
COMPILED_OUTPUT_FM_CONFIGS_FILE: str = "compiled_output_fm_configs.json"
