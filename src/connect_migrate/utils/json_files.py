"""Small helpers around reading and writing JSON files."""

import json
from pathlib import Path
from typing import Any, Iterator, Union


PathLike = Union[str, Path]


def read_json(path: PathLike) -> Any:
    """Load and return the JSON content at ``path``."""
    with open(path, "r") as f:
        return json.load(f)


def write_json(path: PathLike, obj: Any, indent: int = 2) -> None:
    """Write ``obj`` to ``path`` as indented JSON (overwrites)."""
    with open(path, "w") as f:
        json.dump(obj, f, indent=indent)


def iter_json_files(directory: PathLike) -> Iterator[Path]:
    """Yield every ``*.json`` file directly under ``directory``."""
    d = Path(directory)
    if not d.exists():
        return
    for entry in sorted(d.glob("*.json")):
        if entry.is_file():
            yield entry
