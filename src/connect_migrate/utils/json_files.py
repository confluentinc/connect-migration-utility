"""Small helpers around reading and writing JSON files.

All file I/O explicitly uses UTF-8 to avoid platform-dependent encoding
(Windows ``cp1252`` etc.) corrupting connector names and values that
contain non-ASCII characters.

Writes are atomic: ``write_json`` writes to a sibling tempfile and then
``os.replace``s it into place, so a crash mid-write can't leave a
half-written file at ``path``.
"""

import json
import os
import tempfile
from pathlib import Path
from typing import Any, Iterator, Union


PathLike = Union[str, Path]


def read_json(path: PathLike) -> Any:
    """Load and return the JSON content at ``path``."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: PathLike, obj: Any, indent: int = 2) -> None:
    """Atomically write ``obj`` to ``path`` as indented UTF-8 JSON."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        prefix=path.name + ".",
        suffix=".tmp",
        dir=str(path.parent),
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(obj, f, indent=indent)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def iter_json_files(directory: PathLike) -> Iterator[Path]:
    """Yield every ``*.json`` file directly under ``directory``."""
    d = Path(directory)
    if not d.exists():
        return
    for entry in sorted(d.glob("*.json")):
        if entry.is_file():
            yield entry
