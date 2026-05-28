"""Back-compat shim: forwards to :func:`connect_migrate.cli.migrate_cli.main`.

Preserves the existing customer command ``python src/migrate_connector_script.py ...``.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from connect_migrate.cli.migrate_cli import main

if __name__ == "__main__":
    main()
