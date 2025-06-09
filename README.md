# Connector Migration Tool

This tool automates the process of discovering connector configurations from Kafka Connect workers and generating corresponding FM configurations.

## Features

- Discovers connector configurations from multiple Kafka Connect workers
- Supports redaction of sensitive information
- Parses JDBC URLs to extract connection details
- Generates FM-compatible configurations
- Comprehensive logging of the migration process

## Directory Structure

```
connect-migration-utility/
├── src/
│   ├── main.py              # Main script orchestrating the migration
│   ├── config_discovery.py  # Handles discovering connector configs
│   └── connector_comparator.py  # Processes and maps connector configs
├── output/
│   ├── connectors.json      # Discovered connector configurations
│   ├── all_fm_configs.json  # All generated FM configurations
│   └── fm_configs/          # Individual FM configuration files
├── templates/               # Template files (if needed)
├── requirements.txt         # Python dependencies
└── README.md               # This file
```

## Installation

1. Create a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Usage

Basic usage:
```bash
python src/main.py --worker-urls "http://worker1:8083,http://worker2:8083"
```

With worker URLs from a file:
```bash
python src/main.py --worker-urls-file workers.txt
```

With redaction of sensitive information:
```bash
python src/main.py --worker-urls "http://worker1:8083" --redact
```

Specify custom output directory:
```bash
python src/main.py --worker-urls "http://worker1:8083" --output-dir custom_output
```

## Output

The tool generates the following files in the output directory:

1. `connectors.json`: Contains all discovered connector configurations
2. `all_fm_configs.json`: Contains all generated FM configurations
3. `fm_configs/*.json`: Individual FM configuration files for each connector
4. `migration.log`: Detailed log of the migration process

## Error Handling

The tool includes comprehensive error handling and logging:
- Failed connector discoveries are logged but don't stop the process
- Failed connector processing is logged but doesn't stop the process
- All errors are written to the log file with full stack traces

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request 