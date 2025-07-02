# Connector Migration Tool

This tool automates the process of discovering connector configurations from Kafka Connect workers and generating corresponding FM configurations.

## Features

- Discovers connector configurations from multiple Kafka Connect workers
- **Advanced sensitive data redaction** with 100+ predefined sensitive config keys
- **Worker health checking** to ensure only reachable workers are processed
- **Pattern-based sensitive data detection** for comprehensive security
- **Worker config extraction** and merging capabilities
- **File-based sensitive config loading** for custom sensitive keys
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

### Basic Usage
```bash
python src/main.py --worker-urls "http://worker1:8083,http://worker2:8083"
```

### Advanced Usage Examples

**With worker URLs from a file:**
```bash
python src/main.py --worker-urls-file workers.txt
```

**With redaction of sensitive information:**
```bash
python src/main.py --worker-urls "http://worker1:8083" --redact
```

**With custom sensitive config file:**
```bash
python src/main.py --worker-urls "http://worker1:8083" --sensitive-file sensitive_keys.txt
```

**With worker config file:**
```bash
python src/main.py --worker-urls "http://worker1:8083" --worker-config-file worker_configs.txt
```

**Complete example with all options:**
```bash
python src/main.py \
  --worker-urls "http://worker1:8083,http://worker2:8083" \
  --redact \
  --sensitive-file custom_sensitive_keys.txt \
  --worker-config-file worker_configs.txt \
  --output-dir custom_output \
  --sm-templates templates/sm \
  --fm-templates templates/fm
```

## Command Line Options

| Option | Description |
|--------|-------------|
| `--worker-urls` | Comma-separated list of worker URLs |
| `--worker-urls-file` | Path to file containing worker URLs |
| `--redact` | Redact sensitive configurations |
| `--sensitive-file` | Path to file containing sensitive config keys (one per line) |
| `--worker-config-file` | Path to file containing additional worker configs (key=value format) |
| `--output-dir` | Output directory for all files (default: output) |
| `--sm-templates` | Directory containing SM template files |
| `--fm-templates` | Directory containing FM template files |

## Sensitive Data Handling

The tool includes comprehensive sensitive data detection:

### Predefined Sensitive Keys
The tool comes with 100+ predefined sensitive configuration keys covering:
- AWS credentials and keys
- Azure authentication tokens
- Database passwords
- SSL/TLS certificates and passwords
- OAuth tokens and secrets
- API keys for various services
- And many more...

### Pattern-Based Detection
Automatically detects sensitive data using patterns:
- `password`
- `token`
- `secret`
- `credential`

### Custom Sensitive Keys
You can provide additional sensitive keys in a file:
```bash
# sensitive_keys.txt
custom.api.key
my.secret.token
internal.password
```

## Worker Configuration

The tool can load additional worker-level configurations from a file:
```bash
# worker_configs.txt
key.converter=org.apache.kafka.connect.json.JsonConverter
value.converter=org.apache.kafka.connect.json.JsonConverter
producer.bootstrap.servers=localhost:9092
consumer.bootstrap.servers=localhost:9092
```

## Output

The tool generates the following files in the output directory:

1. `connectors.json`: Contains all discovered connector configurations and worker configs
2. `all_fm_configs.json`: Contains all generated FM configurations
3. `fm_configs/*.json`: Individual FM configuration files for each connector
4. `migration.log`: Detailed log of the migration process

### Output Structure
```json
{
  "connectors": {
    "connector_name": {
      "name": "connector_name",
      "worker": "http://worker1:8083",
      "type": "source",
      "tasks": [...],
      "config": {...}
    }
  },
  "worker_configs": {
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter"
  }
}
```

## Error Handling

The tool includes comprehensive error handling and logging:
- Failed connector discoveries are logged but don't stop the process
- Failed connector processing is logged but doesn't stop the process
- Worker health checking ensures only reachable workers are processed
- All errors are written to the log file with full stack traces

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request 