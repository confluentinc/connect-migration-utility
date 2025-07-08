# Kafka Connector Migration Utility

A powerful tool to migrate Self-Managed (SM) Kafka Connect configurations to Fully Managed (FM) configurations on Confluent Cloud. This utility intelligently maps SM connector configs to FM configs using semantic matching, template-based mappings, and comprehensive error reporting.

## 🚀 Features

- **Intelligent Mapping**: Uses semantic matching with configurable thresholds to map SM properties to FM properties
- **Template-Based Processing**: Leverages FM templates to understand supported configurations and mappings
- **Transform & Predicate Filtering**: Automatically filters out unsupported transforms and their associated predicates
- **JDBC URL Parsing**: Automatically extracts database connection details from JDBC URLs
- **Comprehensive Error Reporting**: Detailed mapping errors with actionable guidance
- **HTTP Integration**: Fetches SM templates from worker URLs and FM transforms from Confluent Cloud API
- **Fallback Support**: Uses local fallback files when HTTP calls fail

## 📋 Prerequisites

- Python 3.8+
- Self-Managed Kafka Connect worker URLs
- Confluent Cloud environment with API access(optional)

## 🛠️ Installation

```bash
# Clone the repository
git clone <repository-url>
cd connect-migration-utility

# Install dependencies
pip install -r requirements.txt
```

## 📁 Project Structure

```
connect-migration-utility/
├── src/
│   ├── connector_comparator.py    # Main migration logic
│   ├── config_discovery.py        # HTTP client for fetching templates
│   ├── semantic_matcher.py        # Semantic matching engine
│   └── main.py                    # CLI entry point
├── templates/
│   ├── fm/                        # FM template files
│   └── sm/                        # SM template files (optional)
├── output/                        # Generated FM configs
├── fm_transforms_list.json        # Fallback FM transforms
└── README.md
```

## 🎯 Usage

### Basic Usage - From Config File

```bash
python src/main.py --config-file connectors.json --output-dir output/
```

### Basic Usage - From Worker Discovery

```bash
python src/main.py --worker-urls "http://worker1:8083,http://worker2:8083" --output-dir output/
```

### Advanced Usage with Confluent Cloud Integration

```bash
python src/main.py \
  --config-file connectors.json \
  --output-dir output/ \
  --env-id <environment-id> \
  --lkc-id <lkc-id> \
  --bearer-token <api-token>
```

### Secure Bearer Token Input

```bash
python src/main.py \
  --config-file connectors.json \
  --output-dir output/ \
  --env-id <environment-id> \
  --lkc-id <lkc-id> \
  --prompt-bearer-token
```

### Command Line Options

| Option | Description | Required |
|--------|-------------|----------|
| `--config-file` | Path to JSON file containing connector configurations | No* |
| `--worker-urls` | Comma-separated list of worker URLs | No* |
| `--worker-urls-file` | Path to file containing worker URLs | No* |
| `--output-dir` | Output directory for all files (default: output) | No |
| `--env-id` | Confluent Cloud environment ID | No |
| `--lkc-id` | Confluent Cloud LKC cluster ID | No |
| `--bearer-token` | Confluent Cloud bearer token (api_key:api_secret) | No |
| `--prompt-bearer-token` | Prompt for bearer token securely | No |
| `--redact` | Redact sensitive configurations | No |
| `--sensitive-file` | Path to file containing sensitive config keys | No |
| `--worker-config-file` | Path to file containing additional worker configs | No |

*Either `--config-file` or `--worker-urls`/`--worker-urls-file` is required

## 📊 Input Format

### Single Connector
```json
{
  "name": "my-connector",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "connection.url": "jdbc:mysql://localhost:3306/mydb",
    "table.whitelist": "users",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "predicates": "predicate_0",
    "transforms.unwrap.predicate": "predicate_0",
    "predicates.predicate_0.type": "org.apache.kafka.connect.transforms.predicates.HasHeaderKey",
    "predicates.predicate_0.name": "header_key"
  }
}
```

### Multiple Connectors
```json
[
  {
    "name": "connector-1",
    "config": { ... }
  },
  {
    "name": "connector-2", 
    "config": { ... }
  }
}
```

## 🔄 Mapping Process

The migration utility follows a sophisticated multi-step mapping process:

### 1. Direct Matching
- Exact property name matches between SM and FM configs
- Validates values against recommended values from FM templates

### 2. Template-Based Mappings
- **Switch Mappings**: Maps SM values to FM values based on conditions
- **Value Mappings**: Direct value assignments from SM to FM
- **Template Variables**: Handles `${variable}` references in mappings

### 3. Static Mappings
- Predefined mappings for common properties (converters, etc.)
- Connector-type specific mappings (source vs sink)

### 4. Semantic Matching
- Uses NLP-based similarity scoring (threshold: 0.7)
- Matches properties based on name and description similarity
- Prevents false positives with additional validation

### 5. Transform & Predicate Processing
- Filters transforms based on FM support
- Automatically filters predicates associated with unsupported transforms
- Provides detailed error messages for filtered items

## ⚠️ Mapping Errors

The utility provides comprehensive error reporting to help you understand and resolve mapping issues:

### Transform Errors

**Unsupported Transform Type**
```
"Transform 'unwrap' of type 'io.debezium.transforms.ExtractNewRecordState' is not supported in Fully Managed Connector. Potentially Custom SMT can be used."
```
*Solution: Consider implementing the transform as a Custom SMT in Confluent Cloud*

**Missing Transform Type**
```
"Transform 'transform_1' has no type specified"
```
*Solution: Add the missing `transforms.transform_1.type` property*

### Predicate Errors

**Associated with Unsupported Transform**
```
"Predicate 'predicate_0' is filtered out because it's associated with an unsupported transform."
```
*Solution: The predicate is automatically filtered when its associated transform is not supported*

### Property Mapping Errors

**Unmapped Properties**
```
"Config 'custom.property' not exposed for fully managed connector"
```
*Solution: The property is not available in FM - check if it's needed or can be replaced*

**Invalid Values**
```
"Value 'invalid_value' for 'property.name' is not in recommended values: ['value1', 'value2']"
```
*Solution: Use one of the recommended values or check the default value*

**Required Properties Missing**
```
"Required property 'connection.url' needs a value but none was provided"
```
*Solution: Add the missing required property to your SM config*

**Semantic Match Failures**
```
"Failed to map property 'sm_property' - no semantic match found"
```
*Solution: The property couldn't be automatically mapped - may need manual configuration*

### Validation Errors

**Config Defs Filtering**
```
"Config 'internal.property' not exposed for fully managed connector"
```
*Solution: Property filtered out as it's not in FM template config_defs*

## 🔧 Configuration

### Semantic Matching Threshold
Adjust the similarity threshold in `connector_comparator.py`:
```python
semantic_threshold=0.7  # Default: 0.7 (70% similarity)
```


## 📈 Output Format

### Successful Migration
```json
{
  "name": "my-connector",
  "sm_config": { /* original SM config */ },
  "config": {
    "connector.class": "JdbcSource",
    "input.key.format": "AVRO",
    "input.value.format": "AVRO",
    "connection.url": "jdbc:mysql://localhost:3306/mydb",
    "table.whitelist": "users"
  },
  "mapping_errors": [],
  "unmapped_configs": []
}
```

### Migration with Errors
```json
{
  "name": "my-connector",
  "sm_config": { /* original SM config */ },
  "config": { /* successfully mapped config */ },
  "mapping_errors": [
    "Transform 'unwrap' of type 'io.debezium.transforms.ExtractNewRecordState' is not supported in Fully Managed Connector. Potentially Custom SMT can be used.",
    "Predicate 'predicate_0' is filtered out because it's associated with an unsupported transform."
  ],
  "unmapped_configs": ["custom.property"]
}
```

## 🚨 Troubleshooting

### Common Issues

**HTTP Connection Errors**
- Verify worker URLs are accessible
- Check network connectivity
- Ensure proper authentication for Confluent Cloud API

**Template Not Found**
- Verify FM template files exist in `templates/fm/`
- Check template naming convention
- Ensure connector.class matches template

**Semantic Matching Issues**
- Adjust similarity threshold if needed
- Review property descriptions in templates
- Consider adding static mappings for common cases

**Transform Filtering**
- Review FM transform support documentation
- Consider Custom SMT alternatives
- Check if transforms are essential for your use case

### Debug Mode

Enable debug logging for detailed processing information:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## 📄 License

[Add your license information here]

## 🆘 Support

For issues and questions:
- Create an issue in the repository
- Check the troubleshooting section
- Review the mapping errors documentation above

---

**Note**: This utility is designed to assist with migration but may require manual review and adjustment of the generated configurations for production use. 