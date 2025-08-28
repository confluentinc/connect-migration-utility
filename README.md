# Kafka Connector Migration Utility

A powerful tool to migrate self-managed connectors to fully-managed connectors on Confluent Cloud. This utility maps self-managed connector configurations to fully-managed connector configurations and gives comprehensive errors and warnings during the migration. 

Follow the steps below to migrate your connectors to Confluent Cloud.

## Prerequisites

- Python 3.8+
- self-managed Kafka Connect worker URLs
- Confluent Cloud environment with API access. (optional)

## Installation

Follow the steps below to install the migration tool:

1. Clone the Migration utility repository.

```bash
# Clone the repository
git clone <repository-url>
```

2. Go to the clone repository and install the dependencies:

```bash
cd connect-migration-utility

# Install dependencies
pip install -r requirements.txt
```


## Migrate 

This section covers the steps about using the migration utility tool to migrate your connector(s).

### Step 1: Translate the connector configuration

To get started with migration, you first need to translate the configuration of the self-managed connector you want to migrate. 
You can provide the connector configurations in the following ways:

1. Worker URL (Configurations are fetched directly from the workers)
2. Configuration directory or configuration file 

#### Using the worker URL

Run the following command to check the the migration feasibilty using the worker URL:

```bash
python src/main.py --worker-urls "http://worker1:8083,http://worker2:8083" --output-dir output/
```

##### SSL Certificate Verification

While using the worker URL, the utility makes HTTP(s) requests to fetch templates and configurations. By default, SSL certificate verification is **enabled** for security. However, you can disable it if you're working with self-signed certificates or internal services. Refer to the command below:

```bash
python src/main.py --worker-urls "http://worker1:8083,http://worker2:8083" --output-dir output/ --disable-ssl-verify
```

#### Using configuration file or directory

If you are providing the configuration directory or a file, you must provide it in the input format shown below:

##### Input format

The examples below show the sample input format of the connector configuration for the migration process.

> Only valid if you are using the configuration file or the configuration directory.

1. **Single Connector**:

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

2. **Multiple connectors**:


```json
{
  "connectors": {
    "my_connector1": {
      "name": "my_connector1",
      "config": {
        "connector.class": "MyClass1"
      }
    },
    "my_connector2": {
      "name": "my_connector2",
      "config": {
        "connector.class": "MyClass2"
      }
    }
  }
}
```

```json
[
  {
    "another_connector1": {
      "name": "another_connector1",
      "config": {
        "connector.class": "AnotherClass1"
      }
    }
  },
  {
    "another_connector2": {
      "name": "another_connector2",
      "config": {
        "connector.class": "AnotherClass2"
      }
    }
  }
]
```

```json
{
  "conn1": {
    "name": "conn1",
    "config": {
      "connector.class": "Class1"
    }
  },
  "conn2": {
    "name": "conn2",
    "config": {
      "connector.class": "Class2"
    }
  }
}
```

```json
{
  "special_connector1": {
    "Info": {
      "name": "special_connector1",
      "config": {
        "connector.class": "SpecialClass1"
      }
    }
  },
  "special_connector2": {
    "Info": {
      "name": "special_connector2",
      "config": {
        "connector.class": "SpecialClass2"
      }
    }
  }
}
```



##### Using directory of configuration files

Run the following command to check the the migration feasibilty using the directory of configuration file:

```bash
python src/main.py --config-dir configDir --output-dir output/
```

##### Using a configuration file

Run the following command to check the the migration feasibilty using the configuration file:

```bash
python src/main.py --config-file connectors.json --output-dir output/
```


> [!NOTE]
> If you want to include the latest transforms available on Confluent Cloud, you can provide additional information as shown in the command below:
> 
> ```bash
> python src/main.py \
>   --config-file connectors.json \
>   --output-dir output/ \
>   --env-id <environment-id> \
>   --lkc-id <lkc-id> \
>   --bearer-token <api-token>
> ```
> 
> If you want to use a secure bearer token:
> 
> ```bash
> python src/main.py \
>   --config-file connectors.json \
>   --output-dir output/ \
>   --env-id <environment-id> \
>   --lkc-id <lkc-id> \
>   --prompt-bearer-token
> ```


#### Output format

When you run the commands to check the feasibilty, it gives an output in the output directory, that will have the connector configuration, mentioning the 
mapping errors and the warnings. The directory structure looks like this:

```
output/
├── summary.txt                                    # A file that has a summary generated. 
├── discovered_configs
│   ├── successful_configs                      
│       ├── fm_configs                             # fully managed connector configuration JSON file
|       ├── all files                              #  JSON files that have the self-managed and fully-managed configurations, mapping errors and mapping warnings
|   ├── unsuccessful_configs
|       ├── all files
|       ├── fm_configs
```

The examples below show the sample output format of the connector configuration.

##### Successful Migration (successful_configs)

The example below show the output that has no errors and warnings. 
If the output has no mapping errors, you can move to Step 2.

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
}
```

##### Migration with Errors (unsuccessful_configs)

The example below show the output that has mapping errors and warnings.
You must fix the mapping errors before moving further. If you migrate the connector with mapping errors in the configurations, it will lead to errors.


```json
{
  "name": "my-connector",
  "sm_config": { /* original SM config */ },
  "config": { /* successfully mapped config */ },
  "mapping_errors": [
    "Transform 'unwrap' of type 'io.debezium.transforms.ExtractNewRecordState' is not supported in fully-managed Connector. Potentially Custom SMT can be used.",
    "Predicate 'predicate_0' is filtered out because it's associated with an unsupported transform."
  ],
}
```
### Command line options

The table below shows the command line options valid for the python translation script:

| Option | Description | Required |
|--------|-------------|----------|
| `--config-file` | Path to JSON file containing connector configurations | No* |
| `--config-dir` | Path of directory with multiple json connector configuration files | No* |
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
| `--disable-ssl-verify` | Disable SSL certificate verification for HTTPS requests | No |

*Either `--config-file` or `--config-dir` or `--worker-urls`/`--worker-urls-file` is required



### Step 2: Migrate the connector

Once you have fixed the mapping errors (if any), you can follow the steps below to complete the migration:


#### Prerequisites

- A Kafka cluster on Confluent Cloud (Cluster ID, Environment ID).


#### Create a connector with No Data loss/duplication

To create a connector with no data loss and data duplication, run the command below to create a fully-managed connector:

```bash
command
```
> The python script stops the self-managed connector and fetches the latest offset and creates a fully-managed connector on Confluent Cloud using the fetched offset.

#### Create a connector with no downtime 

To create a connector with no downtime, run the command below to create a fully-managed connector:

```bash
command
```
> The python script fetched the latest offset without stopping the connector and creates a fully-managed connector on Confluent Cloud using the fetched offset. This option may cause data duplication as the self-managed connector is still running.


#### Create connector without any offset consideration

If you want to create a fully-managed connector without stopping the self-managed connector and without any offset consideration, run the following command:

```bash
command
```
> The python script creates the fully-managed connector on Confluent Cloud using the translated configurations.

#### Command Line Options

Table with command line options


## Errors

### Mapping Errors

The utility provides comprehensive error reporting to help you understand and resolve mapping issues:

#### Transform Errors

**Unsupported Transform Type**
```
"Transform 'unwrap' of type 'io.debezium.transforms.ExtractNewRecordState' is not supported in fully-managed connectors. Potentially Custom SMT can be used."
```
*Solution: Consider implementing the transform as a Custom SMT in Confluent Cloud*

#### Predicate Errors

**Associated with Unsupported Transform**
```
"Predicate 'predicate_0' is filtered out because it's associated with an unsupported transform."
```
*Solution: The predicate is automatically filtered when its associated transform is not supported. To include the predicate along the transform, create a Custom SMT.*

#### Property Mapping Errors

**Unmapped Properties**
```
"Config 'custom.property' not exposed for fully managed connector"
```
*Solution: The property is not available in FM - check if it's needed or can be replaced or reach out Confluent support*

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

#### Validation Errors

**Config Defs Filtering**
```
"Config 'internal.property' not exposed for fully managed connector"
```
*Solution: Property filtered out as it's not in FM template config_defs*


## Troubleshooting

### Common Issues

**HTTP Connection Errors**
- Verify worker URLs are accessible.
- Check network connectivity.
- Ensure proper authentication for Confluent Cloud API.

**Template Not Found**
- Verify FM template files exist in `templates/fm/`.
- Check template naming convention.
- Ensure connector.class matches template.

**Transform Filtering**
- Review FM transform support documentation.
- Consider Custom SMT alternatives.
- Check if transforms are essential for your use case.


## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

Apache 2.0 License

## Support

For issues and questions:
- Create an issue in the repository.
- Check the troubleshooting section.
- Review the mapping errors documentation above.

---

**Note**: This utility is designed to assist with migration but may require manual review and adjustment of the generated configurations for production use. 