# Kafka Connector Migration Utility

A powerful tool to migrate self-managed connectors to fully-managed connectors on Confluent Cloud. This utility maps self-managed connector configurations to fully-managed connector configurations and gives comprehensive errors and warnings during the migration. 

Follow the steps below to migrate your connectors to Confluent Cloud.

## Prerequisites

- Python 3.8+
- Self-managed Kafka Connect worker URLs
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

### Step 1: Specify the connector configuration

To get started with migration, you first need to translate the configuration of the self-managed connector you want to migrate. 
You can provide the connector configurations in the following ways:

1. Worker URL (Configurations are fetched directly from the workers).
2. Configuration directory or configuration file.

#### Using the worker URL

Run the following command to check the the migration feasibilty using the worker URL:

```bash
python src/discovery_script.py --worker-urls "http://worker1:8083,http://worker2:8083" --output-dir output/
```

##### Basic Authentication

If your Connect worker REST API requires basic authentication, provide the username and password:

```bash
python src/discovery_script.py \
  --worker-urls "http://worker1:8083,http://worker2:8083" \
  --output-dir output/ \
  --worker-username "myuser" \
  --worker-password "mypassword"
```

> [!NOTE]
> Both `--worker-username` and `--worker-password` must be provided together. If only one is provided, basic authentication will be disabled.

##### SSL certificate verification

While using the worker URL, the utility makes HTTP(s) requests to fetch configurations. By default, SSL certificate verification is **enabled** for security. However, you can disable it if you're working with self-signed certificates or internal services. Refer to the command below:

```bash
python src/discovery_script.py --worker-urls "http://worker1:8083,http://worker2:8083" --output-dir output/ --disable-ssl-verify
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

Run the following command to check the migration feasibilty using the directory of configuration file:

```bash
python src/discovery_script.py --config-dir configDir --output-dir output/
```

##### Using a configuration file

Run the following command to check the the migration feasibilty using the configuration file:

```bash
python src/discovery_script.py --config-file connectors.json --output-dir output/
```


> [!NOTE]
> If you want to include the latest transforms available on Confluent Cloud, you can provide additional information as shown in the command below:
> 
> ```bash
> python src/discovery_script.py \
>   --config-file connectors.json \
>   --output-dir output/ \
>   --environment-id <environment-id> \
>   --cluster-id <lkc-id> \
>   --bearer-token <api-token>
> ```
> 
> If your Connect worker requires basic authentication:
> 
> ```bash
> python src/discovery_script.py \
>   --config-file connectors.json \
>   --output-dir output/ \
>   --environment-id <environment-id> \
>   --cluster-id <lkc-id> \
>   --bearer-token <api-token> \
>   --worker-username <worker-username> \
>   --worker-password <worker-password>
> ```
> 
> If you want to use a secure bearer token:
> 
> ```bash
> python src/main.py \
>   --config-file connectors.json \
>   --output-dir output/ \
>   --environment-id <environment-id> \
>   --cluster-id <cluster-id> \
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

##### Migration summary (summary.txt)

The utility automatically generates migration summary reports in the `summary.txt` file after each migration process. 
It provides detailed analytics about the migration process, including:

- **Success/Failure Statistics**: Track migration success rates and identify common failure patterns.
- **Connector Type Analysis**: Analyze which connector types were most/least successful.
- **Error Analysis**: Categorize and count different types of migration errors.
- **Per-Cluster Analysis**: Detailed breakdown by connector cluster.
- **Mapping Error Details**: Specific error messages with occurrence counts.

The example below shows a sample summary in the output:

```
===============================================================================
 Overall Summary
===============================================================================
Number of Connector clusters scanned: 2
Total Connector configurations scanned: 25
Total Connectors that can be successfully migrated: 22
Total Connectors that have errors in migration: 3

===============================================================================
Summary By Connector Type
===============================================================================
✅ Connector types (successful across all clusters):
  - io.confluent.connect.jdbc.JdbcSinkConnector: 15
  - io.confluent.connect.elasticsearch.ElasticsearchSinkConnector: 7

❌ Connector types (with errors across all clusters):
  - io.confluent.connect.jdbc.JdbcSourceConnector: 3

===============================================================================
 Per-Cluster Summary (sorted by successful configurations for migration)
===============================================================================
Cluster Details: cluster-1
  Total Connector configurations scanned: 15
  Total Connectors that can be successfully migrated: 13
    ✅ Connector types (successful):
      - io.confluent.connect.jdbc.JdbcSinkConnector: 10
      - io.confluent.connect.elasticsearch.ElasticsearchSinkConnector: 3
  Total Connectors that have errors in migration: 2
    ❌ Connector types (with errors):
      - io.confluent.connect.jdbc.JdbcSourceConnector: 2
    ⚠️ Mapping errors:
      - 'Transform 'complex_transform' is not supported': found in 2 file(s)
```

##### Successful migration (successful_configs)

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

##### Migration with errors (unsuccessful_configs)

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

The table below shows the command line options valid for the python translation script (`discovery_script.py`):

| Option | Description | Required |
|--------|-------------|----------|
| `--config-file` | Path to JSON file containing connector configurations | No* |
| `--config-dir` | Path of directory with multiple json connector configuration files | No* |
| `--worker-urls` | Comma-separated list of worker URLs | No* |
| `--worker-urls-file` | Path to file containing worker URLs | No* |
| `--worker-username` | Username for basic authentication with Connect worker REST API | No |
| `--worker-password` | Password for basic authentication with Connect worker REST API | No |
| `--output-dir` | Output directory for all files (default: output) | No |
| `--env-id` | Confluent Cloud environment ID | No |
| `--lkc-id` | Confluent Cloud LKC cluster ID | No |
| `--bearer-token` | Confluent Cloud bearer token (api_key:api_secret) | No |
| `--prompt-bearer-token` | Prompt for bearer token securely | No |
| `--redact` | Redact sensitive configurations | No |
| `--sensitive-file` | Path to file containing sensitive config keys | No |
| `--worker-config-file` | Path to file containing additional worker configs | No |
| `--disable-ssl-verify` | Disable SSL certificate verification for HTTPS requests | No |

*Either `--config-file` or `--config-dir` or `--worker-urls`/`--worker-urls-file` is required.



### Step 2: Migrate the connector

Once you have fixed the mapping errors (if any), you can follow the steps below to complete the migration:


#### Prerequisites

- A Kafka cluster on Confluent Cloud (Cluster ID, Environment ID).


#### Create a connector with no data loss/duplication

To create a connector with no data loss and data duplication, run the command below to create a fully-managed connector:

```bash
python src/migrate_connector_script.py --worker-urls "<WORKER_URL>" --cluster-id "<CLUSTER_ID>" --environment-id "<ENVIRONMENT_ID>" --migration-mode "stop_create_latest_offset" --bearer-token "<BEARER_TOKEN>" --fm-config-dir "<INPUT_FM_CONFIGS_DIR>" --kafka-api-key "<KAFKA-API-KEY>" --kafka-api-secret "<KAFKA-API-SECRET>" --migration-output-dir "<CREATE_CONNECTOR_OUTPUT_DIRECTORY>"
```
> The python script stops the self-managed connector and fetches the latest offset and creates a fully-managed connector on Confluent Cloud using the fetched offset.

#### Create a connector with no downtime 

To create a connector with no downtime, run the command below to create a fully-managed connector:

```bash
python src/migrate_connector_script.py --worker-urls "<WORKER_URL>" --cluster-id "<CLUSTER_ID>" --environment-id "<ENVIRONMENT_ID>" --migration-mode "create_latest_offset" --bearer-token "<BEARER_TOKEN>" --fm-config-dir "<INPUT_FM_CONFIGS_DIR>" --kafka-api-key "<KAFKA-API-KEY>" --kafka-api-secret "<KAFKA-API-SECRET>" --migration-output-dir "<CREATE_CONNECTOR_OUTPUT_DIRECTORY>"
```

If your Connect worker requires basic authentication, add `--worker-username` and `--worker-password` parameters.

> The python script fetches the latest offset without stopping the connector and creates a fully-managed connector on Confluent Cloud using the fetched offset. This option may cause data duplication as the self-managed connector is still running.


#### Create connector without any offset consideration

If you want to create a fully-managed connector without stopping the self-managed connector and without any offset consideration, run the following command:

```bash
python src/migrate_connector_script.py --worker-urls "<WORKER_URL>" --cluster-id "<CLUSTER_ID>" --environment-id "<ENVIRONMENT_ID>" --migration-mode "create" --bearer-token "<BEARER_TOKEN>" --fm-config-dir "<INPUT_FM_CONFIGS_DIR>" --kafka-api-key "<KAFKA-API-KEY>" --kafka-api-secret "<KAFKA-API-SECRET>" --migration-output-dir "<CREATE_CONNECTOR_OUTPUT_DIRECTORY>"
```
> The python script creates the fully-managed connector on Confluent Cloud using the fully-managed configurations.

#### Command line options

The table below shows the command line options valid for the python migration script (`migrate_connector_script.py`):

| Option | Description | Required |
|--------|-------------|----------|
| `--fm-config-dir` | Path of directory with multiple fully-managed connector configuration files | Yes |
| `--worker-urls` | Comma-separated list of worker URLs | No *(1)* |
| `--worker-username` | Username for basic authentication with Connect worker REST API | No |
| `--worker-password` | Password for basic authentication with Connect worker REST API | No |
| `--migration-output-dir` | Output directory for migration output files (default: migration_output) | No |
| `--environment-id` | Confluent Cloud environment ID | Yes |
| `--cluster-id` | Confluent Cloud LKC cluster ID | Yes |
| `--bearer-token` | Confluent Cloud bearer token (api_key:api_secret) | Yes *(2)* |
| `--prompt-bearer-token` | Prompt for bearer token securely | Yes *(2)* |
| `--disable-ssl-verify` | Disable SSL certificate verification for HTTPS requests | No |
| `--migration-mode` | Connector migration mode. Options - [`stop_create_latest_offset`, `create`, `create_latest_offset`] | Yes |
| `--kafka-auth-mode` | Current mode for authentication between Kafka client (connector) and Kafka broker. Options ['SERVICE_ACCOUNT','KAFKA_API_KEY']. (default: `kafka_api_key`) | No |
| `--kafka-api-key` | Kafka API key for authentication. | Yes *(3)* |
| `--kafka-api-secret` | Kafka API secret for authentication. | Yes *(3)* |
| `--kafka-service-account-id` | Service account ID for authentication. | Yes *(3)* |



*(1)* - Required when the migration mode is set to `stop_create_latest_offset` or `create_latest_offset`.

*(2)* - If you are using `--bearer-token`,  `--prompt-bearer-token` is not applicable and vice versa.

*(3)* - You can use one of the two authentication modes. When `kafka-auth-mode` is set to `KAFKA_API_KEY`, you need to provide the Kafka API key and secret. If it is set to `SERVICE_ACCOUNT`, you need to provide the Kafka Service account ID.



## Mapping errors

The utility provides comprehensive error reporting to help you understand and resolve mapping issues. Some of the mapping errors are listed below:

### Template errors

**Fully-managed template not found**

```
No FM template found for connector class: io.debezium.connector.sqlserver.SqlServerConnector

*Solution: Reach out to Confluent support.
```

### Property mapping errors

**Required properties not translated**

```
"Required FM Config 'connection.host' could not be derived from given configs.",

*Solution: Reach out to Confluent support.*
```

**Invalid values**
```
"FM Config 'input.key.format' value 'BYTES' is not in the recommended values list: ['AVRO', 'JSON_SR', 'PROTOBUF', 'STRING']"
```
*Solution: Use one of the recommended values or check the default value*

**Required properties missing**
```
"Required property 'connection.url' needs a value but none was provided"
```
*Solution: Add the missing required property to your SM config*

**Semantic match failures**
```
"Failed to map property 'sm_property' - no semantic match found"
```
*Solution: The property couldn't be automatically mapped - may need manual configuration*


### Transform errors

**Unsupported transform type**

**Example 1**:

```
"Transform 'AddStreamTimestampField' of type 'com.github.jcustenborder.kafka.connect.transform.common.TimestampNowField$Value' is not supported in Fully Managed Connector. Potentially Custom SMT can be used.
```
*Solution: Consider implementing the transform as a Custom SMT in Confluent Cloud*

**Example 2**:

```
"Predicate 'predicate_0' is filtered out because it's associated with an unsupported transform."
```
*Solution: The predicate is automatically filtered when its associated transform is not supported. To include the predicate along the transform, create a Custom SMT.*


## Mapping warnings

The utility provides mapping warnings for certain configurations that may affect the connector behavior. Refer the examples below:

**Value mismatch**

**Example 1**:

```
"errors.log.include.messages : FM config has constant value 'false' but user provided 'true'. User given value will be ignored."
```

**Example 2**:

```
"Unused connector config 'catalog.pattern'. Given value will be ignored. Default value will be used if any."
```



## License

Apache 2.0 License

## Support

For issues and questions, reach out to [Confluent support](https://support.confluent.io/hc/en-us).

---

**Note**: This utility is designed to assist with migration but may require manual review and adjustment of the generated configurations for production use. 