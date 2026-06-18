# DishtaYantra — Configuration & Cloud Messaging

## YAML configuration (recommended)

The configuration system is now format-agnostic. Provide either
`config/application.yaml` (preferred) or `config/application.properties` -
the loader auto-detects the format by extension, and the server prefers
YAML when both exist.

YAML is flattened to the same dotted-key namespace used everywhere else, so
nothing else in the codebase changes:

```yaml
db:
  sqlite:
    path: data/dishtayantra.db        # -> key "db.sqlite.path"
server:
  port: 5002                          # -> "server.port" (read with get_int)
holiday_calendars:                    # -> "holiday_calendars" = "USA,CANADA"
  - USA                               #    plus "holiday_calendars.0" = "USA"
  - CANADA
```

### Variable substitution (both formats)

`${...}` substitution works identically in YAML and `.properties`:

- `${other.key}` - reference another config value (nested refs resolved).
- `${ENV_VAR}` - reference an environment variable.
- `${ENV_VAR:default}` - use the env var, or fall back to `default`.

Precedence (highest first): command-line `--key=value`, environment
variables, then config-file values.

```yaml
app:
  secret_key: "${SECRET_KEY:dev-only-change-me}"
logs:
  base: /var/log/dishtayantra
  archive: "${logs.base}/archive"     # nested reference
```

> Values are read as strings and coerced on access via `get_int`,
> `get_bool`, `get_list`, so `port: 5002` and `port: "${PORT:5002}"` behave
> identically.

### Migrating from .properties

No action required - `.properties` files keep working. To switch, translate
each `a.b.c=value` line into nested YAML and keep your `${...}` expressions
verbatim. The shipped `application.yaml` mirrors `application.properties`
exactly and is a ready reference.

`PropertiesConfigurator` is retained as an alias for the format-neutral
`ConfigurationManager`; either name works.

## AWS & Azure managed messaging

In addition to the object-store pub/sub (`s3://`, `azureblob://`,
`gcs://`), DAGs can now use native managed messaging. Add
`"resilient": true` to any of these for retry / outage-buffering /
reconnect.

### Amazon (requires `boto3`)

```jsonc
// SQS - queue semantics, long-polling, optional FIFO
{"destination": "sqs://orders-queue", "region": "us-east-1"}
{"source": "sqs://orders-queue", "region": "us-east-1",
 "wait_time_seconds": 10, "delete_on_read": true}

// Kinesis - ordered sharded streams
{"destination": "kinesis://market-ticks", "region": "us-east-1",
 "partition_key_field": "symbol"}
{"source": "kinesis://market-ticks", "region": "us-east-1",
 "iterator_type": "LATEST"}

// SNS - publish-only fan-out (consume via an SQS queue subscribed to it)
{"destination": "sns://alerts-topic", "region": "us-east-1"}
```

### Azure (requires `azure-servicebus` / `azure-eventhub`)

```jsonc
// Service Bus - queues and topics/subscriptions
{"destination": "servicebus://queue/orders",
 "connection_string": "${AZURE_SERVICEBUS_CONNECTION_STRING}"}
{"source": "servicebus://topic/ticks/risk-subscription",
 "connection_string": "${AZURE_SERVICEBUS_CONNECTION_STRING}"}

// Event Hubs - high-throughput streaming
{"destination": "eventhubs://market-data",
 "connection_string": "${AZURE_EVENTHUBS_CONNECTION_STRING}",
 "partition_key_field": "symbol"}
{"source": "eventhubs://market-data",
 "connection_string": "${AZURE_EVENTHUBS_CONNECTION_STRING}",
 "consumer_group": "$Default"}
```

Each SDK is imported lazily, so these modules load even when the cloud
library is not installed; a clear ImportError is raised only when you
actually construct an endpoint that needs it.
