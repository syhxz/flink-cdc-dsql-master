# Flink CDC PostgreSQL to PostgreSQL/Aurora/DSQL Connector

A real-time data synchronization tool based on Apache Flink CDC 3.5, supporting full snapshot + incremental CDC + automatic DDL sync.

## Features

- **Full + Incremental Sync** — Initial full snapshot, then WAL-based incremental CDC
- **DDL Auto-Sync** — Captures DDL via PostgreSQL event triggers, auto-syncs ADD/DROP/RENAME/ALTER COLUMN
- **Complete Type Support** — INT, BIGINT, DECIMAL, BOOLEAN, TEXT, VARCHAR, DATE, TIME, TIMESTAMP, TIMESTAMPTZ, BYTEA, JSON/JSONB, UUID, INET
- **Batch Write** — UPSERT mode with configurable batch size and timeout
- **Multi-Target** — Supports Aurora PostgreSQL / Amazon DSQL / Standard PostgreSQL

## Quick Start

### Configuration Example

```yaml
source:
  type: postgres
  hostname: source-host
  port: 5432
  username: dbmgr
  password: "your_password"
  database-name: source_db
  schema-name: public
  tables: public.users,public.orders
  slot.name: flink_cdc_slot
  scan.startup.mode: snapshot
  decoding.plugin.name: pgoutput
  ddl-capture.enabled: true

sink:
  type: postgres              # or: dsql
  host: target-host
  port: 5432
  database: target_db
  schema: public
  username: dbmgr
  password: "your_password"
  use-iam-auth: false
  batch-size: 1000
  batch-timeout: 1min
  schema-change.policy: evolve  # evolve|ignore|exception

pipeline:
  name: "My CDC Pipeline"
  parallelism: 1

checkpoint:
  interval: 10s
  mode: EXACTLY_ONCE
```

### Build

```bash
mvn package -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dsql \
    -am -DskipTests -Dmaven.test.skip=true \
    -Dspotless.check.skip=true -Drat.skip=true
```

### Deploy

Copy the following jars to `$FLINK_HOME/lib/`:
- `flink-cdc-pipeline-connector-dsql-3.5-SNAPSHOT.jar`
- `flink-cdc-pipeline-connector-postgres-3.5-SNAPSHOT.jar`

### Run

```bash
$FLINK_CDC_HOME/bin/flink-cdc.sh pipeline.yaml --flink-home $FLINK_HOME
```

## Configuration

### Source Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `hostname` | (required) | PostgreSQL host |
| `port` | 5432 | Port |
| `username` | (required) | Username |
| `password` | (required) | Password |
| `database-name` | (required) | Database name |
| `tables` | (required) | Table list (comma-separated) |
| `slot.name` | flink_cdc_slot | Replication slot name |
| `ddl-capture.enabled` | false | Enable DDL capture (requires EVENT TRIGGER privilege) |

### Sink Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `type` | (required) | `postgres` or `dsql` |
| `host` | (required) | Target database host |
| `database` | (required) | Database name |
| `username` | (required) | Username |
| `password` | (required) | Password |
| `batch-size` | 1000 | Batch commit size |
| `batch-timeout` | 5min | Batch timeout |
| `schema-change.policy` | evolve | DDL policy: evolve/ignore/exception |

## DDL Synchronization

With `ddl-capture.enabled: true`, the pipeline automatically creates an event trigger on the source database at startup:

```
Source DDL -> Event Trigger -> flink_cdc_ddl_command table -> CDC capture -> Parse to SchemaChangeEvent -> Execute ALTER TABLE on target
```

Supported: ADD COLUMN / DROP COLUMN / RENAME COLUMN / ALTER COLUMN TYPE / DROP TABLE / TRUNCATE TABLE

## Detailed Documentation

See: [DSQL Connector README](flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dsql/README.md)

## License

Apache License 2.0
