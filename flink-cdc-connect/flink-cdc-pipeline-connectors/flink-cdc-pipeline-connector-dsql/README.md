# Flink CDC DSQL/PostgreSQL Sink Connector

Real-time data synchronization connector from PostgreSQL source to PostgreSQL-compatible targets (Aurora PostgreSQL, Amazon DSQL, Standard PostgreSQL). Supports full snapshot + incremental CDC + DDL auto-sync.

## Features

- **Full Snapshot Sync** — Auto-sync all existing data on first startup
- **Incremental CDC** — Real-time capture of INSERT / UPDATE / DELETE
- **DDL Auto-Sync** — ADD COLUMN / DROP COLUMN / RENAME COLUMN / ALTER COLUMN TYPE
- **Complete Type Support** — INTEGER, BIGINT, DECIMAL, BOOLEAN, TEXT, VARCHAR, DATE, TIME, TIMESTAMP, TIMESTAMPTZ, BYTEA, JSON/JSONB, UUID, INET
- **Batch Write** — Configurable batch size and timeout, UPSERT mode
- **Fault Tolerance** — Flink Checkpoint + Exactly-Once semantics
- **Connection Pool** — HikariCP with IAM authentication support

## Quick Start

### Pipeline Configuration

```yaml
source:
  type: postgres
  hostname: localhost
  port: 5432
  username: dbmgr
  password: "your_password"
  database-name: source_db
  schema-name: public
  tables: public.users,public.orders
  slot.name: flink_cdc_slot
  scan.startup.mode: snapshot
  decoding.plugin.name: pgoutput
  ddl-capture.enabled: true          # Enable DDL capture

sink:
  type: postgres                     # or: dsql
  host: your-aurora-endpoint.rds.amazonaws.com
  port: 5432
  database: target_db
  schema: public
  username: dbmgr
  password: "your_password"
  use-iam-auth: false
  batch-size: 1000
  batch-timeout: 1min
  schema-change.policy: evolve       # DDL sync policy

pipeline:
  name: "My CDC Pipeline"
  parallelism: 1

checkpoint:
  interval: 10s
  mode: EXACTLY_ONCE
```

## Configuration Parameters

### Source Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `hostname` | (required) | PostgreSQL host address |
| `port` | 5432 | Port |
| `username` | (required) | Username |
| `password` | (required) | Password |
| `database-name` | (required) | Database name |
| `schema-name` | public | Schema name |
| `tables` | (required) | Table list, comma-separated |
| `slot.name` | flink_cdc_slot | Replication slot name |
| `scan.startup.mode` | snapshot | Startup mode: snapshot / latest-offset |
| `decoding.plugin.name` | pgoutput | Logical decoding plugin |
| `ddl-capture.enabled` | false | **Enable DDL capture (requires EVENT TRIGGER privilege)** |

### Sink Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `type` | (required) | `postgres` or `dsql` |
| `host` | (required) | Target database host |
| `port` | 5432 | Port |
| `database` | (required) | Database name |
| `schema` | public | Target schema |
| `username` | (required) | Username |
| `password` | (required) | Password |
| `use-iam-auth` | true | Use IAM authentication |
| `batch-size` | 1000 | Batch commit size |
| `batch-timeout` | 5min | Batch commit timeout |
| `max-pool-size` | 10 | Connection pool max size |
| `enable-full-load` | true | Enable full load mode |
| `schema-change.policy` | evolve | **DDL sync policy** |

### DDL Sync Policy (`schema-change.policy`)

| Value | Behavior |
|-------|----------|
| `evolve` | Automatically execute DDL on target (default) |
| `ignore` | Skip DDL changes, only log |
| `exception` | Stop pipeline when DDL detected |

## DDL Synchronization

### How It Works

```
Source DDL (e.g. ALTER TABLE ADD COLUMN)
    |
PostgreSQL Event Trigger captures it
    |
INSERT INTO flink_cdc_ddl_command table
    |
CDC captures the INSERT event
    |
PostgresEventDeserializer parses DDL text
    |
Generates AddColumnEvent / DropColumnEvent / RenameColumnEvent
    |
SchemaCoordinator -> DsqlMetadataApplier
    |
Execute ALTER TABLE on target (when policy=evolve)
```

### Supported DDL Operations

| DDL Operation | Supported | Notes |
|---------------|-----------|-------|
| CREATE TABLE | Yes | Auto-created during initial sync |
| ALTER TABLE ADD COLUMN | Yes | Requires ddl-capture enabled |
| ALTER TABLE DROP COLUMN | Yes | Requires ddl-capture enabled |
| ALTER TABLE RENAME COLUMN | Yes | Requires ddl-capture enabled |
| ALTER TABLE ALTER COLUMN TYPE | Yes | Requires ddl-capture enabled |
| DROP TABLE | Yes | Requires ddl-capture enabled |
| TRUNCATE TABLE | Yes | Requires ddl-capture enabled |
| CREATE INDEX | No | Must create manually on target |

### DDL Capture Privilege Requirements

When `ddl-capture.enabled: true`, the source connector auto-creates on the source database:
- `public.flink_cdc_ddl_command` table
- `public.flink_cdc_capture_ddl()` function
- `flink_cdc_intercept_ddl` event trigger

**Required privileges:**
- `CREATE TABLE`
- `CREATE FUNCTION`
- `CREATE EVENT TRIGGER` (usually requires superuser)

**Behavior when insufficient privileges:** Pipeline starts normally, DDL sync unavailable, ERROR logged. Data sync is not affected.

## Data Type Mapping

| PostgreSQL Type | Flink CDC Type | Target PostgreSQL Type |
|----------------|----------------|----------------------|
| integer / int4 | INT | INTEGER |
| bigint / int8 | BIGINT | BIGINT |
| smallint / int2 | SMALLINT | SMALLINT |
| real / float4 | FLOAT | REAL |
| double precision | DOUBLE | DOUBLE PRECISION |
| numeric(p,s) | DECIMAL(p,s) | DECIMAL(p,s) |
| boolean | BOOLEAN | BOOLEAN |
| text | STRING | TEXT |
| varchar(n) | VARCHAR(n) | VARCHAR(n) |
| char(n) | CHAR(n) | CHAR(n) |
| date | DATE | DATE |
| time | TIME(0) | TIME |
| timestamp | TIMESTAMP(6) | TIMESTAMP |
| timestamptz | TIMESTAMP_LTZ(6) | TIMESTAMPTZ |
| bytea | BYTES | BYTEA |
| json / jsonb | STRING | TEXT |
| uuid | STRING | TEXT |
| inet | STRING | TEXT |

## Build

```bash
cd flink-cdc-dsql-master-main
mvn package -pl flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-dsql \
    -am -DskipTests -Dmaven.test.skip=true \
    -Dspotless.check.skip=true -Drat.skip=true
```

Output: `flink-cdc-pipeline-connector-dsql/target/flink-cdc-pipeline-connector-dsql-3.5-SNAPSHOT.jar`

## Deploy

Copy the following jars to `$FLINK_HOME/lib/`:
- `flink-cdc-pipeline-connector-dsql-3.5-SNAPSHOT.jar`
- `flink-cdc-pipeline-connector-postgres-3.5-SNAPSHOT.jar`

## Run

```bash
$FLINK_CDC_HOME/bin/flink-cdc.sh pipeline.yaml --flink-home $FLINK_HOME
```

## Cleanup DDL Capture Objects

If DDL sync is no longer needed, run on the source database:

```sql
DROP EVENT TRIGGER IF EXISTS flink_cdc_intercept_ddl;
DROP FUNCTION IF EXISTS public.flink_cdc_capture_ddl();
DROP TABLE IF EXISTS public.flink_cdc_ddl_command;
```

## Known Limitations

1. **CREATE INDEX not supported** — Flink CDC framework does not support index events; create manually on target
2. **inet type loses CIDR prefix** — `192.168.1.1/32` syncs as `192.168.1.1`
3. **DDL capture requires superuser** — Event triggers need elevated privileges; graceful degradation when insufficient
4. **PK swap not supported** — Exchanging primary key values between rows in one transaction may fail
