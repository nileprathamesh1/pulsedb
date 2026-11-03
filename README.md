# PulseDB

Real-time Change Data Capture pipeline that taps into the PostgreSQL Write-Ahead Log and streams every database change to Apache Kafka.

## Stack

- Java 21, Spring Boot 3.2.3, Maven
- Debezium 2.5.0 (embedded engine — no Kafka Connect needed)
- Apache Kafka (Confluent 7.4.0)
- Redis 7.2 (cache invalidation)
- PostgreSQL 15 (with logical replication enabled)
- Prometheus + Grafana (observability)

## Architecture

```
PostgreSQL WAL (logical replication)
    ↓ Debezium Embedded Engine
ChangeEventHandler  →  ChangeEvent (typed Java object)
    ↓ ChangeEventProducer
Kafka Topics: pulsedb.users | pulsedb.orders | pulsedb.products
    ↓ Three independent consumer groups:
CacheInvalidationConsumer  →  Redis DELETE(key)
AuditLogConsumer           →  pulse_audit_log INSERT
WebhookDeliveryConsumer    →  HTTP POST to registered URLs
```

## Running Locally

### 1. Start infrastructure

```bash
docker-compose up -d
```

This starts PostgreSQL 15 (with WAL enabled), Kafka, Zookeeper, Redis, Prometheus, and Grafana.

Wait ~30 seconds for everything to be healthy:

```bash
docker-compose ps
```

### 2. Start PulseDB

```bash
mvn spring-boot:run
```

You'll see Debezium connect and take an initial snapshot of the existing rows:

```
Starting PulseDB — Real-time CDC Pipeline
Connecting to PostgreSQL WAL via Debezium embedded engine...
Captured INSERT on table=users rowId=1 lsn=12345
Captured INSERT on table=users rowId=2 lsn=12349
Captured INSERT on table=products rowId=1 lsn=12353
```

### 3. Trigger a change

Connect to PostgreSQL and insert a row:

```bash
docker exec -it pulsedb-postgres psql -U pulsedb_user -d pulsedb
```

```sql
INSERT INTO orders (user_id, amount, status) VALUES (1, 2999.00, 'PENDING');
UPDATE users SET name = 'Alice Johnson-Smith' WHERE email = 'alice@example.com';
DELETE FROM products WHERE name = 'Mechanical Keyboard';
```

Watch the PulseDB logs — you'll see each change stream through in real time.

### 4. Register a webhook

```bash
curl -X POST http://localhost:8080/api/webhooks \
  -H "Content-Type: application/json" \
  -d '{
    "tableName": "orders",
    "targetUrl": "https://webhook.site/your-unique-id",
    "operations": ["INSERT", "UPDATE"]
  }'
```

Get a free webhook test URL at https://webhook.site

### 5. View metrics

- Prometheus: http://localhost:9090
- Grafana: http://localhost:3000 (admin / admin)

Key metrics to watch:
- `pulsedb_events_captured_total` — WAL reads per table
- `pulsedb_events_published_total` — Kafka publishes
- `pulsedb_capture_latency_ms` — end-to-end latency
- `pulsedb_webhooks_delivered_total` — successful webhook deliveries

## API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | /api/webhooks | Register a new webhook |
| GET | /api/webhooks | List all webhooks |
| DELETE | /api/webhooks/{id} | Delete a webhook |
| PUT | /api/webhooks/{id}/pause | Pause a webhook |
| GET | /actuator/health | Health check |
| GET | /actuator/prometheus | Prometheus metrics |

## Project Structure

```
src/main/java/com/pulsedb/
├── PulseDbApplication.java         Main class
├── model/
│   ├── ChangeEvent.java            Core event object flowing through the pipeline
│   ├── OperationType.java          INSERT / UPDATE / DELETE / READ enum
│   └── WebhookSubscription.java    Webhook subscriber model
├── config/
│   ├── DebeziumConfig.java         Debezium engine properties (WAL connection)
│   ├── KafkaConfig.java            Topics, producer, consumer factory
│   └── JacksonConfig.java          ObjectMapper with Java time module
├── cdc/
│   ├── DebeziumEngineManager.java  Starts/stops Debezium on its own thread
│   └── ChangeEventHandler.java     Converts raw WAL records → ChangeEvents
├── kafka/
│   └── ChangeEventProducer.java    Routes ChangeEvents to correct Kafka topics
├── consumer/
│   ├── CacheInvalidationConsumer.java  Deletes stale Redis keys
│   ├── AuditLogConsumer.java           Writes to pulse_audit_log
│   └── WebhookDeliveryConsumer.java    HTTP POST to subscribers with retry
├── service/
│   └── WebhookSubscriptionService.java  In-memory subscription registry
├── api/
│   └── WebhookController.java      REST endpoints for webhook management
└── metrics/
    └── PulseMetrics.java           Prometheus counters and timers
```

## How WAL Replication Works

1. PostgreSQL writes every change to the WAL *before* committing to the actual table
2. `wal_level=logical` in docker-compose enables human-readable WAL decoding
3. Debezium creates a **replication slot** (`pulsedb_slot`) — PostgreSQL uses this to track how far Debezium has read so it doesn't garbage-collect WAL files we haven't processed
4. Debezium connects as a replication client and reads from the slot
5. The `pgoutput` plugin decodes binary WAL into structured change records
6. Debezium's embedded engine calls `ChangeEventHandler.handleChangeEvent()` for every record
7. We parse the JSON payload, build a `ChangeEvent`, and publish to Kafka
