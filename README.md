# IIP — File Adapter

Spring Boot Kafka consumer — fans out canonical intern records into a CSV payroll feed. One of the sibling repos that make up the platform (see [`docs`](https://github.com/Azaken1248/iip-docs) for architecture, use cases, data model, and the implementation plan).

**Responsibilities (Release 1, UC-6):** consume `intern.created` as its own consumer group (`file-adapter`), check a local dedup store before appending, and follow the generic adapter pattern for retry/DLQ (see [Architecture §6](https://github.com/Azaken1248/iip-docs/blob/main/01-architecture.md)).

**Single-writer constraint:** this adapter must run as exactly one instance — concurrent writers would interleave and corrupt `interns.csv` (see [Architecture AD-6](https://github.com/Azaken1248/iip-docs/blob/main/01-architecture.md)).

## Run

```bash
./mvnw spring-boot:run
```

Requires a reachable Kafka broker (`KAFKA_BOOTSTRAP_SERVERS`) — see the [`infra`](https://github.com/Azaken1248/iip-infra) repo for local Docker Compose. Output path and dedup store path are configured via `FILE_OUTPUT_PATH` / `DEDUP_STORE_PATH`.

## Build & test

```bash
./mvnw verify
```

## Health

`GET /actuator/health`, `GET /actuator/metrics`.
