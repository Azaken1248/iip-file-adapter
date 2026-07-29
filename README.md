# IIP — File Adapter

Spring Boot Kafka consumer — fans out canonical intern records into a CSV payroll feed. One of the sibling repos that make up the platform (see [`docs`](https://github.com/Azaken1248/iip-docs) for architecture, use cases, data model, and the implementation plan).

**Responsibilities (Release 1, UC-6):** consume `intern.created` as its own consumer group (`file-adapter`), check a local dedup store before appending, and follow the generic adapter pattern for retry/DLQ (see [Architecture §6](https://github.com/Azaken1248/iip-docs/blob/main/01-architecture.md)).

**Single-writer constraint:** this adapter must run as exactly one instance — concurrent writers would interleave and corrupt `interns.csv` (see [Architecture AD-6](https://github.com/Azaken1248/iip-docs/blob/main/01-architecture.md)).

## Run

```bash
./mvnw spring-boot:run
```

Requires a reachable Kafka broker (`KAFKA_BOOTSTRAP_SERVERS`) — see the [`infra`](https://github.com/Azaken1248/iip-infra) repo for local Docker Compose. Output path and dedup store path are configured via `FILE_OUTPUT_PATH` / `DEDUP_STORE_PATH`.

## Envelope validation (Release 4)

Every consumed message is checked against the platform's envelope schema
before anything else happens — before deserialization, before the dedup guard, before the CSV append. The schema
is fetched once at startup from the Schema Registry
(`ENVELOPE_SCHEMA_SOURCE=registry`, `SCHEMA_REGISTRY_URL`); this image ships
no copy of it, so a deployment that forgets those two fails at startup rather
than processing unvalidated messages.

It validates for itself even though the source service also validates on the
way out. That is not redundancy: the guarantee this adapter offers has to hold
when the thing upstream of it is wrong, and *upstream is correct* is not
something it can check at 3am. A message that fails is quarantined to
`iip.dlq` as `SCHEMA_VIOLATION` without retrying — identical bytes against an
identical schema fail identically.

What arrives is still plain canonical JSON. The registry owns the schema, not
the byte format: no magic byte, no schema id, no client library needed to read
a topic.

## Build & test

```bash
./mvnw verify
```

## Health

`GET /actuator/health`, `GET /actuator/metrics`.
