# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

A multi-module Maven project demonstrating Apache Flink (1.20.1, Java 11) streaming jobs against
a local Kafka + PostgreSQL + HDFS stack. Both jobs consume the same `credit-card-transactions`
Kafka topic and showcase different Flink capabilities:

- **flink-demo-1** — Fraud detection using **Flink CEP**. Detects 3 strictly-consecutive
  transactions per user within a 1-minute window (Processing Time) and emits `FraudAlert`s to the
  `fraud-alerts` topic.
- **flink-demo-2** — Monthly per-card consumption aggregation. Keys by `(cardNumber, yearMonth)`,
  accumulates monthly spend with an **event-time timer** firing at month-end, enriches each card's
  credit limit via **Flink CDC** (PostgreSQL WAL → broadcast state), then classifies the card level
  with a **Drools** decision table. Emits `MonthlyConsumptionFact` to `credit-card-level-results`.
- **flink-demo-common** — Shared `CreditCardTransaction` model + `TransactionDeserializer`. Both
  app modules depend on it.

Note: source comments and log messages are written in Traditional Chinese; match that style when
editing existing files.

## Architecture

### Module dependency
`flink-demo-1` and `flink-demo-2` each depend on `flink-demo-common`. The parent `pom.xml` holds all
`dependencyManagement` (versions), common dependencies, and the shared `maven-shade-plugin`
configuration. Each app module only overrides `maven-shade-plugin.mainClass` (a parent property) to
set its uber-jar entry point.

### Dependency scoping — this matters at runtime
- Flink core (`flink-streaming-java`, `flink-clients`), Hadoop client, and Lombok are `provided`.
  They are NOT in the shaded jar — they come from the Flink container image / mounted classpath.
- **Hadoop client jars are downloaded at runtime** by `scripts/start-flink.sh` from Maven Central
  into `lib/hadoop/` and mounted into every Flink container via `HADOOP_CLASSPATH`. This is why HDFS
  checkpointing works despite Hadoop being `provided`.
- `flink-cep`, `flink-connector-kafka`, `flink-connector-postgres-cdc`, `flink-statebackend-rocksdb`,
  and Drools ARE bundled into the shaded jar.
- The shade config excludes SLF4J/Log4j so container-provided logging is used.

### Runtime topology (all containers on Podman network `flink-network`)
Jobs address services by container hostname, hardcoded in the `*Job.java` classes:
- `kafka:9092` — Kafka broker (Confluent CP-Kafka, KRaft combined mode)
- `namenode:9000` — HDFS, used for checkpoint storage (`hdfs://namenode:9000/flink/checkpoints/...`)
- `postgres:5432` — database `carddb`, table `public.card_limit` (demo-2 CDC source)
- `jobmanager:8081` — Flink JobManager REST/UI; 2 TaskManagers (3 slots each)

### flink-demo-2 CDC + broadcast join (the non-obvious part)
`Demo2Job` connects the keyed transaction stream with a **broadcast** stream of `card_limit` updates,
processed by `MonthlyAggregationFunction` (a `KeyedBroadcastProcessFunction`):
- CDC updates land in broadcast state (`CARD_LIMIT_STATE_DESCRIPTOR`); deletes remove the entry.
- `processElement` accumulates the running monthly total in `ValueState` (40-day TTL) and registers
  an event-time timer at the start of the next month.
- `onTimer` reads the card limit from broadcast state (default `100000.0`), runs the Drools rule
  engine, emits the result, and flags the month finished. Transactions arriving after month-end go to
  the `LATE_TRANSACTION_TAG` side output.
- Requires `card_limit` to have `REPLICA IDENTITY FULL` and the DB role to have `REPLICATION` — set
  up by `conf/postgres/init/01-ddl.sql`. Uses the `pgoutput` decoding plugin.

The Drools decision table lives at `app/flink-demo-2/src/main/resources/rules/CardLevelRules.xlsx`
and is loaded by `CardLevelRuleEngine` from the classpath.

## Common commands

Scripts live in `scripts/` and are **bash scripts that drive Podman** (not Docker). They assume the
containers described above. On Windows they are run via Git Bash / the Bash tool.

### Infrastructure lifecycle
```bash
scripts/start-all.sh      # start hdfs, flink, kafka, postgres (in that order)
scripts/stop-all.sh       # stop all
scripts/restart-all.sh    # restart all stacks
# individual: start-hdfs.sh / start-flink.sh / start-kafka.sh / start-postgresql.sh (+ stop-/restart-)
```
Web UIs: Flink `http://localhost:8081`, Kafka UI `http://localhost:9080`.
TaskManager remote-debug (JDWP) ports: `5005` (TM1), `5006` (TM2).

### Build + submit a job
`job-submit.sh` does the full loop: `mvn clean package` the module → copy the jar into `app/jars/`
(mounted to `/opt/flink/usrlib` in the client container) → `flink run` it via
`podman exec flink-client`.
```bash
scripts/job-submit.sh flink-demo-1            # build + submit, parallelism 1
scripts/job-submit.sh flink-demo-2 3          # parallelism 3
scripts/job-submit.sh flink-demo-1 1 --extra-flink-args
scripts/job-submit-all.sh                     # build + submit every app module (skips *common*)
```

### Manage running jobs
```bash
scripts/job-list.sh                # flink list -a
scripts/job-cancel.sh <job_id>     # cancel one job
scripts/job-cancel-all.sh          # cancel all RUNNING jobs
```

### Maven directly (without deploying)
```bash
mvn clean package                                    # build all modules (shaded jars)
mvn clean package -am -pl app/flink-demo-2 -DskipTests  # one module + its deps (what job-submit does)
mvn test                                             # run all tests
mvn -pl app/flink-demo-2 test                        # tests for one module
mvn -pl app/flink-demo-2 test -Dtest=CardLevelRuleEngineTest             # single test class
mvn -pl app/flink-demo-2 test -Dtest=CardLevelRuleEngineTest#methodName  # single test method
```
Tests use JUnit 5 and Flink's test harness (`flink-test-utils`, `flink-streaming-java:tests`) for
`KeyedProcessFunction`/timer testing.

## Adding a new app module

1. Create `app/<name>/` with a `pom.xml` whose parent is `flink-demo-parent` and that sets the
   `maven-shade-plugin.mainClass` property to your job's main class.
2. Register `<module>app/<name></module>` in the root `pom.xml`.
3. Depend on `flink-demo-common` for the shared transaction model.
4. `scripts/job-submit-all.sh` will pick it up automatically (it iterates `app/*` and skips
   `*common*` and `jars`).