# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This is Uber's internal fork of Apache Kafka (version `3.9.x`), a distributed event streaming platform. It is a multi-module Gradle project combining Java and Scala (2.13 default). Current version: `3.9.16-uber-snapshot`.

## Build Commands

```bash
# Build all jars
./gradlew jar

# Clean build
./gradlew clean

# Rebuild auto-generated RPC message classes (needed when switching branches or modifying message JSON schemas)
./gradlew processMessages processTestMessages

# Build release tarball (output in core/build/distributions/)
./gradlew clean releaseTarGz
```

## Testing

```bash
# Run all unit + integration tests
./gradlew test

# Run only unit tests or only integration tests
./gradlew unitTest
./gradlew integrationTest

# Run tests in a specific module
./gradlew clients:test
./gradlew core:test

# Run a specific test class
./gradlew clients:test --tests RequestResponseTest

# Run a specific test method
./gradlew core:test --tests kafka.api.ProducerFailureHandlingTest.testCannotSendToInternalTopic
./gradlew clients:test --tests org.apache.kafka.clients.MetadataTest.testTimeToNextUpdate

# Force re-run without code changes
./gradlew test --rerun

# Run Kafka Streams tests
./gradlew :streams:testAll

# Run with more parallelism control
./gradlew test -PmaxParallelForks=4

# System/integration tests (requires Docker)
./gradlew clean systemTestLibs
bash tests/docker/run_tests.sh
# Specific test file: TC_PATHS="tests/kafkatest/tests/streams/..." bash tests/docker/run_tests.sh
```

## Code Quality

```bash
# Checkstyle + import order check (use JDK 11 or 17, NOT JDK 21)
./gradlew checkstyleMain checkstyleTest spotlessCheck

# Fix import ordering before PRs (requires JDK 11+, not 21)
./gradlew spotlessApply

# Static analysis for bugs
./gradlew spotbugsMain spotbugsTest -x test
```

## Benchmarks

```bash
# Run all JMH benchmarks
./jmh-benchmarks/jmh.sh

# Run a specific benchmark
./jmh-benchmarks/jmh.sh LRUCacheBenchmark
```

## Running a Broker Locally (KRaft mode)

```bash
KAFKA_CLUSTER_ID="$(./bin/kafka-storage.sh random-uuid)"
./bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
./bin/kafka-server-start.sh config/kraft/server.properties
```

## Architecture

### Module Structure

| Module | Language | Purpose |
|--------|----------|---------|
| `clients` | Java | Producer, Consumer, Admin client APIs |
| `core` | Scala | Main Kafka broker logic |
| `connect/` | Java | Kafka Connect framework for external integrations |
| `streams` | Java | Kafka Streams DSL and Processor API |
| `raft` | Java | KRaft consensus protocol (replaces ZooKeeper) |
| `metadata` | Java | Cluster metadata management for KRaft mode |
| `group-coordinator` | Java | Consumer group coordination logic |
| `transaction-coordinator` | Java | Transactional producer management |
| `server` | Java/Scala | Broker server bootstrap and shared broker code |
| `server-common` | Java | Shared utilities across server-side modules |
| `storage` | Java | Log segment storage layer and tiered storage API |
| `tools` | Java | CLI tools (`kafka-topics.sh`, etc.) |
| `shell` | Scala | Interactive Kafka shell |
| `jmh-benchmarks` | Java | JMH microbenchmarks |
| `remote-storage-managers/hdfs` | Java | Uber-specific HDFS tiered storage backend |

### Key Architectural Concepts

**KRaft mode (primary):** ZooKeeper is deprecated. The controller is now embedded in Kafka itself using the KRaft (Kafka Raft) consensus protocol. `core/src/main/scala/kafka/server/KafkaRaftServer.scala` is the KRaft-mode entry point; `BrokerServer.scala` and `ControllerServer.scala` are the broker/controller roles.

**Protocol message code generation:** All Kafka wire protocol messages are defined as JSON schemas in `clients/src/main/resources/common/message/*.json`. These are compiled into Java `*Data` classes by the `generator` module via `./gradlew processMessages`. Do not edit generated files in `clients/build/`—edit the `.json` schema instead.

**Request handling pipeline (broker):** `KafkaRequestHandler` → `KafkaApis` → (ReplicaManager / GroupCoordinator / etc.). `KafkaApis.scala` is the central dispatch point for all client-facing API requests.

**Checkstyle import control:** Each module has its own import control file in `checkstyle/import-control-*.xml`. Cross-module imports are restricted to prevent dependency cycles.

### Uber-Specific Additions

- Internal Maven repos at `artifactory.uber.internal:4587` (configured in `build.gradle`)
- `remote-storage-managers/hdfs`: HDFS-backed tiered storage
- `udeploy/`: Deployment tooling
- Version format: `3.9.x-uber-snapshot` / `3.9.x-uber` for releases

## Common Build Options

Pass with `-P` flag, e.g. `./gradlew -PmaxParallelForks=1 test`:

- `maxParallelForks`: number of parallel test JVMs (defaults to available processors)
- `ignoreFailures`: continue despite test failures
- `showStandardStreams`: print test stdout/stderr to console
- `enableTestCoverage`: enable JaCoCo coverage (adds ~15-20% overhead)
- `scalaVersion`: use `2.12` or `2.13` (default 2.13)

## Version Bumping

When changing the version in `gradle.properties`, also update:
- `docs/js/templateData.js`
- `tests/kafkatest/__init__.py`
- `tests/kafkatest/version.py` (variable `DEV_VERSION`)
- `streams/quickstart/pom.xml` and related files

## Releasing a New Server Build

The examples below use `3.9.2-uber` as the release version. Adjust accordingly.

### 1. Prepare the dev branch

On `3.9.x-dev`, land a diff that:
- Adds release notes for the version being released to `RELEASE_NOTES.md` (e.g. for `3.9.2-uber`).
- Bumps `gradle.properties` to the **next** snapshot version (e.g. `3.9.2-uber-snapshot` → `3.9.3-uber-snapshot`).

> Skip the version bump step for hotfix/patch releases.

### 2. Cut the release branch and tag

```bash
git checkout 3.9.x-dev

# Create a branch named after the release version
git checkout -b 3.9.2-uber

# Set the exact release version (remove -snapshot suffix)
vi +26 gradle.properties   # e.g. 3.9.3-uber-snapshot -> 3.9.2-uber

git commit -am "Release Kafka 3.9.2-uber version"

# Tag the branch to make it immutable
git tag uber/3.9.2-uber

# Push branch and tag
git push -f origin 3.9.2-uber
git push -f origin tags/uber/3.9.2-uber
```

### 3. Trigger the Odin build

1. Go to **https://up.uberinternal.com/s/odin-kafka/e/production/deploy**
2. Click **New Build**.
3. Set **Branch/Sha/Tag** to `tags/uber/3.9.2-uber`.
