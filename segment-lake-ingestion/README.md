# segment-lake-ingestion

Converts finished tiered Kafka segments into Hudi tables on OCS, without going through Spark or
the incumbent pipeline. See `docs/design/segment-direct-lake-ingestion-implementation.md` at the
repo root for the full design and commit-by-commit plan this module was built against.

## Pipeline

For each partition, `ConverterWorker` tails `__remote_log_metadata` (via a plain `KafkaConsumer`,
`auto.offset.reset=earliest`) and, for every segment that reaches `COPY_SEGMENT_FINISHED`:

1. **Skip** — if `OffsetTracker` (reconstructed from the target Hudi table's own commit timeline)
   shows the segment, or its offset range, is already committed.
2. **Fetch** — read the segment through the configured `RemoteStorageManager`.
3. **Decode** — parse the Heatpipe header and Avro-decode each record via `HeatpipeAvroDecoder`,
   unwrapping the `msg` union to the inner record. Records that fail any step are appended to the
   dead-letter file instead of stopping the segment.
4. **Write** — group decoded records by their (possibly per-record) Avro schema and write each
   group as one Hudi commit via `HudiSegmentWriter`, tagged with the segment's offset range so a
   restart can resume without re-ingesting.

Up to `max.concurrent.segments` segments are processed concurrently.

### Fallback modes

The full decode-and-write pipeline only turns on once every setting `ConverterConfig.decodeAndWriteEnabled()`
checks is present. Otherwise the worker degrades gracefully, matching earlier commits' behavior:

- No `remote.storage.manager.class.name` → **discovery only** (logs each finished segment).
- RSM set, but no schema client / Hudi table configured → **fetch only** (also logs a data-record
  count per segment, to validate the read path).

### Backfill

There is no separate `--backfill` mode. A fresh consumer group (`group.id`) against
`__remote_log_metadata` with `auto.offset.reset=earliest` already replays the entire compacted
topic from the beginning, which is equivalent to backfilling from `RemoteLogMetadataManager` for
every partition currently tiered — without a second discovery code path to keep in sync with the
main loop. To re-run against an already-ingested table, use a new `group.id`; `OffsetTracker`
still skips segments the target table has already committed.

## Running

```bash
./gradlew :segment-lake-ingestion:shadowJar
java -cp segment-lake-ingestion/build/libs/segment-lake-ingestion-*-all.jar \
    org.apache.kafka.lake.ConverterWorker --config config/segment-lake-converter.properties
```

See `config/segment-lake-converter.properties` for a commented sample covering every stage, and
the design doc's config reference table for the full list of keys.

## Metrics

`ConverterMetrics` counts segments processed/skipped/failed, records written/dead-lettered, and
cumulative commit latency, logged every 60s and once more at shutdown. No external metrics backend
(JMX/M3) is wired up yet — that depends on how this worker is deployed and is left to the pilot.

## Out of scope for this module

- **Pilot A/B harness** comparing this table against the incumbent pipeline's (time-to-queryable
  p50/p99, TCO, broker impact, correctness) is not local code: it needs a running pilot against
  real production tables and dashboards. Track it separately once the pilot topics are chosen.
- **Schema evolution reconciliation** across a topic's `schemaVersion`s at the Hudi table level
  (union/latest-wins) is noted as a risk in the design doc but not implemented; today, a schema
  change on a topic simply produces a new commit with a different Avro schema, relying on Hudi's
  own schema-on-read behavior rather than an explicit reconciliation step.
