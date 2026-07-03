# segment-lake-ingestion — modularity / best-practices / efficiency review

Review of the `segment-lake-ingestion` module as of branch `segment-writer-exp`. Findings are
grouped by theme and ordered by value; the phased plan at the end sequences them into
independently-committable changes. Severity: **P0** = correctness bug, **P1** = reliability gap,
**P2** = quality/efficiency.

---

## A. Correctness & concurrency (do first)

### A1 (P0) — `OffsetTracker` is used concurrently but is not thread-safe
`ConverterWorker.runLoop` submits `pipeline.process(segment)` to a fixed thread pool of
`max.concurrent.segments` (default **4**). `process()` reads `offsetTracker.isProcessed()` and
`decodeAndWrite()` calls `offsetTracker.markProcessed()` — both on executor threads. `OffsetTracker`
backs its state with plain `HashSet`/`HashMap`. Concurrent `HashMap` read+write can corrupt the map
(lost updates; in the worst case a spinning resize). The `ConverterWorker` javadoc claiming mutation
is "confined to the single-writer discovery loop" is factually wrong — `markProcessed` runs on the
worker threads.
**Fix:** `ConcurrentHashMap` + `ConcurrentHashMap.newKeySet()`; `merge(..., Math::max)` is already
atomic there. Correct the javadoc.

### A2 (P1→P0 at concurrency>1) — concurrent Hudi writes/table-init to one table
`HoodieJavaWriteClient` is a single-writer engine. With parallelism > 1, multiple threads call
`HudiSegmentWriter.write()` on the **same table**: (a) `ensureTableInitialized` can race (two threads
both see `TableNotFound` and both `initTable`), and (b) concurrent `startCommit`/`commit` from
independent clients conflict on the timeline. The class comment even says it is "not safe for
concurrent writers against the same base path" — which the worker violates.
**Fix (recommended):** split the pipeline into a concurrent fetch+decode stage and a **single-writer
commit stage** (producer/consumer queue), so exactly one thread ever commits. Alternative: enable
Hudi OCC with a lock provider — heavier and still slower than serialising the cheap commit step.

### A3 (P0) — null record values NPE instead of dead-lettering
`decodeInto` passes `record.value()` straight to the decoder; a null-value record (tombstone /
null payload) makes `HeatpipeHeader.parse(null)` throw `NullPointerException`, which is **not** an
`InvalidHeatpipeHeaderException`, so it escapes the decoder's dead-letter path and fails the whole
segment. `Record.hasValue()` exists for exactly this.
**Fix:** guard in `HeatpipeAvroDecoder.decode` (null value → dead-letter) and/or skip
`!record.hasValue()` in the worker.

---

## B. Reliability

### B1 (P1) — failed segments are silently dropped
`process()` catches `RuntimeException`/`IOException`, logs, increments `segmentFailed`, and moves on.
Meanwhile the discovery consumer runs with `enable.auto.commit=true` and polls on a different thread,
so the metadata record's offset is committed regardless of whether its segment was written. A write
failure therefore means the segment is **never retried** (won't be re-polled) and **never landed** —
silent data loss until a full re-run with a new `group.id`. `HudiWriteException`'s own javadoc says it
"is fatal… the caller should retry", but nothing retries.
**Fix:** manual offset commit only after a segment is durably processed (or a bounded in-process
retry + dead-letter-the-segment on exhaustion). Couples naturally with the single-writer stage (A2).

### B2 (P1) — multi-schema segment breaks exactly-once
`writeAll` writes one Hudi commit **per Avro schema** in the segment, each tagged with the *same*
segment offset range/id. If the worker crashes after commit #1 and before commit #2, on restart
`OffsetTracker.isProcessed` sees the segment id (from commit #1) and **skips the whole segment**,
permanently losing the other schema groups.
**Fix:** make a segment atomic — one commit per segment (write all schema groups under a single
`startCommit`/`commit`), or only record the id once every group has committed.

### B3 (P2) — `SegmentAssembler.pending` grows unbounded
`COPY_SEGMENT_STARTED` entries that never receive a `FINISHED` (aborted copies, deleted segments)
stay in the `pending` map forever — a slow leak in a long-running tail.
**Fix:** evict on terminal states (delete/abort) and/or bound by size or age with a warning.

---

## C. Modularity

### C1 (P2, highest structural value) — `ConverterWorker` does too much
The 328-line `ConverterWorker` mixes CLI parsing, property loading, the threaded run loop, **and**
the entire `Pipeline` inner class, which itself does both dependency wiring (a factory concern) and
per-segment orchestration (skip→fetch→decode→write). Because `Pipeline` is a private inner class,
the core flow has **zero unit tests**.
**Fix:** extract `SegmentProcessor` (the `process`/`decodeAndWrite` logic) and a `PipelineFactory`
(the `build`/wiring) as top-level classes in a `pipeline` package; leave `ConverterWorker` as a thin
`main` (parse args, build, run loop). Unlocks tests for the orchestration and the fallback modes.

### C2 (P2) — the `locate` package is misleading
`locate` holds only `RsmProvider`, and its own javadoc says location is *not* computed here (the RSM
resolves it from `customMetadata`). The name no longer describes the contents.
**Fix:** move `RsmProvider` into `read` (the read/storage layer) or a `storage` package and delete
`locate`.

### C3 (P2) — Hudi key derivation leaks into `ConverterWorker`
`stringField` and the record-key / partition-path lambdas are a Hudi-write concern living in the
worker.
**Fix:** move to the `write` package as a small `RecordKeyExtractor` strategy built from
`HudiWriterConfig`, decoupling the worker from key derivation.

### C4 (P2) — duplicated reflective instantiation
`RsmProvider.create` and `SchemaClientProvider.create` both implement "load class by name → optional
child-first loader → `configure`".
**Fix:** a shared `Plugins.newInstance(className, classPath, type, configs)` helper.

---

## D. Best practices

- **D1 (P2)** — reuse `org.apache.kafka.common.utils.Utils.toArray(ByteBuffer)` in
  `HeatpipeAvroDecoder` and `FileDeadLetterSink` instead of hand-rolled buffer→array copies.
- **D2 (P2)** — the exception hierarchy is inconsistent (`InvalidHeatpipeHeaderException` and
  `HudiWriteException` are unchecked, `SchemaFetchException` is checked, no common base). Introduce a
  common unchecked base (`SegmentLakeException`, mirroring `KafkaException`) and reconsider making
  `SchemaFetchException` unchecked.
- **D3 (P2)** — `ConverterMetrics` is log-only `AtomicLong`s; `commitLatencyMsTotal` as a bare
  cumulative sum isn't actionable. Move to `org.apache.kafka.common.metrics.Metrics` (JMX-exposable,
  ecosystem-consistent) and record count + max (or a histogram) for latency.
- **D4 (P2)** — add tests for the currently-untested `SegmentProcessor`/`PipelineFactory` (after C1)
  and `TopicMetadataSource` (poll → assemble → allowlist filtering, using a `MockConsumer`).

---

## E. Efficiency

- **E1 (P2)** — `HudiSegmentWriter.write` constructs a **new** `HoodieJavaWriteClient` on every call
  (per schema group, per segment). Client construction reads the timeline and initialises engine
  state — expensive per small segment. Reuse a client on the single-writer commit thread (A2),
  resetting per-commit state rather than reallocating. Weigh against the current "no state leak"
  rationale.
- **E2 (P2)** — `decodeAndWrite` accumulates **all** decoded `GenericRecord`s of a segment in
  `decodedBySchema` before writing, so peak heap ≈ decoded-segment-size × concurrency — which partly
  negates the streaming-read work already done. Stream records into the write client in bounded
  chunks (multiple `insert()` calls within a single `startCommit`/`commit`, preserving B2 atomicity).

---

## Phased plan (each phase independently committable, with tests + module gates)

Gate every phase on:
`./gradlew :segment-lake-ingestion:test --offline -x checkstyle* -x spotbugs*` then
`:checkstyleMain :checkstyleTest :spotbugsMain` on-network.

**Phase 1 — correctness (small, high value):** A1 (thread-safe `OffsetTracker` + fix javadoc),
A3 (null-value dead-letter), D1 (reuse `Utils.toArray`). Add `OffsetTracker` concurrency test and a
null-value decode test.

**Phase 2 — reliability / write path:** A2 + B1 + E1 together — introduce a single-writer commit
stage fed by a bounded queue, manual offset commit after durable processing, bounded retry then
segment-level dead-letter, and a reused write client on that thread. B2 (one commit per segment)
and E2 (chunked decode→single commit) fold in here since they all touch the write path.

**Phase 3 — modularity + tests:** C1 (extract `SegmentProcessor` + `PipelineFactory`), C2 (drop
`locate`), C3 (`RecordKeyExtractor`), C4 (`Plugins` helper), then D4 (orchestration +
`TopicMetadataSource` tests now that they're testable). B3 (`SegmentAssembler` eviction) fits here.

**Phase 4 — polish:** D2 (exception base), D3 (Kafka `Metrics` + latency count/max).

Suggested order: **Phase 1 → 2 → 3 → 4.** Phase 1 is safe to land immediately; Phase 2 is the
largest and should be reviewed on its own; Phase 3 is mechanical refactoring backed by new tests;
Phase 4 is cosmetic/observability.
