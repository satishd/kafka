# Segment-Direct Lake Ingestion — Implementation Plan

**Author**: satishd@uber.com  **Status:** Draft · **Date:** 2026-07-02 · **Companion to:** [`segment-direct-lake-ingestion.md`](./segment-direct-lake-ingestion.md)

This is the task-level build plan: module wiring, a **sequence of commits on a local feature
branch**, the concrete existing APIs to call, class skeletons, tests, and acceptance criteria.
Read the design doc first for rationale and the two resolved decisions (OCS=OCI;
Heatpipe+Schema Service).

---

## 0. Correction vs. the design doc — no broker classloader isolation needed

The design doc said "classloader isolation mirroring the HDFS RSM." That trick exists because
the RSM runs *inside the broker JVM* with conflicting Hadoop/OCI deps. **This module runs as a
separate process**, so it does **not** need the broker child-first classloader.

The real requirements are simpler:
- It is a **leaf Gradle module** — nothing in `core`/`clients`/`storage` depends on it, so its
  heavy deps (Hudi/Avro/Parquet/Hadoop/heatpipe4j) never leak into the broker build.
- It ships a **standalone runnable artifact** via a **shadow/uber jar**.
- It talks to the RSM through the `RemoteStorageManager` **interface**, instantiating the
  concrete impl by config-driven class name + classpath — the same pattern the broker's
  `RemoteLogManager` uses — so we neither fork nor hard-depend on the HDFS RSM internals.

---

## 1. Prerequisites (before the first commit)

- [ ] Confirm module name: `segment-lake-ingestion` (used throughout below).
- [ ] File a tracking Jira; use its key as the commit-message prefix (repo convention, e.g.
      `KAFKA-XXXXX:` / `DKAFC-XXXX:`). Placeholder below: `<TICKET>`.
- [ ] Resolve internal artifact coordinates + versions from Artifactory:
      `org.apache.hudi:hudi-java-client`, `oci-hdfs-full` (already used by the RSM — copy its
      coordinates from `build.gradle`), CloudLake CFS jars (`hadoop-cfs-shaded`), `heatpipe4j`.
- [ ] Get a **dev** setup: a dev Kafka cluster with tiered storage on OCS, 1 append-only
      Heatpipe topic, an OCS/`cfs://` bucket writable for the Hudi table, and Schema Service
      reachability (Muttley sidecar `localhost:5436`).
- [ ] Decide Hudi output scheme with the CloudLake team: `cfs://` (preferred) vs `oci://`.

---

## 2. Branch setup

Work happens on a single local feature branch cut from the current dev branch, with one
self-contained commit per step below. Each commit must build and pass its own tests so the
history stays bisectable and any commit can later be turned into a review unit if needed.

```bash
git checkout 3.9.x-dev
git pull --ff-only
git checkout -b feature/segment-lake-ingestion
```

Commit conventions for this branch:
- One logical step per commit; **every commit compiles** (`./gradlew :segment-lake-ingestion:jar`)
  and its tests pass.
- Message format: `<TICKET>: <imperative summary>` (mirrors recent history on `3.9.x-dev`).
- Keep each commit reviewable (~200–400 changed lines where practical).
- Do **not** push or open a PR until asked; this plan produces local commits only.

---

## 3. Commit sequence

Each commit maps to a design-doc milestone (M0–M5). Suggested commit messages are given per
step.

### Commit 1 — Module scaffold + build wiring + metadata discovery (M0)

> `<TICKET>: add segment-lake-ingestion module and remote metadata discovery`

**Build wiring**

`settings.gradle` — add alongside the other `include` entries:
```gradle
include ':segment-lake-ingestion'
```

`build.gradle` — new `project(':segment-lake-ingestion')` block:
```gradle
project(':segment-lake-ingestion') {
  archivesBaseName = "kafka-segment-lake-ingestion"
  apply plugin: 'com.github.johnrengelman.shadow'   // for the standalone worker jar

  dependencies {
    implementation project(':clients')          // records, KafkaConsumer, Uuid, TopicIdPartition
    implementation project(':storage:api')      // RemoteStorageManager, RemoteLogSegmentMetadata
    implementation project(':storage')          // RemoteLogMetadataSerde, RemoteLogManagerConfig
    implementation project(':server-common')

    implementation libs.slf4jApi
    implementation libs.argparse4j             // CLI, as other tools use

    implementation libs.hudiJavaClient         // NEW — pin via gradle/dependencies.gradle
    implementation libs.avro
    implementation libs.hadoopClient
    implementation libs.ociHdfsFull            // reuse RSM coordinates
    implementation libs.heatpipe4j             // NEW

    testImplementation libs.junitJupiter
    testImplementation libs.mockitoCore
    testImplementation project(':clients').sourceSets.test.output
  }

  shadowJar {
    archiveClassifier = 'all'
    mergeServiceFiles()
  }
}
```
- Add the new version pins to `gradle/dependencies.gradle` (the `libs` catalog).
- Add `checkstyle/import-control-segment-lake-ingestion.xml` (copy an existing module's file and
  adjust the package root to `org.apache.kafka.lake`).

**New files**
- `.../lake/config/ConverterConfig.java` — typed config (see §5).
- `.../lake/discovery/MetadataSource.java` — interface: `Iterator<RemoteLogSegmentMetadata> poll()`.
- `.../lake/discovery/TopicMetadataSource.java` — `KafkaConsumer<byte[],byte[]>` on
  `__remote_log_metadata`; deserialize each value with `RemoteLogMetadataSerde`; keep only
  `RemoteLogSegmentMetadata` whose `state() == RemoteLogSegmentState.COPY_SEGMENT_FINISHED`.
- `.../lake/ConverterWorker.java` — `main()` + argparse4j; wires config → source; logs
  discovered segments.

**Key API usage**
```java
// TopicMetadataSource
RemoteLogMetadataSerde serde = new RemoteLogMetadataSerde();
for (ConsumerRecord<byte[],byte[]> r : consumer.poll(timeout)) {
    RemoteLogMetadata md = serde.deserialize(r.value());
    if (md instanceof RemoteLogSegmentMetadata) {
        RemoteLogSegmentMetadata seg = (RemoteLogSegmentMetadata) md;
        if (seg.state() == RemoteLogSegmentState.COPY_SEGMENT_FINISHED) emit(seg);
    }
    // NOTE: also observe RemoteLogSegmentMetadataUpdate records — a COPY_SEGMENT_STARTED
    // segment is finalized via an *update* record carrying the terminal state + customMetadata.
    // Track by RemoteLogSegmentId and only emit once the FINISHED state + customMetadata are known.
}
```
> Verify the exact `RemoteLogMetadataSerde` package (`org.apache.kafka.server.log.remote.metadata.storage.serialization`) and that update records must be merged onto the started record to get `customMetadata`. This merge is the one subtle bit of discovery.

**Tests**
- Feed synthetic serialized metadata (started + finished-update) → assert only fully-finalized
  segments with `customMetadata` are emitted.

**Acceptance / commit gate:** `./gradlew :segment-lake-ingestion:jar :segment-lake-ingestion:shadowJar`
builds clean; unit tests pass; running the worker against dev prints discovered
`COPY_SEGMENT_FINISHED` segments with offsets + bucket.

---

### Commit 2 — Segment locate + fetch + parse (M1, highest risk)

> `<TICKET>: fetch and parse remote segments via RemoteStorageManager`

**New files**
- `.../lake/locate/RsmProvider.java` — instantiates `RemoteStorageManager` from config
  (`className` + `classpath`) and calls `configure(configMap)`. Mirror the broker's approach in
  `RemoteLogManager` (config-driven class name + classpath loader). Keep a single configured
  instance.
- `.../lake/read/SegmentReader.java` — `Iterable<Record> read(RemoteLogSegmentMetadata)`.

**Key API usage**
```java
// SegmentReader
try (InputStream in = rsm.fetchLogSegment(metadata, 0)) {
    ByteBuffer buf = ByteBuffer.wrap(in.readAllBytes());     // segment .log bytes
    MemoryRecords records = MemoryRecords.readableRecords(buf);
    for (Record record : records.records()) {                // records() decompresses batches
        // record.value() -> ByteBuffer (Heatpipe-wrapped Avro); record.offset(); record.timestamp()
    }
}
```
- Location is resolved *inside* `fetchLogSegment` from `metadata.customMetadata()` — the worker
  does **not** build paths. `RemoteStorageManager.fetchLogSegment(metadata, startPosition)` is
  in `storage/api/.../RemoteStorageManager.java`.
- Working reference for the full setup: `remote-storage-managers/hdfs/src/test/.../tools/DumpHDFSRemoteLogSegment.java`.

**Tests**
- Integration (tagged, dev-only): fetch one real segment; assert record count ≈ `endOffset -
  startOffset + 1` (account for control/txn batches) and that `record.offset()` spans
  `[startOffset, endOffset]`.

**Acceptance / commit gate:** given one metadata record, the worker fetches and counts records
end-to-end; module still builds and unit tests pass.

---

### Commit 3 — Heatpipe/Avro decode + dead-letter (M2)

> `<TICKET>: decode Heatpipe Avro record values via Schema Service`

**New files**
- `.../lake/decode/HeatpipeHeader.java` — parse the 8-byte V2 / 4-byte V1 header (magic check,
  `schemaVersion` uint16 BE, `metaVersion`, `headerLen`).
- `.../lake/decode/RecordDecoder.java` — interface `Optional<GenericRecord> decode(String topic, ByteBuffer value)`.
- `.../lake/decode/HeatpipeAvroDecoder.java` — use `heatpipe4j` to fetch the schema for
  `(topic, schemaVersion)` from Schema Service, Avro-decode the body, unwrap the `msg` field
  (`["null", inner]` union) → inner `GenericRecord`.
- `.../lake/decode/DeadLetterSink.java` — write undecodable `(topic, offset, rawBytes, reason)`
  to an object-store path (or a Kafka topic).

**Header parse (well-grounded, use as fallback / validation even if heatpipe4j does it):**
```
b0,b1 = value[0],value[1]
if (b0,b1)==(0x30,0x46):  version=1; schemaVersion=BE16(value[2],value[3]); headerLen=4
else if value[0..3]==0x79,0x32,0xD4,0x6C:
       version=2; metaVersion=value[5]; schemaVersion=BE16(value[6],value[7]); headerLen=8
else:  dead-letter (ErrInvalidHeaderVersion)
avroPayload = value[headerLen:]
```
- Prefer letting `heatpipe4j` own header parse + schema fetch + `msg` unwrap; the above is for
  tests and for the manual path if the library API differs from expectations.
- Failure modes → dead-letter (not fatal): bad magic, schema fetch failure, null `msg`.

**Tests**
- Header parse unit tests: V1, V2, bad magic, truncated.
- `msg` union unwrap (null vs record).
- Dead-letter invoked on each failure mode; good records still flow.

**Acceptance / commit gate:** a fetched segment decodes to inner `GenericRecord`s; corrupt
payloads land in the dead-letter sink without stopping the batch; unit tests pass.

---

### Commit 4 — Hudi write to OCS (M3)

> `<TICKET>: write decoded segment records as a Hudi commit on OCS`

**New files**
- `.../lake/write/HudiSegmentWriter.java` — one segment → one Hudi commit.

**Key API usage (`hudi-java-client`, no Spark):**
```java
HoodieWriteConfig cfg = HoodieWriteConfig.newBuilder()
    .withPath(tableBasePath)                    // cfs://ns-cloudlake/... or oci://bucket@ns/...
    .withSchema(innerAvroSchema.toString())
    .withEngineType(EngineType.JAVA)
    // bulk_insert, target file size, etc.
    .build();

try (HoodieJavaWriteClient<HoodieAvroPayload> client =
         new HoodieJavaWriteClient<>(new HoodieJavaEngineContext(hadoopConf), cfg)) {
    String instant = client.startCommit();
    List<HoodieRecord<HoodieAvroPayload>> recs = rows.stream()
        .map(g -> new HoodieAvroRecord<>(
             new HoodieKey(recordKey(g), partitionPath(g)),
             new HoodieAvroPayload(Option.of(g))))
        .collect(toList());
    List<WriteStatus> ws = client.bulkInsert(recs, instant);
    client.commit(instant, ws, Option.of(commitExtraMetadata));   // extra-metadata = Commit 5
}
```
- Table base path is the "new OCS object" root; each commit writes new Parquet objects +
  `.hoodie` files. Register the FS scheme (`fs.oci.impl=com.oracle.bmc.hdfs.BmcFileSystem` or
  CFS config) in the `hadoopConf` — reuse the RSM's OCI config as reference.
- Append-only → `bulkInsert`/`insert`, no precombine/dedup. `recordKey` can be
  `topic-partition-offset`; `partitionPath` per topic convention (e.g. date).

**Tests**
- Integration (dev): write one segment; read the table back (Hudi Java read or a Spark/Presto
  spot check) and assert row count matches decoded count.

**Acceptance / commit gate:** one segment becomes one queryable Hudi commit on OCS; module
builds and tests pass.

---

### Commit 5 — Exactly-once: offsets in `.hoodie` + skip logic (M4)

> `<TICKET>: make ingestion idempotent via offsets in the Hudi timeline`

**New files**
- `.../lake/offset/OffsetTracker.java` — read/write the ingestion state in commit extra-metadata.

**Write side** — build `commitExtraMetadata` per commit from the segment:
```java
// per partition: leaderEpoch -> endOffset, plus the processed segment id
Map<String,String> extra = new HashMap<>();
extra.put("kafka.topicIdPartition", seg.remoteLogSegmentId().topicIdPartition().toString());
extra.put("kafka.segmentId", seg.remoteLogSegmentId().id().toString());
extra.put("kafka.startOffset", Long.toString(seg.startOffset()));
extra.put("kafka.endOffset",   Long.toString(seg.endOffset()));
extra.put("kafka.leaderEpochs", encode(seg.segmentLeaderEpochs())); // NavigableMap<Integer,Long>
```

**Read side (on startup)** — reconstruct processed set + high-water offsets:
```java
HoodieTableMetaClient mc = HoodieTableMetaClient.builder()
    .setConf(hadoopConf).setBasePath(tableBasePath).build();
mc.getActiveTimeline().getCommitsTimeline().filterCompletedInstants()
  .getInstantsAsStream().forEach(i -> {
     HoodieCommitMetadata cm = HoodieCommitMetadata.fromBytes(
         mc.getActiveTimeline().getInstantDetails(i).get(), HoodieCommitMetadata.class);
     Map<String,String> e = cm.getExtraMetadata();
     processedSegmentIds.add(e.get("kafka.segmentId"));
     highWater.merge(e.get("kafka.topicIdPartition"), Long.parseLong(e.get("kafka.endOffset")), Math::max);
  });
```
- **Skip rule:** drop any discovered segment whose `segmentId` is already in `processedSegmentIds`,
  or whose `[startOffset,endOffset]` is fully ≤ the partition high-water — idempotent replay.
- Idempotency mechanism = the processed-segment set (preferred over deterministic instant time,
  which Hudi ties to timeline ordering).

**Tests**
- Fake timeline with prior commits → assert already-ingested segments are skipped.
- Restart simulation: process N segments, restart, reprocess the same discovery batch → no
  duplicate commits.

**Acceptance / commit gate:** killing and restarting the worker mid-run produces no duplicate
rows; unit tests pass.

---

### Commit 6 — Worker loop, packaging, pilot harness (M5)

> `<TICKET>: continuous converter loop, metrics, and pilot harness`

**New files / changes**
- Flesh out `ConverterWorker` into a continuous loop: `poll → for each segment { skip? → fetch
  → decode → write+commit }` with bounded concurrency, backoff, and metrics.
- `.../lake/metrics/*` — counters: segments processed/skipped/dead-lettered, records written,
  commit latency, lag (metadata offset vs processed).
- Ops: `README.md` (run command, config), a sample config file, a `--backfill` mode
  (`listRemoteLogSegments(tp)` instead of tailing).
- Pilot A/B harness comparing this table vs the incumbent pipeline table (deck slide 10:
  time-to-queryable p50/p99, TCO, broker impact, correctness).

**Acceptance / commit gate:** continuous ingestion of 1–2 append-only topics on dev for a
sustained run; A/B metrics collected; full module test suite passes.

---

## 4. Sequencing & dependencies

```
Commit 1 (scaffold+discover) → Commit 2 (fetch+parse) → Commit 3 (decode)
   → Commit 4 (hudi write) → Commit 5 (exactly-once) → Commit 6 (loop+pilot)
```
Commit 2 carries the **highest integration risk** (RSM instantiation + packed single-object
read); prioritize proving it. Commit 3 and Commit 4 are independent enough that they can be
developed in parallel once Commit 2's `Iterable<Record>` contract is stable — but keep them as
separate, ordered commits on the branch.

If a later commit forces a change to an earlier one, prefer `git rebase -i
feature/segment-lake-ingestion` to fold the fix into the right commit and keep every commit
green, rather than appending a "fixup" commit.

---

## 5. Config reference (`ConverterConfig`)

| Key | Purpose |
|-----|---------|
| `bootstrap.servers` | Kafka cluster for `__remote_log_metadata`. |
| `remote.log.metadata.topic.name` | default `__remote_log_metadata`. |
| `consumer.group.id`, `poll.timeout.ms`, `max.poll.records` | discovery consumer. |
| `remote.storage.manager.class.name` / `.class.path` | RSM impl to load (reuse broker values). |
| `rsm.config.*` | pass-through to `RemoteStorageManager.configure(...)` (buckets, OCI creds). |
| `schema.service.url` | Heatpipe Schema Service (Muttley sidecar). |
| `hudi.table.base.path` | `cfs://…` or `oci://bucket@namespace/prefix`. |
| `hudi.table.name`, `hudi.record.key.fields`, `hudi.partition.path.field`, `hudi.write.operation` | Hudi table. |
| `deadletter.path` (or `.topic`) | undecodable records sink. |
| `topics.allowlist` | pilot scoping (1–2 append-only topics). |
| `max.concurrent.segments` | worker parallelism. |

---

## 6. Risks / verify-before-coding

- **Metadata finalization:** `customMetadata` (the bucket) arrives on the
  `RemoteLogSegmentMetadataUpdate` (FINISHED), not the initial STARTED record — discovery must
  merge them by `RemoteLogSegmentId`. (Confirm in Commit 1.)
- **RSM instantiation from a non-broker process:** confirm `configure(...)` needs only plain
  config (no broker internals). Fallback: depend directly on `remote-storage-managers/hdfs`.
  (Confirm in Commit 2.)
- **Compression/control batches:** iterate `MemoryRecords.records()` (decompresses); filter
  control batches; expect record count ≤ `endOffset-startOffset+1`.
- **heatpipe4j API shape:** confirm the Java client method names for schema fetch + decode; the
  manual header parse in Commit 3 is the documented fallback.
- **Hudi on OCI FS semantics:** confirm rename/list behavior is acceptable (prefer `cfs://`).
- **Schema evolution:** inner Avro schema changes across a topic's `schemaVersion`s; reconcile
  the Hudi table schema at commit time (union/latest-wins) — design in Commit 4, harden in
  Commit 6.
