# Segment-Direct Lake Ingestion — Design Plan

**Author**: satishd@uber.com  **Status:** Draft · **Date:** 2026-07-02 · **Repo:** Uber Kafka fork (`3.9.x`)
**Source deck:** [Segment-Direct Lake Ingestion](https://docs.google.com/presentation/d/1t5RzbX0-Dm9M-8aiG5_9WbX3O_CrNGl9YSK9R_cw8gE)

---

## 1. Goal

Build a new, **stateless off-broker module** that turns Kafka's already-tiered log
segments into a Hudi table on object storage, **without** a Flink/Spark streaming sink.

Pipeline (per the deck): take remote-segment metadata → locate the physical segment →
fetch & parse it → decode the Avro record values → convert to Hudi → write to a new
object-storage (OCS/OCI) object, recording Kafka offsets in the Hudi commit timeline for
exactly-once resume.

### Why

For **append-only** streams (logs, clickstream, metrics, audit) the streaming sink is pure
overhead. The segments already exist in tiered storage; a decoupled converter reads them
with **zero broker CPU** and no always-on Flink/Spark cluster.

### Non-goals (v1)

- CDC / upsert / merge-on-read topics — keep those on Flink/Spark (deck slide 8).
- Async clustering / compaction (deck step 6) — later.
- The "shared segment" unified-storage variant (deck slide 11) — later.

---

## 2. Resolved key decisions (previously unknown)

Two unknowns were researched against Uber internal docs/code and are now settled:

### 2.1 "OCS" = OCI Object Storage — there is no `ocs://` scheme

- **OCS is Uber shorthand for Oracle OCI Object Storage** (Skystream program), same class
  as S3/GCS.
- Hadoop schemes available: **`oci://<bucket>@<namespace>/<prefix>`** (OCI HDFS connector
  `com.oracle.bmc.hdfs.BmcFileSystem`) or **`cfs://ns-cloudlake/…`** (CloudLake logical FS,
  preferred for lake tables — hides bucket layout/re-sharding). TerraBlob's S3 API is *not*
  recommended for stateful Hudi (rename/list semantics).
- Kafka tiered storage **already migrated HDFS → OCS** (JIRAs PLANPLAT-9685, PLANPLAT-10966).
  The repo's RSM already writes `oci://…` and stores the bucket URI in each segment's
  `CustomMetadata`.
- **Decision:** write the Hudi table to `cfs://` (preferred) or `oci://`, reusing the OCI
  connector already isolated in `remote-storage-managers/hdfs`. Both source segments and the
  lake live in OCI object storage (this is also the slide-11 shared-storage opportunity).

### 2.2 Avro = Heatpipe + Uber Schema Service (NOT Confluent Schema Registry)

- Kafka value = **8-byte Heatpipe header + Avro body**.
  - Header V2 (8 bytes): bytes 0–3 magic `79 32 D4 6C`, byte 4 reserved, byte 5 meta-version,
    **bytes 6–7 schema version (uint16 BE, per-topic)**.
  - Legacy Header V1 (4 bytes): magic `30 46` + 2-byte schema version.
- "Schema id" is a **per-topic schema version**, not a global registry id. Lookup key
  `(topic, schemaVersion)` against **Uber Schema Service** (`GET /schemas/{topic}/{version}/PRODUCER`,
  typically via Muttley sidecar `localhost:5436`).
- Avro body is a **`sortsol.metadata` wrapper**; the business payload is the **`msg`** field
  (a `["null", inner]` union in newer meta versions), alongside tracing/Hadoop/`prev_msg` fields.
- **Decision:** decode with **`heatpipe4j`** (parses header, fetches schema, composes
  wrapper+inner, decodes), then unwrap `msg`. The worker therefore needs network access to
  Schema Service in addition to the object store (still zero broker involvement).

---

## 3. Where it lives

New Gradle subproject registered in `settings.gradle`, built as an **isolated artifact with
its own classloader** — mirroring exactly how the HDFS RSM isolates the OCI connector
(`build.gradle` classloader wiring). This keeps heavy Hudi/Avro/Parquet/Hadoop/heatpipe4j
deps out of `core`/`clients`. It produces a **standalone worker binary**, not broker code.

```
segment-lake-ingestion/                      # proposed module name
  src/main/java/org/apache/kafka/lake/
    discovery/  MetadataSource       # tail __remote_log_metadata; filter COPY_SEGMENT_FINISHED
    locate/     SegmentLocator       # metadata -> physical object (delegate to RSM)
    read/       SegmentReader        # fetch + parse RecordBatch v2
    decode/     HeatpipeAvroDecoder  # 8-byte header + Schema Service -> GenericRecord (msg)
    write/      HudiSegmentWriter    # GenericRecord -> one Hudi commit on OCS
    offset/     OffsetTracker        # read/write offsets in the .hoodie timeline
    ConverterWorker.java             # orchestrates the 6 steps
```

---

## 4. Pipeline design & what to reuse

The `.log` bytes are **Kafka's binary record-batch format**, not Avro. "Avro" is the record
*value* payload inside each batch (Heatpipe-wrapped). So: Kafka batch → per-record value →
Heatpipe/Avro decode → `msg` → Hudi row.

| Step | Action | Reuse / New |
|------|--------|-------------|
| **1. DISCOVER** | Consume `__remote_log_metadata`, deserialize with `RemoteLogMetadataSerde`, keep `RemoteLogSegmentState.COPY_SEGMENT_FINISHED`. Backfill mode: `RemoteLogMetadataManager.listRemoteLogSegments(tp[, leaderEpoch])`. | Reuse serde/RLMM. A plain `KafkaConsumer` is cleaner for an off-broker worker than a full `TopicBasedRemoteLogMetadataManager`. |
| **2. LOCATE** | Resolve the physical object from `RemoteLogSegmentMetadata`. Bucket is in `CustomMetadata` (`RemoteLogSegmentMetadata.java` ~L389); path = `bucket + /<tp>-<topicId>/<segmentUuid>` (`RSMUtils.getSegmentRemoteDir`). | **Don't reimplement path logic** — instantiate the existing `RemoteStorageManager` and let it resolve location internally. |
| **3. FETCH + PARSE** | `rsm.fetchLogSegment(metadata)` → `InputStream` → `MemoryRecords`/`FileRecords` → iterate `RecordBatch` → `Record`. | Reuse `RemoteStorageManager.fetchLogSegment` (`RemoteStorageManager.java` ~L111). Working template: `DumpHDFSRemoteLogSegment.java` (test tool). Parse via `org.apache.kafka.common.record.*`. |
| **4. DECODE** | Per record value: parse Heatpipe header → `(topic, schemaVersion, metaVersion)` → decode Avro via `heatpipe4j` → unwrap `msg` union. Header-invalid / schema-fetch-fail / null-msg → dead-letter. | **New.** `heatpipe4j` + Schema Service client. |
| **5. WRITE** | Batch a segment's decoded rows into **one Hudi commit** (deck slide 12: offset range → one commit). Append-only → Hudi `bulk_insert`/insert, no dedup. Table base path = `cfs://…` or `oci://…`. | **New.** Use **Hudi Java client** (`hudi-java-client`) — no Spark (matches deck). Hudi is Avro-native, so feed the inner `GenericRecord` directly. |
| **6. COMMIT OFFSETS** | Store `{partition: {leaderEpoch: endOffset}}` in the Hudi commit's extra-metadata (`.hoodie` timeline). | **New.** From `metadata.segmentLeaderEpochs()` + `endOffset()`. |

### Key data source: `RemoteLogSegmentMetadata`

Fields consumed (storage/api `…/server/log/remote/storage/RemoteLogSegmentMetadata.java`):
`remoteLogSegmentId` (topicIdPartition + UUID), `startOffset`/`endOffset` (inclusive),
`segmentLeaderEpochs` (NavigableMap epoch→startOffset), `segmentSizeInBytes`, `state`,
`customMetadata` (bucket URI).

---

## 5. Exactly-once & fault tolerance (deck slide 9)

- **Unit of work = one segment = one Hudi commit.** Offsets/epochs come straight off
  `RemoteLogSegmentMetadata`.
- On startup, `OffsetTracker` reads the **latest `.hoodie` commit's extra-metadata** to
  reconstruct last-ingested `{leaderEpoch: endOffset}` per partition — the lake is the source
  of truth, no separate state store.
- Any segment whose `[startOffset, endOffset]` is already covered is **skipped** →
  idempotent replay.
- Make the Hudi instant time **deterministic** from segment UUID + offset range so a
  crash-retry of the same segment is a no-op, not a duplicate.

---

## 6. Dependencies & build isolation

Kept in the isolated module only: `org.apache.hudi:hudi-java-client`, `org.apache.avro:avro`,
Parquet, `hadoop-client`, the OCI HDFS connector (`oci-hdfs-full`) and/or CloudLake CFS jars,
and `heatpipe4j`. Hudi writes purely through Hadoop `FileSystem`, so OCS support = registering
the scheme (`fs.oci.impl` / CFS config) + a base path — analogous to today's OCI write path in
`HDFSRemoteStorageManager.copyLogSegmentData`.

---

## 7. Configuration (initial sketch)

- Metadata topic: bootstrap servers, `__remote_log_metadata` name, consumer group, poll size.
- RSM: reuse the broker's RSM class + config so `fetchLogSegment` resolves buckets identically.
- Schema Service: endpoint (Muttley sidecar), timeout, cache size.
- Hudi: table base path (`cfs://…`/`oci://…`), table name, record/partition key mapping,
  write operation (`bulk_insert`), target file size.
- Dead-letter: output path (object store) or topic.
- Topic allowlist (pilot: 1–2 append-only topics).

---

## 8. Testing strategy

- **Unit:** Heatpipe header parse (V1/V2, bad magic), `msg` union unwrap, offset/epoch →
  commit-metadata mapping, skip-logic against a fake `.hoodie` timeline.
- **Integration:** reuse `DumpHDFSRemoteLogSegment` style setup to fetch a real segment from a
  dev OCS bucket; assert record count and decoded fields.
- **End-to-end (pilot):** produce to a dev topic → let tiering copy → run converter → query
  the Hudi table; verify row counts and offset continuity across a forced restart.

---

## 9. Milestones

1. **M0 — Skeleton:** new isolated Gradle module + config plumbing. Consume
   `__remote_log_metadata` with `RemoteLogMetadataSerde`; print `COPY_SEGMENT_FINISHED`. *(DISCOVER)*
2. **M1 — Read path:** given one metadata record, fetch + parse the segment via the existing
   RSM, count records. *(LOCATE+FETCH+PARSE — highest-risk integration, do early.)*
3. **M2 — Decode:** `heatpipe4j` header parse + Schema Service fetch + `msg` unwrap; dead-letter path.
4. **M3 — Hudi write:** one segment → one Hudi commit of Parquet on OCS (`cfs://`/`oci://`).
5. **M4 — Exactly-once:** offsets in `.hoodie`; restart/skip; deterministic idempotent retries.
6. **M5 — Worker loop + pilot:** continuous DISCOVER→…→COMMIT for 1–2 append-only topics;
   A/B comparison harness (deck slide 10: time-to-queryable, TCO, broker impact, correctness).

---

## 10. Open decisions (non-blocking)

1. `cfs://` vs `oci://` for the Hudi output (CloudLake team's steer). Recommend `cfs://`.
2. Schema evolution: Hudi table schema must track the topic's evolving Heatpipe schema versions
   (reconcile at commit time).
3. Continuous long-running worker vs bounded backfill job (build reader/writer to serve both;
   default continuous for the pilot).
4. In-repo module vs standalone service repo (proposing in-repo isolated module → standalone binary).

---

## 11. Key risk

The Uber RSM packs **all indexes + the log into a single remote object** behind a 25-byte
`LogSegmentDataHeader` (`remote-storage-managers/hdfs/.../LogSegmentDataHeader.java`, version 0).
**Reuse `RemoteStorageManager.fetchLogSegment`** (which handles this) rather than parsing the
packed layout by hand. M1 exists to de-risk exactly this.
