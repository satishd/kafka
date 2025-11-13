# Release Kafka 3.9.11-uber build (11/13/2025)
- Streaming tool version: 2.0.1~14962.gbp6f3bf2
- Fix negative remote data pending upload metric.
- Added metrics to configure alert for remote LIST_OFFSETS requests and errors.
- Removed garbage characters when printing CustomMetadata in the RemoteLogSegmentMetadata.
- Added circuit-breaker to reduce the copy / delete segment calls from Kafka to OCI when there is degradation in the object storage.
- Provision to dynamically change the state of the circuit breaker for the copy / delete segment calls. 

# Release Kafka 3.9.10-uber build (10/22/2025)
- Enable parallel remote reads feature for remote storage with fix for heap-memory leak.
- Added feature flag to enable/disable parallel remote reads feature.

# Release Kafka 3.9.9-uber build (10/15/2025)
- Guardrails for prefetch directory
- Ensure no stale segments in prefetch directory

# Release Kafka 3.9.8-uber build (10/08/2025)
- Revert parallel remote reads feature for remote storage.

# Release Kafka 3.9.7-uber build (09/30/2025)
- Make OCI readAhead and prefetch configs as dynamic
- Don't mark the remote-read requests as failed when ReplicaNotAvailableException thrown
- Ensure update instructions are not used alone in Dockerfiles
- Use SafeInputStream to handle server-side backoff wait on remote-read errors

# Release Kafka 3.9.6-uber build (09/25/2025)
- Simplify finding the remote metadata partition script for given topics.
- Revert "DKAFC-5965: Enable privileged FetchSession for remote log metadata client."
- Fix local log size metrics for remote storage topics.
- Emit FetchLookback metrics when reading from local-log.
- Log bucket information while printing the remote log metadata.
- Prefetch - Download task optimization. (Beta)
- Implement server-side backoff retry when facing remote-read errors.

# Release Kafka 3.9.5-uber build
- Utility script to check whether remote storage can be disabled for a given topics.
- JMX Prometheus exporter version upgraded from 0.12.0 to 1.0.1
- Extend pod information to FetchResponse v16+, ProduceResponse v10+, ShareAckResponse v0+, and ShareFetchResponse v0+
- Make remote storage manager configs as non-sensitive

# Release Kafka 3.9.4-uber build

- Fix SizeInPercent and LocalSizeInPercent metrics
- Bump kafkasecurity jar to 2.0.14 version
- Quota manager integration for prefetch
- Enable privileged FetchSession for remote log metadata client

# Release Kafka 3.9.3-uber build

- Reduce the remote log metadata init latch await timeout
- Added metrics when a segment is read from prefetch directory
- Fix scala 2.12.19 compilation error in Kafka 3.9 test classes

# Release Kafka 3.9.2-uber build

- Upgrade hadoop client to v2.8.2.61
- Remote Segment Prefetch
- kafka-log-dirs command fail to handle stray replicas
- Remote data upload pending metrics should be updated when remote storage is unavailable
- Gracefully handle error while building remoteLogAuxState in ReplicaFetcherThread
- Allow reading from remote storage for multiple partitions in one fetchRequest
- Multiple fixes on the RLMM initialization path.
- LocalSizeInPercent metrics for remote storage topics.
- Use CountDownLatch to reduce the frequency of ReplicaNotAvailableException thrown back to clients.

# Release Kafka 3.9.1-uber build
- This is the first version of Kafka 3.9.1-uber build.
- This build is a fork of Apache Kafka 3.9.0 + required changes from 3.9.1 build.
- It contains custom Uber specific features. See https://t3.uberinternal.com/browse/DKAFC-5085 and https://docs.google.com/spreadsheets/d/18yFnfZDlVY7Og1gJ7aYzGefyYCcL10l3o8FBLvri82M/edit?gid=0#gid=0 for detailed changelog.