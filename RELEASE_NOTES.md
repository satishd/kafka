# Release Kafka 3.9.24-uber build (06/15/2026)
- use latest streaming-tools that fixes the port binding error in scripts
- KAFKA-19294: Fix BrokerLifecycleManager RPC timeouts (#19745) (#153)
- KAFKA-14619; KRaft validate snapshot id are at batch boundaries (#17500) (#158)
- KAFKA-18837: Ensure controller quorum timeouts and backoffs are non-negative (#161)
- KAFKA-18859 honor the error message of UnregisterBrokerResponse (#19027) (#162
- KAFKA-15371 MetadataShell is stuck when bootstrapping (#19419) (#164)
- KAFKA-19350 Don't propagate the error caused by CreateTopicPolicy to FatalFaultHandler #19857 (#165)
- KAFKA-19497; Topic replay code does not handle creation and deletion in the same delta #20242 (#166)
- KAFKA-19690 Add epoch check before verification guard check to prevent unexpected fatal error (#20577) (#154)
- KAFKA-19719 --no-initial-controllers should not assume kraft.version=1 (#20624) (#155)
- Open JMX connection lazily per scrape in DefaultKafkaJmxCollector (#126)
- KAFKA-20380; backwards compatible advertised.listeners when it is not defined (#22219) (#156)
- KAFKA-17431: Support invalid static configs for KRaft so long as dynamic configs are valid #18949 (#174)
- KAFKA-19130: Do not add fenced brokers to BrokerRegistrationTracker on startup #19454 (#175)
- KAFKA-19354: KRaft observer should send fetch to best node (#19854) (#170)
- KAFKA-19605; Fix the busy loop occurring in kraft client observers #20354 (#176)
- KAFKA-18061 AddRaftVoter responds with error message "NONE" instead of null #17930 (#167)

# Release Kafka 3.9.23-uber build (06/07/2026)
-  Fix ISR expansion rate limit not applied at startup in ZK mode (#160)
-  KAFKA-17803: LogSegment#read should return the base offset of the batch that contains startOffset rather than startOffset (#17528) (#157)
-  KAFKA-17508: Adding some guard for fallback deletion logic (#159)
-  KAFKA-18345; Prevent livelocked elections (#19658) (#151)
-  KAFKA-18106: Generate LeaderAndIsrUpdates on unclean shutdown (#18045) (#152)
-  KAFKA-18138: Controller must add all extant brokers to BrokerHeartbeatTracker when activating (#140)
-  KAFKA-17793: Improve kcontroller robustness against long delays (#17502) (#139)
-  KAFKA-18920: The kcontrollers must set kraft.version in ApiVersionsResponse (#19127) (#148)
-  KAFKA-18281: Kafka is improperly validating non-advertised listeners for routable controller addresses (#18387) (#149)
-  KAFKA-17713: Don't generate snapshot when published metadata is not batch-aligned (#137)
-  KAFKA-18583; Fix getPartitionReplicaEndpoints for KRaft (#18657) (#150)
-  KAFKA-17973: Relax Restriction for Voters Set Change (#17728) (#146)
-  KAFKA-17030; Unattached voters will fetch from bootstrap servers (#17352) (#147)
-  KAFKA-18028 the effective kraft version of --no-initial-controllers should be 1 rather than 0 (#17836) (#145)
-  KAFKA-18063: SnapshotRegistry should not leak memory (#17898) (#144)
-  KAFKA-18001: Support UpdateRaftVoterRequest in KafkaNetworkChannel (#17773) (#143)
-  Add a cancel all option for kafka partition reassignment script (#125)
-  Append upki.properties to modified config for SSL in add-controller cmd (#141)
-  Add heap size in controller quorum scripts (#138)

# Release Kafka 3.9.22-uber build (05/26/2026)
- Upgrade streaming_tools version to latest 2.0.1~15196.gbpc45c34 (concurrent replacement fixes) (#116)
- KAFKA-19590 Add prefix to TopicBasedRemoteLogMetadataManagerConfig to… (#119)
- Pass SSL command-config in wait_for_controller_ready (#115)
- [MINOR] Ignore LLM tool cache and settings directories in .gitignore (#112)
- DKAFC-7217: Update prefetch config defaults in HDFS RSM config (#113)

# Release Kafka 3.9.21-uber build (05/13/2026)
- Serve JMX exporter config and fallback metrics on one port (#109)
- DKAFC-6838: Prefetch the current segment to reduce spiky OCI GET calls (#108)
- upgrade streaming_tools version to latest 2.0.1~15176.gbpb0f7f0, fix port issue (#104)
- KAFKA-19858 Set default min.insync.replicas=2 for __remote_log_metadata (#103)
- Split JMX exporter metrics across two HTTP ports (#101)
- kafka-reassign-partitions batching and incremental (sliding-window) execute (#100)

# Release Kafka 3.9.20-uber build (04/24/2026)
- DKAFC-7153: Update kafka-artifact-uploader.py to upload 3.9 jars
- DKAFC-7004: Handle empty bucket cases in a remote storage provider
- DKAFC-7157: Validation to prevent HDFS as a storage provider

# Release Kafka 3.9.19-uber build (04/22/2026)
- DKAFC-7002: Remove HDFS Hedged reads feature from Kafka
- DKAFC-7003: Refactor FileSystemManager to clarify that readAhead feature unused
- DKAFC-7131: Extend fetchLookback metric to track remote reads
- skip using 'unkown'in metric name if it has no associated type or name (JMXExporter)

# Release Kafka 3.9.18-uber build (04/06/2026)
- Add KRaft mode broker storage directory formatting (#85)
- Fix controller registration deadlock after RPC timeout (#86)
- Add hybrid JMX exporter for graphite to M3 metrics migration (#78)

# Release Kafka 3.9.17-uber build (03/26/2026)
- Bump streaming-tools version 2.0.1~15149.gbp02cc20.
- DKAFC-7030: Fix resource leaks while handling error in RSM fetchAuxiliaryFiles

# Release Kafka 3.9.16-uber build (03/23/2026)
- Support controller and broker mode differentiation in startup script [kraft]
- Add CLAUDE.md with build, test, and architecture guidance
- [cherry-pick] 2.9 add rate limit logic (#69)
- Fix the scala 2.12 compilation issues in tests (#72)
- Use PodReplicaPlacer in Controller
- DKAFC-6997: Don't initialize FS inputStream when txn index file is empty (#73)
- DKAFC-7021: Update the copy-circuit breaker handling logic in RSM (#75)
- Set broker.id/node.id from worker's node.id state file (#74)
- PodReplicaPlacer detects placement rule confliction
- DKAFC-7030: Read all associated remote segment file in one request (#77)
- DKAFC-7004: Don't instantiate the FileSystem eagerly for HDFS buckets (#80)

# Release Kafka 3.9.15-uber build (02/17/2026)
- Bump oc-hdfs connector client to v2.9.2.74
- Install confluent-kafka using pip for use in streaming tools script

# Release Kafka 3.9.14-uber build (02/09/2026)
- Streaming tool version: 2.0.1~15110.gbpfc0ac2
- Use Direct range FS InputStream when reading from OCI (#57)
- Upgrade oci-hdfs connector client to v2.8.2.72 (#60)
- Support pod isolation with PodReplicaPlacer
- Use circuit breaker for prefetch segment download failures (#54)
- Avoid positional reads in CachedInputStream when reading from OCI (#58)
- Add pod support in UsableBroker for canary isolation

# Release Kafka 3.9.13-uber build (02/02/2026)
- Streaming tool version: 2.0.1~15082.gbp889939
- Revert "DKAFC-6839: Increase prefetch segment expiration to 15 mins to avoid thrashing"
- move rebuild log ot /var/log so that it can be sent to logging

# Release Kafka 3.9.12-uber build (01/09/2026)
- Reduce the HEAD call when deleting the segments in OCI storage.
- Bump streaming-tools to 15011.gbpc9523f version.
- Support for ISR Blocklist, new replica exclude list, leader deprioritized list, follower fetch latest offset feature, and URPByBrokerId metric on Kraft.
- Implemented rate limiter on segment upload in the connector layer.
- ConfigCommand#validatePropsKey should accept `$` symbol.
- DKAFC-6801: Remote Connector metrics should include reading Auxiliary files.
- Change log level to debug when remote LIST_OFFSETS call fail to avoid noisy logs.
- KAFKA-20026: Reduce the list metadata calls to RLMM during segment cleanup.
- DKAFC-6837: Use OCI direct FS input stream for segment prefetch (#47).
- DKAFC-6839: Increase prefetch segment expiration to 15 mins to avoid thrashing (#49).
- BugFix:
  - KAFKA-19970: Add configurable TTL for tiered storage index cache eviction (#39)
  - DKAFC-6830: Remote log size in DESCRIBE_LOG_DIRS API should exclude upload retries (#46)

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
