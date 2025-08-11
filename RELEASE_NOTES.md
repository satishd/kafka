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