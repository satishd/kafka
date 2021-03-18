package integration.kafka.api

import kafka.api.TransactionsTest
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.log.remote.metadata.storage.RLMMWithTopicStorage
import org.apache.kafka.common.log.remote.storage.LocalTieredStorage
import org.apache.kafka.common.log.remote.storage.LocalTieredStorage.STORAGE_DIR_PROP

import java.util.Properties
import scala.collection.Seq

class TransactionsTestWithTieredStore extends TransactionsTest {

  override val numServers = 3

  override def generateConfigs: Seq[KafkaConfig] = {
    val overridingProps = serverProps()
    //
    // The directory of the second-tier storage needs to be constant across all instances of storage managers
    // in every broker and throughout the test. Indeed, as brokers are restarted during the test.
    //
    // You can override this property with a fixed path of your choice if you wish to use a non-temporary
    // directory to access its content after a test terminated.
    //
    overridingProps.setProperty(STORAGE_DIR_PROP, TestUtils.tempDir().getAbsolutePath)
    TestUtils.createBrokerConfigs(numServers, zkConnect).map(KafkaConfig.fromProps(_, overridingProps))
  }

  override def serverProps(): Properties = {
    val overridingProps = super.serverProps()
    //
    // Configure the tiered storage in Kafka. Set an interval of 100 ms for the remote log manager background
    // activity to ensure the tiered storage has enough room to be exercised within the lifetime of a test.
    //
    // The replication factor of the remote log metadata topic needs to be chosen so that in resiliency
    // tests, metadata can survive the loss of one replica for its topic-partitions.
    //
    // The second-tier storage system is mocked via the LocalTieredStorage instance which persists transferred
    // data files on the local file system.
    //
    overridingProps.put(KafkaConfig.RemoteLogStorageEnableProp, true.toString)
    overridingProps.setProperty(KafkaConfig.RemoteLogStorageManagerProp, classOf[LocalTieredStorage].getName)
    overridingProps.setProperty(KafkaConfig.RemoteLogMetadataManagerProp, classOf[RLMMWithTopicStorage].getName)
    overridingProps.setProperty(KafkaConfig.RemoteLogManagerTaskIntervalMsProp, 100.toString)
    overridingProps.setProperty(KafkaConfig.RemoteLogMetadataTopicPartitionsProp, 3.toString)
    overridingProps.setProperty(KafkaConfig.RemoteLogMetadataTopicReplicationFactorProp, 2.toString)

    //
    // This configuration ensures inactive log segments are deleted fast enough so that
    // the integration tests can confirm a given log segment is present only in the second-tier storage.
    // Note that this does not impact the eligibility of a log segment to be offloaded to the
    // second-tier storage.
    //
    overridingProps.setProperty(KafkaConfig.LogCleanupIntervalMsProp, 100.toString)
    overridingProps.setProperty(LocalTieredStorage.DELETE_ON_CLOSE_PROP, true.toString)
    overridingProps
  }

  override def topicProps(): Properties = {
    val overridingProps = super.topicProps()
    //
    // Ensure offset and time indexes are generated for every record.
    //
    overridingProps.put(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, 1.toString)
    //
    // Leverage the use of the segment index size to create a log-segment accepting one and only one record.
    // The minimum size of the indexes is that of an entry, which is 8 for the offset index and 12 for the
    // time index. Hence, since the topic is configured to generate index entries for every record with, for
    // a "small" number of records (i.e. such that the average record size times the number of records is
    // much less than the segment size), the number of records which hold in a segment is the multiple of 12
    // defined below.
    //
    overridingProps.put(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, (12 * 1).toString)
    //
    // To verify records physically absent from Kafka's storage can be consumed via the second tier storage, we
    // want to delete log segments as soon as possible. When tiered storage is active, an inactive log
    // segment is not eligible for deletion until it has been offloaded, which guarantees all segments
    // should be offloaded before deletion, and their consumption is possible thereafter.
    //
    overridingProps.put(TopicConfig.RETENTION_BYTES_CONFIG, 1.toString)
    overridingProps
  }
}
