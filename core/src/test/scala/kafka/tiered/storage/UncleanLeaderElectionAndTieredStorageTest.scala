package kafka.tiered.storage

import org.junit.jupiter.api.{Disabled, Test}

/**
 * Test Cases (C):
 */
final class UncleanLeaderElectionAndTieredStorageTest extends TieredStorageTestHarness {
  private val (leader, follower, _, topicA, p0) = (0, 1, 2, "topicA", 0)

  override protected def brokerCount: Int = 3

  override protected def writeTestSpecifications(builder: TieredStorageTestBuilder): Unit = {
    val assignment = Map(p0 -> Seq(leader, follower))

    builder
      .createTopic(topicA, partitionsCount = 1, replicationFactor = 2, maxBatchCountPerSegment = 1, assignment)
      .produce(topicA, p0, ("k1", "v1"))

      .stop(follower)
      .produce(topicA, p0, ("k2", "v2"), ("k3", "v3"))
      .withBatchSize(topicA, p0, 1)
      .expectSegmentToBeOffloaded(leader, topicA, p0, baseOffset = 1, ("k1", "v1"))

      .stop(leader)
      .start(follower)
      .expectLeader(topicA, p0, follower)
      .produce(topicA, p0, ("k4", "v4"), ("k5", "v5"))
      .withBatchSize(topicA, p0, 1)
      .expectSegmentToBeOffloaded(follower, topicA, p0, baseOffset = 1, ("k2", "v2"))
  }

  @Disabled
  @Test
  override def executeTieredStorageTest(): Unit = {

  }
}