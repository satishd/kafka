package unit.kafka.server

import kafka.server.BrokerTopicStats
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class BrokerTopicTierLagMetricsTest {
  @Test
  def defaultTierLagIsZero(): Unit = {
    assertEquals(0L, new BrokerTopicStats().tierLagStats("topic").lag())
  }

  @Test
  def partitionLagsAreCumulated(): Unit = {
    val tierLagStats = new BrokerTopicStats().tierLagStats("topic")
    tierLagStats.setLag(0, 3)
    tierLagStats.setLag(2, 5)

    assertEquals(8L, tierLagStats.lag())
  }

  @Test
  def partitionLagsCanBeUpdated(): Unit = {
    val tierLagStats = new BrokerTopicStats().tierLagStats("topic")
    tierLagStats.setLag(0, 3)
    tierLagStats.setLag(2, 5)

    assertEquals(8L, tierLagStats.lag())

    tierLagStats.setLag(2, 1)
    assertEquals(4L, tierLagStats.lag())

    tierLagStats.setLag(1, 1)
    assertEquals(5L, tierLagStats.lag())
  }

  @Test
  def partitionsCanBeRemovedFromLag(): Unit = {
    val tierLagStats = new BrokerTopicStats().tierLagStats("topic")
    tierLagStats.setLag(0, 3)
    tierLagStats.setLag(2, 5)

    tierLagStats.removePartition(0)
    assertEquals(5L, tierLagStats.lag())

    tierLagStats.removePartition(2)
    assertEquals(0L, tierLagStats.lag())

    tierLagStats.removePartition(3)
    assertEquals(0L, tierLagStats.lag())
  }
}
