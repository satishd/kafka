/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package kafka.server

import org.apache.kafka.common.{Node, Uuid}
import org.apache.kafka.common.message.ControllerRegistrationResponseData
import org.apache.kafka.common.metadata.{FeatureLevelRecord, RegisterControllerRecord}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.ControllerRegistrationResponse
import org.apache.kafka.common.utils.ExponentialBackoff
import org.apache.kafka.image.loader.{LogDeltaManifest, SnapshotManifest}
import org.apache.kafka.image.{MetadataDelta, MetadataImage, MetadataProvenance}
import org.apache.kafka.metadata.{ListenerInfo, RecordTestUtils, VersionRange}
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.raft.{LeaderAndEpoch, QuorumConfig}
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.config.{KRaftConfigs, ServerLogConfigs}
import org.apache.kafka.test.TestUtils
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.{Test, Timeout}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util
import java.util.{OptionalInt, Properties}
import java.util.concurrent.{CompletableFuture, TimeUnit}
import scala.jdk.CollectionConverters._

@Timeout(value = 60)
class ControllerRegistrationManagerTest {
  private val controller1 = new Node(1, "localhost", 7000)

  private def configProperties = {
    val properties = new Properties()
    properties.setProperty(ServerLogConfigs.LOG_DIRS_CONFIG, "/tmp/foo")
    properties.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "controller")
    properties.setProperty(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, s"CONTROLLER:PLAINTEXT")
    properties.setProperty(SocketServerConfigs.LISTENERS_CONFIG, s"CONTROLLER://localhost:8001")
    properties.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
    properties.setProperty(KRaftConfigs.NODE_ID_CONFIG, "1")
    properties.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, s"1@localhost:8000,2@localhost:5000,3@localhost:7000")
    properties
  }

  private def createSupportedFeatures(
    highestSupportedMetadataVersion: MetadataVersion
  ): java.util.Map[String, VersionRange] = {
    val results = new util.HashMap[String, VersionRange]()
    results.put(MetadataVersion.FEATURE_NAME, VersionRange.of(
      MetadataVersion.MINIMUM_KRAFT_VERSION.featureLevel(),
      highestSupportedMetadataVersion.featureLevel()))
    results
  }

  private def newControllerRegistrationManager(
    context: RegistrationTestContext,
  ): ControllerRegistrationManager = {
    new ControllerRegistrationManager(context.config.nodeId,
      context.clusterId,
      context.time,
      "controller-registration-manager-test-",
      createSupportedFeatures(MetadataVersion.IBP_3_7_IV0),
      false,
      RecordTestUtils.createTestControllerRegistration(1, false).incarnationId(),
      ListenerInfo.create(context.config.controllerListeners.map(_.toJava).asJava),
      new ExponentialBackoff(1, 2, 100, 0.02))
  }

  private def registeredInLog(manager: ControllerRegistrationManager): Boolean = {
    val registeredInLog = new CompletableFuture[Boolean]
    manager.eventQueue.append(() => {
      registeredInLog.complete(manager.registeredInLog)
    })
    registeredInLog.get(30, TimeUnit.SECONDS)
  }

  private def rpcStats(manager: ControllerRegistrationManager): (Boolean, Long, Long) = {
    val failedAttempts = new CompletableFuture[(Boolean, Long, Long)]
    manager.eventQueue.append(() => {
      failedAttempts.complete((manager.pendingRpc, manager.successfulRpcs, manager.failedRpcs))
    })
    failedAttempts.get(30, TimeUnit.SECONDS)
  }

  private def doMetadataUpdate(
    prevImage: MetadataImage,
    manager: ControllerRegistrationManager,
    metadataVersion: MetadataVersion,
    registrationModifier: RegisterControllerRecord => Option[RegisterControllerRecord]
  ): MetadataImage = {
    val delta = new MetadataDelta.Builder().
      setImage(prevImage).
      build()
    if (!prevImage.features().metadataVersion().equals(metadataVersion)) {
      delta.replay(new FeatureLevelRecord().
        setName(MetadataVersion.FEATURE_NAME).
        setFeatureLevel(metadataVersion.featureLevel()))
    }
    if (metadataVersion.isControllerRegistrationSupported) {
      for (i <- Seq(1, 2, 3)) {
        registrationModifier(RecordTestUtils.createTestControllerRegistration(i, false)).foreach {
          registration => delta.replay(registration)
        }
      }
    }
    val provenance = new MetadataProvenance(100, 200, 300)
    val newImage = delta.apply(provenance)
    val manifest = if (!prevImage.features().metadataVersion().equals(metadataVersion)) {
      new SnapshotManifest(provenance, 1000)
    } else {
      new LogDeltaManifest.Builder().
        provenance(provenance).
        leaderAndEpoch(new LeaderAndEpoch(OptionalInt.of(1), 100)).
        numBatches(1).
        elapsedNs(100).
        numBytes(200).
        build()
    }
    manager.onMetadataUpdate(delta, newImage, manifest)
    newImage
  }

  @Test
  def testCreateAndClose(): Unit = {
    val context = new RegistrationTestContext(configProperties)
    val manager = newControllerRegistrationManager(context)
    assertFalse(registeredInLog(manager))
    assertEquals((false, 0, 0), rpcStats(manager))
    manager.close()
  }

  @Test
  def testCreateStartAndClose(): Unit = {
    val context = new RegistrationTestContext(configProperties)
    val manager = newControllerRegistrationManager(context)
    try {
      manager.start(context.mockChannelManager)
      assertFalse(registeredInLog(manager))
      assertEquals((false, 0, 0), rpcStats(manager))
    } finally {
      manager.close()
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = Array(false, true))
  def testRegistration(metadataVersionSupportsRegistration: Boolean): Unit = {
    val context = new RegistrationTestContext(configProperties)
    val metadataVersion = if (metadataVersionSupportsRegistration) {
      MetadataVersion.IBP_3_7_IV0
    } else {
      MetadataVersion.IBP_3_6_IV0
    }
    val manager = newControllerRegistrationManager(context)
    try {
      if (!metadataVersionSupportsRegistration) {
        context.mockClient.prepareUnsupportedVersionResponse(_ => true)
      } else {
        context.controllerNodeProvider.node.set(controller1)
      }
      manager.start(context.mockChannelManager)
      assertFalse(registeredInLog(manager))
      assertEquals((false, 0, 0), rpcStats(manager))
      val image = doMetadataUpdate(MetadataImage.EMPTY,
        manager,
        metadataVersion,
        r => if (r.controllerId() == 1) None else Some(r))
      if (!metadataVersionSupportsRegistration) {
        assertFalse(registeredInLog(manager))
        assertEquals((false, 0, 0), rpcStats(manager))
      } else {
        TestUtils.retryOnExceptionWithTimeout(30000, () => {
          assertEquals((true, 0, 0), rpcStats(manager))
        })
        context.mockClient.prepareResponseFrom(new ControllerRegistrationResponse(
          new ControllerRegistrationResponseData()), controller1)
        TestUtils.retryOnExceptionWithTimeout(30000, () => {
          context.mockChannelManager.poll()
          assertEquals((false, 1, 0), rpcStats(manager))
        })
        assertFalse(registeredInLog(manager))
        doMetadataUpdate(image,
          manager,
          metadataVersion,
          r => Some(r))
        assertTrue(registeredInLog(manager))
      }
    } finally {
      manager.close()
    }
  }

  @Test
  def testWrongIncarnationId(): Unit = {
    val context = new RegistrationTestContext(configProperties)
    val manager = newControllerRegistrationManager(context)
    try {
      // We try to send an RPC, because the incarnation ID is wrong.
      context.controllerNodeProvider.node.set(controller1)
      doMetadataUpdate(MetadataImage.EMPTY,
        manager,
        MetadataVersion.IBP_3_7_IV0,
        r => Some(r.setIncarnationId(new Uuid(456, r.controllerId()))))
      manager.start(context.mockChannelManager)
      TestUtils.retryOnExceptionWithTimeout(30000, () => {
        assertEquals((true, 0, 0), rpcStats(manager))
      })

      // Complete the RPC.
      context.mockClient.prepareResponseFrom(new ControllerRegistrationResponse(
        new ControllerRegistrationResponseData()), controller1)
      TestUtils.retryOnExceptionWithTimeout(30000, () => {
        context.mockChannelManager.poll()
        assertEquals((false, 1, 0), rpcStats(manager))
      })

      // If the incarnation ID is still wrong, we'll resend again.
      doMetadataUpdate(MetadataImage.EMPTY,
        manager,
        MetadataVersion.IBP_3_7_IV0,
        r => Some(r.setIncarnationId(new Uuid(457, r.controllerId()))))
      TestUtils.retryOnExceptionWithTimeout(30000, () => {
        context.mockChannelManager.poll()
        assertEquals((true, 1, 0), rpcStats(manager))
      })
    } finally {
      manager.close()
    }
  }

  @Test
  def testRetransmitRegistration(): Unit = {
    val context = new RegistrationTestContext(configProperties)
    val manager = newControllerRegistrationManager(context)
    try {
      context.controllerNodeProvider.node.set(controller1)
      manager.start(context.mockChannelManager)
      context.mockClient.prepareResponseFrom(new ControllerRegistrationResponse(
        new ControllerRegistrationResponseData().
          setErrorCode(Errors.UNKNOWN_CONTROLLER_ID.code()).
          setErrorMessage("Unknown controller 1")), controller1)
      context.mockClient.prepareResponseFrom(new ControllerRegistrationResponse(
        new ControllerRegistrationResponseData()), controller1)
      doMetadataUpdate(MetadataImage.EMPTY,
        manager,
        MetadataVersion.IBP_3_7_IV0,
        r => if (r.controllerId() == 1) None else Some(r))
      TestUtils.retryOnExceptionWithTimeout(30000, () => {
        context.time.sleep(10) // Advance mock time to trigger retry
        context.mockChannelManager.poll()
        assertEquals((false, 1, 0), rpcStats(manager))
      })
    } finally {
      manager.close()
    }
  }

  @Test
  def testTimeoutResetsPendingRpc(): Unit = {
    val context = new RegistrationTestContext(configProperties)
    val manager = newControllerRegistrationManager(context)

    try {
      // Phase 1: Queue a registration request that cannot be sent (no controller node set)
      manager.start(context.mockChannelManager)
      doMetadataUpdate(MetadataImage.EMPTY,
        manager,
        MetadataVersion.IBP_3_7_IV0,
        r => if (r.controllerId() == 1) None else Some(r))

      // Verify request is pending but unsent (pendingRpc=true, successfulRpcs=0, failedRpcs=0)
      TestUtils.retryOnExceptionWithTimeout(30000, () => {
        context.mockChannelManager.poll()
        assertEquals((true, 0, 0), rpcStats(manager))
      })

      // Phase 2: Trigger timeout by advancing time beyond the retry timeout threshold
      // Add margin to ensure we exceed the 60-second retryTimeoutMs
      val timeoutMargin = 1000L
      context.time.sleep(context.mockChannelManager.retryTimeoutMs + timeoutMargin)

      // Verify timeout was processed and retry scheduled
      // Note: We don't assert on pendingRpc here because the retry may fire immediately.
      // After timeout, pendingRpc is set to false, but then a retry is scheduled with
      // exponential backoff starting at 1ms. Since we've advanced time by 61000ms,
      // the retry (scheduled for +1ms) has already passed and may fire in the same poll().
      TestUtils.retryOnExceptionWithTimeout(30000, () => {
        context.mockChannelManager.poll()
        val (_, successfulRpcs, failedRpcs) = rpcStats(manager)
        assertEquals(0, successfulRpcs, "No successful RPCs yet")
        assertEquals(1, failedRpcs, "Timeout should increment failedRpcs to 1")
      })

      // Phase 3: Enable the retry to succeed by setting controller node
      context.controllerNodeProvider.node.set(controller1)
      context.mockClient.prepareResponseFrom(new ControllerRegistrationResponse(
        new ControllerRegistrationResponseData()), controller1)

      // Advance time to allow scheduled retry to fire and wait for it to complete successfully
      context.time.sleep(10)
      TestUtils.retryOnExceptionWithTimeout(30000, () => {
        context.mockChannelManager.poll()
        val (pendingRpc, successfulRpcs, failedRpcs) = rpcStats(manager)
        assertFalse(pendingRpc, "No RPC should be pending after successful retry")
        assertEquals(1, successfulRpcs, "Retry should succeed")
        assertEquals(0, failedRpcs, "Failed RPC count is reset to 0 after success")
      })
    } finally {
      manager.close()
    }
  }
}
