/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.utils

import kafka.cluster.PendingRequests
import org.junit.{Assert, Test}
import org.scalatest.Assertions.intercept

class PendingRequestsTest {

  @Test
  def testPendingRequestsIllegalState(): Unit = {
    val pendingRequests = new PendingRequests
    val fns = (1 to 16).map(_ => () => doTestPendingRequestsIllegalState(pendingRequests))
    val x = System.currentTimeMillis()
    TestUtils.assertConcurrent("ConcurrentPendingRequests", fns, 30000)
    val y = System.currentTimeMillis()
    System.out.println(s"Took ${y - x} msecs")
  }

  private def doTestPendingRequestsIllegalState(pr: PendingRequests) = {
    val x = 10L
    (1 to 2000000).foreach { _ =>
      pr.add(x)
      pr.remove(x)
      pr.remove(x)
      pr.add(x)
    }
  }

  @Test
  def testAddRemoveContains(): Unit = {
    // This test does not check for concurrent threads but checks operations for a single thread
    val pendingRequests = new PendingRequests
    val x = 100L
    Assert.assertEquals(1, pendingRequests.add(x))
    Assert.assertEquals(2, pendingRequests.add(x))
    Assert.assertEquals(3, pendingRequests.add(x))

    Assert.assertTrue(pendingRequests.contains(x))

    Assert.assertTrue(pendingRequests.remove(x))
    Assert.assertTrue(pendingRequests.contains(x))

    Assert.assertTrue(pendingRequests.remove(x))
    Assert.assertTrue(pendingRequests.contains(x))

    Assert.assertTrue(pendingRequests.remove(x))
    Assert.assertFalse(pendingRequests.contains(x))

    Assert.assertFalse(pendingRequests.remove(x))
    Assert.assertTrue(pendingRequests.isEmpty())
    Assert.assertEquals(1, pendingRequests.add(x))
  }
}
