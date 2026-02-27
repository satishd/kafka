/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.server.util;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class RateLimiterTest {

    private MockTime mockTime;
    private RateLimiter rateLimiter;

    @BeforeEach
    public void setup() {
        mockTime = new MockTime();
        rateLimiter = new RateLimiter(1.0, mockTime);
    }

    @Test
    public void testTryAcquireFailsBeforeIntervalElapsed() {
        assertFalse(rateLimiter.tryAcquire());
    }

    @Test
    public void testTryAcquireSucceedsAfterIntervalElapsed() {
        mockTime.sleep(1001);
        assertTrue(rateLimiter.tryAcquire());
    }

    @Test
    public void testTryAcquireFailsImmediatelyAfterSuccessfulAcquire() {
        mockTime.sleep(1001);
        assertTrue(rateLimiter.tryAcquire());
        assertFalse(rateLimiter.tryAcquire());
    }

    @Test
    public void testTryAcquireSucceedsAgainAfterAnotherInterval() {
        mockTime.sleep(1001);
        assertTrue(rateLimiter.tryAcquire());
        mockTime.sleep(1001);
        assertTrue(rateLimiter.tryAcquire());
    }

    @Test
    public void testUpdateRateLimitToFasterRate() {
        rateLimiter.updateRateLimit(10.0); // 10 permits/sec -> 100ms interval
        mockTime.sleep(101);
        assertTrue(rateLimiter.tryAcquire());
    }

    @Test
    public void testUpdateRateLimitToSlowerRate() {
        rateLimiter.updateRateLimit(0.5); // 0.5 permits/sec -> 2000ms interval
        mockTime.sleep(1001);
        assertFalse(rateLimiter.tryAcquire(), "Should fail because 1001ms < 2000ms interval");
        mockTime.sleep(1000);
        assertTrue(rateLimiter.tryAcquire(), "Should succeed after total 2001ms > 2000ms interval");
    }

    @Test
    public void testMaxRateAlwaysPermitsAfterMinimalTime() {
        RateLimiter maxRateLimiter = new RateLimiter(Double.MAX_VALUE, mockTime);
        mockTime.sleep(1);
        assertTrue(maxRateLimiter.tryAcquire());
    }

    @Test
    public void testConcurrentAccessOnlyOneThreadAcquires() throws Exception {
        mockTime.sleep(1001);

        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch go = new CountDownLatch(1);

        CompletableFuture<Boolean> firstAttempt = CompletableFuture.supplyAsync(() -> {
            ready.countDown();
            awaitLatch(go);
            return rateLimiter.tryAcquire();
        });
        CompletableFuture<Boolean> secondAttempt = CompletableFuture.supplyAsync(() -> {
            ready.countDown();
            awaitLatch(go);
            return rateLimiter.tryAcquire();
        });

        assertTrue(ready.await(3, TimeUnit.SECONDS), "Both threads should be ready");
        go.countDown();

        boolean firstResult = firstAttempt.get(3, TimeUnit.SECONDS);
        boolean secondResult = secondAttempt.get(3, TimeUnit.SECONDS);

        assertTrue(firstResult ^ secondResult, "Exactly one thread should acquire the permit");
    }

    @Test
    public void testConcurrentAccessSecondThreadSucceedsAfterTimeAdvance() throws Exception {
        mockTime.sleep(1001);
        assertTrue(rateLimiter.tryAcquire());

        CompletableFuture<Boolean> attemptBeforeAdvance = CompletableFuture.supplyAsync(
                () -> rateLimiter.tryAcquire());
        assertFalse(attemptBeforeAdvance.get(3, TimeUnit.SECONDS),
                "Should fail before time advances");

        mockTime.sleep(1001);

        CompletableFuture<Boolean> attemptAfterAdvance = CompletableFuture.supplyAsync(
                () -> rateLimiter.tryAcquire());
        assertTrue(attemptAfterAdvance.get(3, TimeUnit.SECONDS),
                "Should succeed after time advances");
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            latch.await(3, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
