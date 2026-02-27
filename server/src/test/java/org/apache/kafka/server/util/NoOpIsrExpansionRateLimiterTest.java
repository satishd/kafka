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

import static org.junit.jupiter.api.Assertions.assertTrue;

public class NoOpIsrExpansionRateLimiterTest {

    private NoOpIsrExpansionRateLimiter limiter;

    @BeforeEach
    public void setup() {
        limiter = new NoOpIsrExpansionRateLimiter();
    }

    @Test
    public void testTryAcquireAlwaysReturnsTrue() {
        for (int brokerId = 0; brokerId < 10; brokerId++) {
            assertTrue(limiter.tryAcquire(brokerId));
        }
    }

    @Test
    public void testTryAcquireReturnsTrueEvenAfterUpdatingBrokerIds() {
        limiter.updateBrokerIds("1:2:3");
        assertTrue(limiter.tryAcquire(1));
        assertTrue(limiter.tryAcquire(2));
        assertTrue(limiter.tryAcquire(3));
    }

    @Test
    public void testTryAcquireReturnsTrueEvenAfterUpdatingRateLimit() {
        limiter.updateRateLimit(0.001);
        limiter.updateBrokerIds("1");
        assertTrue(limiter.tryAcquire(1));
        assertTrue(limiter.tryAcquire(1));
    }

    @Test
    public void testTryAcquireReturnsTrueForMultipleConsecutiveCalls() {
        limiter.updateBrokerIds("1");
        for (int i = 0; i < 100; i++) {
            assertTrue(limiter.tryAcquire(1));
        }
    }
}
