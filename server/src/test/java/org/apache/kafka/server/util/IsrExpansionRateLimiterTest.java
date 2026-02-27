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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class IsrExpansionRateLimiterTest {

    private RateLimiter rateLimiter;
    private IsrExpansionRateLimiter isrExpansionRateLimiter;

    @BeforeEach
    public void setup() {
        rateLimiter = mock(RateLimiter.class);
        isrExpansionRateLimiter = new IsrExpansionRateLimiter(rateLimiter);
    }

    @Test
    public void testUpdateBrokerIds() {
        isrExpansionRateLimiter.updateBrokerIds("1:2:3");
        assertEquals(new HashSet<>(Arrays.asList(1, 2, 3)), isrExpansionRateLimiter.getBrokerIdsToRateLimit());
    }

    @Test
    public void testUpdateBrokerIdsWithSingleId() {
        isrExpansionRateLimiter.updateBrokerIds("5");
        assertEquals(Collections.singleton(5), isrExpansionRateLimiter.getBrokerIdsToRateLimit());
    }

    @Test
    public void testUpdateBrokerIdsReplacesExistingIds() {
        isrExpansionRateLimiter.updateBrokerIds("1:2:3");
        assertEquals(new HashSet<>(Arrays.asList(1, 2, 3)), isrExpansionRateLimiter.getBrokerIdsToRateLimit());

        isrExpansionRateLimiter.updateBrokerIds("4:5");
        assertEquals(new HashSet<>(Arrays.asList(4, 5)), isrExpansionRateLimiter.getBrokerIdsToRateLimit());
    }

    @Test
    public void testUpdateRateLimitDelegatesToRateLimiter() {
        isrExpansionRateLimiter.updateRateLimit(10.0);

        verify(rateLimiter, times(1)).updateRateLimit(10.0);
        verifyNoMoreInteractions(rateLimiter);
    }

    @Test
    public void testTryAcquirePassesThroughForNonLimitedBrokerId() {
        isrExpansionRateLimiter.updateBrokerIds("1:2:3");

        assertTrue(isrExpansionRateLimiter.tryAcquire(4));
        assertTrue(isrExpansionRateLimiter.tryAcquire(5));
        verifyNoMoreInteractions(rateLimiter);
    }

    @Test
    public void testTryAcquireDelegatesToRateLimiterForLimitedBrokerId() {
        isrExpansionRateLimiter.updateBrokerIds("1:2:3");
        when(rateLimiter.tryAcquire()).thenReturn(true);

        assertTrue(isrExpansionRateLimiter.tryAcquire(1));

        verify(rateLimiter, times(1)).tryAcquire();
        verifyNoMoreInteractions(rateLimiter);
    }

    @Test
    public void testTryAcquireReturnsFalseWhenRateLimiterDenies() {
        isrExpansionRateLimiter.updateBrokerIds("1:2:3");
        when(rateLimiter.tryAcquire()).thenReturn(false);

        assertFalse(isrExpansionRateLimiter.tryAcquire(1));

        verify(rateLimiter, times(1)).tryAcquire();
        verifyNoMoreInteractions(rateLimiter);
    }

    @Test
    public void testTryAcquirePassesThroughWhenNoBrokerIdsConfigured() {
        assertTrue(isrExpansionRateLimiter.tryAcquire(1));
        assertTrue(isrExpansionRateLimiter.tryAcquire(2));
        verifyNoMoreInteractions(rateLimiter);
    }

    @Test
    public void testGetBrokerIdsToRateLimitReturnsEmptyByDefault() {
        assertEquals(Collections.emptySet(), isrExpansionRateLimiter.getBrokerIdsToRateLimit());
    }
}
