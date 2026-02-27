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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.stream.Collectors;

public class IsrExpansionRateLimiter {

    private static final Logger log = LoggerFactory.getLogger(IsrExpansionRateLimiter.class);

    private final RateLimiter rateLimiter;
    private volatile Set<Integer> brokerIdsToRateLimit = Collections.emptySet();

    public IsrExpansionRateLimiter(RateLimiter rateLimiter) {
        this.rateLimiter = rateLimiter;
    }

    public void updateRateLimit(double permitsPerSecond) {
        log.info("IsrExpansionRateLimiter updated to {} per second", permitsPerSecond);
        rateLimiter.updateRateLimit(permitsPerSecond);
    }

    public void updateBrokerIds(String brokerIds) {
        log.info("IsrExpansionRateLimiter broker ids updated to {}", brokerIds);
        Set<Integer> ids = Arrays.stream(brokerIds.trim().split(":"))
                .map(Integer::parseInt)
                .collect(Collectors.toSet());
        brokerIdsToRateLimit = Collections.unmodifiableSet(ids);
    }

    public boolean tryAcquire(int followerId) {
        if (!brokerIdsToRateLimit.contains(followerId)) {
            // pass through for the brokers we don't want to restrict
            return true;
        } else {
            return rateLimiter.tryAcquire();
        }
    }

    // Visible for testing
    Set<Integer> getBrokerIdsToRateLimit() {
        return brokerIdsToRateLimit;
    }
}
