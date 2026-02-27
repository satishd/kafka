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

import org.apache.kafka.common.utils.Time;

public class RateLimiter {

    private final Time time;
    private double maxIntervalMillis;
    private long lastPermitTime;

    public RateLimiter(double permitsPerSecond, Time time) {
        this.time = time;
        this.maxIntervalMillis = 1000.0 / permitsPerSecond;
        this.lastPermitTime = time.milliseconds();
    }

    public synchronized void updateRateLimit(double newPermitsPerSecond) {
        this.maxIntervalMillis = 1000.0 / newPermitsPerSecond;
    }

    public synchronized boolean tryAcquire() {
        long currentTime = time.milliseconds();
        if (currentTime - lastPermitTime > maxIntervalMillis) {
            lastPermitTime = currentTime;
            return true;
        } else {
            return false;
        }
    }
}
