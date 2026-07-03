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
package org.apache.kafka.lake.metrics;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

/**
 * In-process counters for the converter worker, periodically dumped to the log.
 *
 * <p>No external metrics backend is wired up yet (JMX/M3 emission is left to the environment this
 * worker is deployed into); this class only gives operators visibility via logs during the pilot,
 * per the design doc's Commit 6 scope.
 */
public class ConverterMetrics {

    private static final Logger LOG = LoggerFactory.getLogger(ConverterMetrics.class);

    private final AtomicLong segmentsProcessed = new AtomicLong();
    private final AtomicLong segmentsSkipped = new AtomicLong();
    private final AtomicLong segmentsFailed = new AtomicLong();
    private final AtomicLong recordsWritten = new AtomicLong();
    private final AtomicLong recordsDeadLettered = new AtomicLong();
    private final AtomicLong commitLatencyMsTotal = new AtomicLong();

    public void segmentProcessed() {
        segmentsProcessed.incrementAndGet();
    }

    public void segmentSkipped() {
        segmentsSkipped.incrementAndGet();
    }

    public void segmentFailed() {
        segmentsFailed.incrementAndGet();
    }

    public void recordsWritten(long count) {
        recordsWritten.addAndGet(count);
    }

    public void recordsDeadLettered(long count) {
        recordsDeadLettered.addAndGet(count);
    }

    public void commitLatencyMs(long millis) {
        commitLatencyMsTotal.addAndGet(millis);
    }

    public void logSummary() {
        LOG.info("Converter metrics: segmentsProcessed={} segmentsSkipped={} segmentsFailed={} "
                        + "recordsWritten={} recordsDeadLettered={} commitLatencyMsTotal={}",
                segmentsProcessed.get(), segmentsSkipped.get(), segmentsFailed.get(),
                recordsWritten.get(), recordsDeadLettered.get(), commitLatencyMsTotal.get());
    }
}
