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
package com.uber.kafka.graphite;

import org.apache.kafka.server.metrics.KafkaYammerMetrics;

import com.yammer.metrics.core.Clock;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricPredicate;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Sampling;
import com.yammer.metrics.core.Summarizable;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.VirtualMachineMetrics;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import com.yammer.metrics.stats.Snapshot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.Thread.State;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * A simple reporter which sends out application metrics to a
 * <a href="http://graphite.wikidot.com/faq">Graphite</a> server periodically.
 * This is a direct copy from <a href=
 * "http://grepcode.com/file/repo1.maven.org/maven2/com.yammer.metrics/metrics-graphite/2.1.2/com/yammer/metrics/reporting/GraphiteReporter.java">
 * com.yammer.metrics.reporting</a>. No logic change except some unwanted
 * metrics were removed.
 */
public class GraphiteReporter extends AbstractPollingReporter implements MetricProcessor<Long> {
    private static final Logger LOG = LoggerFactory.getLogger(GraphiteReporter.class);
    protected final String prefix;
    protected final MetricPredicate predicate;
    protected final MetricPredicate nonZeroPredicate;
    protected final Locale locale = Locale.US;
    protected final Clock clock;
    protected final SocketProvider socketProvider;
    protected final VirtualMachineMetrics vm;
    protected Writer writer;
    public boolean printVMMetrics = true;

    protected final Meter statsSent;
    protected final Meter statsExcluded;
    protected final Meter statsExcludedForZero;

    /**
     * Creates a new {@link GraphiteReporter}.
     *
     * @param host
     *            is graphite server
     * @param port
     *            is port on which graphite server is running
     * @param prefix
     *            is prepended to all names reported to graphite
     * @throws IOException
     *             if there is an error connecting to the Graphite server
     */
    public GraphiteReporter(String host, int port, String prefix) throws IOException {
        this(KafkaYammerMetrics.defaultRegistry(), host, port, prefix);
    }

    /**
     * Creates a new {@link GraphiteReporter}.
     *
     * @param metricsRegistry
     *            the metrics registry
     * @param host
     *            is graphite server
     * @param port
     *            is port on which graphite server is running
     * @param prefix
     *            is prepended to all names reported to graphite
     * @throws IOException
     *             if there is an error connecting to the Graphite server
     */
    public GraphiteReporter(MetricsRegistry metricsRegistry, String host, int port, String prefix) throws IOException {
        this(metricsRegistry,
                prefix,
                MetricPredicate.ALL,
                MetricPredicate.ALL,
                new DefaultSocketProvider(host, port),
                Clock.defaultClock());
    }

    /**
     * Creates a new {@link GraphiteReporter}.
     *
     * @param metricsRegistry
     *            the metrics registry
     * @param prefix
     *            is prepended to all names reported to graphite
     * @param predicate
     *            filters metrics to be reported
     * @param nonZeroPredicate
     *            filters zero value metrics to be reported
     * @param socketProvider
     *            a {@link SocketProvider} instance
     * @param clock
     *            a {@link Clock} instance
     * @throws IOException
     *             if there is an error connecting to the Graphite server
     */
    public GraphiteReporter(MetricsRegistry metricsRegistry, String prefix, MetricPredicate predicate,
            MetricPredicate nonZeroPredicate, SocketProvider socketProvider, Clock clock) throws IOException {
        this(metricsRegistry, prefix, predicate, nonZeroPredicate, socketProvider, clock,
                VirtualMachineMetrics.getInstance());
    }

    /**
     * Creates a new {@link GraphiteReporter}.
     *
     * @param metricsRegistry
     *            the metrics registry
     * @param prefix
     *            is prepended to all names reported to graphite
     * @param predicate
     *            filters metrics to be reported
     * @param nonZeroPredicate
     *            filters zero value metrics to be reported
     * @param socketProvider
     *            a {@link SocketProvider} instance
     * @param clock
     *            a {@link Clock} instance
     * @param vm
     *            a {@link VirtualMachineMetrics} instance
     * @throws IOException
     *             if there is an error connecting to the Graphite server
     */
    public GraphiteReporter(MetricsRegistry metricsRegistry, String prefix, MetricPredicate predicate,
            MetricPredicate nonZeroPredicate, SocketProvider socketProvider, Clock clock, VirtualMachineMetrics vm)
                    throws IOException {
        this(metricsRegistry, prefix, predicate, nonZeroPredicate, socketProvider, clock, vm, "graphite-reporter");
    }

    /**
     * Creates a new {@link GraphiteReporter}.
     *
     * @param metricsRegistry
     *            the metrics registry
     * @param prefix
     *            is prepended to all names reported to graphite
     * @param predicate
     *            filters metrics to be reported
     * @param nonZeroPredicate
     *            filters zero value metrics to be reported
     * @param socketProvider
     *            a {@link SocketProvider} instance
     * @param clock
     *            a {@link Clock} instance
     * @param vm
     *            a {@link VirtualMachineMetrics} instance
     * @throws IOException
     *             if there is an error connecting to the Graphite server
     */
    public GraphiteReporter(MetricsRegistry metricsRegistry, String prefix, MetricPredicate predicate,
            MetricPredicate nonZeroPredicate, SocketProvider socketProvider, Clock clock, VirtualMachineMetrics vm,
            String name) throws IOException {
        super(metricsRegistry, name);
        this.socketProvider = socketProvider;
        this.vm = vm;

        this.clock = clock;

        if (prefix != null) {
            // Pre-append the "." so that we don't need to make anything
            // conditional later.
            this.prefix = prefix + ".";
        } else {
            this.prefix = "";
        }
        this.predicate = predicate;
        this.nonZeroPredicate = nonZeroPredicate;
        statsSent = metricsRegistry.newMeter(new MetricName("kafka.stats", "GraphiteReporter", "StatsSentPerSec", null,
                "kafka.stats:type=GraphiteReporter,name=StatsSentPerSec"), "stats", TimeUnit.SECONDS);
        statsExcluded = metricsRegistry
                .newMeter(
                        new MetricName("kafka.stats", "GraphiteReporter", "StatsExcludedPerSec", null,
                                "kafka.stats:type=GraphiteReporter,name=StatsExcludedPerSec"),
                        "stats", TimeUnit.SECONDS);
        statsExcludedForZero = metricsRegistry.newMeter(
                new MetricName("kafka.stats", "GraphiteReporter", "StatsExcludedForZeroPerSec", null,
                        "kafka.stats:type=GraphiteReporter,name=StatsExcludedForZeroPerSec"),
                "stats", TimeUnit.SECONDS);
    }

    @Override
    public void run() {
        Socket socket = null;
        try {
            socket = this.socketProvider.get();
            writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.UTF_8));

            final long epoch = clock.time() / 1000;
            if (this.printVMMetrics) {
                printVmMetrics(epoch);
            }
            printRegularMetrics(epoch);
            writer.flush();
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Error writing to Graphite", e);
            } else {
                LOG.warn("Error writing to Graphite: {}", e.getMessage());
            }
            if (writer != null) {
                try {
                    writer.flush();
                } catch (IOException e1) {
                    LOG.error("Error while flushing writer:", e1);
                }
            }
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    LOG.error("Error while closing socket:", e);
                }
            }
            writer = null;
        }
    }

    protected void printRegularMetrics(final Long epoch) {
        SortedMap<String, SortedMap<MetricName, Metric>> full, filtered;
        full = getMetricsRegistry().groupedMetrics();
        filtered = getMetricsRegistry().groupedMetrics(predicate);
        statsExcluded.mark(full.size() - filtered.size());
        for (Entry<String, SortedMap<MetricName, Metric>> entry : filtered.entrySet()) {
            for (Entry<MetricName, Metric> subEntry : entry.getValue().entrySet()) {
                final Metric metric = subEntry.getValue();
                if (metric != null) {
                    try {
                        if (nonZeroPredicate.matches(subEntry.getKey(), metric)) {
                            metric.processWith(this, subEntry.getKey(), epoch);
                        } else {
                            statsExcludedForZero.mark();
                        }
                    } catch (Exception ignored) {
                        LOG.error("Error printing regular metrics:", ignored);
                    }
                }
            }
        }
    }

    protected void sendInt(long timestamp, String name, String valueName, long value) {
        sendToGraphite(timestamp, name, valueName + " " + String.format(locale, "%d", value));
    }

    protected void sendFloat(long timestamp, String name, String valueName, double value) {
        sendToGraphite(timestamp, name, valueName + " " + String.format(locale, "%2.2f", value));
    }

    protected void sendObjToGraphite(long timestamp, String name, String valueName, Object value) {
        sendToGraphite(timestamp, name, valueName + " " + String.format(locale, "%s", value));
    }

    protected void sendToGraphite(long timestamp, String name, String value) {
        try {
            if (!prefix.isEmpty()) {
                writer.write(prefix);
            }
            String sanitized = sanitizeString(name);
            writer.write(sanitized);
            writer.write('.');
            writer.write(value);
            writer.write(' ');
            writer.write(Long.toString(timestamp));
            writer.write('\n');
            writer.flush();
            statsSent.mark();
            LOG.debug("Sending stats: {}{}.{} {}", prefix, sanitized, value, timestamp);
        } catch (IOException e) {
            LOG.error("Error sending to Graphite:", e);
        }
    }

    protected String sanitizeName(MetricName name) {
        final StringBuilder sb = new StringBuilder()
                .append(name.getGroup())
                .append('.')
                .append(name.getType())
                .append('.');
        if (name.hasScope()) {
            sb.append(name.getScope())
                    .append('.');
        }
        return sb.append(name.getName()).toString();
    }

    protected String sanitizeString(String s) {
        return s.replace(' ', '-');
    }

    @Override
    public void processGauge(MetricName name, Gauge<?> gauge, Long epoch) throws IOException {
        sendObjToGraphite(epoch, sanitizeName(name), "value", gauge.value());
    }

    @Override
    public void processCounter(MetricName name, Counter counter, Long epoch) throws IOException {
        sendInt(epoch, sanitizeName(name), "count", counter.count());
    }

    @Override
    public void processMeter(MetricName name, Metered meter, Long epoch) throws IOException {
        final String sanitizedName = sanitizeName(name);

        // NOTE: Only send 1MinuteRate to Graphite to reduce metrics traffic.
        /*
         * sendInt(epoch, sanitizedName, "count", meter.count());
         * sendFloat(epoch, sanitizedName, "meanRate", meter.meanRate());
         */
        sendFloat(epoch, sanitizedName, "1MinuteRate", meter.oneMinuteRate());
        /*
         * sendFloat(epoch, sanitizedName, "5MinuteRate",
         * meter.fiveMinuteRate()); sendFloat(epoch, sanitizedName,
         * "15MinuteRate", meter.fifteenMinuteRate());
         */
    }

    @Override
    public void processHistogram(MetricName name, Histogram histogram, Long epoch) throws IOException {
        final String sanitizedName = sanitizeName(name);
        sendSummarizable(epoch, sanitizedName, histogram);
        sendSampling(epoch, sanitizedName, histogram);
    }

    @Override
    public void processTimer(MetricName name, Timer timer, Long epoch) throws IOException {
        processMeter(name, timer, epoch);
        final String sanitizedName = sanitizeName(name);
        sendSummarizable(epoch, sanitizedName, timer);
        sendSampling(epoch, sanitizedName, timer);
    }

    protected void sendSummarizable(long epoch, String sanitizedName, Summarizable metric) throws IOException {
        sendFloat(epoch, sanitizedName, "min", metric.min());
        sendFloat(epoch, sanitizedName, "max", metric.max());
        sendFloat(epoch, sanitizedName, "mean", metric.mean());
        sendFloat(epoch, sanitizedName, "stddev", metric.stdDev());
    }

    protected void sendSampling(long epoch, String sanitizedName, Sampling metric) throws IOException {
        final Snapshot snapshot = metric.getSnapshot();
        sendFloat(epoch, sanitizedName, "median", snapshot.getMedian());
        sendFloat(epoch, sanitizedName, "75percentile", snapshot.get75thPercentile());
        sendFloat(epoch, sanitizedName, "95percentile", snapshot.get95thPercentile());
        sendFloat(epoch, sanitizedName, "98percentile", snapshot.get98thPercentile());
        sendFloat(epoch, sanitizedName, "99percentile", snapshot.get99thPercentile());
        sendFloat(epoch, sanitizedName, "999percentile", snapshot.get999thPercentile());
    }

    protected void printVmMetrics(long epoch) {
        sendFloat(epoch, "jvm.memory", "heap_usage", vm.heapUsage());
        sendFloat(epoch, "jvm.memory", "non_heap_usage", vm.nonHeapUsage());
        for (Entry<String, Double> pool : vm.memoryPoolUsage().entrySet()) {
            sendFloat(epoch, "jvm.memory.memory_pool_usages", sanitizeString(pool.getKey()), pool.getValue());
        }

        sendInt(epoch, "jvm", "daemon_thread_count", vm.daemonThreadCount());
        sendInt(epoch, "jvm", "thread_count", vm.threadCount());
        sendInt(epoch, "jvm", "uptime", vm.uptime());
        sendFloat(epoch, "jvm", "fd_usage", vm.fileDescriptorUsage());

        for (Entry<State, Double> entry : vm.threadStatePercentages().entrySet()) {
            sendFloat(epoch, "jvm.thread-states", entry.getKey().toString().toLowerCase(Locale.US), entry.getValue());
        }

        for (Entry<String, VirtualMachineMetrics.GarbageCollectorStats> entry : vm.garbageCollectors().entrySet()) {
            final String name = "jvm.gc." + sanitizeString(entry.getKey());
            sendInt(epoch, name, "time", entry.getValue().getTime(TimeUnit.MILLISECONDS));
            sendInt(epoch, name, "runs", entry.getValue().getRuns());
        }
    }

    public static class DefaultSocketProvider implements SocketProvider {

        private final String host;
        private final int port;

        public DefaultSocketProvider(String host, int port) {
            this.host = host;
            this.port = port;

        }

        @Override
        public Socket get() throws Exception {
            return new Socket(this.host, this.port);
        }

    }
}
