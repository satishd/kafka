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

import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NonZeroMetricPredicate extends RegexMetricPredicate {
    private static final Logger LOG = LoggerFactory.getLogger(NonZeroMetricPredicate.class);
    private static final double MIN_NON_ZERO_VALUE = 0.001f;

    public NonZeroMetricPredicate(String regex) {
        super(regex);
    }

    /**
     * This method is used to determine whether a metric should be included for
     * reporting A metric will not be reported if both of the conditions are
     * true: 1) the metric name is matched with the config
     * kafka.graphite.metrics.exclude.when.zero.regex 2) the metric value is
     * numeric and less than MIN_NON_ZERO_VALUE
     * 
     * @param name
     *            name of the metric
     * @param metric
     *            metric object
     * @return Whether the metric should be included. If true, the metric shall
     *         be reported. If false, the metric shall be skipped.
     */
    @Override
    public boolean matches(MetricName name, Metric metric) {
        if (super.matches(name, metric)) {
            LOG.debug("Metric not excluded for name: {}", name.getMBeanName());
            return true;
        } else {
            return !isMetricZero(name, metric);
        }
    }

    private boolean isMetricZero(MetricName name, Metric metric) {
        if (metric instanceof Gauge) {
            return isZero(name, ((Gauge) metric).value());
        } else if (metric instanceof Meter) {
            return isZero(name, ((Meter) metric).oneMinuteRate());
        } else if (metric instanceof Counter) {
            return isZero(name, ((Counter) metric).count());
        } else if (metric instanceof Histogram) {
            return isZero(name, ((Histogram) metric).count());
        } else if (metric instanceof Timer) {
            return isZero(name, ((Timer) metric).count());
        } else {
            LOG.debug("Metric not excluded for type: {}", name.getMBeanName());
            return false;
        }
    }

    private boolean isZero(MetricName name, Object value) {
        if (value instanceof Number) {
            Number numberValue = (Number) value;
            if (numberValue.doubleValue() < MIN_NON_ZERO_VALUE) {
                LOG.debug("Metric excluded for value: {} {}", name.getMBeanName(), value);
                return true;
            }
        }
        LOG.debug("Metric not excluded for value: {} {}", name.getMBeanName(), value);
        return false;
    }
}
