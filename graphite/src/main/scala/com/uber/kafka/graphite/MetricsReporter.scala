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

package com.uber.kafka.graphite

import com.yammer.metrics.core.{Clock, Gauge, Metric, MetricName, MetricPredicate}

import java.util.concurrent.TimeUnit
import kafka.metrics.{KafkaMetricsConfig, KafkaMetricsReporter, KafkaMetricsReporterMBean}
import kafka.utils.VerifiableProperties
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.slf4j.LoggerFactory
import scala.concurrent.Promise


private object MetricsReporter extends KafkaMetricsReporterMBean {
  
  class Reporter(graphiteHost: String,
                 graphitePort: Int,
                 groupPrefix: String,
                 metricSeparator: Option[Char],
                 metricPredicate: MetricPredicate,
                 nonZeroMetricPredicate: MetricPredicate,
                 pollingPeriodSecs: Long) extends
      GraphiteReporter(KafkaYammerMetrics.defaultRegistry,
                       groupPrefix,
                       metricPredicate,
                       nonZeroMetricPredicate,
                       new GraphiteReporter.DefaultSocketProvider(graphiteHost, graphitePort),
                       Clock.defaultClock) {

    // automatically start polling
    start(pollingPeriodSecs, TimeUnit.SECONDS)

    override def run = {
      try {
        super.run()
      } catch {
        case e: Exception =>
          logger.warn("Error reporting metrics", e)
      }
    }

    override def processGauge(name: MetricName, gauge: Gauge[_], epoch: java.lang.Long) = {
      (name.getGroup, name.getType, name.getName) match {
        case ("kafka.log", "Log", "LogStartTimestamp") =>
          gauge.value match {
            case value: Long =>
              // capture earliest log timestamp and compute delta in seconds
              sendToGraphite(epoch, sanitizeName(name), "delta " + (epoch - value / 1000))
            case _ => logger.warn("Gauge is of wrong type: " + gauge)
          }
        case _ =>
      }

      super.processGauge(name, gauge, epoch)
    }

    override def sanitizeName(name: MetricName): String = {
      // The following rewrites the metric name so that all the additional tags are not lost.
      // NOTE: This is essentially resurrecting the format of kafka 0.8.1
      name.getGroup + '.' + name.getType + '.' + name.getMBeanName.split(',').tail.flatMap(kv => {
        kv.split('=') match {
          case Array(_, v) => metricSeparator.map(c => v.replace('.', c)).orElse(Some(v))
          case _ => {
            logger.warn("Unrecognized key-value format: " + name)
            None
          }
        }
      }).mkString(".")
    }
  }

  def apply(props: VerifiableProperties) = {
    initReporter.success(pollingPeriodSecs => try {
      val graphiteHost = props.getString("kafka.graphite.metrics.host", "localhost")
      val graphitePort = props.getInt("kafka.graphite.metrics.port", 2002)
      val groupPrefix = props.getString("kafka.graphite.metrics.group", "kafka")
      val metricsSeparator = {
        val separator = props.getString("kafka.graphite.metrics.separator", "")
        if (separator != "") {
          Some(separator(0))
        } else None
      }

      val metricPredicate = props.getString("kafka.graphite.metrics.exclude.regex", "") match {
        case "" => MetricPredicate.ALL
        case regex => new RegexMetricPredicate(regex)
      }

      val nonZeroMetricPredicate = props.getString("kafka.graphite.metrics.exclude.when.zero.regex", "") match {
        case "" => MetricPredicate.ALL
        case regex => new NonZeroMetricPredicate(regex)
      }

      Some(new Reporter(graphiteHost, graphitePort, groupPrefix, metricsSeparator, metricPredicate,
        nonZeroMetricPredicate, pollingPeriodSecs))
    } catch {
      case e: Throwable => {
        logger.error("Cannot initialize Kafka Graphite metrics reporter: " + e)
        None
      }
    })

    startReporter(new KafkaMetricsConfig(props).pollingIntervalSecs)
  }

  private val logger = LoggerFactory.getLogger(getClass)

  // helper with pre-parsed parameters, i.e. host, port, etc.
  private val initReporter = Promise[(Long) => Option[Reporter]]()

  private var reporter: Option[Reporter] = None

  override val getMBeanName = "kafka:type=com.uber.kafka.graphite.MetricsReporter"

  override def startReporter(pollingPeriodSecs: Long) = {
    stopReporter() // just to be safe
    this.synchronized {
      reporter = initReporter.future.value.flatMap(_.get(pollingPeriodSecs))
      logger.info("Started Kafka Graphite metrics reporter polling at " + pollingPeriodSecs)
    }
  }

  override def stopReporter() = {
    this.synchronized {
      reporter.map(r => {
        r.shutdown()
        logger.info("Stopped Kafka Graphite metrics reporter")
      })
      reporter = None
    }
  }
}

class MetricsReporter extends KafkaMetricsReporter {
  def init(props: VerifiableProperties) = {
    MetricsReporter(props)
  }
}

class RegexMetricPredicate (regex : String) extends MetricPredicate {
  val pattern = regex.r
	
  override def matches(name : MetricName, metric : Metric) : Boolean = {
    name.getName() match {
      case pattern(_) => false
      case _ => true
    }
  }
}

