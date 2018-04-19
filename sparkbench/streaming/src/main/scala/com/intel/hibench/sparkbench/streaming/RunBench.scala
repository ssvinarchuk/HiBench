/*
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

package com.intel.hibench.sparkbench.streaming

import java.nio.ByteBuffer
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}

import com.intel.hibench.common.HiBenchConfig
import com.intel.hibench.common.streaming.metrics.MetricsUtil
import com.intel.hibench.common.streaming.{ConfigLoader, Platform, StreamBenchConfig, TestCase}
import com.intel.hibench.sparkbench.streaming.application._
import com.intel.hibench.sparkbench.streaming.util.SparkBenchConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka09.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

/**
  * The entry point of Spark Streaming benchmark
  */
object RunBench {

  def main(args: Array[String]) {
    val conf = new ConfigLoader(args(0))

    // Load configuration
    val master = conf.getProperty(HiBenchConfig.SPARK_MASTER)

    val batchInterval = conf.getProperty(StreamBenchConfig.SPARK_BATCH_INTERVAL).toInt
    val receiverNumber = conf.getProperty(StreamBenchConfig.SPARK_RECEIVER_NUMBER).toInt
    val copies = conf.getProperty(StreamBenchConfig.SPARK_STORAGE_LEVEL).toInt
    val enableWAL = conf.getProperty(StreamBenchConfig.SPARK_ENABLE_WAL).toBoolean
    val checkPointPath = conf.getProperty(StreamBenchConfig.SPARK_CHECKPOINT_PATH)
    val directMode = conf.getProperty(StreamBenchConfig.SPARK_USE_DIRECT_MODE).toBoolean
    val benchName = conf.getProperty(StreamBenchConfig.TESTCASE)
    val topic = conf.getProperty(StreamBenchConfig.KAFKA_TOPIC)
    val streamPath = conf.getProperty(StreamBenchConfig.STRAMS_PATH)
    val streamTopic = streamPath + ":" + topic
    val zkHost = conf.getProperty(StreamBenchConfig.ZK_HOST)
    val consumerGroup = conf.getProperty(StreamBenchConfig.CONSUMER_GROUP)
    val brokerList = conf.getProperty(StreamBenchConfig.KAFKA_BROKER_LIST)
    val debugMode = conf.getProperty(StreamBenchConfig.DEBUG_MODE).toBoolean
    val recordPerInterval = conf.getProperty(StreamBenchConfig.DATAGEN_RECORDS_PRE_INTERVAL).toLong
    val intervalSpan: Int = conf.getProperty(StreamBenchConfig.DATAGEN_INTERVAL_SPAN).toInt

    val windowDuration: Long = conf.getProperty(StreamBenchConfig.FixWINDOW_DURATION).toLong
    val windowSlideStep: Long = conf.getProperty(StreamBenchConfig.FixWINDOW_SLIDESTEP).toLong

    val coreNumber = conf.getProperty(HiBenchConfig.YARN_EXECUTOR_NUMBER).toInt * conf.getProperty(HiBenchConfig.YARN_EXECUTOR_CORES).toInt

    val producerNum = conf.getProperty(StreamBenchConfig.DATAGEN_PRODUCER_NUMBER).toInt
    val reporterTopic = MetricsUtil.getTopic(Platform.SPARK, streamTopic, producerNum, recordPerInterval, intervalSpan)
    println("Source Topic: " + streamTopic)
    println("Reporter Topic: " + reporterTopic)
    val topicPartitions = conf.getProperty(StreamBenchConfig.KAFKA_TOPIC_PARTITIONS).toInt

    // Test execution time in milliseconds
    val execTime: Long = conf.getProperty(StreamBenchConfig.EXECUTION_TIME_MS).toLong

    //TODO add property which handle this
    // Remove stream and all topics before start data processing
     MetricsUtil.deleteStream(streamPath)

    //TODO add property which handle this
    // Create stream and topic where original data should be
    MetricsUtil.createStream(streamPath)
    MetricsUtil.createTopic(streamPath, topic, topicPartitions)

    // Create topic where we generate data with processing timestamps with format
    // (time stamps when data was generated in original topic, time stamps when data was processed in spark)
    MetricsUtil.createTopic(streamPath, reporterTopic.substring(reporterTopic.indexOf(":") + 1), topicPartitions)

    val probability = conf.getProperty(StreamBenchConfig.SAMPLE_PROBABILITY).toDouble
    // init SparkBenchConfig, it will be passed into every test case
    val config = SparkBenchConfig(master, benchName, batchInterval, receiverNumber, copies,
      enableWAL, checkPointPath, directMode, zkHost, consumerGroup, streamTopic, reporterTopic,
      brokerList, debugMode, coreNumber, probability, windowDuration, windowSlideStep, execTime)

    run(config)
  }

  private def run(config: SparkBenchConfig) {

    val deserializer = new StringDeserializer()
    // select test case based on given benchName
    val testCase: BenchBase = TestCase.withValue(config.benchName) match {
      case TestCase.IDENTITY => new Identity()
      case TestCase.REPARTITION => new Repartition()
      case TestCase.WORDCOUNT => new WordCount()
      case TestCase.FIXWINDOW => new FixWindow(config.windowDuration, config.windowSlideStep)
      case other =>
        throw new Exception(s"test case ${other} is not supported")
    }

    // defind streaming context
    val conf = new SparkConf().setMaster(config.master).setAppName(config.benchName)
      .set("spark.streaming.kafka.consumer.poll.ms", "5000")
    val ssc = new StreamingContext(conf, Milliseconds(config.batchInterval))
    ssc.checkpoint(config.checkpointPath)

    if (!config.debugMode) {
      ssc.sparkContext.setLogLevel("ERROR")
    }

    println("Source topic in config: " + config.sourceTopic)

    val consumerStrategy =
      ConsumerStrategies.Subscribe[String, String](Set(config.sourceTopic), config.kafkaParams)

    val lines = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      consumerStrategy)

    // convent key from String to Long, it stands for event creation time.
    val parsedLines = lines.map { cr => (cr.key().toLong, cr.value()) }

    testCase.process(parsedLines, config)

    ssc.start()
 
    println("EXECUTION TIME: " + config.executionTime)

    ssc.awaitTerminationOrTimeout(config.executionTime)
    ssc.stop()
  }

}
