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
package com.intel.hibench.common.streaming.metrics

import java.lang.Exception

import com.intel.hibench.common.streaming.Platform

import scala.sys.process._

object MetricsUtil {

  val TOPIC_CONF_FILE_NAME = "metrics_topic.conf"

  def getTopic(platform: Platform, sourceTopic: String, producerNum: Int,
               recordPerInterval: Long, intervalSpan: Int): String = {
    val topic = s"${sourceTopic}_${producerNum}_${recordPerInterval}" +
      s"_${intervalSpan}_${System.currentTimeMillis()}"
    println(s"metrics is being written to kafka topic $topic")
    topic
  }

  def createTopic(streamPath: String, topicName: String, partitions: Int): Unit = {
    val createCommand = s"maprcli stream topic create -path $streamPath -topic $topicName -partitions $partitions"
    println(createCommand)
    val result = createCommand !!;
    println(s"Creating topic $topicName. Result : $result")
  }

  def deleteStream(streamPath: String) : Unit = {
    val deleteStream = s"maprcli stream delete -path $streamPath"
    println(s"Try to remove topic $streamPath")
    try {
      deleteStream !!
    } catch {
      case _: Exception => println(s"Path $streamPath wasn't exist")
    }
  }

  def deleteTopic(streamPath: String, topicName: String) : Unit = {
    val deleteStream = s"maprcli stream topic delete -path $streamPath -topic $topicName"
    println(s"Try to remove topic $streamPath:$topicName")
    try {
      deleteStream !!
    } catch {
      case _: Exception => println(s"Topic $topicName wasn't exist")
    }
  }

  def createStream(streamPath: String): Unit = {
    s"/opt/mapr/bin/maprcli stream create -path $streamPath" !
  }
}
