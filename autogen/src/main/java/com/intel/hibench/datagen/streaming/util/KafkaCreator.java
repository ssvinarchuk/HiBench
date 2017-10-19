package com.intel.hibench.datagen.streaming.util;

import com.intel.hibench.common.streaming.metrics.MetricsUtil;

public class KafkaCreator {

    public KafkaCreator(String streamPath, String topicName) {
        this.streamPath = streamPath;
        this.topicName = topicName;
    }

    private String streamPath;
    private String topicName;


    public String getStreamPath() {
        return streamPath;
    }

    public String getTopicName() {
        return topicName;
    }

    public void createPathAndTopic(String path, String topicName, Integer partitions) {
        MetricsUtil.deleteStream(path);
        MetricsUtil.createStream(path);
        MetricsUtil.createTopic(path, topicName, partitions);
    }

}