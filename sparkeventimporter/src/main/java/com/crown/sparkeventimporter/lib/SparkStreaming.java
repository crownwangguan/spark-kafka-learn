package com.crown.sparkeventimporter.lib;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.Collection;
import java.util.Map;

public class SparkStreaming {

    public SparkStreaming() {
    }

    // TODO: create a configuration class to read, replace hardcode configuration
    public static JavaStreamingContext getJavaStreamingContext() {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("Spark Importer");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        return new JavaStreamingContext(conf, Durations.seconds(3));
    }

    // TODO: create a configuration class to read, replace hardcode configuration
    public static JavaInputDStream<ConsumerRecord<String, String>> createJavaDStream(JavaStreamingContext ssc,
                                                                                      Collection<String> topics,
                                                                                      Map<String, Object> kafkaParams) {
        return KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );
    }
}
