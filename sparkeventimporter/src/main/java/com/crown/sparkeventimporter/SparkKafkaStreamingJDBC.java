package com.crown.sparkeventimporter;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class SparkKafkaStreamingJDBC {

    private static final Logger logger = LoggerFactory.getLogger(SparkKafkaStreamingJDBC.class);

    // TODO: create a configuration class to read, replace hardcode configuration
    private static JavaStreamingContext getJavaStreamingContext() {
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("Spark Importer");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        return new JavaStreamingContext(conf, Durations.seconds(3));
    }

    // TODO: create a configuration class to read, replace hardcode configuration
    private static JavaInputDStream<ConsumerRecord<String, String>> createJavaDStream(JavaStreamingContext ssc,
                                                                                      Collection<String> topics,
                                                                                      Map<String, Object> kafkaParams) {
        return KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        );
    }

    public static void main(String[] args) {

        logger.info("Start importer with arguments {}", Arrays.toString(args));

        // connect to cluster and create context
        final JavaStreamingContext ssc = getJavaStreamingContext();

        // Create map of Kafka params
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("bootstrap.servers", "10.0.2.15:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);

        //List of topics to listen to.
        Collection<String> topics
                = Arrays.asList("jdbc-source-jdbc_source");

        //Setup a processing map function that only returns the payload.
        JavaDStream<String> retval = createJavaDStream(ssc, topics, kafkaParams).map(
                new Function<ConsumerRecord<String, String>, String>() {

                    private static final long serialVersionUID = 1L;

                    public String call(ConsumerRecord<String, String> stringStringConsumerRecord) throws Exception {
                        return stringStringConsumerRecord.value();
                    }
                }
        );

        //print the payload.
        retval.print();

        //Start streaming.
        ssc.start();

        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //Keep the program alive.
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }
}
