package com.crown.sparkeventimporter;

import com.crown.sparkeventimporter.lib.CustomAccuMap;
import com.crown.sparkeventimporter.lib.SparkStreaming;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.text.SimpleDateFormat;
import java.util.*;

public class ProjectWebClicksDataProcessor {
    private static final Logger logger = LoggerFactory.getLogger(ProjectWebClicksDataProcessor.class);

    public static void main(String[] args) {
        logger.info("Start importer with arguments {}", Arrays.toString(args));
        // connect to cluster and create context
        final JavaStreamingContext javaStreamingContext = SparkStreaming.getJavaStreamingContext();
        // Create a map of Kafka params
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        // List of Kafka brokers to listen to.
        kafkaParams.put("bootstrap.servers",
                "10.0.2.15:9092");
        kafkaParams.put("key.deserializer",
                StringDeserializer.class);
        kafkaParams.put("value.deserializer",
                StringDeserializer.class);
        kafkaParams.put("group.id",
                "use_a_separate_group_id_for_each_stream");
        // Do you want to start from the earliest record or the latest?
        kafkaParams.put("auto.offset.reset",
                "earliest");
        kafkaParams.put("enable.auto.commit",
                false);

        // List of topics to listen to.
        Collection<String> topics
                = Arrays.asList("use-case-webclicks");

        // Create a Spark DStream with the kafka topics.
        final JavaInputDStream<ConsumerRecord<String, String>> stream =
                SparkStreaming.createJavaDStream(javaStreamingContext, topics, kafkaParams);

        try {
            SparkContext sparkContext = JavaSparkContext.toSparkContext(javaStreamingContext.sparkContext());
            final CustomAccuMap clicksMap = new CustomAccuMap();
            sparkContext.register(clicksMap);

            // Setup a DB Connection to save summary
            Class.forName("com.mysql.jdbc.Driver").newInstance();
            final Connection execConn = DriverManager
                    .getConnection("jdbc:mysql://10.0.2.15:3306/"
                            + "exec_reports?user=cloudera&password=cloudera");

            JavaDStream<String> retval =
                    stream.map(new Function<ConsumerRecord<String, String>, String>() {
                        private static final long serialVersionUID = 1L;
                        @Override
                        public String call(ConsumerRecord<String, String> v1) throws Exception {
                            try {
                                String jsonString = v1.value();

                                ObjectMapper objectMapper = new ObjectMapper();
                                JsonNode docRoot = objectMapper.readTree(jsonString);
                                JsonNode payload = objectMapper.readTree(docRoot.get("payload").asText());

                                Long clickDateEpoch = payload.get("timestamp").asLong();
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:00:00");
                                String clickDateStr = sdf.format(new Date(clickDateEpoch));

                                String eventCategory = payload.get("eventCategory").asText();

                                System.out.println("Records extracted "
                                        + clickDateStr + "  " + eventCategory);

                                if (eventCategory.equals("firstPage")) {
                                    Map<String, Double> dataMap = new HashMap<String, Double>();
                                    dataMap.put(clickDateStr, 1.0);
                                    clicksMap.add(dataMap);
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                            return v1.value();
                        }
                    });

            //Output operation required to trigger all transformations.
            retval.print();

            //Executes at the Driver. Saves summarized data to the database.
            retval.foreachRDD(new VoidFunction<JavaRDD<String>>() {
                @Override
                public void call(JavaRDD<String> stringJavaRDD) throws Exception {
                    System.out.println("executing foreachRDD");
                    System.out.println("Mapped values " + clicksMap.value());

                    Iterator dIterator = clicksMap.value().keySet().iterator();
                    while (dIterator.hasNext()) {
                        String salesDate = (String) dIterator.next();
                        String updateSQL = "UPDATE exec_summary SET WEB_HITS = WEB_HITS + " + clicksMap.value().get(salesDate);

                        System.out.println(updateSQL);
                        execConn.createStatement().executeUpdate(updateSQL);
                    }
                    clicksMap.reset();
                }
            });

            javaStreamingContext.start();

            try {
                javaStreamingContext.awaitTermination();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            while (true) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
