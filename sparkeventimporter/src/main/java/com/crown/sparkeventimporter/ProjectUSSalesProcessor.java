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


public class ProjectUSSalesProcessor {

    private static final Logger logger = LoggerFactory.getLogger(ProjectUSSalesProcessor.class);

    public static void main(String[] args) {

        logger.info("Start importer with arguments {}", Arrays.toString(args));
        // connect to cluster and create context
        final JavaStreamingContext ssc = SparkStreaming.getJavaStreamingContext();
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
                = Arrays.asList("use-case-garment_sales");

        // Create a Spark DStream with the kafka topics.
        final JavaInputDStream<ConsumerRecord<String, String>> stream = SparkStreaming.createJavaDStream(ssc, topics, kafkaParams);
        try {
            // Setup accumulator
            final CustomAccuMap salesMap = new CustomAccuMap();
            SparkContext sc = JavaSparkContext
                    .toSparkContext(ssc.sparkContext());
            sc.register(salesMap);

            // Setup a DB Connection to save summary

            Class.forName("com.mysql.jdbc.Driver").newInstance();
            final Connection execConn = DriverManager
                    .getConnection("jdbc:mysql://10.0.2.15:3306/"
                            + "exec_reports?user=cloudera&password=cloudera");
            JavaDStream<String> retval = stream.map(new Function<ConsumerRecord<String, String>, String>() {
                private static final long serialVersionUID = 1L;
                public String call(ConsumerRecord<String, String> stringStringConsumerRecord) throws Exception {
                    try {

                        //Convert the payload to a Json node and then extract relevant data.
                        String jsonString = stringStringConsumerRecord.value();
                        ObjectMapper objectMapper = new ObjectMapper();
                        JsonNode docRoot = objectMapper.readTree(jsonString);

                        Long orderDateEpoch = docRoot.get("payload")
                                .get("SALES_DATE").asLong();
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:00:00");
                        String orderDateStr = sdf.format(new Date(orderDateEpoch));

                        Double orderValue = docRoot.get("payload")
                                .get("ORDER_VALUE").asDouble();
                        System.out.println("Records extracted "
                                + orderDateStr + "  " + orderValue);

                        //Add the data extracted to a map
                        Map<String, Double> dataMap = new HashMap<String, Double>();
                        dataMap.put(orderDateStr, orderValue);
                        //Add the map to the accumulator
                        salesMap.add(dataMap);

                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    return stringStringConsumerRecord.value();
                }
            });

            //Output operation required to trigger all transformations.
            retval.print();

            //Executes at the Driver. Saves summarized data to the database.
            retval.foreachRDD(new VoidFunction<JavaRDD<String>>() {

                @Override
                public void call(JavaRDD<String> arg0) throws Exception {
                    System.out.println("executing foreachRDD");
                    System.out.println("Mapped values " + salesMap.value());

                    Iterator dIterator = salesMap.value().keySet().iterator();
                    while (dIterator.hasNext()) {
                        String salesDate = (String) dIterator.next();
                        String updateSql = "UPDATE exec_reports.exec_summary SET SALES = SALES + "
                                + salesMap.value().get(salesDate);
                        System.out.println(updateSql);
                        execConn.createStatement().executeUpdate(updateSql);
                    }

                    salesMap.reset();
                }

            });

            // Start streaming.
            ssc.start();

            try {
                ssc.awaitTermination();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            // streamingContext.close();

            // Keep the program alive.
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
