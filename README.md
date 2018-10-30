# Spark with Kafka learning

This repository is to get general idea about how to use spark with kafka

## Running environment

You can find virtual envrionment at: [cloudera quickstart vm](https://www.cloudera.com/downloads/quickstart_vms/5-13.html)

## First example: reading data from jdbc using kafka connector

#### Command to run kafka-connect with a JDBC Source connector and a task:

    export CLASSPATH=/home/cloudera/spark-data-engg/includes/*

    [using default localhost server]
    /usr/lib/kafka/bin/connect-standalone.sh /usr/lib/kafka/config/connect-standalone.properties /home/cloudera/spark-data-engg/connect-jdbc-source.properties

#### Command to subscribe and listen

    /usr/lib/kafka/bin/kafka-console-consumer.sh 
    --zookeeper localhost:2181
    --topic jdbc-source-jdbc_source
    --from-beginning

#### Run the code

    bin/spark-submit jdbc_kafka_spark_stream.py

## Second example: US sales data, using kafka with spark streaming read mysql data

#### Command to run kafka-connect with a JDBC Source connector and a task:

    export CLASSPATH=/home/cloudera/spark-data-engg/includes/*

    [using default localhost server]
    /usr/lib/kafka/bin/connect-standalone.sh /usr/lib/kafka/config/connect-standalone.properties /home/cloudera/spark-data-engg/connect-us-source.properties

#### Command to subscribe and listen

    /usr/lib/kafka/bin/kafka-console-consumer.sh 
    --zookeeper localhost:2181
    --topic use-case-garment_sales
    --from-beginning

#### Run the code

    bin/spark-submit us_sales_kafka_stream.py