import os
import time
import json

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
	sc = SparkContext(appName="PythonSparkStreamingKafka")
	sc.setLogLevel("WARN")


	ssc = StreamingContext(sc, 5)

	kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', "use_a_separate_group_id_for_each_stream", {'jdbc-source-jdbc_source':1})

	lines = kafkaStream.map(lambda x: json.loads(x[1]))
	time_dstream = lines.map(lambda dbentry: dbentry['payload'])
	time_dstream.pprint()

	ssc.start()  
	ssc.awaitTermination()
