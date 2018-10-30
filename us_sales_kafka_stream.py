import os
import time
import json

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

if __name__ == "__main__":
	sc = SparkContext(appName="USSalesSparkStreamingKafka")
	sc.setLogLevel("WARN")
	sqlContext = SQLContext(sc)
	ssc = StreamingContext(sc, 3)

	kafkaStream = KafkaUtils.createStream(ssc, 'localhost:2181', "use_a_separate_group_id_for_each_stream", {'use-case-garment_sales':1}, {"auto.offset.reset": "smallest"})
	lines = kafkaStream.map(lambda x: json.loads(x[1]))
	time_dstream = lines.map(lambda tweet: tweet['payload'])
	time_dstream.pprint()
	df = sqlContext.read.format("jdbc").options(url="jdbc:mysql://10.0.2.15:3306/exec_reports",driver = "com.mysql.jdbc.Driver",dbtable = "exec_summary",user="cloudera",password="cloudera").load()
	print(df.head())
	ssc.start()  
	ssc.awaitTermination()
