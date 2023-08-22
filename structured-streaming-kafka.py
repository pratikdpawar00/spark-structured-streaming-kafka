from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

KAFKA_TOPIC_NAME_CONS = "test"
KAFKA_OUTPUT_TOPIC_NAME_CONS = "topic"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

if __name__ == "__main__":
    print("PySpark Structured Streaming with Kafka Demo Application Started ")

    packages = [
        f'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2',
        'org.apache.kafka:kafka-clients:3.2.1'
    ]
    spark = SparkSession.builder \
        .master("local") \
        .appName("kafka-example") \
        .config("spark.jars.packages", ",".join(packages)) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # streaming DataFrame that reads from testtopic
    transaction_detail_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("subscribe", KAFKA_TOPIC_NAME_CONS) \
        .option("startingOffsets", "latest") \
        .load()

    print("Printing Schema of transaction_detail_df: ")
    transaction_detail_df.printSchema()

    transaction_detail_df1 = transaction_detail_df.selectExpr("CAST(value AS STRING)", "timestamp")

    # schema for the transaction_detail data
    transaction_detail_schema = StructType() \
        .add("transaction_id", StringType()) \
        .add("transaction_card_type", StringType()) \
        .add("transaction_amount", StringType()) \
        .add("transaction_datetime", StringType())

    transaction_detail_df2 = transaction_detail_df1\
        .select(from_json(col("value"), transaction_detail_schema).alias("transaction_detail"), "timestamp")

    transaction_detail_df3 = transaction_detail_df2.select("transaction_detail.*", "timestamp")

    # aggregate - find total_transaction_amount by grouping transaction_card_type
    transaction_detail_df4 = transaction_detail_df3.groupBy("transaction_card_type")\
        .agg({'transaction_amount': 'sum'}).select("transaction_card_type", \
        col("sum(transaction_amount)").alias("total_transaction_amount"))

    print("Printing Schema of transaction_detail_df4: ")
    transaction_detail_df4.printSchema()

    transaction_detail_df5 = transaction_detail_df4.withColumn("key", lit(100))\
                                                    .withColumn("value", concat(lit("{'transaction_card_type': '"), \
                                                    col("transaction_card_type"), lit("', 'total_transaction_amount: '"), \
                                                    col("total_transaction_amount").cast("string"), lit("'}")))

    print("Printing Schema of transaction_detail_df5: ")
    transaction_detail_df5.printSchema()

    # Write final result into console for debugging purpose
    trans_detail_write_stream = transaction_detail_df5 \
        .writeStream \
        .trigger(processingTime='1 seconds') \
        .outputMode("update") \
        .option("truncate", "false")\
        .format("console") \
        .start()

    # Write key-value data from a DataFrame to a specific Kafka topic specified in an option
    trans_detail_write_stream_1 = transaction_detail_df5 \
        .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("topic", KAFKA_OUTPUT_TOPIC_NAME_CONS) \
        .trigger(processingTime='1 seconds') \
        .outputMode("update") \
        .option("checkpointLocation", "/home/hadoop/PycharmProjects/structuredstreamingkafka/py_checkpoint") \
        .start()

    trans_detail_write_stream.awaitTermination()

'''
========================================================================================================
								OUTPUT
========================================================================================================
/usr/bin/python3.8 /home/hadoop/PycharmProjects/structuredstreamingkafka/structured-streaming-kafka.py
PySpark Structured Streaming with Kafka Demo Application Started ...
:: loading settings :: url = jar:file:/home/hadoop/.local/lib/python3.8/site-packages/pyspark/jars/ivy-2.4.0.jar!/org/apache/ivy/core/settings/ivysettings.xml
Ivy Default Cache set to: /home/hadoop/.ivy2/cache
The jars for the packages stored in: /home/hadoop/.ivy2/jars
org.apache.spark#spark-sql-kafka-0-10_2.12 added as a dependency
org.apache.kafka#kafka-clients added as a dependency
:: resolving dependencies :: org.apache.spark#spark-submit-parent-d0cc20fb-0634-4937-8cf7-78563035e5d6;1.0
	confs: [default]
	found org.apache.spark#spark-sql-kafka-0-10_2.12;3.1.2 in central
	found org.apache.spark#spark-token-provider-kafka-0-10_2.12;3.1.2 in central
	found org.spark-project.spark#unused;1.0.0 in central
	found org.apache.commons#commons-pool2;2.6.2 in central
	found org.apache.kafka#kafka-clients;3.2.1 in central
	found com.github.luben#zstd-jni;1.5.2-1 in central
	found org.lz4#lz4-java;1.8.0 in central
	found org.xerial.snappy#snappy-java;1.1.8.4 in central
	found org.slf4j#slf4j-api;1.7.36 in central
:: resolution report :: resolve 1065ms :: artifacts dl 82ms
	:: modules in use:
	com.github.luben#zstd-jni;1.5.2-1 from central in [default]
	org.apache.commons#commons-pool2;2.6.2 from central in [default]
	org.apache.kafka#kafka-clients;3.2.1 from central in [default]
	org.apache.spark#spark-sql-kafka-0-10_2.12;3.1.2 from central in [default]
	org.apache.spark#spark-token-provider-kafka-0-10_2.12;3.1.2 from central in [default]
	org.lz4#lz4-java;1.8.0 from central in [default]
	org.slf4j#slf4j-api;1.7.36 from central in [default]
	org.spark-project.spark#unused;1.0.0 from central in [default]
	org.xerial.snappy#snappy-java;1.1.8.4 from central in [default]
	:: evicted modules:
	org.apache.kafka#kafka-clients;2.6.0 by [org.apache.kafka#kafka-clients;3.2.1] in [default]
	---------------------------------------------------------------------
	|                  |            modules            ||   artifacts   |
	|       conf       | number| search|dwnlded|evicted|| number|dwnlded|
	---------------------------------------------------------------------
	|      default     |   10  |   0   |   0   |   1   ||   9   |   0   |
	---------------------------------------------------------------------
:: retrieving :: org.apache.spark#spark-submit-parent-d0cc20fb-0634-4937-8cf7-78563035e5d6
	confs: [default]
	0 artifacts copied, 9 already retrieved (0kB/45ms)
23/08/16 21:05:03 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
Printing Schema of transaction_detail_df: 
root
 |-- key: binary (nullable = true)
 |-- value: binary (nullable = true)
 |-- topic: string (nullable = true)
 |-- partition: integer (nullable = true)
 |-- offset: long (nullable = true)
 |-- timestamp: timestamp (nullable = true)
 |-- timestampType: integer (nullable = true)

Printing Schema of transaction_detail_df4: 
root
 |-- transaction_card_type: string (nullable = true)
 |-- total_transaction_amount: double (nullable = true)

Printing Schema of transaction_detail_df5: 
root
 |-- transaction_card_type: string (nullable = true)
 |-- total_transaction_amount: double (nullable = true)
 |-- key: integer (nullable = false)
 |-- value: string (nullable = true)

-------------------------------------------
Batch: 0
-------------------------------------------
+---------------------+------------------------+---+-----+
|transaction_card_type|total_transaction_amount|key|value|
+---------------------+------------------------+---+-----+
+---------------------+------------------------+---+-----+

-------------------------------------------
Batch: 1
-------------------------------------------
+---------------------+------------------------+---+---------------------------------------------------------------------------+
|transaction_card_type|total_transaction_amount|key|value                                                                      |
+---------------------+------------------------+---+---------------------------------------------------------------------------+
|MasterCard           |93.82                   |100|{'transaction_card_type': 'MasterCard', 'total_transaction_amount: '93.82'}|
+---------------------+------------------------+---+---------------------------------------------------------------------------+

-------------------------------------------
Batch: 2
-------------------------------------------
+---------------------+------------------------+---+-----------------------------------------------------------------------------+
|transaction_card_type|total_transaction_amount|key|value                                                                        |
+---------------------+------------------------+---+-----------------------------------------------------------------------------+
|Maestro              |611.07                  |100|{'transaction_card_type': 'Maestro', 'total_transaction_amount: '611.07'}    |
|Visa                 |35.34                   |100|{'transaction_card_type': 'Visa', 'total_transaction_amount: '35.34'}        |
|MasterCard           |1279.28                 |100|{'transaction_card_type': 'MasterCard', 'total_transaction_amount: '1279.28'}|
+---------------------+------------------------+---+-----------------------------------------------------------------------------+


'''
