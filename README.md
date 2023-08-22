# spark-structured-streaming-kafka

# Project decription

In this project data messages produced in Json format ( key-value ) & send in to kafka topic test, 
spark application reading streaming DataFrame that reads from test (kafka topic)
and perform aggregate find total transaction amount by grouping transaction card type,
write final result into console & key-value data from a DataFrame to a specific Kafka specified topic.


# Project libraries & Version of applications used in project

SCALA-VERSION = 2.12
SPARK_VERSION = 3.1.2
KAFKA_VERSION = 3.2.1
PYTHON_VERSION = 3.9

Install kafka-pyhton package in python, execute below terminal or cmd

#pip3 install kafka-python==2.0.2

Execution steps :-

1) start zookeeper server & kafka server
   set $KAFKA_HOME variable path in bashrc
   
   $KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties

   $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties

3) create topics named  test & topic
 
   $KAFKA_HOME/kafka-topics.sh --bootstrap-server localhost:9092 --topic test --create

   $KAFKA_HOME/kafka-topics.sh --bootstrap-server localhost:9092 --topic topic --create

2) run structured-streaming-kafka.py ==> see output in topic ( topic created in kafka server)

3) run json-kafka-producer.py  ==> see output in test ( topic created in kafka server) 

4) now observe output in test & topic 


