from kafka import KafkaProducer
from datetime import datetime
import time
from json import dumps
import random

KAFKA_TOPIC_NAME_CONS = "test"
KAFKA_BOOTSTRAP_SERVERS_CONS = 'localhost:9092'

if __name__ == "__main__":
    print("Kafka Producer Application Started ... ")

    kafka_producer_obj = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS_CONS,
                             value_serializer=lambda x: dumps(x).encode('utf-8'))

    transaction_card_type_list = ["Visa", "MasterCard", "Maestro"]

    message = None
    for i in range(10):
        i = i + 1
        message = {}
        print("Sending message to Kafka topic: " + str(i))
        event_datetime = datetime.now()

        message["transaction_id"] = str(i)
        message["transaction_card_type"] = random.choice(transaction_card_type_list)
        message["transaction_amount"] = round(random.uniform(5.5,555.5), 2)
        message["transaction_datetime"] = event_datetime.strftime("%Y-%m-%d %H:%M:%S")

        print("Message to be sent: ", message)
        kafka_producer_obj.send(KAFKA_TOPIC_NAME_CONS, message)
        time.sleep(1)


'''
/usr/bin/python3.8 /home/hadoop/PycharmProjects/structuredstreamingkafka/json-kafka-producer.py
Kafka Producer Application Started ... 
Sending message to Kafka topic: 1
Message to be sent:  {'transaction_id': '1', 'transaction_card_type': 'MasterCard', 'transaction_amount': 93.82, 'transaction_datetime': '2023-08-16 21:06:05'}
Sending message to Kafka topic: 2
Message to be sent:  {'transaction_id': '2', 'transaction_card_type': 'MasterCard', 'transaction_amount': 439.89, 'transaction_datetime': '2023-08-16 21:06:06'}
Sending message to Kafka topic: 3
Message to be sent:  {'transaction_id': '3', 'transaction_card_type': 'Visa', 'transaction_amount': 35.34, 'transaction_datetime': '2023-08-16 21:06:07'}
Sending message to Kafka topic: 4
Message to be sent:  {'transaction_id': '4', 'transaction_card_type': 'MasterCard', 'transaction_amount': 87.42, 'transaction_datetime': '2023-08-16 21:06:08'}
Sending message to Kafka topic: 5
Message to be sent:  {'transaction_id': '5', 'transaction_card_type': 'Maestro', 'transaction_amount': 40.13, 'transaction_datetime': '2023-08-16 21:06:09'}
Sending message to Kafka topic: 6
Message to be sent:  {'transaction_id': '6', 'transaction_card_type': 'MasterCard', 'transaction_amount': 275.91, 'transaction_datetime': '2023-08-16 21:06:10'}
Sending message to Kafka topic: 7
Message to be sent:  {'transaction_id': '7', 'transaction_card_type': 'Maestro', 'transaction_amount': 188.77, 'transaction_datetime': '2023-08-16 21:06:11'}
Sending message to Kafka topic: 8
Message to be sent:  {'transaction_id': '8', 'transaction_card_type': 'Maestro', 'transaction_amount': 382.17, 'transaction_datetime': '2023-08-16 21:06:12'}
Sending message to Kafka topic: 9
Message to be sent:  {'transaction_id': '9', 'transaction_card_type': 'MasterCard', 'transaction_amount': 349.49, 'transaction_datetime': '2023-08-16 21:06:13'}
Sending message to Kafka topic: 10
Message to be sent:  {'transaction_id': '10', 'transaction_card_type': 'MasterCard', 'transaction_amount': 32.75, 'transaction_datetime': '2023-08-16 21:06:14'}

Process finished with exit code 0

    
'''