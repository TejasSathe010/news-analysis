import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json

KAFKA_BROKER_URL = 'kafka:9092'
TOPIC_NAME = 'raw_news'

def main():
    while True:
        try:
            consumer = KafkaConsumer(TOPIC_NAME, bootstrap_servers=KAFKA_BROKER_URL,
                                     value_deserializer=lambda x: json.loads(x.decode('utf-8')))
            break
        except NoBrokersAvailable:
            print("Kafka is not available yet. Retrying in 5 seconds...")
            time.sleep(5)
    
    print("Kafka is now available. Starting data ingestion.")

    for message in consumer:
        article = message.value
        print("Received article from Kafka:", article['title'])

if __name__ == "__main__":
    main()
