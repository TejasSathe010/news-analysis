from kafka import KafkaConsumer
import json

KAFKA_BROKER_URL = 'localhost:9092'
TOPIC_NAME = 'news'

print("Connecting to Kafka broker...")
consumer = KafkaConsumer(TOPIC_NAME,
                         bootstrap_servers=KAFKA_BROKER_URL,
                         auto_offset_reset='earliest',
                         group_id='test-group')

print("Subscribed to topic:", TOPIC_NAME)

for message in consumer:
    print("Received message:")
    print(json.loads(message.value.decode('utf-8')))
