import requests
import time
from kafka import KafkaProducer
import json

NEWS_API_KEY = 'ad296d7431cd404185afcf709fb7a719'
NEWS_API_URL = 'https://newsapi.org/v2/top-headlines'
KAFKA_BROKER_URL = 'kafka:9092'
TOPIC_NAME = 'raw_news'

def fetch_news():
    params = {
        'country': 'us',
        'apiKey': NEWS_API_KEY
    }
    response = requests.get(NEWS_API_URL, params=params)
    if response.status_code == 200:
        return response.json()['articles']
    else:
        print("Failed to fetch news:", response.status_code)
        return []

def main():
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER_URL,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    while True:
        news_articles = fetch_news()
        for article in news_articles:
            producer.send(TOPIC_NAME, value=article)
            print("Sent article to Kafka:", article['title'])
        producer.flush()
        time.sleep(600) 

if __name__ == "__main__":
    main()
