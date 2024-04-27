import torch
from transformers import BertTokenizer, BertForSequenceClassification
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
import json

# Load pre-trained BERT model and tokenizer
tokenizer = BertTokenizer.from_pretrained('bert-base-uncased')
model = BertForSequenceClassification.from_pretrained('bert-base-uncased', num_labels=2)

# Load the fine-tuned model for sentiment analysis
model.load_state_dict(torch.load('sentiment_model.pth', map_location=torch.device('cpu')))
model.eval()

KAFKA_BROKER_URL = 'kafka:9092'
TOPIC_NAME = 'raw_news'

# Function to perform sentiment analysis
def analyze_sentiment(text):
    # Tokenize the text
    inputs = tokenizer(text, return_tensors='pt', max_length=512, truncation=True)
    # Perform forward pass
    outputs = model(**inputs)
    # Get predicted probabilities
    probs = torch.softmax(outputs.logits, dim=1).detach().numpy()[0]
    # Return the probability of positive sentiment
    return probs[1]

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
        title = article['title']
        sentiment_score = analyze_sentiment(title)
        print("Received article from Kafka:", title)
        print("Sentiment Score:", sentiment_score)
        # Here you can perform further actions based on the sentiment score

if __name__ == "__main__":
    main()
