from kafka import KafkaConsumer
import json
import pandas as pd
from transformers import pipeline
import os

# Kafka Consumer setup
consumer = KafkaConsumer(
    "tweets",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

# Load Hugging Face Sentiment Model
sentiment_pipeline = pipeline("sentiment-analysis")

CSV_FILE = "tweet_sentiment.csv"


for message in consumer:
    tweet = message.value
    sentiment = sentiment_pipeline(tweet["text"])[0]

    result = {
        "tweet_id": tweet["id"],
        "text": tweet["text"],
        "label": sentiment["label"],
        "score": sentiment["score"]
    }

    print(f"Processed: {result}")

    # Write to CSV immediately
    df = pd.DataFrame([result])
    df.to_csv(CSV_FILE, mode="a", header=not os.path.exists(CSV_FILE), index=False)
    print("Saved to CSV")
