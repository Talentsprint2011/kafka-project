# Kafka Producer script
import tweepy
from kafka import KafkaProducer
import json

# Twitter API credentials (Replace with your own keys)
BEARER_TOKEN = "YOUR BEARER TOKEN"

# Set up Twitter API client
client = tweepy.Client(bearer_token=BEARER_TOKEN)

# Kafka Producer setup
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    batch_size=16384,  # Increase batch size to send more messages at once
    linger_ms=5000     # Wait for 1 second to accumulate more messages before sending
)

TOPIC_NAME = "tweets"

def fetch_and_send_tweets():
    #query = "Kafka -is:retweet lang:en"
    query = "#AI OR #Tech -is:retweet"
    response = client.search_recent_tweets(query=query, max_results=10)

    if response.data:
        for tweet in response.data:
            tweet_data = {"id": tweet.id, "text": tweet.text}
            producer.send(TOPIC_NAME, tweet_data)
            print(f"Sent: {tweet_data}")

if __name__ == "__main__":
    fetch_and_send_tweets()
