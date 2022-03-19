import json
import os
from dotenv import load_dotenv
from tweepy import Stream
from kafka_producer import producer
# load env keys
load_dotenv()

ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")
ACCESS_TOKEN_SECRET = os.environ.get("ACCESS_TOKEN_SECRET")
API_KEY = os.environ.get("API_KEY")
API_SECRET = os.environ["API_SECRET"]
TOPIC = "tweets_loader"
TWEET_KEYWORDS = ["covid19", "corona virus"]


# creating stream listener
class MYStreamListener(Stream):
    def on_data(self, raw_data):
        data = json.loads(raw_data)

        # convert data to bytes and load into kafka producer
        tweets = bytearray(data.encode("utf-8"))
        producer.send(TOPIC, tweets)

        print(data)
        file = json.dumps(data)
        with open("data.json", "a") as f:
            f.write(f"{file}\n")

        return True

    def on_request_error(self, status_code):
        print(status_code)
        return False


# creating function to start streaming
def stream_tweets(word):
    stream_listener = MYStreamListener(access_token=ACCESS_TOKEN, access_token_secret=ACCESS_TOKEN_SECRET,
                                       consumer_key=API_KEY, consumer_secret=API_SECRET)
    stream_listener.filter(track=word)


# start streaming twitter data
stream_tweets(TWEET_KEYWORDS)
