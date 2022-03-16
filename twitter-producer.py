import json
import os
import tweepy

from tweepy import Stream, StreamingClient
# from tweepy.streaming import Stream
from tweepy import OAuthHandler

# ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
# ACCESS_TOKEN_SECRET = os.getenv("ACCESS_TOKEN_SECRET")
# API_KEY = os.getenv("API_KEY")
# API_SECRET = os.getenv("API_SECRET")
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")
ACCESS_TOKEN_SECRET = os.environ.get("ACCESS_TOKEN_SECRET")
API_KEY = os.environ.get("API_KEY")
# API_SECRET = os.environ["API_SECRET"]
print(os.environ.get("API_KEY"))
print(os.environ["USER"])


class MYStreamListener(Stream):
    def on_data(self, raw_data):
        data = json.loads(raw_data)
        print(data)
        file = json.dumps(data)
        with open("data.json", "a") as f:
            f.write(f"{file}\n")

        return data

        # return True

    def on_request_error(self, status_code):
        print(status_code)
        return False


def stream_tweets(word):
    stream_listener = MYStreamListener(access_token=ACCESS_TOKEN, access_token_secret=ACCESS_TOKEN_SECRET,
                                       consumer_key=API_KEY, consumer_secret=API_SECRET)
    stream_listener.filter(track=word)

# stream_tweets(["covid19", "corona virus"])
