import tweepy
import json


class MyStreamListener(tweepy.StreamListener):
    def __init__(self, api):
        self.api = api
        self.me = api.me()

    def on_status(self, tweet):
        tweet = json.dumps(tweet._json)
        print(tweet)

    def on_error(self, status):
        print("Error detected")

# Authenticate to Twitter
access_token_key = ""
access_token_secret = ""

consumer_key = ""
consumer_secret = ""

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token_key, access_token_secret)

# Create API object
api = tweepy.API(auth, wait_on_rate_limit=True,
    wait_on_rate_limit_notify=True)


spain_boxbound = [-9.39288367353, 35.946850084, 3.03948408368, 43.7483377142]

tweets_listener = MyStreamListener(api)
stream = tweepy.Stream(api.auth, tweets_listener)
stream.filter(locations=spain_boxbound, languages=["es"])
