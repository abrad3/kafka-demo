# from https://elkhayati.me/kafka-python-twitter/
import os
from tweepy.streaming import Stream
from tweepy import Stream
from kafka import KafkaProducer

access_token = os.getenv('ACCESS_TOKEN') 
access_token_secret = os.getenv('ACCESS_TOKEN_SECRET')
api_key =  os.getenv('API_KEY')
api_secret = os.getenv('API_SECRET')

class StdOutListener(Stream):
    def on_status(self, status):
      if hasattr(status, "retweeted_status"):  # Check if Retweet
        try:
            data=status.retweeted_status.extended_tweet["full_text"]
            print(data)
        except AttributeError:
            data=status.retweeted_status.text
            print(data)
      else:
        try:
            data=status.extended_tweet["full_text"]
            print(data)
        except AttributeError:
            data=status.text
            print(data)
      producer.send("test-topic", data.encode('utf-8'))
      return True

producer = KafkaProducer(bootstrap_servers='192.168.122.8:30412')
l = StdOutListener(api_key, api_secret, access_token, access_token_secret)
l.filter(track=["#cat", "puppy"], filter_level="low")
