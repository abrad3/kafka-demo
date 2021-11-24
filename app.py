# from https://elkhayati.me/kafka-python-twitter/
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
import json

access_token = os.getenv('ACCESS_TOKEN') 
access_token_secret = os.getenv('ACCESS_TOKEN_SECRET')
api_key =  os.getenv('API_KEY')
api_secret = os.getenv('API_SECRET')

class StdOutListener(StreamListener):
    def on_data(self, data):
        json_ = json.loads(data) 
        producer.send("basic", json_["text"].encode('utf-8'))
        return True
    def on_error(self, status):
        print (status)

producer = KafkaProducer(bootstrap_servers='localhost:9092')
l = StdOutListener()
auth = OAuthHandler(api_key, api_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
stream.filter(track=sys.argv[1:])
