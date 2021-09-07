import tweepy
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import json
import sys
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.futures import Future
from google.auth import jwt


class TweetPubsub(tweepy.StreamListener):
    def __init__(self):
        self.project_id = 'annular-garage-325314'
        self.topic_id = 'twitter'
        self.batch_settings = pubsub_v1.types.BatchSettings(max_messages = 100, max_bytes = 1024)
        self.publisher = pubsub_v1.PublisherClient(self.batch_settings)
        self.topic_path = self.publisher.topic_path(self.project_id, self.topic_id)
        self.publish_futures = []
        self.i = 0
        self.l = []
    def twitter_api_connect(self, twitter_credentials):
        auth = tweepy.OAuthHandler(twitter_credentials['API_KEY'], twitter_credentials['API_SECRET'])
        auth.set_access_token(twitter_credentials['ACCESS_TOKEN'], twitter_credentials['ACCESS_TOKEN_SECRET'])
        api = tweepy.API(auth)
        self.twitter_api = api

    def pubsub_connect(self, json_key_file):
        service_account_info = json.load(open(json_key_file))
        audience = "https://pubsub.googleapis.com/google.pubsub.v1.Publisher"
        credentials = jwt.Credentials.from_service_account_info(service_account_info, audience=audience)
        publisher = pubsub_v1.PublisherClient(credentials=credentials)
        self.publisher = publisher

    #These are the listener methods for tweepy class
    def on_data(self, raw_data):
        '''
        This method streams the data collected from the Twitter via the Tweepy API
        :param raw_data:
        :return:
        '''
        json_data = json.loads(raw_data)
        extracted_data = {"id": json_data['id'], "text":json_data['text']}
        print(extracted_data)
        self.publish_to_topic(extracted_data)

    def on_error(self, status_code):
        if status_code == 420:
            return False
    def get_callback(self, publish_future, data):
        def callback(publish_future):
            try:
                    # Wait 60 seconds for the publish call to succeed.
                print(publish_future.result(timeout=60))
            except futures.TimeoutError:
                print(f"Publishing {data} timed out.")

    def publish_to_topic(self, data):
        if self.i == 100:
            s_data = json.dumps(self.l)
            publish_future = self.publisher.publish(self.topic_path, data = s_data.encode('utf-8'))
            print(type(publish_future))
            publish_future.add_done_callback(self.get_callback(publish_future, data))
            self.publish_futures.append(publish_future)
            self.i = 0
            self.l = []
            return
        else:
            self.i += 1
            self.l.append(data)
            return



# Setting up the variables

with open('twitter_credentials.json') as file:
    twitter_credentials = json.load(file)
gcp_json_key = "cloud_credentials.json"

if __name__ == "__main__":
    stream_to_pubsub = TweetPubsub()
    stream_to_pubsub.twitter_api_connect(twitter_credentials=twitter_credentials)
    stream_to_pubsub.pubsub_connect(json_key_file=gcp_json_key)
    stream = tweepy.Stream(auth=stream_to_pubsub.twitter_api.auth, listener=stream_to_pubsub)
    stream.filter(track=["python", "java", "php"], languages=["en"])
    

    
