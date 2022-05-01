import json
import logging

import bson
import tweepy

from kafka_io.kafka_pub_sub import create_topics, publish
from utils.utils import load_config_yaml

producer_conf = load_config_yaml()


class TweetStreamListener(tweepy.StreamListener):

    def __init__(self, topic_name):
        self.topic_name = topic_name
        self.count = 0
        try:
            create_topics(list(topic_name))
        except Exception as err:
            logging.exception(err)

    def on_data(self, raw_data):
        self.process_data(raw_data)

    def process_data(self, raw_data):
        tweet = json.loads(raw_data)
        tweet_text = ""
        print(tweet.keys())
        if all(x in tweet.keys() for x in ['lang', 'created_at']) and tweet['lang'] == 'en':
            if 'retweeted_status' in tweet.keys():
                if 'quoted_status' in tweet['retweeted_status'].keys():
                    if 'extended_tweet' in tweet['retweeted_status']['quoted_status'].keys():
                        tweet_text = tweet['retweeted_status']['quoted_status']['extended_tweet']['full_text']
                elif 'extended_tweet' in tweet['retweeted_status'].keys():
                    tweet_text = tweet['retweeted_status']['extended_tweet']['full_text']
            elif tweet['truncated'] == 'true':
                tweet_text = tweet['extended_tweet']['full_text']

            else:
                tweet_text = tweet['text']
        if tweet_text:
            data = {
                'created_at': tweet['created_at'],
                'message': tweet_text.replace(',', '')
            }
            publish(self.topic_name, value=json.dumps(data, default=bson.json_util.default).encode('utf-8'))

    def on_error(self, status_code):
        if status_code == 420:
            # returning False in on_error disconnects the stream
            return False


class StreamTweets():

    def __init__(self, auth, listener):
        self.stream = tweepy.Stream(auth, listener)

    def start(self, language, track_keywords):
        self.stream.filter(languages=language,
                           track=track_keywords)


if __name__ == "__main__":
    # bootstrap_servers=[‘localhost:9092’] : sets the host and port the producer
    # should contact to bootstrap initial cluster metadata. It is not necessary to set this here,
    # since the default is localhost:9092.
    #
    # value_serializer=lambda x: dumps(x).encode(‘utf-8’): function of how the data
    # should be serialized before sending to the broker. Here, we convert the data to
    # a json file and encode it to utf-8.

    listener = TweetStreamListener(producer_conf['kafka']['topic'])

    consumer_key = producer_conf['twitter']['consumer_key']
    consumer_secret = producer_conf['twitter']['consumer_secret']
    access_token = producer_conf['twitter']['access_token']
    access_secret = producer_conf['twitter']['access_secret']

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)

    stream = StreamTweets(auth, listener)

    # Converting string to float to get cordinates
    language = producer_conf['tweepy']['language'].split(' ')
    hashtags = producer_conf['tweepy']['hashtags']
    stream.start(language, hashtags)
    # stream.start('en', ["insurtech","insurance","tesla"])
