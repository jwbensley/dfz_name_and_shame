import logging
import tweepy

from dnas.config import config as cfg
from dnas.twitter_auth import twitter_auth
    
class twitter:
    """
    Class for interacting with Twitter API using Tweepy.
    """

    max_len = 280

    def __init__(self):

        self.client = tweepy.Client(
            consumer_key=twitter_auth.consumer_key,
            consumer_secret=twitter_auth.consumer_secret,
            access_token=twitter_auth.access_token,
            access_token_secret=twitter_auth.access_token_secret
        )


    def tweet(self, msg):
        if msg.hidden:
            return
        
        r = self.client.create_tweet(text=msg.hdr)
        print(f"Single Tweet: https://twitter.com/{cfg.twitter_user}/status/{r.data['id']}")

    def tweet_paged(self, msg):
        if msg.hidden:
            return
        
        r = self.client.create_tweet(text=msg.hdr)
        print(f"Single Tweet: https://twitter.com/{cfg.twitter_user}/status/{r.data['id']}")
        for chunk in self.split_tweet(msg.body):
            r = self.client.create_tweet(text=chunk, in_reply_to_tweet_id=r.data["id"])
            print(f"Paged Tweet: https://twitter.com/{cfg.twitter_user}/status/{r.data['id']}")

    def split_tweet(self, msg):
        """
        Return a tweet message split into a list of 280 character strings
        """
        if len(msg) <= self.max_len:
            return [msg]
        else:
            msgs = []
            while(len(msg) > self.max_len):
                end = self.max_len - 1
                while msg[end] <= " ":
                    end -= 1
                msgs.append(msg[0:end])
                msg = msg[end + 1:]
            msgs.append(msg)
            return msgs

class twitter_msg:
    hdr = ""
    body = ""
    hidden = True
