import logging
import tweepy

from dnas.config import config as cfg
from dnas.twitter_auth import twitter_auth
    
class twitter:
    """
    Class for interacting with Twitter API using Tweepy.
    """

    def __init__(self):

        self.client = tweepy.Client(
            consumer_key=twitter_auth.consumer_key,
            consumer_secret=twitter_auth.consumer_secret,
            access_token=twitter_auth.access_token,
            access_token_secret=twitter_auth.access_token_secret
        )


    def tweet(self, msg):
        """
        Tweet the header of a twitter message obj.
        Then tweet the body as a series of paged replies.
        """
        if not msg:
            raise ValueError(
                f"Missing required arguments: msg={msg}"
            )

        if type(msg) != twitter_msg:
            raise TypeError(
                f"msg is not a twitter_msg: {type(msg)}"
            )

        self.tweet_hdr(msg)
        self.tweet_body(msg)

    def tweet_hdr(self, msg):
        """
        Tweet a message header.
        """
        if not msg:
            raise ValueError(
                f"Missing required arguments: msg={msg}"
            )

        if type(msg) != twitter_msg:
            raise TypeError(
                f"msg is not a twitter_msg: {type(msg)}"
            )

        if msg.hidden:
            logging.debug(f"Skipping hidden Tweet: {msg.hdr}")
            return

        if len(msg.hdr) > cfg.TWITTER_LEN:
            logging.debug(
                f"Skipping Tweet which is too long ({len(msg.hdr)}): {msg.hdr}"
            )
            return

        r = self.client.create_tweet(text=msg.hdr)
        logging.info(
            f"Tweeted: twitter.com/{cfg.TWITTER_USER}/status/{r.data['id']}"
        )
        msg.hdr_id = int(r.data['id'])

    def tweet_body(self, msg):
        """
        Tweet a message body as a series of pages replies to the header.
        """
        if not msg:
            raise ValueError(
                f"Missing required arguments: msg={msg}"
            )

        if type(msg) != twitter_msg:
            raise TypeError(
                f"msg is not a twitter_msg: {type(msg)}"
            )

        if not msg.hdr_id:
            raise ValueError(
                f"Missing required arguments: hdr_id={hdr_id}"
            )

        if type(msg.hdr_id) != int:
            raise TypeError(
                f"hdr_id is not an int: {type(msg.hdr_id)}"
            )

        if msg.hidden:
            logging.debug(f"Skipping hidden tweet: {msg.body}")
            return

        for chunk in self.split_tweet(msg):
            r = self.client.create_tweet(
                text=chunk,
                in_reply_to_tweet_id=msg.hdr_id
            )
            logging.info(
                f"Replied: twitter.com/{cfg.TWITTER_USER}/status/{r.data['id']}"
            )
            msg.body_ids.append(r.data['id'])

    def split_tweet(self, msg):
        """
        Return a Tweet body split into a list of 280 character strings
        """
        if len(msg.body) <= cfg.TWITTER_LEN:
            return [msg.body]
        else:
            chunks = []
            tmp_str = msg.body

            while(len(tmp_str) > cfg.TWITTER_LEN):
                end = cfg.TWITTER_LEN - 1

                while tmp_str[end] != " ":
                    end -= 1
                    if end == 0:
                        raise ValueError(
                            "Reached start of Tweet"
                        )

                chunks.append(tmp_str[0:end])
                tmp_str = tmp_str[end + 1:]

            chunks.append(tmp_str)
            return chunks
