import logging
import tweepy # type: ignore
from typing import List

from dnas.config import config as cfg
from dnas.mrt_archive import mrt_archive
from dnas.mrt_stats import mrt_stats
from dnas.report import report
from dnas.twitter_auth import twitter_auth
from dnas.twitter_msg import twitter_msg
from dnas.whois import whois

class twitter:
    """
    Class for interacting with Twitter API using Tweepy.
    """

    def __init__(self) -> None:

        self.client = tweepy.Client(
            consumer_key=twitter_auth.consumer_key,
            consumer_secret=twitter_auth.consumer_secret,
            access_token=twitter_auth.access_token,
            access_token_secret=twitter_auth.access_token_secret
        )

    def delete(self, tweet_id: int = None):
        """
        Delete a Tweet from twitter.com
        """
        if not tweet_id:
            raise ValueError(
                f"Missing required arguments: tweet_id={tweet_id}"
            )

        if type(tweet_id) != str:
            raise TypeError(
                f"tweet_id is not string: {type(tweet_id)}"
            )

        r = self.client.delete_tweet(tweet_id)
        if r.data["deleted"]:
            logging.info(f"Deleted Tweet {tweet_id}")
        else:
            raise RuntimeError(f"Error deleting Tweet {tweet_id}: {r}")

    @staticmethod
    def gen_tweets(mrt_s: 'mrt_stats' = None) -> List['twitter_msg']:
        """
        Generate Tweets using the data in an mrt stats object.
        """
        if not mrt_s:
            raise ValueError(
                f"Missing required arguments: mrt_s={mrt_s}"
            )

        if type(mrt_s) != mrt_stats:
            raise TypeError(
                f"mrt_s is not an mrt_s object: {type(mrt_s)}"
            )

        msg_q = []

        txt_report = report.gen_txt_report(mrt_s =mrt_s, body = False)
        for hdr in txt_report:
            msg_q.append(
                twitter_msg(
                    hdr = hdr,
                    body = "",
                    hidden = False,
                )
            )

        return msg_q

    def split_tweet(self, msg: 'twitter_msg' = None) -> List[str]:
        """
        Return a Tweet body split into a list of 280 character strings
        """
        if not msg:
            raise ValueError(
                f"Missing required arguments: msg={msg}"
            )

        if type(msg) != twitter_msg:
            raise TypeError(
                f"msg is not a twitter_msg: {type(msg)}"
            )

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

    def tweet(
            self,
            body: bool = False,
            msg: 'twitter_msg' = None,
            print_only: bool = False
        ):
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

        self.tweet_hdr(msg, print_only)
        if body:
            self.tweet_body(msg, print_only)

    def tweet_as_reply(
            self,
            msg: 'twitter_msg' = None,
            print_only: bool = False,
            tweet_id: int = 0,
        ):
        """
        Tweet a message in reply to an existing Tweet.
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

        if print_only:
            logging.info(msg.hdr)
        else:
            r = self.client.create_tweet(
                text=msg.hdr,
                in_reply_to_tweet_id=tweet_id,
            )
            logging.info(
                f"Replied: "
                f"https://twitter.com/{cfg.TWITTER_USER}/status/{r.data['id']}"
            )
            msg.hdr_id = int(r.data["id"])

    def tweet_hdr(self, msg: 'twitter_msg' = None, print_only: bool = False):
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

        if print_only:
            logging.info(msg.hdr)
        else:
            r = self.client.create_tweet(text=msg.hdr)
            logging.info(
                f"Tweeted: "
                f"https://twitter.com/{cfg.TWITTER_USER}/status/{r.data['id']}"
            )
            msg.hdr_id = int(r.data["id"])

    def tweet_body(self, msg: 'twitter_msg' = None, print_only: bool = False):
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

        if not print_only:
            if not msg.hdr_id:
                raise ValueError(
                    f"Missing required arguments: msg.hdr_id={msg.hdr_id}"
                )

            if type(msg.hdr_id) != int:
                raise TypeError(
                    f"msg.hdr_id is not an int: {type(msg.hdr_id)}"
                )

        if msg.hidden:
            logging.debug(f"Skipping hidden Tweet: {msg.body}")
            return

        for chunk in self.split_tweet(msg):
            if print_only:
                logging.info(chunk)
            else:
                r = self.client.create_tweet(
                    text=chunk,
                    in_reply_to_tweet_id=msg.hdr_id
                )
                logging.info(
                    f"Replied: "
                    f"https://twitter.com/{cfg.TWITTER_USER}/status/{r.data['id']}"
                )
                msg.body_ids.append(r.data["id"])

    @staticmethod
    def ymd_to_nice(ymd: str = None) -> str:
        """
        Convert a ymd value to a nice format for Twitter.
        """
        mrt_archive.valid_ymd(ymd)
        return ymd[0:4] + "/" + ymd[4:6] + "/" + ymd[6:8]
