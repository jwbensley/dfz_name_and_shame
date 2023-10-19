#!/usr/bin/env python3

import argparse
import datetime
import logging
import os
import re
import sys
import typing

# Accomodate the use of the dnas library, even when the library isn't installed
sys.path.append(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "../")
)

from dnas.config import config as cfg
from dnas.git import git
from dnas.log import log
from dnas.mrt_stats import mrt_stats
from dnas.redis_db import redis_db
from dnas.twitter import twitter
from dnas.twitter_msg import twitter_msg


def delete(tweet_id: int) -> None:
    """
    Delete a Tweet from twitter.com
    """
    if not tweet_id:
        raise ValueError(f"Missing required arguments: tweet_id={tweet_id}")

    t = twitter()
    t.delete(tweet_id)


def gen_tweets_yest() -> None:
    """
    Generate Tweets based on for yesterday's stats changes and publish them.
    """
    delta = datetime.timedelta(days=1)
    yesterday = datetime.datetime.strftime(
        datetime.datetime.now() - delta, cfg.DAY_FORMAT
    )
    gen_tweets(yesterday)
    tweet(ymd=yesterday, print_only=False)


def gen_tweets(ymd: str) -> None:
    """
    Generate Tweets based on stats for a specific day.
    """
    if not ymd:
        raise ValueError(f"Missing required arguments: ymd={ymd}, use --ymd")

    if type(ymd) != str:
        raise TypeError(f"ymd is not an str: {type(ymd)}")

    rdb = redis_db()
    day_key = mrt_stats.gen_daily_key(ymd)
    day_stats = rdb.get_stats(day_key)
    if not day_stats:
        logging.info(f"No daily stats stored for {ymd}")
        return

    msg_q = twitter.gen_tweets(day_stats)
    if not msg_q:
        logging.info(f"No tweets generated for day {ymd}")

    else:
        logging.info(
            f"Storing {len(msg_q)} tweets under "
            f"{twitter_msg.gen_tweet_q_key(ymd)}"
        )
        for msg in msg_q:
            rdb.add_to_queue(twitter_msg.gen_tweet_q_key(ymd), msg.to_json())

    rdb.close()


def tweet(ymd: str, print_only: bool = False) -> None:
    """
    Tweet all the Tweets in the redis queue for a specific day.
    """
    if not ymd:
        raise ValueError(f"Missing required arguments: ymd={ymd}, use --ymd")

    if type(ymd) != str:
        raise TypeError(f"ymd is not a string: {type(ymd)}")

    rdb = redis_db()
    t = twitter()
    tweet_q = rdb.get_queue_msgs(twitter_msg.gen_tweet_q_key(ymd))
    tweeted_q = rdb.get_queue_msgs(twitter_msg.gen_tweeted_q_key(ymd))

    thread_hdr = twitter_msg(
        hdr=f"Thread for {t.ymd_to_nice(ymd)}. "
        f"Full details at {git.gen_git_url_ymd(ymd)}",
        body="",
        hidden=False,
    )
    t.tweet_hdr(thread_hdr, print_only)

    for tweet in tweet_q:
        if tweet.hdr in [t.hdr for t in tweeted_q]:
            logging.debug(f"Skipping already tweeted message: {tweet.hdr}")
            continue

        if not tweet.hidden:
            t.tweet_as_reply(tweet, print_only, thread_hdr.hdr_id)
            if print_only:
                continue

            rdb.add_to_queue(
                twitter_msg.gen_tweeted_q_key(ymd), tweet.to_json()
            )
            rdb.del_from_queue(
                twitter_msg.gen_tweet_q_key(ymd), tweet.to_json()
            )

    rdb.close()


def parse_args() -> dict:
    """
    Parse the CLI args to this script.
    """
    parser = argparse.ArgumentParser(
        description="Generate and publish Tweets based on stats in redis.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--debug",
        help="Enable debug logging for this script.",
        default=False,
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "--delete",
        help="Delete a Tweet by providing the Tweet ID e.g., 1234567890",
        type=str,
        default=None,
        required=False,
    )
    parser.add_argument(
        "--generate",
        help="Generate Tweets for stats from a specific day, "
        "and add to tweet queue. Use --ymd option.",
        default=False,
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "--print",
        help="Use with --tweet and --ymd to print the Tweets from that day, "
        "instead of tweeting them.",
        default=False,
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "--tweet",
        help="Tweet tweets in the tweet queue for a specific day. "
        "Use with the --ymd option.",
        default=False,
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "--yesterday",
        help="Generate Tweets for yesterdays stat changes, and publish them. "
        "This is a shortcut for --generate --tweet --ymd yyyymmdd",
        default=False,
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "--ymd",
        help="Day in format 'yyyymmdd' e.g., '20220101'.",
        type=str,
        default=None,
        required=False,
    )
    return vars(parser.parse_args())


def main():
    args = parse_args()
    log.setup(
        debug=args["debug"],
        log_src="Tweet generation and posting script",
        log_path=cfg.LOG_TWITTER,
    )

    if args["delete"]:
        delete(args["delete"])

    if args["generate"]:
        gen_tweets(args["ymd"])

    if args["tweet"]:
        tweet(args["print"], args["ymd"])

    if args["yesterday"]:
        gen_tweets_yest()


if __name__ == "__main__":
    main()
