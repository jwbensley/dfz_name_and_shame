#!/usr/bin/env python3

import argparse
import datetime
import logging
import os
import sys

# Accomodate the use of the script, even when the dnas library isn't installed
sys.path.append(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__))
        , "../"
    )
)

from dnas.mrt_stats import mrt_stats
from dnas.redis_db import redis_db
from dnas.twitter import twitter
from dnas.twitter_msg import twitter_msg

def delete(tweet_id):
    """
    Delete a Tweet from twitter.com
    """
    if not tweet_id:
        raise ValueError(
            f"Missing required arguments: tweet_id={tweet_id}"
        )
    t = twitter()
    t.delete(tweet_id)

def gen_tweets_yest():
    """
    Generate Tweets based on for yesterday's stats changes.
    """
    delta = datetime.timedelta(days=1)
    yesterday = datetime.datetime.strftime(datetime.datetime.now() - delta,"%Y%m%d")
    gen_tweets(yesterday)

def gen_tweets(ymd):
    """
    Generate Tweets based on stat changes for a specific day.
    """
    if not ymd:
        raise ValueError(
            f"Missing required arguments: ymd={ymd}, use --ymd"
        )

    if type(ymd) != str:
        raise TypeError(
            f"ymd is not an str: {type(ymd)}"
        )

    rdb = redis_db()
    diff_key = mrt_stats.gen_diff_key(ymd)
    diff = rdb.get_stats(diff_key)
    if not diff:
        logging.info(f"No daily diff stored for {ymd}")
        return

    msg_q = twitter.gen_tweets(diff)
    if not msg_q:
        logging.info(f"No tweets generated for day {ymd}")

    else:
        logging.info(
            f"Storing {len(msg_q)} tweets under "
            f"{twitter_msg.gen_tweet_q_key(ymd)}"
        )
        for msg in msg_q:
            rdb.add_to_queue(
                twitter_msg.gen_tweet_q_key(ymd),
                msg.to_json()
            )

    rdb.close()

def tweet(ymd, print_only):
    """
    Tweet all the Tweets in the redis queue for a specific day.
    """
    if not ymd:
        raise ValueError(
            f"Missing required arguments: ymd={ymd}, use --ymd"
        )

    if type(ymd) != str:
        raise TypeError(
            f"ymd is not an str: {type(ymd)}"
        )

    rdb = redis_db()
    t = twitter()
    msg_q = rdb.get_queue_msgs(twitter_msg.gen_tweet_q_key(ymd))

    for msg in msg_q:
        if not msg.hidden:

            t.tweet(msg, print_only)
            if print_only:
                continue

            rdb.add_to_queue(
                twitter_msg.gen_tweeted_q_key(ymd),
                msg.to_json()
            )
            rdb.del_from_queue(
                twitter_msg.gen_tweet_q_key(ymd),
                msg.to_json()
            )

    rdb.close()

def parse_args():
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
        help="Generate Tweets for stat changes of a specific day, "
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
        help="Generate Tweets for yesterdays stat changes, "
        "and add to tweet queue.",
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

    if args["debug"]:
        level = logging.DEBUG
    else:
        level = logging.INFO
    
    logging.basicConfig(
        format='%(asctime)s %(levelname)s %(message)s', level=level
    )
    logging.info(f"Starting Tweet generation and posting with logging level {level}")

    if args["delete"]:
        delete(args["delete"])

    if args["generate"]:
        gen_tweets(args["ymd"])

    if args["tweet"]:
        tweet(args["ymd"], args["print"])

    if args["yesterday"]:
        gen_tweets_yest()

if __name__ == '__main__':
    main()