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

from dnas.config import config as cfg
from dnas.mrt_stats import mrt_stats
from dnas.redis_db import redis_db
from dnas.twitter import twitter
from dnas.twitter_msg import twitter_msg
from dnas.whois import whois

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

    if type(ymd) != int:
        raise TypeError(
            f"ymd is not an int: {type(ymd)}"
        )

    rdb = redis_db()
    diff_key = mrt_stats.gen_diff_key(ymd)
    diff = rdb.get_stats(diff_key)
    if not diff:
        logging.info(f"No daily diff stored for {ymd}")
        return

    msg_q = []
    if diff.longest_as_path:
        msg = twitter_msg()
        msg.hdr = (
            f"New longest AS path: on the day {diff.ts_ymd_format()} "
            f"{len(diff.longest_as_path)} prefix(es) had an AS path length "
            f"of {len(diff.longest_as_path[0].as_path)} ASNs"
        )

        for mrt_e in diff.longest_as_path:
            msg.body += f"{mrt_e.prefix} from origin ASN(s)"
            for asn in mrt_e.origin_asns:
                as_name = whois.as_lookup(int(asn))
                if as_name:
                    msg.body += f" AS{asn} ({as_name})"
                else:
                    msg.body += f" AS{asn}"
            msg.body += ", "
        msg.body = msg.body[0:-1]

        msg.hidden = False
        msg_q.append(msg)

    if diff.longest_comm_set:
        msg = twitter_msg()
        msg.hdr = (
            f"New longest community set: on the day {diff.ts_ymd_format()} "
            f"{len(diff.longest_comm_set)} prefix(es) had a comm set length"
            f" of {len(diff.longest_comm_set[0].comm_set)} communities"
        )

        for mrt_e in diff.longest_comm_set:
            msg.body += f"{mrt_e.prefix} from origin ASN(s)"
            for asn in mrt_e.origin_asns:
                as_name = whois.as_lookup(int(asn))
                if as_name:
                    msg.body += f" AS{asn} ({as_name})"
                else:
                    msg.body += f" AS{asn}"
            msg.body += ", "
        msg.body = msg.body[0:-2]

        msg.hidden = False
        msg_q.append(msg)

    if diff.most_advt_prefixes:
        msg = twitter_msg()
        msg.hdr = (
            f"New most BGP advertisements per prefix: "
            f"on the day {diff.ts_ymd_format()} "
            f"{len(diff.most_advt_prefixes)} prefix(es) had "
            f"{diff.most_advt_prefixes[0].advt} advertisements"
        )

        msg.body = "Prefix(es):"
        for mrt_e in diff.most_advt_prefixes:
            msg.body += f" {mrt_e.prefix}"
        msg.hidden = False
        msg_q.append(msg)

    if diff.most_upd_prefixes:
        msg = twitter_msg()
        msg.hdr = (
            f"New most BGP updates per prefix: "
            f"on the day {diff.ts_ymd_format()} "
            f"{len(diff.most_upd_prefixes)} prefix(es) had "
            f"{diff.most_upd_prefixes[0].updates} updates"
        )
        msg.body = "Prefix(es):"
        for mrt_e in diff.most_upd_prefixes:
            msg.body += f" {mrt_e.prefix}"
        msg.hidden = False
        msg_q.append(msg)

    if diff.most_withd_prefixes:
        msg = twitter_msg()
        msg.hdr = (
            f"New most BGP withdraws per prefix: "
            f"on the day {diff.ts_ymd_format()} "
            f"{len(diff.most_withd_prefixes)} prefix(es) had "
            f"{diff.most_withd_prefixes[0].withdraws} withdraws"
        )
        msg.body = "Prefix(es):"
        for mrt_e in diff.most_withd_prefixes:
            msg.body += f" {mrt_e.prefix}"
        msg.hidden = False
        msg_q.append(msg)

    if diff.most_advt_origin_asn:
        msg = twitter_msg()
        msg.hdr = (
            f"New most BGP advertisements per origin ASN: "
            f"on the day {diff.ts_ymd_format()} "
            f"{len(diff.most_advt_origin_asn)} origin ASN(s) sent "
            f"{diff.most_advt_origin_asn[0].advt} advertisements"
        )
        msg.body = "Origin ASN(s):"
        for mrt_e in diff.most_advt_origin_asn:
            for asn in mrt_e.origin_asns:
                as_name = whois.as_lookup(int(asn))
                if as_name:
                    msg.body += f" AS{asn} ({as_name})"
                else:
                    msg.body += f" AS{asn}"
            msg.body += ", "
        msg.body = msg.body[0:-2]
        msg.hidden = False
        msg_q.append(msg)

    if diff.most_advt_peer_asn:
        msg = twitter_msg()
        msg.hdr = (
            f"New most BGP advertisements per peer ASN: "
            f"on the day {diff.ts_ymd_format()} "
            f"{len(diff.most_advt_peer_asn)} peer ASN(s) sent "
            f"{diff.most_advt_peer_asn[0].advt} advertisements"
        )
        msg.body = "Peer ASN(s):"
        for mrt_e in diff.most_advt_peer_asn:
            for asn in mrt_e.peer_asn:
                as_name = whois.as_lookup(int(asn))
                if as_name:
                    msg.body += f" AS{asn} ({as_name})"
                else:
                    msg.body += f" AS{asn}"
            msg.body += ", "
        msg.body = msg.body[0:-2]
        msg_q.append(msg)

    if diff.most_upd_peer_asn:
        msg = twitter_msg()
        msg.hdr = (
            f"New most BGP updates per peer ASN: "
            f"on the day {diff.ts_ymd_format()} "
            f"{len(diff.most_upd_peer_asn)} peer ASN(s) sent "
            f"{diff.most_upd_peer_asn[0].updates} updates"
        )
        msg.body = "Peer ASN(s):"
        for mrt_e in diff.most_upd_peer_asn:
            for asn in mrt_e.peer_asn:
                as_name = whois.as_lookup(int(asn))
                if as_name:
                    msg.body += f" AS{asn} ({as_name})"
                else:
                    msg.body += f" AS{asn}"
            msg.body += ", "
        msg.body = msg.body[0:-2]
        msg_q.append(msg)

    if diff.most_withd_peer_asn:
        msg = twitter_msg()
        msg.hdr = (
            f"New most BGP withdraws per peer ASN: "
            f"on the day {diff.ts_ymd_format()} "
            f"{len(diff.most_withd_peer_asn)} peer ASN(s) sent "
            f"{diff.most_withd_peer_asn[0].withdraws} withdraws"
        )
        msg.body = "Peer ASN(s):"
        for mrt_e in diff.most_withd_peer_asn:
            for asn in mrt_e.peer_asn:
                as_name = whois.as_lookup(int(asn))
                if as_name:
                    msg.body += f" AS{asn} ({as_name})"
                else:
                    msg.body += f" AS{asn}"
            msg.body += ", "
        msg.body = msg.body[0:-2]
        msg_q.append(msg)

    if diff.most_origin_asns:
        msg = twitter_msg()
        msg.hdr = (
            f"New most origin ASNs per prefix: "
            f"on the day {diff.ts_ymd_format()} "
            f"{len(diff.most_origin_asns)} prefix(es) had "
            f"{len(diff.most_origin_asns[0].origin_asns)} origin ASNs"
        )
        msg.body = "Prefix(es):"
        for mrt_e in diff.most_origin_asns:
            msg.body += f" {mrt_e.prefix}"
            for asn in mrt_e.origin_asns:
                as_name = whois.as_lookup(int(asn))
                if as_name:
                    msg.body += f" AS{asn} ({as_name})"
                else:
                    msg.body += f" AS{asn}"
            msg.body += ", "
        msg.body = msg.body[0:-2]
        msg.hidden = False
        msg_q.append(msg)

    logging.info(f"Storing {len(msg_q)} tweets under {msg.gen_tweet_q_key()}")
    for msg in msg_q:
        rdb.add_to_tweet_q(
            msg.gen_tweet_q_key(ymd),
            msg.to_json()
        )

    rdb.close()

def tweet(tmd):
    """
    Sent all the tweets in the redis queue for a specific day.
    """
    if not ymd:
        raise ValueError(
            f"Missing required arguments: ymd={ymd}, use --ymd"
        )

    if type(ymd) != int:
        raise TypeError(
            f"ymd is not an int: {type(ymd)}"
        )

    rdb = redis_db()
    t = twitter()
    msg_q = rdb.get_tweet_q(twitter_msg.gen_tweet_q_key(ymd))
    for msg in msg_q:
        if not msg.hidden:
            t.tweet_both(msg)

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
        help="Enable debug logging for this script",
        default=False,
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "--tweet-all",
        help="Tweet all tweets in the tweet queue",
        default=False,
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "--yesterday",
        help="Generate Tweets for yesterdays stat changes, "
        "and add to tweet queue",
        default=False,
        action="store_true",
        required=False,
    )
    parser.add_argument(
        "--ymd",
        help="Generate Tweets for for a specific day's stat changes, "
        "and add to tweet queue",
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

    if args["yesterday"]:
        gen_tweets_yest()

    if args["ymd"]:
        gen_tweets(args["ymd"])

if __name__ == '__main__':
    main()