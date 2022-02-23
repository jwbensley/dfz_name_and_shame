#!/usr/bin/env python3

import argparse
impot datetime
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
from dnas.redis_db import redis_db
from dnas.twitter import twitter
from dnas.twitter import twitter_msg

def gen_tweet_yesterday():
    """
    Generate Tweets based on for yesterday's stats changes.
    """
    delta = datetime.timedelta(days=1)
    yesterday = datetime.datetime.strftime(datetime.datetime.now() - delta,"%Y%m%d")
    gen_tweet(yesterday)


def gen_tweet(ymd):
    """
    Generate Tweets based on stat changes for a specific day.
    """
    if not args["ymd"]:
        raise ValueError(
            f"Missing required arguments: ymd={args['ymd']}, use --ymd"
        )

    rdb = redis_db()
    diff_key = mrt_stats.gen_diff_key(ymd)
    diff = rdb.get_stats(diff_key)
    if not diff_stats:
        logging.info(f"No daily diff stored for {ymd}")
        return

    if diff.longest_as_path:
        msg = twitter_msg()
        msg.hdr = (
            f"New longest AS path: on {diff.timestamp.split('--')[0]} "
            f"{len(diff.longest_as_path)} prefix(es) had an AS path length "
            f"of {len(diff.longest_as_path[0].as_path)} ASNs"
        )

        for mrt_e in diff.longest_as_path:
            msg.body += f"{mrt_e.prefix} from ASN(s)"
            for asn in mrt_e.origin_asns:
                as_name = whois.as_lookup(int(asn))
                if as_name:
                    msg.body += f" AS{asn} ({as_name})"
                else:
                    msg.body += f" AS{asn}"
            msg.body += " "
        msg.body = msg.body[0:-1]

        msg.hidden = False
        msg_q.append(msg)
        print(msg.hdr)
        print(msg.body)
        print("")

    if diff.longest_comm_set:
        msg = twitter_msg()
        msg.hdr = (
            f"New longest comm set: on {diff.timestamp.split('--')[0]} "
            f"{len(diff.longest_comm_set)} prefix(es) had a comm set length"
            f" of {len(diff.longest_comm_set[0].comm_set)} communities"
        )

        for mrt_e in diff.longest_comm_set:
            msg.body += f"{mrt_e.prefix} from ASN(s)"
            for asn in mrt_e.origin_asns:
                as_name = whois.as_lookup(int(asn))
                if as_name:
                    msg.body += f" AS{asn} ({as_name})"
                else:
                    msg.body += f" AS{asn}"
            msg.body += " "
        msg.body = msg.body[0:-1]

        msg.hidden = False
        msg_q.append(msg)
        print(msg.hdr)
        print(msg.body)
        print("")

    if diff.most_advt_prefixes:
        msg = twitter_msg()
        msg.hdr = (
            f"New most BGP advt per prefix: "
            f"on {diff.timestamp.split('--')[0]} "
            f"{len(diff.most_advt_prefixes)} prefix(es) had "
            f"{diff.most_advt_prefixes[0].advt} advt"
        )

        msg.body = "Prefix(es)"
        for mrt_e in diff.most_advt_prefixes:
            msg.body += f" {mrt_e.prefix}"
        msg.hidden = False
        msg_q.append(msg)
        print(msg.hdr)
        print(msg.body)
        print("")

    if diff.most_upd_prefixes:
        msg = twitter_msg()
        msg.hdr = (
            f"New most BGP updates per prefix: "
            f"on {diff.timestamp.split('--')[0]} "
            f"{len(diff.most_upd_prefixes)} prefix(es) had "
            f"{diff.most_upd_prefixes[0].updates} updates"
        )
        msg.body = "Prefix(es)"
        for mrt_e in diff.most_upd_prefixes:
            msg.body += f" {mrt_e.prefix}"
        msg.hidden = False
        msg_q.append(msg)
        print(msg.hdr)
        print(msg.body)
        print("")

    if diff.most_withd_prefixes:
        msg = twitter_msg()
        msg.hdr = (
            f"New most BGP withdraws per prefix: "
            f"on {diff.timestamp.split('--')[0]} "
            f"{len(diff.most_withd_prefixes)} prefix(es) had "
            f"{diff.most_withd_prefixes[0].withdraws} withdraws"
        )
        msg.body = "Prefix(es)"
        for mrt_e in diff.most_withd_prefixes:
            msg.body += f" {mrt_e.prefix}"
        msg.hidden = False
        msg_q.append(msg)
        print(msg.hdr)
        print(msg.body)
        print("")

    if diff.most_advt_origin_asn:
        msg = twitter_msg()
        msg.hdr = (
            f"New most BGP advt per origin ASN: "
            f"on {diff.timestamp.split('--')[0]} "
            f"{len(diff.most_advt_origin_asn)} origin ASN(s) sent "
            f"{diff.most_advt_origin_asn[0].advt} advt"
        )
        msg.body = "Origin ASN(s)"
        for mrt_e in diff.most_advt_origin_asn:
            for asn in mrt_e.origin_asns:
                as_name = whois.as_lookup(int(asn))
                if as_name:
                    msg.body += f" AS{asn} ({as_name})"
                else:
                    msg.body += f" AS{asn}"
        msg.hidden = False
        msg_q.append(msg)
        print(msg.hdr)
        print(msg.body)
        print("")

    """
    if diff.most_advt_peer_asn:
        msg = twitter_msg()
        msg.hdr = (
            f"New most BGP advt per peer ASN: "
            f"on {diff.timestamp.split('--')[0]} "
            f"{len(diff.most_advt_peer_asn)} peer ASN(s) sent "
            f"{diff.most_advt_peer_asn[0].advt} advt"
        )
        msg.body = "Peer ASN(s)"
        for mrt_e in diff.most_advt_peer_asn:
            for asn in mrt_e.peer_asn:
                as_name = whois.as_lookup(int(asn))
                if as_name:
                    msg.body += f" AS{asn} ({as_name})"
                else:
                    msg.body += f" AS{asn}"
        msg_q.append(msg)
        print(msg.hdr)
        print(msg.body)
        print("")

    if diff.most_upd_peer_asn:
        msg = twitter_msg()
        msg.hdr = (
            f"New most BGP updates per peer ASN: "
            f"on {diff.timestamp.split('--')[0]} "
            f"{len(diff.most_upd_peer_asn)} peer ASN(s) sent "
            f"{diff.most_upd_peer_asn[0].updates} updates"
        )
        msg.body = "Peer ASN(s)"
        for mrt_e in diff.most_upd_peer_asn:
            for asn in mrt_e.peer_asn:
                as_name = whois.as_lookup(int(asn))
                if as_name:
                    msg.body += f" AS{asn} ({as_name})"
                else:
                    msg.body += f" AS{asn}"
        msg_q.append(msg)
        print(msg.hdr)
        print(msg.body)
        print("")

    if diff.most_withd_peer_asn:
        msg = twitter_msg()
        msg.hdr = (
            f"New most BGP withdraws per peer ASN: "
            f"on {diff.timestamp.split('--')[0]} "
            f"{len(diff.most_withd_peer_asn)} peer ASN(s) sent "
            f"{diff.most_withd_peer_asn[0].withdraws} withdraws"
        )
        msg.body = "Peer ASN(s)"
        for mrt_e in diff.most_withd_peer_asn:
            for asn in mrt_e.peer_asn:
                as_name = whois.as_lookup(int(asn))
                if as_name:
                    msg.body += f" AS{asn} ({as_name})"
                else:
                    msg.body += f" AS{asn}"
        msg_q.append(msg)
        print(msg.hdr)
        print(msg.body)
        print("")

    """
    if diff.most_origin_asns:
        msg = twitter_msg()
        msg.hdr = (
            f"New most origin ASNs per prefix: "
            f"on {diff.timestamp.split('--')[0]} "
            f"{len(diff.most_origin_asns)} prefix(es) had "
            f"{len(diff.most_origin_asns[0].origin_asns)} origin ASNs"
        )
        msg.body = "Prefix(es)"
        for mrt_e in diff.most_origin_asns:
            msg.body += f" {mrt_e.prefix}:"
            for asn in mrt_e.origin_asns:
                as_name = whois.as_lookup(int(asn))
                if as_name:
                    msg.body += f" {asn} {as_name}"
                else:
                    msg.body += f" {asn}"
        msg.hidden = False
        msg_q.append(msg)
        print(msg.hdr)
        print(msg.body)
        print("")

    """
    for msg in msg_q:
        print(msg.hdr)
        print(msg.body)
        print("")
    """

    """
    t = twitter()
    for msg in msg_q:
        if not msg.hidden:
            t.tweet_paged(msg)
    """

    """
    for diff_key in diff_keys:
        rdb.delete(diff_key)
    """

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
        "--yesterday",
        help="Generate Tweets for yesterdays stat changes, add to tweet queue",
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

if __name__ == '__main__':
    main()