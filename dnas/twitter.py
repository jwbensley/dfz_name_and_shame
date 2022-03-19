import logging
import tweepy # type: ignore
from typing import List

from dnas.config import config as cfg
from dnas.mrt_stats import mrt_stats
from dnas.twitter_auth import twitter_auth
from dnas.twitter_msg import twitter_msg
from dnas.whois import whois

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
    def gen_tweets(mrt_s: mrt_stats = None) -> List[twitter_msg]:
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

        if mrt_s.longest_as_path:
            msg = twitter_msg()
            msg.hdr = (
                f"New longest AS path: on the day {mrt_s.ts_ymd_format()} "
                f"{len(mrt_s.longest_as_path)} prefix(es) had an AS path length "
                f"of {len(mrt_s.longest_as_path[0].as_path)} ASNs"
            )

            for mrt_e in mrt_s.longest_as_path:
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

        if mrt_s.longest_comm_set:
            msg = twitter_msg()
            msg.hdr = (
                f"New longest community set: on the day {mrt_s.ts_ymd_format()} "
                f"{len(mrt_s.longest_comm_set)} prefix(es) had a comm set length"
                f" of {len(mrt_s.longest_comm_set[0].comm_set)} communities"
            )

            for mrt_e in mrt_s.longest_comm_set:
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

        if mrt_s.most_advt_prefixes:
            msg = twitter_msg()
            msg.hdr = (
                f"New most BGP advertisements per prefix: "
                f"on the day {mrt_s.ts_ymd_format()} "
                f"{len(mrt_s.most_advt_prefixes)} prefix(es) had "
                f"{mrt_s.most_advt_prefixes[0].advt} advertisements"
            )

            msg.body = "Prefix(es):"
            for mrt_e in mrt_s.most_advt_prefixes:
                msg.body += f" {mrt_e.prefix}"
            msg.hidden = False
            msg_q.append(msg)

        if mrt_s.most_upd_prefixes:
            msg = twitter_msg()
            msg.hdr = (
                f"New most BGP updates per prefix: "
                f"on the day {mrt_s.ts_ymd_format()} "
                f"{len(mrt_s.most_upd_prefixes)} prefix(es) had "
                f"{mrt_s.most_upd_prefixes[0].updates} updates"
            )
            msg.body = "Prefix(es):"
            for mrt_e in mrt_s.most_upd_prefixes:
                msg.body += f" {mrt_e.prefix}"
            msg.hidden = False
            msg_q.append(msg)

        if mrt_s.most_withd_prefixes:
            msg = twitter_msg()
            msg.hdr = (
                f"New most BGP withdraws per prefix: "
                f"on the day {mrt_s.ts_ymd_format()} "
                f"{len(mrt_s.most_withd_prefixes)} prefix(es) had "
                f"{mrt_s.most_withd_prefixes[0].withdraws} withdraws"
            )
            msg.body = "Prefix(es):"
            for mrt_e in mrt_s.most_withd_prefixes:
                msg.body += f" {mrt_e.prefix}"
            msg.hidden = False
            msg_q.append(msg)

        if mrt_s.most_advt_origin_asn:
            msg = twitter_msg()
            msg.hdr = (
                f"New most BGP advertisements per origin ASN: "
                f"on the day {mrt_s.ts_ymd_format()} "
                f"{len(mrt_s.most_advt_origin_asn)} origin ASN(s) sent "
                f"{mrt_s.most_advt_origin_asn[0].advt} advertisements"
            )
            msg.body = "Origin ASN(s):"
            for mrt_e in mrt_s.most_advt_origin_asn:
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

        if mrt_s.most_advt_peer_asn:
            msg = twitter_msg()
            msg.hdr = (
                f"New most BGP advertisements per peer ASN: "
                f"on the day {mrt_s.ts_ymd_format()} "
                f"{len(mrt_s.most_advt_peer_asn)} peer ASN(s) sent "
                f"{mrt_s.most_advt_peer_asn[0].advt} advertisements"
            )
            msg.body = "Peer ASN(s):"
            for mrt_e in mrt_s.most_advt_peer_asn:
                for asn in mrt_e.peer_asn:
                    as_name = whois.as_lookup(int(asn))
                    if as_name:
                        msg.body += f" AS{asn} ({as_name})"
                    else:
                        msg.body += f" AS{asn}"
                msg.body += ", "
            msg.body = msg.body[0:-2]
            msg_q.append(msg)

        if mrt_s.most_upd_peer_asn:
            msg = twitter_msg()
            msg.hdr = (
                f"New most BGP updates per peer ASN: "
                f"on the day {mrt_s.ts_ymd_format()} "
                f"{len(mrt_s.most_upd_peer_asn)} peer ASN(s) sent "
                f"{mrt_s.most_upd_peer_asn[0].updates} updates"
            )
            msg.body = "Peer ASN(s):"
            for mrt_e in mrt_s.most_upd_peer_asn:
                for asn in mrt_e.peer_asn:
                    as_name = whois.as_lookup(int(asn))
                    if as_name:
                        msg.body += f" AS{asn} ({as_name})"
                    else:
                        msg.body += f" AS{asn}"
                msg.body += ", "
            msg.body = msg.body[0:-2]
            msg_q.append(msg)

        if mrt_s.most_withd_peer_asn:
            msg = twitter_msg()
            msg.hdr = (
                f"New most BGP withdraws per peer ASN: "
                f"on the day {mrt_s.ts_ymd_format()} "
                f"{len(mrt_s.most_withd_peer_asn)} peer ASN(s) sent "
                f"{mrt_s.most_withd_peer_asn[0].withdraws} withdraws"
            )
            msg.body = "Peer ASN(s):"
            for mrt_e in mrt_s.most_withd_peer_asn:
                for asn in mrt_e.peer_asn:
                    as_name = whois.as_lookup(int(asn))
                    if as_name:
                        msg.body += f" AS{asn} ({as_name})"
                    else:
                        msg.body += f" AS{asn}"
                msg.body += ", "
            msg.body = msg.body[0:-2]
            msg_q.append(msg)

        if mrt_s.most_origin_asns:
            msg = twitter_msg()
            msg.hdr = (
                f"New most origin ASNs per prefix: "
                f"on the day {mrt_s.ts_ymd_format()} "
                f"{len(mrt_s.most_origin_asns)} prefix(es) had "
                f"{len(mrt_s.most_origin_asns[0].origin_asns)} origin ASNs"
            )
            msg.body = "Prefix(es):"
            for mrt_e in mrt_s.most_origin_asns:
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

        return msg_q

    def tweet(self, msg: twitter_msg = None, print_only: bool = False):
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
        self.tweet_body(msg, print_only)

    def tweet_hdr(self, msg: twitter_msg = None, print_only: bool = False):
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

    def tweet_body(self, msg: twitter_msg = None, print_only: bool = False):
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

    def split_tweet(self, msg: twitter_msg = None) -> List[str]:
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
