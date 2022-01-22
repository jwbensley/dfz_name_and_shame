
import tweepy

import sys
sys.path.append('./')
from redis_db import redis_db
from twitter_auth import twitter_auth

class twitter:

    user = "bgp_shamer"
    max_len = 280

    def __init__(self):

        self.client = tweepy.Client(
            consumer_key=twitter_auth.consumer_key,
            consumer_secret=twitter_auth.consumer_secret,
            access_token=twitter_auth.access_token,
            access_token_secret=twitter_auth.access_token_secret
        )


    def tweet(self, msg):

        r = self.client.create_tweet(
            text="This Tweet was Tweeted using Tweepy and Twitter API v2!"
        )
        print(f"https://twitter.com/{self.user}/status/{r.data['id']}")

    def tweet_paged(self, msgs):
        r = self.client.create_tweet(text=msgs[0])
        msgs.pop(0)

        for msg in msgs:
            r = self.client.create_tweet(text=msg, in_reply_to_tweet_id=r.data["id"])
            print(f"https://twitter.com/{self.user}/status/{r.data['id']}")

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

def main():

    rdb = redis_db()
    msg_q = []

    diff_keys = rdb.get_keys("DIFF:*")
    for diff_key in diff_keys:
        diff = rdb.get_stats(diff_key)

        if diff.longest_as_path:
            msg = (
                f"New longest AS path on {diff.timestamp.split('--')[0]} "
                f"of {len(diff.longest_as_path[0].as_path)} ASNs for"
            )
            for mrt_e in diff.longest_as_path:
                msg += f" {mrt_e.prefix}"
            msg += ":"
            for asn in diff.longest_as_path[0].as_path:
                msg += f" {asn}"
            msg_q.append(msg)

        if diff.longest_comm_set:
            msg = (
                f"New longest comm set on {diff.timestamp.split('--')[0]} "
                f"of {len(diff.longest_comm_set[0].comm_set)} communities for"
            )
            for mrt_e in diff.longest_comm_set:
                msg += f" {mrt_e.prefix}"
            msg += ":"
            for comm in diff.longest_comm_set[0].comm_set:
                msg += f" {comm}"
            msg_q.append(msg)

        if diff.most_advt_prefixes:
            msg = (
                f"New most advertisements for same prefix "
                f"on {diff.timestamp.split('--')[0]} of "
                f"{diff.most_advt_prefixes[0].advertisements} advertisements for"
            )
            for mrt_e in diff.most_advt_prefixes:
                msg += f" {mrt_e.prefix}"
            msg_q.append(msg)

        if diff.most_upd_prefixes:
            msg = (
                f"New most updates for same prefix "
                f"on {diff.timestamp.split('--')[0]} of "
                f"{diff.most_upd_prefixes[0].updates} updates for"
            )
            for mrt_e in diff.most_upd_prefixes:
                msg += f" {mrt_e.prefix}"
            msg_q.append(msg)

        if diff.most_withd_prefixes:
            msg = (
                f"New most withdraws for same prefix "
                f"on {diff.timestamp.split('--')[0]} of "
                f"{diff.most_withd_prefixes[0].withdraws} withdraws for"
            )
            for mrt_e in diff.most_withd_prefixes:
                msg += f" {mrt_e.prefix}"
            msg_q.append(msg)

        if diff.most_advt_origin_asn:
            msg = (
                f"New most advertisements for same origin ASN "
                f"on {diff.timestamp.split('--')[0]} of "
                f"{diff.most_advt_origin_asn[0].advertisements} advertisements by"
            )
            for mrt_e in diff.most_advt_origin_asn:
                for asn in mrt_e.origin_asns:
                    msg += f" {asn}"
            msg_q.append(msg)


        if diff.most_advt_peer_asn:
            msg = (
                f"New most advertisements for same peer ASN "
                f"on {diff.timestamp.split('--')[0]} of "
                f"{diff.most_advt_peer_asn[0].advertisements} advertisements by "
            )
            for mrt_e in diff.most_advt_peer_asn:
                for asn in mrt_e.peer_asn:
                    msg += f" {asn}"
            msg_q.append(msg)

        if diff.most_upd_peer_asn:
            msg = (
                f"New most updates for same peer ASN "
                f"on {diff.timestamp.split('--')[0]} of "
                f"{diff.most_upd_peer_asn[0].updates} updates by"
            )
            for mrt_e in diff.most_upd_peer_asn:
                for asn in mrt_e.peer_asn:
                    msg += f" {asn}"
            msg_q.append(msg)

        if diff.most_withd_peer_asn:
            msg = (
                f"New most withdraws for same peer ASN "
                f"on {diff.timestamp.split('--')[0]} of "
                f"{diff.most_withd_peer_asn[0].withdraws} withdraws by"
            )
            for mrt_e in diff.most_withd_peer_asn:
                for asn in mrt_e.peer_asn:
                    msg += f" {asn}"
            msg_q.append(msg)

        if diff.most_origin_asns:
            msg = (
                f"New most origin ASNs for same prefix "
                f"on {diff.timestamp.split('--')[0]} of "
                f"{len(diff.most_origin_asns[0].origin_asns)} different ASNs for"
            )
            for mrt_e in diff.most_origin_asns:
                msg += f" {mrt_e.prefix}:"
                for asn in mrt_e.origin_asns:
                    msg += f" {asn}"
            msg_q.append(msg)

    print(msg_q)

    t = twitter()
    msgs = t.split_tweet(msg_q[0])
    print(msgs)
    ##t.tweet_paged(msgs)

    """
    for diff_key in diff_keys:
        rdb.delete(diff_key)
    """

if __name__ == '__main__':
    main()