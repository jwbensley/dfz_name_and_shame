
import sys
sys.path.append('./')
from redis_db import redis_db

def main():

    rdb = redis_db()
    msg_q = []

    diff_keys = rdb.get_keys("DIFF:*")
    for diff_key in diff_keys:
        diff = rdb.get_stats(diff_key)

        if diff.longest_as_path:
            msg = (
                f"New longest AS path at {diff.timestamp} of "
                f"{len(diff.longest_as_path[0].as_path)} ASNs for"
            )
            for mrt_e in diff.longest_as_path:
                msg += f" {mrt_e.prefix}"
            msg += ":"
            for asn in diff.longest_as_path[0].as_path:
                msg += f" {asn}"
            msg_q.append(msg)

        if diff.longest_comm_set:
            msg = (
                f"New longest comm set at {diff.timestamp} of "
                f"{len(diff.longest_comm_set[0].comm_set)} communities for"
            )
            for mrt_e in diff.longest_comm_set:
                msg += f" {mrt_e.prefix}"
            msg += ":"
            for comm in diff.longest_comm_set[0].comm_set:
                msg += f" {comm}"
            msg_q.append(msg)

        if diff.most_advt_prefixes:
            msg = (
                f"New most advertisements for same prefix at "
                f"{diff.timestamp} of "
                f"{diff.most_advt_prefixes[0].advertisements} advertisements for"
            )
            for mrt_e in diff.most_advt_prefixes:
                msg += f" {mrt_e.prefix}"
            msg_q.append(msg)

        if diff.most_upd_prefixes:
            msg = (
                f"New most updates for same prefix at "
                f"{diff.timestamp} of "
                f"{diff.most_upd_prefixes[0].updates} updates for"
            )
            for mrt_e in diff.most_upd_prefixes:
                msg += f" {mrt_e.prefix}"
            msg_q.append(msg)

        if diff.most_withd_prefixes:
            msg = (
                f"New most withdraws for same prefix at "
                f"{diff.timestamp} of "
                f"{diff.most_withd_prefixes[0].withdraws} withdraws for"
            )
            for mrt_e in diff.most_withd_prefixes:
                msg += f" {mrt_e.prefix}"
            msg_q.append(msg)

        if diff.most_advt_origin_asn:
            msg = (
                f"New most advertisements for same origin ASN at "
                f"{diff.timestamp} of "
                f"{diff.most_advt_origin_asn[0].advertisements} advertisements by"
            )
            for mrt_e in diff.most_advt_origin_asn:
                for asn in mrt_e.origin_asns:
                    msg += f" {asn}"
            msg_q.append(msg)


        if diff.most_advt_peer_asn:
            msg = (
                f"New most advertisements for same peer ASN at "
                f"{diff.timestamp} of "
                f"{diff.most_advt_peer_asn[0].advertisements} advertisements by "
            )
            for mrt_e in diff.most_advt_peer_asn:
                for asn in mrt_e.peer_asn:
                    msg += f" {asn}"
            msg_q.append(msg)

        if diff.most_upd_peer_asn:
            msg = (
                f"New most updates for same peer ASN at "
                f"{diff.timestamp} of "
                f"{diff.most_upd_peer_asn[0].updates} updates by"
            )
            for mrt_e in diff.most_upd_peer_asn:
                for asn in mrt_e.peer_asn:
                    msg += f" {asn}"
            msg_q.append(msg)

        if diff.most_withd_peer_asn:
            msg = (
                f"New most withdraws for same peer ASN at "
                f"{diff.timestamp} of "
                f"{diff.most_withd_peer_asn[0].withdraws} withdraws by"
            )
            for mrt_e in diff.most_withd_peer_asn:
                for asn in mrt_e.peer_asn:
                    msg += f" {asn}"
            msg_q.append(msg)

        if diff.most_origin_asns:
            msg = (
                f"New most origin ASNs for same prefix at "
                f"{diff.timestamp} of "
                f"{len(diff.most_origin_asns[0].origin_asns)} different ASNs for"
            )
            for mrt_e in diff.most_origin_asns:
                msg += f" {mrt_e.prefix}:"
                for asn in mrt_e.origin_asns:
                    msg += f" {asn}"
            msg_q.append(msg)

    print(msg_q)


    """
    for diff_key in diff_keys:
        rdb.delete(diff_key)
    """

if __name__ == '__main__':
    main()