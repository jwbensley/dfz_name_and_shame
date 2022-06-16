import json
import os
import re
import shutil
import sys
import unittest

sys.path.append(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__))
        , "../"
    )
)
from dnas.mrt_archives import mrt_archives
from dnas.mrt_stats import mrt_stats
from dnas.mrt_entry import mrt_entry
from dnas.mrt_parser import mrt_parser

class test_mrt_stats(unittest.TestCase):

    def setUp(self):
        """
        Copy the test files to the location they would be in,
        if we had downloaded them from the public archives:
        """
        self.upd_1_fn = "rcc23.updates.20220421.0200.gz"
        self.upd_2_fn = "rcc23.updates.20220501.2305.gz"
        self.upd_3_fn = "sydney.updates.20220601.0230.bz2"
        self.upd_4_fn = "sydney.updates.20220601.0415.bz2"

        self.upd_1_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), self.upd_1_fn
        )
        if not os.path.isfile(self.upd_1_path):
            raise Exception(f"Test MRT file is not found: {self.upd_1_path}")

        self.upd_2_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), self.upd_2_fn
        )
        if not os.path.isfile(self.upd_2_path):
            raise Exception(f"Test MRT file is not found: {self.upd_2_path}")

        self.upd_3_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), self.upd_3_fn
        )
        if not os.path.isfile(self.upd_3_path):
            raise Exception(f"Test MRT file is not found: {self.upd_3_path}")

        self.upd_4_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), self.upd_4_fn
        )
        if not os.path.isfile(self.upd_4_path):
            raise Exception(f"Test MRT file is not found: {self.upd_4_path}")

        mrt_a = mrt_archives()
        for arch in mrt_a.archives:
            if arch.NAME == "RCC_23":
                os.makedirs(arch.MRT_DIR, exist_ok=True)
                self.upd_1_mrt = os.path.join(arch.MRT_DIR, self.upd_1_fn)
                self.upd_2_mrt = os.path.join(arch.MRT_DIR, self.upd_2_fn)
            if arch.NAME == "RV_SYDNEY":
                os.makedirs(arch.MRT_DIR, exist_ok=True)
                self.upd_3_mrt = os.path.join(arch.MRT_DIR, self.upd_3_fn)
                self.upd_4_mrt = os.path.join(arch.MRT_DIR, self.upd_4_fn)

        shutil.copy2(self.upd_1_path, self.upd_1_mrt)
        shutil.copy2(self.upd_2_path, self.upd_2_mrt)
        shutil.copy2(self.upd_3_path, self.upd_3_mrt)
        shutil.copy2(self.upd_4_path, self.upd_4_mrt)

        self.upd_1_stats = mrt_parser.parse_upd_dump(self.upd_1_mrt)
        self.upd_2_stats = mrt_parser.parse_upd_dump(self.upd_2_mrt)
        self.upd_3_stats = mrt_parser.parse_upd_dump(self.upd_3_mrt)
        self.upd_4_stats = mrt_parser.parse_upd_dump(self.upd_4_mrt)

        self.assertIsInstance(self.upd_1_stats, mrt_stats)
        self.assertIsInstance(self.upd_2_stats, mrt_stats)
        self.assertIsInstance(self.upd_3_stats, mrt_stats)
        self.assertIsInstance(self.upd_4_stats, mrt_stats)

        self.upd_1_json = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            self.upd_1_fn + ".json"
        )
        if not os.path.isfile(self.upd_1_json):
            raise Exception(
                f"Test stats JSON dump is not found: {self.upd_1_json}"
            )

        self.upd_3_json = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            self.upd_3_fn + ".json"
        )
        if not os.path.isfile(self.upd_3_json):
            raise Exception(
                f"Test stats JSON dump is not found: {self.upd_3_json}"
            )

        self.upd_1_test = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "rcc23.updates.20220421.0200.gz.test"
        )

    def test_init(self):
        mrt_s = mrt_stats()
        self.assertIsInstance(mrt_s, mrt_stats)
        self.assertIsInstance(mrt_s.bogon_origin_asns, list)
        self.assertEqual(len(mrt_s.bogon_origin_asns), 0)
        self.assertIsInstance(mrt_s.bogon_prefixes, list)
        self.assertEqual(len(mrt_s.bogon_prefixes), 0)
        self.assertIsInstance(mrt_s.longest_as_path, list)
        self.assertEqual(len(mrt_s.longest_as_path), 0)
        self.assertIsInstance(mrt_s.longest_comm_set, list)
        self.assertEqual(len(mrt_s.longest_comm_set), 0)
        self.assertIsInstance(mrt_s.invalid_len, list)
        self.assertEqual(len(mrt_s.invalid_len), 0)
        self.assertIsInstance(mrt_s.most_advt_prefixes, list)
        self.assertEqual(len(mrt_s.most_advt_prefixes), 0)
        self.assertIsInstance(mrt_s.most_bogon_asns, list)
        self.assertEqual(len(mrt_s.most_bogon_asns), 0)
        self.assertIsInstance(mrt_s.most_upd_prefixes, list)
        self.assertEqual(len(mrt_s.most_upd_prefixes), 0)
        self.assertIsInstance(mrt_s.most_withd_prefixes, list)
        self.assertEqual(len(mrt_s.most_withd_prefixes), 0)
        self.assertIsInstance(mrt_s.most_advt_origin_asn, list)
        self.assertEqual(len(mrt_s.most_advt_origin_asn), 0)
        self.assertIsInstance(mrt_s.most_advt_peer_asn, list)
        self.assertEqual(len(mrt_s.most_advt_peer_asn), 0)
        self.assertIsInstance(mrt_s.most_upd_peer_asn, list)
        self.assertEqual(len(mrt_s.most_upd_peer_asn), 0)
        self.assertIsInstance(mrt_s.most_withd_peer_asn, list)
        self.assertEqual(len(mrt_s.most_withd_peer_asn), 0)
        self.assertIsInstance(mrt_s.most_origin_asns, list)
        self.assertEqual(len(mrt_s.most_origin_asns), 0)
        self.assertIsInstance(mrt_s.file_list, list)
        self.assertEqual(len(mrt_s.file_list), 0)
        self.assertIsInstance(mrt_s.timestamp, str)
        self.assertEqual(mrt_s.timestamp, "")
        self.assertIsInstance(mrt_s.total_upd, int)
        self.assertEqual(mrt_s.total_upd, 0)
        self.assertIsInstance(mrt_s.total_advt, int)
        self.assertEqual(mrt_s.total_advt, 0)
        self.assertIsInstance(mrt_s.total_withd, int)
        self.assertEqual(mrt_s.total_withd, 0)

    def test_add(self):
        add_stats_1 = mrt_parser.parse_upd_dump(self.upd_1_mrt)
        self.assertIsInstance(add_stats_1, mrt_stats)

        self.assertRaises(ValueError, add_stats_1.add, None)
        self.assertRaises(TypeError, add_stats_1.add, 123)
        ret = add_stats_1.add(self.upd_2_stats)
        self.assertIsInstance(ret, bool)
        self.assertTrue(ret)

        add_stats_3 = mrt_parser.parse_upd_dump(self.upd_3_mrt)
        add_stats_3.add(self.upd_4_stats)

        self.assertEqual(len(add_stats_1.bogon_origin_asns), 1)
        self.assertEqual(add_stats_1.bogon_origin_asns[0].advt, 0)
        self.assertEqual(
            add_stats_1.bogon_origin_asns[0].as_path,
            ["137409", "17494", "137491", "58689", "137464", "65551"]
        )
        self.assertEqual(add_stats_1.bogon_origin_asns[0].comm_set, [])
        self.assertEqual(
            os.path.basename(add_stats_1.bogon_origin_asns[0].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(add_stats_1.bogon_origin_asns[0].next_hop, "27.111.228.145")
        self.assertEqual(add_stats_1.bogon_origin_asns[0].origin_asns, set(["65551"]))
        self.assertEqual(add_stats_1.bogon_origin_asns[0].peer_asn, "137409")
        self.assertEqual(add_stats_1.bogon_origin_asns[0].prefix, "103.109.236.0/24")
        self.assertEqual(add_stats_1.bogon_origin_asns[0].timestamp, "20220421.0200")
        self.assertEqual(add_stats_1.bogon_origin_asns[0].updates, 0)
        self.assertEqual(add_stats_1.bogon_origin_asns[0].withdraws, 0)

        self.assertEqual(len(add_stats_1.bogon_prefixes), 2)
        self.assertEqual(add_stats_1.bogon_prefixes[0].advt, 0)
        self.assertEqual(add_stats_1.bogon_prefixes[0].as_path, ["136168"])
        self.assertEqual(add_stats_1.bogon_prefixes[0].comm_set, [])
        self.assertEqual(
            os.path.basename(add_stats_1.bogon_prefixes[0].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(add_stats_1.bogon_prefixes[0].next_hop, "27.111.228.170")
        self.assertEqual(add_stats_1.bogon_prefixes[0].origin_asns, set(["136168"]))
        self.assertEqual(add_stats_1.bogon_prefixes[0].peer_asn, "136168")
        self.assertEqual(add_stats_1.bogon_prefixes[0].prefix, "100.96.200.3/32")
        self.assertEqual(add_stats_1.bogon_prefixes[0].timestamp, "20220421.0201")
        self.assertEqual(add_stats_1.bogon_prefixes[0].updates, 0)
        self.assertEqual(add_stats_1.bogon_prefixes[0].withdraws, 0)

        self.assertEqual(add_stats_1.bogon_prefixes[1].advt, 0)
        self.assertEqual(add_stats_1.bogon_prefixes[1].as_path, ["133210", "6939"])
        self.assertEqual(add_stats_1.bogon_prefixes[1].comm_set, [])
        self.assertEqual(
            os.path.basename(add_stats_1.bogon_prefixes[1].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(add_stats_1.bogon_prefixes[1].next_hop, "27.111.228.81")
        self.assertEqual(add_stats_1.bogon_prefixes[1].origin_asns, set(["6939"]))
        self.assertEqual(add_stats_1.bogon_prefixes[1].peer_asn, "133210")
        self.assertEqual(add_stats_1.bogon_prefixes[1].prefix, "192.88.99.0/24")
        self.assertEqual(add_stats_1.bogon_prefixes[1].timestamp, "20220501.2309")
        self.assertEqual(add_stats_1.bogon_prefixes[1].updates, 0)
        self.assertEqual(add_stats_1.bogon_prefixes[1].withdraws, 0)

        self.assertEqual(len(add_stats_1.longest_as_path), 1)
        self.assertEqual(add_stats_1.longest_as_path[0].advt, 0)
        self.assertEqual(
            add_stats_1.longest_as_path[0].as_path,
            [
                "18106", "23106", "264228", "264228", "264228", "264228",
                "264228", "264228", "264228", "264228", "264228", "264228",
                "264228", "264228", "264228", "264228", "264228", "264228",
                "264228", "264228", "264228", "264228", "264228", "264228",
                "264228", "264228", "264228"
            ]
        )
        self.assertEqual(add_stats_1.longest_as_path[0].comm_set,["13538:3000"])
        self.assertEqual(
            os.path.basename(add_stats_1.longest_as_path[0].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(
            add_stats_1.longest_as_path[0].next_hop,
            ["2001:de8:4::1:8106:1", "fe80::bac2:53ff:fedb:2004"]
        )
        self.assertEqual(add_stats_1.longest_as_path[0].origin_asns, set(["264228"]))
        self.assertEqual(add_stats_1.longest_as_path[0].peer_asn, "18106")
        self.assertEqual(add_stats_1.longest_as_path[0].prefix, "2804:2488::/48")
        self.assertEqual(add_stats_1.longest_as_path[0].timestamp, "20220421.0200")
        self.assertEqual(add_stats_1.longest_as_path[0].updates, 0)
        self.assertEqual(add_stats_1.longest_as_path[0].withdraws, 0)

        self.assertEqual(len(add_stats_1.longest_comm_set), 4)
        self.assertIsInstance(add_stats_1.longest_comm_set[0], mrt_entry)
        self.assertEqual(add_stats_1.longest_comm_set[0].advt, 0)
        self.assertEqual(
            add_stats_1.longest_comm_set[0].as_path,
            ["133210", "24482", "57463", "61568", "53202", "53202", "53202",
            "53202", "53202", "52543"]
        )
        self.assertEqual(
            add_stats_1.longest_comm_set[0].comm_set,
            [
                "17:1132", "24482:2", "24482:12040", "24482:12042",
                "53202:1001", "53202:11012", "53202:12010", "53202:14010",
                "53202:14020", "53202:14030", "53202:14040", "53202:14050",
                "53202:14060", "53202:14070", "53202:14080", "53202:14090",
                "53202:14100", "53202:14110", "53202:21012", "57463:57463",
                "61568:110", "61568:120", "61568:150", "61568:160", "61568:170",
                "61568:180", "61568:999", "61568:2080", "61568:2090",
                "61568:65001", "64700:61568", "65400:0", "65400:65400",
                "57463:0:1120", "57463:0:5408", "57463:0:6461", "57463:0:6663",
                "57463:0:6762", "57463:0:6830", "57463:0:6939", "57463:0:8657",
                "57463:0:8757", "57463:0:8763", "57463:0:10906",
                "57463:0:11284", "57463:0:11644", "57463:0:12989",
                "57463:0:13237", "57463:0:14840", "57463:0:20562",
                "57463:0:21574", "57463:0:22356", "57463:0:22381",
                "57463:0:22822", "57463:0:28186", "57463:0:28260",
                "57463:0:28330", "57463:0:28663", "57463:0:32787",
                "57463:0:33891", "57463:0:36351", "57463:0:37100",
                "57463:0:37468", "57463:0:43350", "57463:0:45474",
                "57463:0:52320", "57463:0:52551", "57463:0:52866",
                "57463:0:52937", "57463:0:53162", "57463:0:58453",
                "57463:0:61568", "57463:0:61832", "57463:0:262354",
                "57463:0:262589", "57463:0:262773", "57463:0:262807",
                "57463:0:263009", "57463:0:263276", "57463:0:263324",
                "57463:0:263421", "57463:0:263626", "57463:0:265187",
                "57463:0:267056", "57463:0:267613", "57463:0:268331",
                "57463:0:268696"
            ]
        )
        self.assertEqual(add_stats_1.longest_comm_set[0].filename, self.upd_1_mrt)
        self.assertEqual(
            add_stats_1.longest_comm_set[0].next_hop,
            ["2001:de8:4::13:3210:1", "fe80::8ae6:4b00:6c1:6029"]
        )
        self.assertEqual(add_stats_1.longest_comm_set[0].origin_asns, set(["52543"]))
        self.assertEqual(add_stats_1.longest_comm_set[0].peer_asn, "133210")
        self.assertEqual(add_stats_1.longest_comm_set[0].prefix, "2804:e14:8000::/34")
        self.assertEqual(add_stats_1.longest_comm_set[0].timestamp, "20220421.0201")
        self.assertEqual(add_stats_1.longest_comm_set[0].updates, 0)
        self.assertEqual(add_stats_1.longest_comm_set[0].withdraws, 0)

        self.assertIsInstance(add_stats_1.longest_comm_set[1], mrt_entry)
        self.assertEqual(add_stats_1.longest_comm_set[1].advt, 0)
        self.assertEqual(
            add_stats_1.longest_comm_set[1].as_path,
            ["133210", "24482", "57463", "61568", "53202", "53202", "53202",
            "53202", "53202", "52543"]
        )
        self.assertEqual(
            add_stats_1.longest_comm_set[1].comm_set,
            [
                "17:1132", "24482:2", "24482:12040", "24482:12042",
                "53202:1001", "53202:11012", "53202:12010", "53202:14010",
                "53202:14020", "53202:14030", "53202:14040", "53202:14050",
                "53202:14060", "53202:14070", "53202:14080", "53202:14090",
                "53202:14100", "53202:14110", "53202:21012", "57463:57463",
                "61568:110", "61568:120", "61568:150", "61568:160", "61568:170",
                "61568:180", "61568:999", "61568:2080", "61568:2090",
                "61568:65001", "64700:61568", "65400:0", "65400:65400",
                "57463:0:1120", "57463:0:5408", "57463:0:6461", "57463:0:6663",
                "57463:0:6762", "57463:0:6830", "57463:0:6939", "57463:0:8657",
                "57463:0:8757", "57463:0:8763", "57463:0:10906",
                "57463:0:11284", "57463:0:11644", "57463:0:12989",
                "57463:0:13237", "57463:0:14840", "57463:0:20562",
                "57463:0:21574", "57463:0:22356", "57463:0:22381",
                "57463:0:22822", "57463:0:28186", "57463:0:28260",
                "57463:0:28330", "57463:0:28663", "57463:0:32787",
                "57463:0:33891", "57463:0:36351", "57463:0:37100",
                "57463:0:37468", "57463:0:43350", "57463:0:45474",
                "57463:0:52320", "57463:0:52551", "57463:0:52866",
                "57463:0:52937", "57463:0:53162", "57463:0:58453",
                "57463:0:61568", "57463:0:61832", "57463:0:262354",
                "57463:0:262589", "57463:0:262773", "57463:0:262807",
                "57463:0:263009", "57463:0:263276", "57463:0:263324",
                "57463:0:263421", "57463:0:263626", "57463:0:265187",
                "57463:0:267056", "57463:0:267613", "57463:0:268331",
                "57463:0:268696"
            ]
        )
        self.assertEqual(add_stats_1.longest_comm_set[1].filename, self.upd_1_mrt)
        self.assertEqual(
            add_stats_1.longest_comm_set[1].next_hop,
            ["2001:de8:4::13:3210:1", "fe80::8ae6:4b00:6c1:6029"]
        )
        self.assertEqual(add_stats_1.longest_comm_set[1].origin_asns, set(["52543"]))
        self.assertEqual(add_stats_1.longest_comm_set[1].peer_asn, "133210")
        self.assertEqual(add_stats_1.longest_comm_set[1].prefix, "2804:e14:c000::/34")
        self.assertEqual(add_stats_1.longest_comm_set[1].timestamp, "20220421.0201")
        self.assertEqual(add_stats_1.longest_comm_set[1].updates, 0)
        self.assertEqual(add_stats_1.longest_comm_set[1].withdraws, 0)

        self.assertIsInstance(add_stats_1.longest_comm_set[2], mrt_entry)
        self.assertEqual(add_stats_1.longest_comm_set[2].advt, 0)
        self.assertEqual(
            add_stats_1.longest_comm_set[2].as_path,
            ["133210", "24482", "57463", "61568", "53202", "53202", "53202",
            "53202", "53202", "52543"]
        )
        self.assertEqual(
            add_stats_1.longest_comm_set[2].comm_set,
            [
                "17:1132", "24482:2", "24482:12040", "24482:12042",
                "53202:1001", "53202:11012", "53202:12010", "53202:14010",
                "53202:14020", "53202:14030", "53202:14040", "53202:14050",
                "53202:14060", "53202:14070", "53202:14080", "53202:14090",
                "53202:14100", "53202:14110", "53202:21012", "57463:57463",
                "61568:110", "61568:120", "61568:150", "61568:160", "61568:170",
                "61568:180", "61568:999", "61568:2080", "61568:2090",
                "61568:65001", "64700:61568", "65400:0", "65400:65400",
                "57463:0:1120", "57463:0:5408", "57463:0:6461", "57463:0:6663",
                "57463:0:6762", "57463:0:6830", "57463:0:6939", "57463:0:8657",
                "57463:0:8757", "57463:0:8763", "57463:0:10906",
                "57463:0:11284", "57463:0:11644", "57463:0:12989",
                "57463:0:13237", "57463:0:14840", "57463:0:20562",
                "57463:0:21574", "57463:0:22356", "57463:0:22381",
                "57463:0:22822", "57463:0:28186", "57463:0:28260",
                "57463:0:28330", "57463:0:28663", "57463:0:32787",
                "57463:0:33891", "57463:0:36351", "57463:0:37100",
                "57463:0:37468", "57463:0:43350", "57463:0:45474",
                "57463:0:52320", "57463:0:52551", "57463:0:52866",
                "57463:0:52937", "57463:0:53162", "57463:0:58453",
                "57463:0:61568", "57463:0:61832", "57463:0:262354",
                "57463:0:262589", "57463:0:262773", "57463:0:262807",
                "57463:0:263009", "57463:0:263276", "57463:0:263324",
                "57463:0:263421", "57463:0:263626", "57463:0:265187",
                "57463:0:267056", "57463:0:267613", "57463:0:268331",
                "57463:0:268696"
            ]
        )
        self.assertEqual(add_stats_1.longest_comm_set[2].filename, self.upd_1_mrt)
        self.assertEqual(
            add_stats_1.longest_comm_set[2].next_hop,
            ["2001:de8:4::13:3210:1", "fe80::8ae6:4b00:6c1:6029"]
        )
        self.assertEqual(add_stats_1.longest_comm_set[2].origin_asns, set(["52543"]))
        self.assertEqual(add_stats_1.longest_comm_set[2].peer_asn, "133210")
        self.assertEqual(add_stats_1.longest_comm_set[2].prefix, "2804:e14::/34")
        self.assertEqual(add_stats_1.longest_comm_set[2].timestamp, "20220421.0201")
        self.assertEqual(add_stats_1.longest_comm_set[2].updates, 0)
        self.assertEqual(add_stats_1.longest_comm_set[2].withdraws, 0)

        self.assertIsInstance(add_stats_1.longest_comm_set[3], mrt_entry)
        self.assertEqual(add_stats_1.longest_comm_set[3].advt, 0)
        self.assertEqual(
            add_stats_1.longest_comm_set[3].as_path,
            ["133210", "24482", "57463", "61568", "53202", "53202", "53202",
            "53202", "53202", "52543"]
        )
        self.assertEqual(
            add_stats_1.longest_comm_set[3].comm_set,
            [
                "17:1132", "24482:2", "24482:12040", "24482:12042",
                "53202:1001", "53202:11012", "53202:12010", "53202:14010",
                "53202:14020", "53202:14030", "53202:14040", "53202:14050",
                "53202:14060", "53202:14070", "53202:14080", "53202:14090",
                "53202:14100", "53202:14110", "53202:21012", "57463:57463",
                "61568:110", "61568:120", "61568:150", "61568:160", "61568:170",
                "61568:180", "61568:999", "61568:2080", "61568:2090",
                "61568:65001", "64700:61568", "65400:0", "65400:65400",
                "57463:0:1120", "57463:0:5408", "57463:0:6461", "57463:0:6663",
                "57463:0:6762", "57463:0:6830", "57463:0:6939", "57463:0:8657",
                "57463:0:8757", "57463:0:8763", "57463:0:10906",
                "57463:0:11284", "57463:0:11644", "57463:0:12989",
                "57463:0:13237", "57463:0:14840", "57463:0:20562",
                "57463:0:21574", "57463:0:22356", "57463:0:22381",
                "57463:0:22822", "57463:0:28186", "57463:0:28260",
                "57463:0:28330", "57463:0:28663", "57463:0:32787",
                "57463:0:33891", "57463:0:36351", "57463:0:37100",
                "57463:0:37468", "57463:0:43350", "57463:0:45474",
                "57463:0:52320", "57463:0:52551", "57463:0:52866",
                "57463:0:52937", "57463:0:53162", "57463:0:58453",
                "57463:0:61568", "57463:0:61832", "57463:0:262354",
                "57463:0:262589", "57463:0:262773", "57463:0:262807",
                "57463:0:263009", "57463:0:263276", "57463:0:263324",
                "57463:0:263421", "57463:0:263626", "57463:0:265187",
                "57463:0:267056", "57463:0:267613", "57463:0:268331",
                "57463:0:268696"
            ]
        )
        self.assertEqual(add_stats_1.longest_comm_set[3].filename, self.upd_1_mrt)
        self.assertEqual(
            add_stats_1.longest_comm_set[3].next_hop,
            ["2001:de8:4::13:3210:1", "fe80::8ae6:4b00:6c1:6029"]
        )
        self.assertEqual(add_stats_1.longest_comm_set[3].origin_asns, set(["52543"]))
        self.assertEqual(add_stats_1.longest_comm_set[3].peer_asn, "133210")
        self.assertEqual(add_stats_1.longest_comm_set[3].prefix, "2804:e14:4000::/34")
        self.assertEqual(add_stats_1.longest_comm_set[3].timestamp, "20220421.0201")
        self.assertEqual(add_stats_1.longest_comm_set[3].updates, 0)
        self.assertEqual(add_stats_1.longest_comm_set[3].withdraws, 0)

        self.assertEqual(len(add_stats_1.invalid_len), 8)
        self.assertEqual(add_stats_1.invalid_len[0].advt, 0)
        self.assertEqual(add_stats_1.invalid_len[0].as_path, ["199524", "38082"])
        self.assertEqual(add_stats_1.invalid_len[0].comm_set, [])
        self.assertEqual(
            os.path.basename(add_stats_1.invalid_len[0].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(add_stats_1.invalid_len[0].next_hop, ["2001:de8:4::19:9524:1"])
        self.assertEqual(add_stats_1.invalid_len[0].origin_asns, set(["38082"]))
        self.assertEqual(add_stats_1.invalid_len[0].peer_asn, "199524")
        self.assertEqual(add_stats_1.invalid_len[0].prefix, "2405:4000:800:8::/64")
        self.assertEqual(add_stats_1.invalid_len[0].timestamp, "20220421.0200")
        self.assertEqual(add_stats_1.invalid_len[0].updates, 0)
        self.assertEqual(add_stats_1.invalid_len[0].withdraws, 0)

        self.assertEqual(add_stats_1.invalid_len[1].advt, 0)
        self.assertEqual(
            add_stats_1.invalid_len[1].as_path,
            ["133210", "4788", "38044", "38044", "23736"]
        )
        self.assertEqual(
            add_stats_1.invalid_len[1].comm_set,
            [
                "4788:811", "4788:6300", "4788:6310", "4788:16300",
                "4788:23030", "4788:32011"
            ]
        )
        self.assertEqual(
            os.path.basename(add_stats_1.invalid_len[1].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(
            add_stats_1.invalid_len[1].next_hop,
            ["2001:de8:4::4788:1", "fe80::8ae6:4b00:6c1:6029"]
        )
        self.assertEqual(add_stats_1.invalid_len[1].origin_asns, set(["23736"]))
        self.assertEqual(add_stats_1.invalid_len[1].peer_asn, "133210")
        self.assertEqual(add_stats_1.invalid_len[1].prefix, "2400:7400:0:105::/64")
        self.assertEqual(add_stats_1.invalid_len[1].timestamp, "20220421.0201")
        self.assertEqual(add_stats_1.invalid_len[1].updates, 0)
        self.assertEqual(add_stats_1.invalid_len[1].withdraws, 0)

        self.assertEqual(add_stats_1.invalid_len[2].advt, 0)
        self.assertEqual(
            add_stats_1.invalid_len[2].as_path,
            ["133210", "4788", "38044", "23736"]
        )
        self.assertEqual(
            add_stats_1.invalid_len[2].comm_set,
            [
                "4788:811", "4788:6300", "4788:6310", "4788:16300",
                "4788:23030", "4788:34002"
            ]
        )
        self.assertEqual(
            os.path.basename(add_stats_1.invalid_len[2].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(
            add_stats_1.invalid_len[2].next_hop,
            ["2001:de8:4::4788:1", "fe80::8ae6:4b00:6c1:6029"]
        )
        self.assertEqual(add_stats_1.invalid_len[2].origin_asns, set(["23736"]))
        self.assertEqual(add_stats_1.invalid_len[2].peer_asn, "133210")
        self.assertEqual(add_stats_1.invalid_len[2].prefix, "2400:7400:0:106::/64")
        self.assertEqual(add_stats_1.invalid_len[2].timestamp, "20220421.0201")
        self.assertEqual(add_stats_1.invalid_len[2].updates, 0)
        self.assertEqual(add_stats_1.invalid_len[2].withdraws, 0)

        self.assertEqual(add_stats_1.invalid_len[3].advt, 0)
        self.assertEqual(add_stats_1.invalid_len[3].as_path, ["136168"])
        self.assertEqual(add_stats_1.invalid_len[3].comm_set, [])
        self.assertEqual(
            os.path.basename(add_stats_1.invalid_len[3].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(add_stats_1.invalid_len[3].next_hop, "27.111.228.170")
        self.assertEqual(add_stats_1.invalid_len[3].origin_asns, set(["136168"]))
        self.assertEqual(add_stats_1.invalid_len[3].peer_asn, "136168")
        self.assertEqual(add_stats_1.invalid_len[3].prefix, "100.96.200.3/32")
        self.assertEqual(add_stats_1.invalid_len[3].timestamp, "20220421.0201")
        self.assertEqual(add_stats_1.invalid_len[3].updates, 0)
        self.assertEqual(add_stats_1.invalid_len[3].withdraws, 0)

        self.assertEqual(add_stats_1.invalid_len[4].advt, 0)
        self.assertEqual(
            add_stats_1.invalid_len[4].as_path,
            ["133210", "59318", "59318", "15133"]
        )
        self.assertEqual(
            add_stats_1.invalid_len[4].comm_set, ["15133:4351", "59318:2015"]
        )
        self.assertEqual(
            os.path.basename(add_stats_1.invalid_len[4].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(
            add_stats_1.invalid_len[4].next_hop,
            ["2001:de8:4::13:1207:1", "fe80::8ae6:4b00:6c1:6029"]
        )
        self.assertEqual(add_stats_1.invalid_len[4].origin_asns, set(["15133"]))
        self.assertEqual(add_stats_1.invalid_len[4].peer_asn, "133210")
        self.assertEqual(add_stats_1.invalid_len[4].prefix, "2404:b300:33:1::/64")
        self.assertEqual(add_stats_1.invalid_len[4].timestamp, "20220421.0203")
        self.assertEqual(add_stats_1.invalid_len[4].updates, 0)
        self.assertEqual(add_stats_1.invalid_len[4].withdraws, 0)

        self.assertEqual(add_stats_1.invalid_len[5].advt, 0)
        self.assertEqual(add_stats_1.invalid_len[5].as_path, ["136168"])
        self.assertEqual(add_stats_1.invalid_len[5].comm_set, [])
        self.assertEqual(
            os.path.basename(add_stats_1.invalid_len[5].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(add_stats_1.invalid_len[5].next_hop, "27.111.228.170")
        self.assertEqual(add_stats_1.invalid_len[5].origin_asns, set(["136168"]))
        self.assertEqual(add_stats_1.invalid_len[5].peer_asn, "136168")
        self.assertEqual(add_stats_1.invalid_len[5].prefix, "123.253.228.188/30")
        self.assertEqual(add_stats_1.invalid_len[5].timestamp, "20220421.0204")
        self.assertEqual(add_stats_1.invalid_len[5].updates, 0)
        self.assertEqual(add_stats_1.invalid_len[5].withdraws, 0)

        self.assertEqual(add_stats_1.invalid_len[6].advt, 0)
        self.assertEqual(
            add_stats_1.invalid_len[6].as_path, ["133210", "4788", "54994"]
        )
        self.assertEqual(add_stats_1.invalid_len[6].comm_set, 
            ["4788:801", "4788:810", "4788:6300", "4788:6310"]
        )
        self.assertEqual(
            os.path.basename(add_stats_1.invalid_len[6].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(add_stats_1.invalid_len[6].next_hop,
            ["2001:de8:4::4788:3", "fe80::8ae6:4b00:6c1:6029"]
        )
        self.assertEqual(add_stats_1.invalid_len[6].origin_asns, set(["54994"]))
        self.assertEqual(add_stats_1.invalid_len[6].peer_asn, "133210")
        self.assertEqual(add_stats_1.invalid_len[6].prefix, "2001:e68:20db:10::/64")
        self.assertEqual(add_stats_1.invalid_len[6].timestamp, "20220501.2306")
        self.assertEqual(add_stats_1.invalid_len[6].updates, 0)
        self.assertEqual(add_stats_1.invalid_len[6].withdraws, 0)

        self.assertEqual(add_stats_1.invalid_len[7].advt, 0)
        self.assertEqual(
            add_stats_1.invalid_len[7].as_path,
            ["133210", "4788", "54994"]
        )
        self.assertEqual(add_stats_1.invalid_len[7].comm_set,
            ["4788:801", "4788:810", "4788:6300", "4788:6310"]
        )
        self.assertEqual(
            os.path.basename(add_stats_1.invalid_len[7].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(add_stats_1.invalid_len[7].next_hop,
            ["2001:de8:4::4788:3", "fe80::8ae6:4b00:6c1:6029"]
        )
        self.assertEqual(add_stats_1.invalid_len[7].origin_asns, set(["54994"]))
        self.assertEqual(add_stats_1.invalid_len[7].peer_asn, "133210")
        self.assertEqual(add_stats_1.invalid_len[7].prefix, "2001:e68:20db:11::/64")
        self.assertEqual(add_stats_1.invalid_len[7].timestamp, "20220501.2306")
        self.assertEqual(add_stats_1.invalid_len[7].updates, 0)
        self.assertEqual(add_stats_1.invalid_len[7].withdraws, 0)

        self.assertEqual(len(add_stats_1.most_advt_prefixes), 1)
        self.assertEqual(add_stats_1.most_advt_prefixes[0].advt, 1747)
        self.assertEqual(add_stats_1.most_advt_prefixes[0].as_path, [])
        self.assertEqual(add_stats_1.most_advt_prefixes[0].comm_set, [])
        self.assertEqual(
            os.path.basename(add_stats_1.most_advt_prefixes[0].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(add_stats_1.most_advt_prefixes[0].next_hop, None)
        self.assertEqual(add_stats_1.most_advt_prefixes[0].origin_asns, set())
        self.assertEqual(add_stats_1.most_advt_prefixes[0].peer_asn, None)
        self.assertEqual(add_stats_1.most_advt_prefixes[0].prefix, "89.30.150.0/23")
        self.assertEqual(
            add_stats_1.most_advt_prefixes[0].timestamp, "20220501.2305"
        )
        self.assertEqual(add_stats_1.most_advt_prefixes[0].updates, 0)
        self.assertEqual(add_stats_1.most_advt_prefixes[0].withdraws, 0)

        self.assertEqual(len(add_stats_3.most_bogon_asns), 3)
        self.assertEqual(add_stats_3.most_bogon_asns[0].advt, 0)
        self.assertEqual(add_stats_3.most_bogon_asns[0].as_path, ["6939"])
        self.assertEqual(add_stats_3.most_bogon_asns[0].comm_set, [])
        self.assertEqual(
            os.path.basename(add_stats_3.most_bogon_asns[0].filename),
            os.path.basename(self.upd_3_mrt)
        )
        self.assertEqual(add_stats_3.most_bogon_asns[0].next_hop, None)
        self.assertEqual(add_stats_3.most_bogon_asns[0].origin_asns, set(["23456"]))
        self.assertEqual(add_stats_3.most_bogon_asns[0].peer_asn, None)
        self.assertEqual(add_stats_3.most_bogon_asns[0].prefix, None)
        self.assertEqual(
            add_stats_3.most_bogon_asns[0].timestamp, "20220601.0230"
        )
        self.assertEqual(add_stats_3.most_bogon_asns[0].updates, 0)
        self.assertEqual(add_stats_3.most_bogon_asns[0].withdraws, 0)

        self.assertEqual(add_stats_3.most_bogon_asns[1].advt, 0)
        self.assertEqual(add_stats_3.most_bogon_asns[1].as_path, ["13999"])
        self.assertEqual(add_stats_3.most_bogon_asns[1].comm_set, [])
        self.assertEqual(
            os.path.basename(add_stats_3.most_bogon_asns[1].filename),
            os.path.basename(self.upd_4_mrt)
        )
        self.assertEqual(add_stats_3.most_bogon_asns[1].next_hop, None)
        self.assertEqual(add_stats_3.most_bogon_asns[1].origin_asns, set(["65005"]))
        self.assertEqual(add_stats_3.most_bogon_asns[1].peer_asn, None)
        self.assertEqual(add_stats_3.most_bogon_asns[1].prefix, None)
        self.assertEqual(
            add_stats_3.most_bogon_asns[1].timestamp, "20220601.0415"
        )
        self.assertEqual(add_stats_3.most_bogon_asns[1].updates, 0)
        self.assertEqual(add_stats_3.most_bogon_asns[1].withdraws, 0)


        self.assertEqual(add_stats_3.most_bogon_asns[2].advt, 0)
        self.assertEqual(add_stats_3.most_bogon_asns[2].as_path, ["28210"])
        self.assertEqual(add_stats_3.most_bogon_asns[2].comm_set, [])
        self.assertEqual(
            os.path.basename(add_stats_3.most_bogon_asns[2].filename),
            os.path.basename(self.upd_4_mrt)
        )
        self.assertEqual(add_stats_3.most_bogon_asns[2].next_hop, None)
        self.assertEqual(add_stats_3.most_bogon_asns[2].origin_asns, set(["65530"]))
        self.assertEqual(add_stats_3.most_bogon_asns[2].peer_asn, None)
        self.assertEqual(add_stats_3.most_bogon_asns[2].prefix, None)
        self.assertEqual(
            add_stats_3.most_bogon_asns[2].timestamp, "20220601.0415"
        )
        self.assertEqual(add_stats_3.most_bogon_asns[2].updates, 0)
        self.assertEqual(add_stats_3.most_bogon_asns[2].withdraws, 0)

        self.assertEqual(len(add_stats_1.most_upd_prefixes), 1)
        self.assertEqual(add_stats_1.most_upd_prefixes[0].advt, 0)
        self.assertEqual(add_stats_1.most_upd_prefixes[0].as_path, [])
        self.assertEqual(add_stats_1.most_upd_prefixes[0].comm_set, [])
        self.assertEqual(
            os.path.basename(add_stats_1.most_upd_prefixes[0].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(add_stats_1.most_upd_prefixes[0].next_hop, None)
        self.assertEqual(add_stats_1.most_upd_prefixes[0].origin_asns, set())
        self.assertEqual(add_stats_1.most_upd_prefixes[0].peer_asn, None)
        self.assertEqual(add_stats_1.most_upd_prefixes[0].prefix, "89.30.150.0/23")
        self.assertEqual(add_stats_1.most_upd_prefixes[0].timestamp, "20220501.2305")
        self.assertEqual(add_stats_1.most_upd_prefixes[0].updates, 1782)
        self.assertEqual(add_stats_1.most_upd_prefixes[0].withdraws, 0)

        self.assertEqual(len(add_stats_1.most_withd_prefixes), 1)
        self.assertEqual(add_stats_1.most_withd_prefixes[0].advt, 0)
        self.assertEqual(add_stats_1.most_withd_prefixes[0].as_path, [])
        self.assertEqual(add_stats_1.most_withd_prefixes[0].comm_set, [])
        self.assertEqual(
            os.path.basename(add_stats_1.most_withd_prefixes[0].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(add_stats_1.most_withd_prefixes[0].next_hop, None)
        self.assertEqual(add_stats_1.most_withd_prefixes[0].origin_asns, set())
        self.assertEqual(add_stats_1.most_withd_prefixes[0].peer_asn, None)
        self.assertEqual(
            add_stats_1.most_withd_prefixes[0].prefix, "2a01:9e00:4279::/48"
        )
        self.assertEqual(
            add_stats_1.most_withd_prefixes[0].timestamp, "20220501.2305"
        )
        self.assertEqual(add_stats_1.most_withd_prefixes[0].updates, 0)
        self.assertEqual(add_stats_1.most_withd_prefixes[0].withdraws, 89)

        self.assertEqual(len(add_stats_1.most_advt_origin_asn), 1)
        self.assertEqual(add_stats_1.most_advt_origin_asn[0].advt, 5016)
        self.assertEqual(add_stats_1.most_advt_origin_asn[0].as_path, [])
        self.assertEqual(add_stats_1.most_advt_origin_asn[0].comm_set, [])
        self.assertEqual(
            os.path.basename(add_stats_1.most_advt_origin_asn[0].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(add_stats_1.most_advt_origin_asn[0].next_hop, None)
        self.assertEqual(
            add_stats_1.most_advt_origin_asn[0].origin_asns, set(["20473"])
        )
        self.assertEqual(add_stats_1.most_advt_origin_asn[0].peer_asn, None)
        self.assertEqual(add_stats_1.most_advt_origin_asn[0].prefix, None)
        self.assertEqual(
            add_stats_1.most_advt_origin_asn[0].timestamp, "20220501.2305"
        )
        self.assertEqual(add_stats_1.most_advt_origin_asn[0].updates, 0)
        self.assertEqual(add_stats_1.most_advt_origin_asn[0].withdraws, 0)

        self.assertEqual(len(add_stats_1.most_advt_peer_asn), 1)
        self.assertEqual(add_stats_1.most_advt_peer_asn[0].advt, 21592)
        self.assertEqual(add_stats_1.most_advt_peer_asn[0].as_path, [])
        self.assertEqual(add_stats_1.most_advt_peer_asn[0].comm_set, [])
        self.assertEqual(
            os.path.basename(add_stats_1.most_advt_peer_asn[0].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(add_stats_1.most_advt_peer_asn[0].next_hop, None)
        self.assertEqual(add_stats_1.most_advt_peer_asn[0].origin_asns, set())
        self.assertEqual(add_stats_1.most_advt_peer_asn[0].peer_asn, "18106")
        self.assertEqual(add_stats_1.most_advt_peer_asn[0].prefix, None)
        self.assertEqual(
            add_stats_1.most_advt_peer_asn[0].timestamp, "20220501.2305"
        )
        self.assertEqual(add_stats_1.most_advt_peer_asn[0].updates, 0)
        self.assertEqual(add_stats_1.most_advt_peer_asn[0].withdraws, 0)

        self.assertEqual(len(add_stats_1.most_upd_peer_asn), 1)
        self.assertEqual(add_stats_1.most_upd_peer_asn[0].advt, 0)
        self.assertEqual(add_stats_1.most_upd_peer_asn[0].as_path, [])
        self.assertEqual(add_stats_1.most_upd_peer_asn[0].comm_set, [])
        self.assertEqual(
            os.path.basename(add_stats_1.most_upd_peer_asn[0].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(add_stats_1.most_upd_peer_asn[0].next_hop, None)
        self.assertEqual(add_stats_1.most_upd_peer_asn[0].origin_asns, set())
        self.assertEqual(add_stats_1.most_upd_peer_asn[0].peer_asn, "18106")
        self.assertEqual(add_stats_1.most_upd_peer_asn[0].prefix, None)
        self.assertEqual(
            add_stats_1.most_upd_peer_asn[0].timestamp, "20220501.2305"
        )
        self.assertEqual(add_stats_1.most_upd_peer_asn[0].updates, 21939)
        self.assertEqual(add_stats_1.most_upd_peer_asn[0].withdraws, 0)

        self.assertEqual(len(add_stats_1.most_withd_peer_asn), 1)
        self.assertEqual(add_stats_1.most_withd_peer_asn[0].advt, 0)
        self.assertEqual(add_stats_1.most_withd_peer_asn[0].as_path, [])
        self.assertEqual(add_stats_1.most_withd_peer_asn[0].comm_set, [])
        self.assertEqual(
            os.path.basename(add_stats_1.most_withd_peer_asn[0].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(add_stats_1.most_withd_peer_asn[0].next_hop, None)
        self.assertEqual(add_stats_1.most_withd_peer_asn[0].origin_asns, set())
        self.assertEqual(add_stats_1.most_withd_peer_asn[0].peer_asn, "133210")
        self.assertEqual(add_stats_1.most_withd_peer_asn[0].prefix, None)
        self.assertEqual(
            add_stats_1.most_withd_peer_asn[0].timestamp, "20220421.0200"
        )
        self.assertEqual(add_stats_1.most_withd_peer_asn[0].updates, 0)
        self.assertEqual(add_stats_1.most_withd_peer_asn[0].withdraws, 193)

        self.assertEqual(len(add_stats_1.most_origin_asns), 16)
        self.assertEqual(add_stats_1.most_origin_asns[0].advt, 0)
        self.assertEqual(add_stats_1.most_origin_asns[0].as_path, [])
        self.assertEqual(add_stats_1.most_origin_asns[0].comm_set, [])
        self.assertEqual(
            os.path.basename(add_stats_1.most_origin_asns[0].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(add_stats_1.most_origin_asns[0].next_hop, None)
        self.assertEqual(
            add_stats_1.most_origin_asns[0].origin_asns, set(["61424", "58143"])
        )
        self.assertEqual(add_stats_1.most_origin_asns[0].peer_asn, None)
        self.assertEqual(add_stats_1.most_origin_asns[0].prefix, "5.35.174.0/24")
        self.assertEqual(add_stats_1.most_origin_asns[0].timestamp, "20220421.0200")
        self.assertEqual(add_stats_1.most_origin_asns[0].updates, 0)
        self.assertEqual(add_stats_1.most_origin_asns[0].withdraws, 0)

        self.assertEqual(add_stats_1.most_origin_asns[1].advt, 0)
        self.assertEqual(add_stats_1.most_origin_asns[1].as_path, [])
        self.assertEqual(add_stats_1.most_origin_asns[1].comm_set, [])
        self.assertEqual(
            os.path.basename(add_stats_1.most_origin_asns[1].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(add_stats_1.most_origin_asns[1].next_hop, None)
        self.assertEqual(
            add_stats_1.most_origin_asns[1].origin_asns, set(["28198", "262375"])
        )
        self.assertEqual(add_stats_1.most_origin_asns[1].peer_asn, None)
        self.assertEqual(add_stats_1.most_origin_asns[1].prefix, "177.131.0.0/21")
        self.assertEqual(add_stats_1.most_origin_asns[1].timestamp, "20220421.0200")
        self.assertEqual(add_stats_1.most_origin_asns[1].updates, 0)
        self.assertEqual(add_stats_1.most_origin_asns[1].withdraws, 0)

        self.assertEqual(add_stats_1.most_origin_asns[2].advt, 0)
        self.assertEqual(add_stats_1.most_origin_asns[2].as_path, [])
        self.assertEqual(add_stats_1.most_origin_asns[2].comm_set, [])
        self.assertEqual(
            os.path.basename(add_stats_1.most_origin_asns[2].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(add_stats_1.most_origin_asns[2].next_hop, None)
        self.assertEqual(
            add_stats_1.most_origin_asns[2].origin_asns, set(["396559", "396542"])
        )
        self.assertEqual(add_stats_1.most_origin_asns[2].peer_asn, None)
        self.assertEqual(add_stats_1.most_origin_asns[2].prefix, "2620:74:2a::/48")
        self.assertEqual(add_stats_1.most_origin_asns[2].timestamp, "20220421.0200")
        self.assertEqual(add_stats_1.most_origin_asns[2].updates, 0)
        self.assertEqual(add_stats_1.most_origin_asns[2].withdraws, 0)

        self.assertEqual(add_stats_1.most_origin_asns[3].advt, 0)
        self.assertEqual(add_stats_1.most_origin_asns[3].as_path, [])
        self.assertEqual(add_stats_1.most_origin_asns[3].comm_set, [])
        self.assertEqual(
            os.path.basename(add_stats_1.most_origin_asns[3].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(add_stats_1.most_origin_asns[3].next_hop, None)
        self.assertEqual(
            add_stats_1.most_origin_asns[3].origin_asns, set(["138346", "134382"])
        )
        self.assertEqual(add_stats_1.most_origin_asns[3].peer_asn, None)
        self.assertEqual(add_stats_1.most_origin_asns[3].prefix, "103.88.233.0/24")
        self.assertEqual(add_stats_1.most_origin_asns[3].timestamp, "20220421.0200")
        self.assertEqual(add_stats_1.most_origin_asns[3].updates, 0)
        self.assertEqual(add_stats_1.most_origin_asns[3].withdraws, 0)

        self.assertEqual(add_stats_1.most_origin_asns[4].advt, 0)
        self.assertEqual(add_stats_1.most_origin_asns[4].as_path, [])
        self.assertEqual(add_stats_1.most_origin_asns[4].comm_set, [])
        self.assertEqual(
            os.path.basename(add_stats_1.most_origin_asns[4].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(add_stats_1.most_origin_asns[4].next_hop, None)
        self.assertEqual(
            add_stats_1.most_origin_asns[4].origin_asns, set(["37154", "7420"])
        )
        self.assertEqual(add_stats_1.most_origin_asns[4].peer_asn, None)
        self.assertEqual(add_stats_1.most_origin_asns[4].prefix, "196.46.192.0/19")
        self.assertEqual(add_stats_1.most_origin_asns[4].timestamp, "20220421.0200")
        self.assertEqual(add_stats_1.most_origin_asns[4].updates, 0)
        self.assertEqual(add_stats_1.most_origin_asns[4].withdraws, 0)

        self.assertEqual(add_stats_1.most_origin_asns[5].advt, 0)
        self.assertEqual(add_stats_1.most_origin_asns[5].as_path, [])
        self.assertEqual(add_stats_1.most_origin_asns[5].comm_set, [])
        self.assertEqual(
            os.path.basename(add_stats_1.most_origin_asns[5].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(add_stats_1.most_origin_asns[5].next_hop, None)
        self.assertEqual(
            add_stats_1.most_origin_asns[5].origin_asns, set(["136561", "59362"])
        )
        self.assertEqual(add_stats_1.most_origin_asns[5].peer_asn, None)
        self.assertEqual(add_stats_1.most_origin_asns[5].prefix, "123.253.98.0/23")
        self.assertEqual(add_stats_1.most_origin_asns[5].timestamp, "20220421.0200")
        self.assertEqual(add_stats_1.most_origin_asns[5].updates, 0)
        self.assertEqual(add_stats_1.most_origin_asns[5].withdraws, 0)

        self.assertEqual(add_stats_1.most_origin_asns[6].advt, 0)
        self.assertEqual(add_stats_1.most_origin_asns[6].as_path, [])
        self.assertEqual(add_stats_1.most_origin_asns[6].comm_set, [])
        self.assertEqual(
            os.path.basename(add_stats_1.most_origin_asns[6].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(add_stats_1.most_origin_asns[6].next_hop, None)
        self.assertEqual(
            add_stats_1.most_origin_asns[6].origin_asns, set(["132608", "17806"])
        )
        self.assertEqual(add_stats_1.most_origin_asns[6].peer_asn, None)
        self.assertEqual(add_stats_1.most_origin_asns[6].prefix, "114.130.38.0/24")
        self.assertEqual(add_stats_1.most_origin_asns[6].timestamp, "20220421.0200")
        self.assertEqual(add_stats_1.most_origin_asns[6].updates, 0)
        self.assertEqual(add_stats_1.most_origin_asns[6].withdraws, 0)

        self.assertEqual(add_stats_1.most_origin_asns[7].advt, 0)
        self.assertEqual(add_stats_1.most_origin_asns[7].as_path, [])
        self.assertEqual(add_stats_1.most_origin_asns[7].comm_set, [])
        self.assertEqual(
            os.path.basename(add_stats_1.most_origin_asns[7].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(add_stats_1.most_origin_asns[7].next_hop, None)
        self.assertEqual(
            add_stats_1.most_origin_asns[7].origin_asns, set(["136907", "55990"])
        )
        self.assertEqual(add_stats_1.most_origin_asns[7].peer_asn, None)
        self.assertEqual(add_stats_1.most_origin_asns[7].prefix, "124.71.250.0/24")
        self.assertEqual(add_stats_1.most_origin_asns[7].timestamp, "20220421.0200")
        self.assertEqual(add_stats_1.most_origin_asns[7].updates, 0)
        self.assertEqual(add_stats_1.most_origin_asns[7].withdraws, 0)

        self.assertEqual(add_stats_1.most_origin_asns[8].advt, 0)
        self.assertEqual(add_stats_1.most_origin_asns[8].as_path, [])
        self.assertEqual(add_stats_1.most_origin_asns[8].comm_set, [])
        self.assertEqual(
            os.path.basename(add_stats_1.most_origin_asns[8].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(add_stats_1.most_origin_asns[8].next_hop, None)
        self.assertEqual(
            add_stats_1.most_origin_asns[8].origin_asns, set(["136907", "55990"])
        )
        self.assertEqual(add_stats_1.most_origin_asns[8].peer_asn, None)
        self.assertEqual(add_stats_1.most_origin_asns[8].prefix, "139.9.98.0/24")
        self.assertEqual(add_stats_1.most_origin_asns[8].timestamp, "20220421.0200")
        self.assertEqual(add_stats_1.most_origin_asns[8].updates, 0)
        self.assertEqual(add_stats_1.most_origin_asns[8].withdraws, 0)

        self.assertEqual(add_stats_1.most_origin_asns[9].advt, 0)
        self.assertEqual(add_stats_1.most_origin_asns[9].as_path, [])
        self.assertEqual(add_stats_1.most_origin_asns[9].comm_set, [])
        self.assertEqual(
            os.path.basename(add_stats_1.most_origin_asns[9].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(add_stats_1.most_origin_asns[9].next_hop, None)
        self.assertEqual(
            add_stats_1.most_origin_asns[9].origin_asns, set(["7545", "4739"])
        )
        self.assertEqual(add_stats_1.most_origin_asns[9].peer_asn, None)
        self.assertEqual(add_stats_1.most_origin_asns[9].prefix, "203.19.254.0/24")
        self.assertEqual(add_stats_1.most_origin_asns[9].timestamp, "20220421.0200")
        self.assertEqual(add_stats_1.most_origin_asns[9].updates, 0)
        self.assertEqual(add_stats_1.most_origin_asns[9].withdraws, 0)

        self.assertEqual(add_stats_1.most_origin_asns[10].advt, 0)
        self.assertEqual(add_stats_1.most_origin_asns[10].as_path, [])
        self.assertEqual(add_stats_1.most_origin_asns[10].comm_set, [])
        self.assertEqual(
            os.path.basename(add_stats_1.most_origin_asns[10].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(add_stats_1.most_origin_asns[10].next_hop, None)
        self.assertEqual(
            add_stats_1.most_origin_asns[10].origin_asns, set(["271204", "266181"])
        )
        self.assertEqual(add_stats_1.most_origin_asns[10].peer_asn, None)
        self.assertEqual(add_stats_1.most_origin_asns[10].prefix, "179.49.190.0/23")
        self.assertEqual(add_stats_1.most_origin_asns[10].timestamp, "20220501.2305")
        self.assertEqual(add_stats_1.most_origin_asns[10].updates, 0)
        self.assertEqual(add_stats_1.most_origin_asns[10].withdraws, 0)

        self.assertEqual(add_stats_1.most_origin_asns[11].advt, 0)
        self.assertEqual(add_stats_1.most_origin_asns[11].as_path, [])
        self.assertEqual(add_stats_1.most_origin_asns[11].comm_set, [])
        self.assertEqual(
            os.path.basename(add_stats_1.most_origin_asns[11].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(add_stats_1.most_origin_asns[11].next_hop, None)
        self.assertEqual(
            add_stats_1.most_origin_asns[11].origin_asns, set(["7487", "54396"])
        )
        self.assertEqual(add_stats_1.most_origin_asns[11].peer_asn, None)
        self.assertEqual(add_stats_1.most_origin_asns[11].prefix, "205.197.192.0/21")
        self.assertEqual(add_stats_1.most_origin_asns[11].timestamp, "20220501.2305")
        self.assertEqual(add_stats_1.most_origin_asns[11].updates, 0)
        self.assertEqual(add_stats_1.most_origin_asns[11].withdraws, 0)

        self.assertEqual(add_stats_1.most_origin_asns[12].advt, 0)
        self.assertEqual(add_stats_1.most_origin_asns[12].as_path, [])
        self.assertEqual(add_stats_1.most_origin_asns[12].comm_set, [])
        self.assertEqual(
            os.path.basename(add_stats_1.most_origin_asns[12].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(add_stats_1.most_origin_asns[12].next_hop, None)
        self.assertEqual(
            add_stats_1.most_origin_asns[12].origin_asns, set(["203020", "29802"])
        )
        self.assertEqual(add_stats_1.most_origin_asns[12].peer_asn, None)
        self.assertEqual(add_stats_1.most_origin_asns[12].prefix, "206.123.159.0/24")
        self.assertEqual(add_stats_1.most_origin_asns[12].timestamp, "20220501.2305")
        self.assertEqual(add_stats_1.most_origin_asns[12].updates, 0)
        self.assertEqual(add_stats_1.most_origin_asns[12].withdraws, 0)

        self.assertEqual(add_stats_1.most_origin_asns[13].advt, 0)
        self.assertEqual(add_stats_1.most_origin_asns[13].as_path, [])
        self.assertEqual(add_stats_1.most_origin_asns[13].comm_set, [])
        self.assertEqual(
            os.path.basename(add_stats_1.most_origin_asns[13].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(add_stats_1.most_origin_asns[13].next_hop, None)
        self.assertEqual(
            add_stats_1.most_origin_asns[13].origin_asns, set(["52000", "19318"])
        )
        self.assertEqual(add_stats_1.most_origin_asns[13].peer_asn, None)
        self.assertEqual(add_stats_1.most_origin_asns[13].prefix, "68.168.210.0/24")
        self.assertEqual(add_stats_1.most_origin_asns[13].timestamp, "20220501.2305")
        self.assertEqual(add_stats_1.most_origin_asns[13].updates, 0)
        self.assertEqual(add_stats_1.most_origin_asns[13].withdraws, 0)

        self.assertEqual(add_stats_1.most_origin_asns[14].advt, 0)
        self.assertEqual(add_stats_1.most_origin_asns[14].as_path, [])
        self.assertEqual(add_stats_1.most_origin_asns[14].comm_set, [])
        self.assertEqual(
            os.path.basename(add_stats_1.most_origin_asns[14].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(add_stats_1.most_origin_asns[14].next_hop, None)
        self.assertEqual(
            add_stats_1.most_origin_asns[14].origin_asns, set(["55020", "137951"])
        )
        self.assertEqual(add_stats_1.most_origin_asns[14].peer_asn, None)
        self.assertEqual(add_stats_1.most_origin_asns[14].prefix, "156.241.128.0/22")
        self.assertEqual(add_stats_1.most_origin_asns[14].timestamp, "20220501.2305")
        self.assertEqual(add_stats_1.most_origin_asns[14].updates, 0)
        self.assertEqual(add_stats_1.most_origin_asns[14].withdraws, 0)

        self.assertEqual(add_stats_1.most_origin_asns[15].advt, 0)
        self.assertEqual(add_stats_1.most_origin_asns[15].as_path, [])
        self.assertEqual(add_stats_1.most_origin_asns[15].comm_set, [])
        self.assertEqual(
            os.path.basename(add_stats_1.most_origin_asns[15].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(add_stats_1.most_origin_asns[15].next_hop, None)
        self.assertEqual(
            add_stats_1.most_origin_asns[15].origin_asns, set(["269208", "268347"])
        )
        self.assertEqual(add_stats_1.most_origin_asns[15].peer_asn, None)
        self.assertEqual(add_stats_1.most_origin_asns[15].prefix, "2804:610c::/32")
        self.assertEqual(add_stats_1.most_origin_asns[15].timestamp, "20220501.2305")
        self.assertEqual(add_stats_1.most_origin_asns[15].updates, 0)
        self.assertEqual(add_stats_1.most_origin_asns[15].withdraws, 0)

        self.assertEqual(add_stats_1.total_upd, 57245)
        self.assertEqual(add_stats_1.total_advt, 56752)
        self.assertEqual(add_stats_1.total_withd, 1837)
        self.assertEqual(
            sorted([os.path.basename(file) for file in add_stats_1.file_list]),
            sorted(
                [os.path.basename(self.upd_1_mrt),
                os.path.basename(self.upd_2_mrt)]
            )
        )
        self.assertEqual(add_stats_1.timestamp, "20220501.2305")

    def test_equal_to(self):
        stats = mrt_parser.parse_upd_dump(self.upd_1_mrt)

        self.assertRaises(ValueError, stats.equal_to, None)
        self.assertRaises(TypeError, stats.equal_to, "abc")
        self.assertTrue(stats.equal_to(self.upd_1_stats))
        self.assertFalse(stats.equal_to(self.upd_2_stats))

    def test_from_file(self):
        stats = mrt_stats()
        self.assertRaises(ValueError, stats.from_file, None)
        self.assertRaises(TypeError, stats.from_file, 123)
        self.assertRaises(OSError, stats.from_file, "/2f98h3fwfh4fwp")

        stats.from_file(self.upd_1_json)
        self.assertIsInstance(stats, mrt_stats)
        self.assertTrue(self.upd_1_stats.equal_to(stats))

        stats.from_file(self.upd_3_json)
        self.assertIsInstance(stats, mrt_stats)
        self.assertTrue(self.upd_3_stats.equal_to(stats))

    def test_from_json(self):
        stats = mrt_stats()
        self.assertRaises(ValueError, stats.from_json, None)
        self.assertRaises(TypeError, stats.from_json, 123)

        f = open(self.upd_1_json, "r")
        stats.from_json(f.read())
        f.close()

        self.assertIsInstance(stats, mrt_stats)
        self.assertTrue(stats.equal_to(self.upd_1_stats))

    def test_gen_ts_from_ymd(self):
        self.assertRaises(ValueError, self.upd_1_stats.gen_ts_from_ymd, None)
        self.assertRaises(TypeError, self.upd_1_stats.gen_ts_from_ymd, 123)
        ret = self.upd_1_stats.gen_ts_from_ymd("20220228")
        self.assertIsInstance(ret, str)
        """
        No MRTs available from before 1999, and I assume this code won't be
        running in 2030, I'm a realist :(
        """
        self.assertTrue(
            re.match(
                r"^(1999|20[0-2][0-9])(0[1-9]|1[0-2])(0[1-9]|[1-2][0-9]|3[0-1])\.0000$",
                ret
            )
        )

    def test_gen_daily_key(self):
        self.assertRaises(ValueError, self.upd_1_stats.gen_daily_key, None)
        self.assertRaises(TypeError, self.upd_1_stats.gen_daily_key, 123)
        ret = self.upd_1_stats.gen_daily_key("20220228")
        self.assertIsInstance(ret, str)
        self.assertEqual(ret, "DAILY:20220228")

    def test_gen_diff_key(self):
        self.assertRaises(ValueError, self.upd_1_stats.gen_diff_key, None)
        self.assertRaises(TypeError, self.upd_1_stats.gen_diff_key, 123)
        ret = self.upd_1_stats.gen_diff_key("20220228")
        self.assertIsInstance(ret, str)
        self.assertEqual(ret, "DAILY_DIFF:20220228")

    def test_gen_global_key(self):
        ret = self.upd_1_stats.gen_global_key()
        self.assertIsInstance(ret, str)
        self.assertEqual(ret, "GLOBAL")

    def test_get_diff(self):
        self.assertRaises(ValueError, self.upd_1_stats.get_diff, None)
        self.assertRaises(TypeError, self.upd_1_stats.get_diff, 123)

        diff_1 = self.upd_1_stats.get_diff(self.upd_2_stats)
        self.assertIsInstance(diff_1, mrt_stats)

        diff_3 = self.upd_3_stats.get_diff(self.upd_4_stats)
        self.assertIsInstance(diff_3, mrt_stats)

        self.assertEqual(len(diff_1.bogon_prefixes), 1)
        self.assertEqual(diff_1.bogon_prefixes[0].advt, 0)
        self.assertEqual(diff_1.bogon_prefixes[0].as_path, ["133210", "6939"])
        self.assertEqual(diff_1.bogon_prefixes[0].comm_set, [])
        self.assertEqual(
            os.path.basename(diff_1.bogon_prefixes[0].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(diff_1.bogon_prefixes[0].next_hop, "27.111.228.81")
        self.assertEqual(diff_1.bogon_prefixes[0].origin_asns, set(["6939"]))
        self.assertEqual(diff_1.bogon_prefixes[0].peer_asn, "133210")
        self.assertEqual(diff_1.bogon_prefixes[0].prefix, "192.88.99.0/24")
        self.assertEqual(diff_1.bogon_prefixes[0].timestamp, "20220501.2309")
        self.assertEqual(diff_1.bogon_prefixes[0].updates, 0)
        self.assertEqual(diff_1.bogon_prefixes[0].withdraws, 0)

        self.assertEqual(len(diff_1.longest_as_path), 1)
        self.assertEqual(diff_1.longest_as_path[0].advt, 0)
        self.assertEqual(
            diff_1.longest_as_path[0].as_path,
            [
                "18106", "23106", "264228", "264228", "264228", "264228",
                "264228", "264228", "264228", "264228", "264228", "264228",
                "264228", "264228", "264228", "264228", "264228", "264228",
                "264228", "264228", "264228", "264228", "264228", "264228",
                "264228", "264228", "264228"
            ]
        )
        self.assertEqual(diff_1.longest_as_path[0].comm_set,["13538:3000"])
        self.assertEqual(
            os.path.basename(diff_1.longest_as_path[0].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(
            diff_1.longest_as_path[0].next_hop,
            ["2001:de8:4::1:8106:1", "fe80::bac2:53ff:fedb:2004"]
        )
        self.assertEqual(diff_1.longest_as_path[0].origin_asns, set(["264228"]))
        self.assertEqual(diff_1.longest_as_path[0].peer_asn, "18106")
        self.assertEqual(diff_1.longest_as_path[0].prefix, "2804:2488::/48")
        self.assertEqual(diff_1.longest_as_path[0].timestamp, "20220501.2305")
        self.assertEqual(diff_1.longest_as_path[0].updates, 0)
        self.assertEqual(diff_1.longest_as_path[0].withdraws, 0)

        self.assertEqual(len(diff_1.longest_comm_set), 4)


        self.assertIsInstance(diff_1.longest_comm_set[0], mrt_entry)
        self.assertEqual(diff_1.longest_comm_set[0].advt, 0)
        self.assertEqual(
            diff_1.longest_comm_set[0].as_path,
            ["18106", "57463", "61568", "268267"]
        )
        self.assertEqual(
            diff_1.longest_comm_set[0].comm_set,
            [
                "13538:3000", "57463:0:1120", "57463:0:5408", "57463:0:6461",
                "57463:0:6663", "57463:0:6762", "57463:0:6830", "57463:0:6939",
                "57463:0:8657", "57463:0:8757", "57463:0:8763", "57463:0:10906",
                "57463:0:11284", "57463:0:11644", "57463:0:12989", "57463:0:13237",
                "57463:0:14840", "57463:0:20562", "57463:0:21574", "57463:0:22356",
                "57463:0:22381", "57463:0:22822", "57463:0:28186", "57463:0:28260",
                "57463:0:28330", "57463:0:28663", "57463:0:32787", "57463:0:33891",
                "57463:0:36351", "57463:0:37100", "57463:0:37468", "57463:0:43350",
                "57463:0:45474", "57463:0:52320", "57463:0:52551", "57463:0:52866",
                "57463:0:52937", "57463:0:53162", "57463:0:58453", "57463:0:61568",
                "57463:0:61832", "57463:0:262354", "57463:0:262589",
                "57463:0:262773", "57463:0:262807", "57463:0:263009",
                "57463:0:263276", "57463:0:263324", "57463:0:263421",
                "57463:0:263626", "57463:0:265187", "57463:0:267056",
                "57463:0:267613", "57463:0:268331", "57463:0:268696"
            ]
        )
        self.assertEqual(diff_1.longest_comm_set[0].filename, self.upd_2_mrt)
        self.assertEqual(
            diff_1.longest_comm_set[0].next_hop,
            ["2001:de8:4::1:8106:1", "fe80::bac2:53ff:fedb:2004"]
        )
        self.assertEqual(diff_1.longest_comm_set[0].origin_asns, set(["268267"]))
        self.assertEqual(diff_1.longest_comm_set[0].peer_asn, "18106")
        self.assertEqual(diff_1.longest_comm_set[0].prefix, "2804:4e88::/34")
        self.assertEqual(diff_1.longest_comm_set[0].timestamp, "20220501.2305")
        self.assertEqual(diff_1.longest_comm_set[0].updates, 0)
        self.assertEqual(diff_1.longest_comm_set[0].withdraws, 0)

        self.assertIsInstance(diff_1.longest_comm_set[1], mrt_entry)
        self.assertEqual(diff_1.longest_comm_set[1].advt, 0)
        self.assertEqual(
            diff_1.longest_comm_set[1].as_path,
            ["18106", "57463", "61568", "265080", "270793"]
        )
        self.assertEqual(
            diff_1.longest_comm_set[1].comm_set,
            [
                "13538:3000", "57463:0:1120", "57463:0:5408", "57463:0:6461",
                "57463:0:6663", "57463:0:6762", "57463:0:6830", "57463:0:6939",
                "57463:0:8657", "57463:0:8757", "57463:0:8763", "57463:0:10906",
                "57463:0:11284", "57463:0:11644", "57463:0:12989", "57463:0:13237",
                "57463:0:14840", "57463:0:20562", "57463:0:21574", "57463:0:22356",
                "57463:0:22381", "57463:0:22822", "57463:0:28186", "57463:0:28260",
                "57463:0:28330", "57463:0:28663", "57463:0:32787", "57463:0:33891",
                "57463:0:36351", "57463:0:37100", "57463:0:37468", "57463:0:43350",
                "57463:0:45474", "57463:0:52320", "57463:0:52551", "57463:0:52866",
                "57463:0:52937", "57463:0:53162", "57463:0:58453", "57463:0:61568",
                "57463:0:61832", "57463:0:262354", "57463:0:262589",
                "57463:0:262773", "57463:0:262807", "57463:0:263009",
                "57463:0:263276", "57463:0:263324", "57463:0:263421",
                "57463:0:263626", "57463:0:265187", "57463:0:267056",
                "57463:0:267613", "57463:0:268331", "57463:0:268696"
            ]
        )
        self.assertEqual(diff_1.longest_comm_set[1].filename, self.upd_2_mrt)
        self.assertEqual(
            diff_1.longest_comm_set[1].next_hop,
            ["2001:de8:4::1:8106:1", "fe80::bac2:53ff:fedb:2004"]
        )
        self.assertEqual(diff_1.longest_comm_set[1].origin_asns, set(["270793"]))
        self.assertEqual(diff_1.longest_comm_set[1].peer_asn, "18106")
        self.assertEqual(diff_1.longest_comm_set[1].prefix, "2804:7180:100::/40")
        self.assertEqual(diff_1.longest_comm_set[1].timestamp, "20220501.2305")
        self.assertEqual(diff_1.longest_comm_set[1].updates, 0)
        self.assertEqual(diff_1.longest_comm_set[1].withdraws, 0)

        self.assertIsInstance(diff_1.longest_comm_set[2], mrt_entry)
        self.assertEqual(diff_1.longest_comm_set[2].advt, 0)
        self.assertEqual(
            diff_1.longest_comm_set[2].as_path,
            ["18106", "57463", "61568", "265080", "270793"]
        )
        self.assertEqual(
            diff_1.longest_comm_set[2].comm_set,
            [
                "13538:3000", "57463:0:1120", "57463:0:5408", "57463:0:6461",
                "57463:0:6663", "57463:0:6762", "57463:0:6830", "57463:0:6939",
                "57463:0:8657", "57463:0:8757", "57463:0:8763", "57463:0:10906",
                "57463:0:11284", "57463:0:11644", "57463:0:12989", "57463:0:13237",
                "57463:0:14840", "57463:0:20562", "57463:0:21574", "57463:0:22356",
                "57463:0:22381", "57463:0:22822", "57463:0:28186", "57463:0:28260",
                "57463:0:28330", "57463:0:28663", "57463:0:32787", "57463:0:33891",
                "57463:0:36351", "57463:0:37100", "57463:0:37468", "57463:0:43350",
                "57463:0:45474", "57463:0:52320", "57463:0:52551", "57463:0:52866",
                "57463:0:52937", "57463:0:53162", "57463:0:58453", "57463:0:61568",
                "57463:0:61832", "57463:0:262354", "57463:0:262589",
                "57463:0:262773", "57463:0:262807", "57463:0:263009",
                "57463:0:263276", "57463:0:263324", "57463:0:263421",
                "57463:0:263626", "57463:0:265187", "57463:0:267056",
                "57463:0:267613", "57463:0:268331", "57463:0:268696"
            ]
        )
        self.assertEqual(diff_1.longest_comm_set[2].filename, self.upd_2_mrt)
        self.assertEqual(
            diff_1.longest_comm_set[2].next_hop,
            ["2001:de8:4::1:8106:1", "fe80::bac2:53ff:fedb:2004"]
        )
        self.assertEqual(diff_1.longest_comm_set[2].origin_asns, set(["270793"]))
        self.assertEqual(diff_1.longest_comm_set[2].peer_asn, "18106")
        self.assertEqual(diff_1.longest_comm_set[2].prefix, "2804:7180::/40")
        self.assertEqual(diff_1.longest_comm_set[2].timestamp, "20220501.2305")
        self.assertEqual(diff_1.longest_comm_set[2].updates, 0)
        self.assertEqual(diff_1.longest_comm_set[2].withdraws, 0)

        self.assertIsInstance(diff_1.longest_comm_set[3], mrt_entry)
        self.assertEqual(diff_1.longest_comm_set[3].advt, 0)
        self.assertEqual(
            diff_1.longest_comm_set[3].as_path,
            ["18106", "57463", "61568", "264293", "267429", "267956", "267956",
            "267956", "267956", "267956", "267956", "268182"]
        )
        self.assertEqual(
            diff_1.longest_comm_set[3].comm_set,
            [
                "13538:3000", "57463:0:1120", "57463:0:5408", "57463:0:6461",
                "57463:0:6663", "57463:0:6762", "57463:0:6830", "57463:0:6939",
                "57463:0:8657", "57463:0:8757", "57463:0:8763", "57463:0:10906",
                "57463:0:11284", "57463:0:11644", "57463:0:12989", "57463:0:13237",
                "57463:0:14840", "57463:0:20562", "57463:0:21574", "57463:0:22356",
                "57463:0:22381", "57463:0:22822", "57463:0:28186", "57463:0:28260",
                "57463:0:28330", "57463:0:28663", "57463:0:32787", "57463:0:33891",
                "57463:0:36351", "57463:0:37100", "57463:0:37468", "57463:0:43350",
                "57463:0:45474", "57463:0:52320", "57463:0:52551", "57463:0:52866",
                "57463:0:52937", "57463:0:53162", "57463:0:58453", "57463:0:61568",
                "57463:0:61832", "57463:0:262354", "57463:0:262589",
                "57463:0:262773", "57463:0:262807", "57463:0:263009",
                "57463:0:263276", "57463:0:263324", "57463:0:263421",
                "57463:0:263626", "57463:0:265187", "57463:0:267056",
                "57463:0:267613", "57463:0:268331", "57463:0:268696"
            ]
        )
        self.assertEqual(diff_1.longest_comm_set[3].filename, self.upd_2_mrt)
        self.assertEqual(
            diff_1.longest_comm_set[3].next_hop,
            ["2001:de8:4::1:8106:1", "fe80::bac2:53ff:fedb:2004"]
        )
        self.assertEqual(diff_1.longest_comm_set[3].origin_asns, set(["268182"]))
        self.assertEqual(diff_1.longest_comm_set[3].peer_asn, "18106")
        self.assertEqual(diff_1.longest_comm_set[3].prefix, "2804:5950::/33")
        self.assertEqual(diff_1.longest_comm_set[3].timestamp, "20220501.2305")
        self.assertEqual(diff_1.longest_comm_set[3].updates, 0)
        self.assertEqual(diff_1.longest_comm_set[3].withdraws, 0)

        self.assertEqual(len(diff_1.invalid_len), 6)
        self.assertEqual(diff_1.invalid_len[0].advt, 0)
        self.assertEqual(diff_1.invalid_len[0].as_path, ["199524", "38082"])
        self.assertEqual(diff_1.invalid_len[0].comm_set, [])
        self.assertEqual(
            os.path.basename(diff_1.invalid_len[0].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(diff_1.invalid_len[0].next_hop, ["2001:de8:4::3:8082:1"])
        self.assertEqual(diff_1.invalid_len[0].origin_asns, set(["38082"]))
        self.assertEqual(diff_1.invalid_len[0].peer_asn, "199524")
        self.assertEqual(diff_1.invalid_len[0].prefix, "2405:4000:800:8::/64")
        self.assertEqual(diff_1.invalid_len[0].timestamp, "20220501.2305")
        self.assertEqual(diff_1.invalid_len[0].updates, 0)
        self.assertEqual(diff_1.invalid_len[0].withdraws, 0)

        self.assertEqual(diff_1.invalid_len[1].advt, 0)
        self.assertEqual(
            diff_1.invalid_len[1].as_path,
            ["133210", "59318", "59318", "15133"]
        )
        self.assertEqual(
            diff_1.invalid_len[1].comm_set, ["15133:4351", "59318:2015"]
        )
        self.assertEqual(
            os.path.basename(diff_1.invalid_len[1].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(
            diff_1.invalid_len[1].next_hop,
            ["2001:de8:4::13:1207:1", "fe80::8ae6:4b00:6c1:6029"]
        )
        self.assertEqual(diff_1.invalid_len[1].origin_asns, set(["15133"]))
        self.assertEqual(diff_1.invalid_len[1].peer_asn, "133210")
        self.assertEqual(diff_1.invalid_len[1].prefix, "2404:b300:33:1::/64")
        self.assertEqual(diff_1.invalid_len[1].timestamp, "20220501.2306")
        self.assertEqual(diff_1.invalid_len[1].updates, 0)
        self.assertEqual(diff_1.invalid_len[1].withdraws, 0)

        self.assertEqual(diff_1.invalid_len[2].advt, 0)
        self.assertEqual(diff_1.invalid_len[2].as_path, ["133210", "4788", "54994"])
        self.assertEqual(diff_1.invalid_len[2].comm_set, 
            ["4788:801", "4788:810", "4788:6300", "4788:6310"]
        )
        self.assertEqual(
            os.path.basename(diff_1.invalid_len[2].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(diff_1.invalid_len[2].next_hop,
            ["2001:de8:4::4788:3", "fe80::8ae6:4b00:6c1:6029"]
        )
        self.assertEqual(diff_1.invalid_len[2].origin_asns, set(["54994"]))
        self.assertEqual(diff_1.invalid_len[2].peer_asn, "133210")
        self.assertEqual(diff_1.invalid_len[2].prefix, "2001:e68:20db:10::/64")
        self.assertEqual(diff_1.invalid_len[2].timestamp, "20220501.2306")
        self.assertEqual(diff_1.invalid_len[2].updates, 0)
        self.assertEqual(diff_1.invalid_len[2].withdraws, 0)


        self.assertEqual(diff_1.invalid_len[3].advt, 0)
        self.assertEqual(
            diff_1.invalid_len[3].as_path, ["133210", "4788", "54994"]
        )
        self.assertEqual(diff_1.invalid_len[3].comm_set,
            ["4788:801", "4788:810", "4788:6300", "4788:6310"]
        )
        self.assertEqual(
            os.path.basename(diff_1.invalid_len[3].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(diff_1.invalid_len[3].next_hop,
            ["2001:de8:4::4788:3", "fe80::8ae6:4b00:6c1:6029"]
        )
        self.assertEqual(diff_1.invalid_len[3].origin_asns, set(["54994"]))
        self.assertEqual(diff_1.invalid_len[3].peer_asn, "133210")
        self.assertEqual(diff_1.invalid_len[3].prefix, "2001:e68:20db:11::/64")
        self.assertEqual(diff_1.invalid_len[3].timestamp, "20220501.2306")
        self.assertEqual(diff_1.invalid_len[3].updates, 0)
        self.assertEqual(diff_1.invalid_len[3].withdraws, 0)

        self.assertEqual(diff_1.invalid_len[4].advt, 0)
        self.assertEqual(
            diff_1.invalid_len[4].as_path, ["133210", "4788", "38044", "23736"]
        )
        self.assertEqual(
            diff_1.invalid_len[4].comm_set,
            [
                "4788:811", "4788:6300", "4788:6310", "4788:16300",
                "4788:23030", "4788:34002"
            ]
        )
        self.assertEqual(
            os.path.basename(diff_1.invalid_len[4].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(
            diff_1.invalid_len[4].next_hop,
            ["2001:de8:4::4788:3", "fe80::8ae6:4b00:6c1:6029"]
        )
        self.assertEqual(diff_1.invalid_len[4].origin_asns, set(["23736"]))
        self.assertEqual(diff_1.invalid_len[4].peer_asn, "133210")
        self.assertEqual(diff_1.invalid_len[4].prefix, "2400:7400:0:106::/64")
        self.assertEqual(diff_1.invalid_len[4].timestamp, "20220501.2307")
        self.assertEqual(diff_1.invalid_len[4].updates, 0)
        self.assertEqual(diff_1.invalid_len[4].withdraws, 0)

        self.assertEqual(diff_1.invalid_len[5].advt, 0)
        self.assertEqual(
            diff_1.invalid_len[5].as_path,
            ["133210", "4788", "38044", "38044", "23736"]
        )
        self.assertEqual(
            diff_1.invalid_len[5].comm_set,
            [
                "4788:811", "4788:6300", "4788:6310", "4788:16300",
                "4788:23030", "4788:32011"
            ]
        )
        self.assertEqual(
            os.path.basename(diff_1.invalid_len[5].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(
            diff_1.invalid_len[5].next_hop,
            ["2001:de8:4::4788:3", "fe80::8ae6:4b00:6c1:6029"]
        )
        self.assertEqual(diff_1.invalid_len[5].origin_asns, set(["23736"]))
        self.assertEqual(diff_1.invalid_len[5].peer_asn, "133210")
        self.assertEqual(diff_1.invalid_len[5].prefix, "2400:7400:0:105::/64")
        self.assertEqual(diff_1.invalid_len[5].timestamp, "20220501.2307")
        self.assertEqual(diff_1.invalid_len[5].updates, 0)
        self.assertEqual(diff_1.invalid_len[5].withdraws, 0)

        self.assertEqual(len(diff_1.most_advt_prefixes), 1)
        self.assertEqual(diff_1.most_advt_prefixes[0].advt, 884)
        self.assertEqual(diff_1.most_advt_prefixes[0].as_path, [])
        self.assertEqual(diff_1.most_advt_prefixes[0].comm_set, [])
        self.assertEqual(
            os.path.basename(diff_1.most_advt_prefixes[0].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(diff_1.most_advt_prefixes[0].next_hop, None)
        self.assertEqual(diff_1.most_advt_prefixes[0].origin_asns, set())
        self.assertEqual(diff_1.most_advt_prefixes[0].peer_asn, None)
        self.assertEqual(diff_1.most_advt_prefixes[0].prefix, "89.30.150.0/23")
        self.assertEqual(diff_1.most_advt_prefixes[0].timestamp, "20220501.2305")
        self.assertEqual(diff_1.most_advt_prefixes[0].updates, 0)
        self.assertEqual(diff_1.most_advt_prefixes[0].withdraws, 0)

        self.assertIsInstance(diff_3.most_bogon_asns, list)
        self.assertEqual(len(diff_3.most_bogon_asns), 2)
        self.assertEqual(diff_3.most_bogon_asns[0].advt, 0)
        self.assertEqual(diff_3.most_bogon_asns[0].as_path, ["13999"])
        self.assertEqual(diff_3.most_bogon_asns[0].comm_set, [])
        self.assertEqual(
            os.path.basename(diff_3.most_bogon_asns[0].filename),
            os.path.basename(self.upd_4_mrt)
        )
        self.assertEqual(diff_3.most_bogon_asns[0].next_hop, None)
        self.assertEqual(diff_3.most_bogon_asns[0].origin_asns, set(["65005"]))
        self.assertEqual(diff_3.most_bogon_asns[0].peer_asn, None)
        self.assertEqual(diff_3.most_bogon_asns[0].prefix, None)
        self.assertEqual(
            diff_3.most_bogon_asns[0].timestamp, "20220601.0415"
        )
        self.assertEqual(diff_3.most_bogon_asns[0].updates, 0)
        self.assertEqual(diff_3.most_bogon_asns[0].withdraws, 0)

        self.assertEqual(diff_3.most_bogon_asns[1].advt, 0)
        self.assertEqual(diff_3.most_bogon_asns[1].as_path, ["28210"])
        self.assertEqual(diff_3.most_bogon_asns[1].comm_set, [])
        self.assertEqual(
            os.path.basename(diff_3.most_bogon_asns[1].filename),
            os.path.basename(self.upd_4_mrt)
        )
        self.assertEqual(diff_3.most_bogon_asns[1].next_hop, None)
        self.assertEqual(diff_3.most_bogon_asns[1].origin_asns, set(["65530"]))
        self.assertEqual(diff_3.most_bogon_asns[1].peer_asn, None)
        self.assertEqual(diff_3.most_bogon_asns[1].prefix, None)
        self.assertEqual(
            diff_3.most_bogon_asns[1].timestamp, "20220601.0415"
        )
        self.assertEqual(diff_3.most_bogon_asns[1].updates, 0)
        self.assertEqual(diff_3.most_bogon_asns[1].withdraws, 0)

        self.assertEqual(len(diff_1.most_upd_prefixes), 1)
        self.assertEqual(diff_1.most_upd_prefixes[0].advt, 0)
        self.assertEqual(diff_1.most_upd_prefixes[0].as_path, [])
        self.assertEqual(diff_1.most_upd_prefixes[0].comm_set, [])
        self.assertEqual(
            os.path.basename(diff_1.most_upd_prefixes[0].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(diff_1.most_upd_prefixes[0].next_hop, None)
        self.assertEqual(diff_1.most_upd_prefixes[0].origin_asns, set())
        self.assertEqual(diff_1.most_upd_prefixes[0].peer_asn, None)
        self.assertEqual(diff_1.most_upd_prefixes[0].prefix, "89.30.150.0/23")
        self.assertEqual(diff_1.most_upd_prefixes[0].timestamp, "20220501.2305")
        self.assertEqual(diff_1.most_upd_prefixes[0].updates, 898)
        self.assertEqual(diff_1.most_upd_prefixes[0].withdraws, 0)

        self.assertEqual(len(diff_1.most_withd_prefixes), 1)
        self.assertEqual(diff_1.most_withd_prefixes[0].advt, 0)
        self.assertEqual(diff_1.most_withd_prefixes[0].as_path, [])
        self.assertEqual(diff_1.most_withd_prefixes[0].comm_set, [])
        self.assertEqual(
            os.path.basename(diff_1.most_withd_prefixes[0].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(diff_1.most_withd_prefixes[0].next_hop, None)
        self.assertEqual(diff_1.most_withd_prefixes[0].origin_asns, set())
        self.assertEqual(diff_1.most_withd_prefixes[0].peer_asn, None)
        self.assertEqual(
            diff_1.most_withd_prefixes[0].prefix, "2a01:9e00:4279::/48"
        )
        self.assertEqual(
            diff_1.most_withd_prefixes[0].timestamp, "20220501.2305"
        )
        self.assertEqual(diff_1.most_withd_prefixes[0].updates, 0)
        self.assertEqual(diff_1.most_withd_prefixes[0].withdraws, 89)

        self.assertEqual(len(diff_1.most_advt_origin_asn), 1)
        self.assertEqual(diff_1.most_advt_origin_asn[0].advt, 2628)
        self.assertEqual(diff_1.most_advt_origin_asn[0].as_path, [])
        self.assertEqual(diff_1.most_advt_origin_asn[0].comm_set, [])
        self.assertEqual(
            os.path.basename(diff_1.most_advt_origin_asn[0].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(diff_1.most_advt_origin_asn[0].next_hop, None)
        self.assertEqual(
            diff_1.most_advt_origin_asn[0].origin_asns, set(["20473"])
        )
        self.assertEqual(diff_1.most_advt_origin_asn[0].peer_asn, None)
        self.assertEqual(diff_1.most_advt_origin_asn[0].prefix, None)
        self.assertEqual(
            diff_1.most_advt_origin_asn[0].timestamp, "20220501.2305"
        )
        self.assertEqual(diff_1.most_advt_origin_asn[0].updates, 0)
        self.assertEqual(diff_1.most_advt_origin_asn[0].withdraws, 0)

        self.assertEqual(len(diff_1.most_advt_peer_asn), 1)
        self.assertEqual(diff_1.most_advt_peer_asn[0].advt, 11595)
        self.assertEqual(diff_1.most_advt_peer_asn[0].as_path, [])
        self.assertEqual(diff_1.most_advt_peer_asn[0].comm_set, [])
        self.assertEqual(
            os.path.basename(diff_1.most_advt_peer_asn[0].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(diff_1.most_advt_peer_asn[0].next_hop, None)
        self.assertEqual(diff_1.most_advt_peer_asn[0].origin_asns, set())
        self.assertEqual(diff_1.most_advt_peer_asn[0].peer_asn, "18106")
        self.assertEqual(diff_1.most_advt_peer_asn[0].prefix, None)
        self.assertEqual(diff_1.most_advt_peer_asn[0].timestamp, "20220501.2305")
        self.assertEqual(diff_1.most_advt_peer_asn[0].updates, 0)
        self.assertEqual(diff_1.most_advt_peer_asn[0].withdraws, 0)

        self.assertEqual(len(diff_1.most_upd_peer_asn), 1)
        self.assertEqual(diff_1.most_upd_peer_asn[0].advt, 0)
        self.assertEqual(diff_1.most_upd_peer_asn[0].as_path, [])
        self.assertEqual(diff_1.most_upd_peer_asn[0].comm_set, [])
        self.assertEqual(
            os.path.basename(diff_1.most_upd_peer_asn[0].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(diff_1.most_upd_peer_asn[0].next_hop, None)
        self.assertEqual(diff_1.most_upd_peer_asn[0].origin_asns, set())
        self.assertEqual(diff_1.most_upd_peer_asn[0].peer_asn, "18106")
        self.assertEqual(diff_1.most_upd_peer_asn[0].prefix, None)
        self.assertEqual(diff_1.most_upd_peer_asn[0].timestamp, "20220501.2305")
        self.assertEqual(diff_1.most_upd_peer_asn[0].updates, 11781)
        self.assertEqual(diff_1.most_upd_peer_asn[0].withdraws, 0)

        self.assertEqual(len(diff_1.most_withd_peer_asn), 1)
        self.assertEqual(diff_1.most_withd_peer_asn[0].advt, 0)
        self.assertEqual(diff_1.most_withd_peer_asn[0].as_path, [])
        self.assertEqual(diff_1.most_withd_peer_asn[0].comm_set, [])
        self.assertEqual(
            os.path.basename(diff_1.most_withd_peer_asn[0].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(diff_1.most_withd_peer_asn[0].next_hop, None)
        self.assertEqual(diff_1.most_withd_peer_asn[0].origin_asns, set())
        self.assertEqual(diff_1.most_withd_peer_asn[0].peer_asn, "18106")
        self.assertEqual(diff_1.most_withd_peer_asn[0].prefix, None)
        self.assertEqual(
            diff_1.most_withd_peer_asn[0].timestamp, "20220501.2305"
        )
        self.assertEqual(diff_1.most_withd_peer_asn[0].updates, 0)
        self.assertEqual(diff_1.most_withd_peer_asn[0].withdraws, 186)

        self.assertEqual(len(diff_1.most_origin_asns), 9)
        self.assertEqual(diff_1.most_origin_asns[0].advt, 0)
        self.assertEqual(diff_1.most_origin_asns[0].as_path, [])
        self.assertEqual(diff_1.most_origin_asns[0].comm_set, [])
        self.assertEqual(
            os.path.basename(diff_1.most_origin_asns[0].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(diff_1.most_origin_asns[0].next_hop, None)
        self.assertEqual(
            diff_1.most_origin_asns[0].origin_asns, set(["28198", "262375"])
        )
        self.assertEqual(diff_1.most_origin_asns[0].peer_asn, None)
        self.assertEqual(diff_1.most_origin_asns[0].prefix, "177.131.0.0/21")
        self.assertEqual(diff_1.most_origin_asns[0].timestamp, "20220501.2305")
        self.assertEqual(diff_1.most_origin_asns[0].updates, 0)
        self.assertEqual(diff_1.most_origin_asns[0].withdraws, 0)

        self.assertEqual(diff_1.most_origin_asns[1].advt, 0)
        self.assertEqual(diff_1.most_origin_asns[1].as_path, [])
        self.assertEqual(diff_1.most_origin_asns[1].comm_set, [])
        self.assertEqual(
            os.path.basename(diff_1.most_origin_asns[1].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(diff_1.most_origin_asns[1].next_hop, None)
        self.assertEqual(
            diff_1.most_origin_asns[1].origin_asns, set(["271204", "266181"])
        )
        self.assertEqual(diff_1.most_origin_asns[1].peer_asn, None)
        self.assertEqual(diff_1.most_origin_asns[1].prefix, "179.49.190.0/23")
        self.assertEqual(diff_1.most_origin_asns[1].timestamp, "20220501.2305")
        self.assertEqual(diff_1.most_origin_asns[1].updates, 0)
        self.assertEqual(diff_1.most_origin_asns[1].withdraws, 0)

        self.assertEqual(diff_1.most_origin_asns[2].advt, 0)
        self.assertEqual(diff_1.most_origin_asns[2].as_path, [])
        self.assertEqual(diff_1.most_origin_asns[2].comm_set, [])
        self.assertEqual(
            os.path.basename(diff_1.most_origin_asns[2].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(diff_1.most_origin_asns[2].next_hop, None)
        self.assertEqual(
            diff_1.most_origin_asns[2].origin_asns, set(["396559", "396542"])
        )
        self.assertEqual(diff_1.most_origin_asns[2].peer_asn, None)
        self.assertEqual(diff_1.most_origin_asns[2].prefix, "2620:74:2a::/48")
        self.assertEqual(diff_1.most_origin_asns[2].timestamp, "20220501.2305")
        self.assertEqual(diff_1.most_origin_asns[2].updates, 0)
        self.assertEqual(diff_1.most_origin_asns[2].withdraws, 0)

        self.assertEqual(diff_1.most_origin_asns[3].advt, 0)
        self.assertEqual(diff_1.most_origin_asns[3].as_path, [])
        self.assertEqual(diff_1.most_origin_asns[3].comm_set, [])
        self.assertEqual(
            os.path.basename(diff_1.most_origin_asns[3].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(diff_1.most_origin_asns[3].next_hop, None)
        self.assertEqual(
            diff_1.most_origin_asns[3].origin_asns, set(["37154", "7420"])
        )
        self.assertEqual(diff_1.most_origin_asns[3].peer_asn, None)
        self.assertEqual(diff_1.most_origin_asns[3].prefix, "196.46.192.0/19")
        self.assertEqual(diff_1.most_origin_asns[3].timestamp, "20220501.2305")
        self.assertEqual(diff_1.most_origin_asns[3].updates, 0)
        self.assertEqual(diff_1.most_origin_asns[3].withdraws, 0)

        self.assertEqual(diff_1.most_origin_asns[4].advt, 0)
        self.assertEqual(diff_1.most_origin_asns[4].as_path, [])
        self.assertEqual(diff_1.most_origin_asns[4].comm_set, [])
        self.assertEqual(
            os.path.basename(diff_1.most_origin_asns[4].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(diff_1.most_origin_asns[4].next_hop, None)
        self.assertEqual(
            diff_1.most_origin_asns[4].origin_asns, set(["7487", "54396"])
        )
        self.assertEqual(diff_1.most_origin_asns[4].peer_asn, None)
        self.assertEqual(diff_1.most_origin_asns[4].prefix, "205.197.192.0/21")
        self.assertEqual(diff_1.most_origin_asns[4].timestamp, "20220501.2305")
        self.assertEqual(diff_1.most_origin_asns[4].updates, 0)
        self.assertEqual(diff_1.most_origin_asns[4].withdraws, 0)

        self.assertEqual(diff_1.most_origin_asns[5].advt, 0)
        self.assertEqual(diff_1.most_origin_asns[5].as_path, [])
        self.assertEqual(diff_1.most_origin_asns[5].comm_set, [])
        self.assertEqual(
            os.path.basename(diff_1.most_origin_asns[5].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(diff_1.most_origin_asns[5].next_hop, None)
        self.assertEqual(
            diff_1.most_origin_asns[5].origin_asns, set(["203020", "29802"])
        )
        self.assertEqual(diff_1.most_origin_asns[5].peer_asn, None)
        self.assertEqual(diff_1.most_origin_asns[5].prefix, "206.123.159.0/24")
        self.assertEqual(diff_1.most_origin_asns[5].timestamp, "20220501.2305")
        self.assertEqual(diff_1.most_origin_asns[5].updates, 0)
        self.assertEqual(diff_1.most_origin_asns[5].withdraws, 0)

        self.assertEqual(diff_1.most_origin_asns[6].advt, 0)
        self.assertEqual(diff_1.most_origin_asns[6].as_path, [])
        self.assertEqual(diff_1.most_origin_asns[6].comm_set, [])
        self.assertEqual(
            os.path.basename(diff_1.most_origin_asns[6].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(diff_1.most_origin_asns[6].next_hop, None)
        self.assertEqual(
            diff_1.most_origin_asns[6].origin_asns, set(["52000", "19318"])
        )
        self.assertEqual(diff_1.most_origin_asns[6].peer_asn, None)
        self.assertEqual(diff_1.most_origin_asns[6].prefix, "68.168.210.0/24")
        self.assertEqual(diff_1.most_origin_asns[6].timestamp, "20220501.2305")
        self.assertEqual(diff_1.most_origin_asns[6].updates, 0)
        self.assertEqual(diff_1.most_origin_asns[6].withdraws, 0)

        self.assertEqual(diff_1.most_origin_asns[7].advt, 0)
        self.assertEqual(diff_1.most_origin_asns[7].as_path, [])
        self.assertEqual(diff_1.most_origin_asns[7].comm_set, [])
        self.assertEqual(
            os.path.basename(diff_1.most_origin_asns[7].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(diff_1.most_origin_asns[7].next_hop, None)
        self.assertEqual(
            diff_1.most_origin_asns[7].origin_asns, set(["55020", "137951"])
        )
        self.assertEqual(diff_1.most_origin_asns[7].peer_asn, None)
        self.assertEqual(diff_1.most_origin_asns[7].prefix, "156.241.128.0/22")
        self.assertEqual(diff_1.most_origin_asns[7].timestamp, "20220501.2305")
        self.assertEqual(diff_1.most_origin_asns[7].updates, 0)
        self.assertEqual(diff_1.most_origin_asns[7].withdraws, 0)

        self.assertEqual(diff_1.most_origin_asns[8].advt, 0)
        self.assertEqual(diff_1.most_origin_asns[8].as_path, [])
        self.assertEqual(diff_1.most_origin_asns[8].comm_set, [])
        self.assertEqual(
            os.path.basename(diff_1.most_origin_asns[8].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(diff_1.most_origin_asns[8].next_hop, None)
        self.assertEqual(
            diff_1.most_origin_asns[8].origin_asns, set(["269208", "268347"])
        )
        self.assertEqual(diff_1.most_origin_asns[8].peer_asn, None)
        self.assertEqual(diff_1.most_origin_asns[8].prefix, "2804:610c::/32")
        self.assertEqual(diff_1.most_origin_asns[8].timestamp, "20220501.2305")
        self.assertEqual(diff_1.most_origin_asns[8].updates, 0)
        self.assertEqual(diff_1.most_origin_asns[8].withdraws, 0)

        self.assertEqual(diff_1.total_upd, 29688)
        self.assertEqual(diff_1.total_advt, 29396)
        self.assertEqual(diff_1.total_withd, 950)
        self.assertEqual(diff_1.file_list, [])
        self.assertEqual(diff_1.timestamp, "")

    def test_get_diff_larger(self):
        self.assertRaises(ValueError, self.upd_1_stats.get_diff_larger, None)
        self.assertRaises(TypeError, self.upd_1_stats.get_diff_larger, 123)

        diff_1 = self.upd_1_stats.get_diff_larger(self.upd_2_stats)
        self.assertIsInstance(diff_1, mrt_stats)

        diff_3 = self.upd_3_stats.get_diff_larger(self.upd_4_stats)
        self.assertIsInstance(diff_3, mrt_stats)

        self.assertEqual(len(diff_1.bogon_origin_asns), 0)
        self.assertEqual(diff_1.bogon_origin_asns, [])

        self.assertEqual(len(diff_1.bogon_prefixes), 0)
        self.assertEqual(diff_1.bogon_prefixes, [])

        self.assertEqual(len(diff_1.longest_as_path), 0)
        self.assertEqual(diff_1.longest_as_path, [])

        self.assertEqual(len(diff_1.longest_comm_set), 0)
        self.assertEqual(diff_1.longest_comm_set, [])

        self.assertEqual(len(diff_1.invalid_len), 0)
        self.assertEqual(diff_1.invalid_len, [])

        self.assertEqual(len(diff_1.most_advt_prefixes), 1)
        self.assertEqual(diff_1.most_advt_prefixes[0].advt, 884)
        self.assertEqual(diff_1.most_advt_prefixes[0].as_path, [])
        self.assertEqual(diff_1.most_advt_prefixes[0].comm_set, [])
        self.assertEqual(
            os.path.basename(diff_1.most_advt_prefixes[0].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(diff_1.most_advt_prefixes[0].next_hop, None)
        self.assertEqual(diff_1.most_advt_prefixes[0].origin_asns, set())
        self.assertEqual(diff_1.most_advt_prefixes[0].peer_asn, None)
        self.assertEqual(diff_1.most_advt_prefixes[0].prefix, "89.30.150.0/23")
        self.assertEqual(diff_1.most_advt_prefixes[0].timestamp, "20220501.2305")
        self.assertEqual(diff_1.most_advt_prefixes[0].updates, 0)
        self.assertEqual(diff_1.most_advt_prefixes[0].withdraws, 0)

        self.assertEqual(len(diff_1.most_bogon_asns), 0)
        self.assertEqual(diff_1.most_bogon_asns, [])

        self.assertEqual(len(diff_3.most_bogon_asns), 0)
        self.assertEqual(diff_3.most_bogon_asns, [])

        self.assertEqual(len(diff_1.most_upd_prefixes), 1)
        self.assertEqual(diff_1.most_upd_prefixes[0].advt, 0)
        self.assertEqual(diff_1.most_upd_prefixes[0].as_path, [])
        self.assertEqual(diff_1.most_upd_prefixes[0].comm_set, [])
        self.assertEqual(
            os.path.basename(diff_1.most_upd_prefixes[0].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(diff_1.most_upd_prefixes[0].next_hop, None)
        self.assertEqual(diff_1.most_upd_prefixes[0].origin_asns, set())
        self.assertEqual(diff_1.most_upd_prefixes[0].peer_asn, None)
        self.assertEqual(diff_1.most_upd_prefixes[0].prefix, "89.30.150.0/23")
        self.assertEqual(diff_1.most_upd_prefixes[0].timestamp, "20220501.2305")
        self.assertEqual(diff_1.most_upd_prefixes[0].updates, 898)
        self.assertEqual(diff_1.most_upd_prefixes[0].withdraws, 0)

        self.assertEqual(len(diff_1.most_withd_prefixes), 1)
        self.assertEqual(diff_1.most_withd_prefixes[0].advt, 0)
        self.assertEqual(diff_1.most_withd_prefixes[0].as_path, [])
        self.assertEqual(diff_1.most_withd_prefixes[0].comm_set, [])
        self.assertEqual(
            os.path.basename(diff_1.most_withd_prefixes[0].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(diff_1.most_withd_prefixes[0].next_hop, None)
        self.assertEqual(diff_1.most_withd_prefixes[0].origin_asns, set())
        self.assertEqual(diff_1.most_withd_prefixes[0].peer_asn, None)
        self.assertEqual(
            diff_1.most_withd_prefixes[0].prefix, "2a01:9e00:4279::/48"
        )
        self.assertEqual(
            diff_1.most_withd_prefixes[0].timestamp, "20220501.2305"
        )
        self.assertEqual(diff_1.most_withd_prefixes[0].updates, 0)
        self.assertEqual(diff_1.most_withd_prefixes[0].withdraws, 89)

        self.assertEqual(len(diff_1.most_advt_origin_asn), 1)
        self.assertEqual(diff_1.most_advt_origin_asn[0].advt, 2628)
        self.assertEqual(diff_1.most_advt_origin_asn[0].as_path, [])
        self.assertEqual(diff_1.most_advt_origin_asn[0].comm_set, [])
        self.assertEqual(
            os.path.basename(diff_1.most_advt_origin_asn[0].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(diff_1.most_advt_origin_asn[0].next_hop, None)
        self.assertEqual(
            diff_1.most_advt_origin_asn[0].origin_asns, set(["20473"])
        )
        self.assertEqual(diff_1.most_advt_origin_asn[0].peer_asn, None)
        self.assertEqual(diff_1.most_advt_origin_asn[0].prefix, None)
        self.assertEqual(
            diff_1.most_advt_origin_asn[0].timestamp, "20220501.2305"
        )
        self.assertEqual(diff_1.most_advt_origin_asn[0].updates, 0)
        self.assertEqual(diff_1.most_advt_origin_asn[0].withdraws, 0)

        self.assertEqual(len(diff_1.most_advt_peer_asn), 1)
        self.assertEqual(diff_1.most_advt_peer_asn[0].advt, 11595)
        self.assertEqual(diff_1.most_advt_peer_asn[0].as_path, [])
        self.assertEqual(diff_1.most_advt_peer_asn[0].comm_set, [])
        self.assertEqual(
            os.path.basename(diff_1.most_advt_peer_asn[0].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(diff_1.most_advt_peer_asn[0].next_hop, None)
        self.assertEqual(diff_1.most_advt_peer_asn[0].origin_asns, set())
        self.assertEqual(diff_1.most_advt_peer_asn[0].peer_asn, "18106")
        self.assertEqual(diff_1.most_advt_peer_asn[0].prefix, None)
        self.assertEqual(diff_1.most_advt_peer_asn[0].timestamp, "20220501.2305")
        self.assertEqual(diff_1.most_advt_peer_asn[0].updates, 0)
        self.assertEqual(diff_1.most_advt_peer_asn[0].withdraws, 0)

        self.assertEqual(len(diff_1.most_upd_peer_asn), 1)
        self.assertEqual(diff_1.most_upd_peer_asn[0].advt, 0)
        self.assertEqual(diff_1.most_upd_peer_asn[0].as_path, [])
        self.assertEqual(diff_1.most_upd_peer_asn[0].comm_set, [])
        self.assertEqual(
            os.path.basename(diff_1.most_upd_peer_asn[0].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(diff_1.most_upd_peer_asn[0].next_hop, None)
        self.assertEqual(diff_1.most_upd_peer_asn[0].origin_asns, set())
        self.assertEqual(diff_1.most_upd_peer_asn[0].peer_asn, "18106")
        self.assertEqual(diff_1.most_upd_peer_asn[0].prefix, None)
        self.assertEqual(diff_1.most_upd_peer_asn[0].timestamp, "20220501.2305")
        self.assertEqual(diff_1.most_upd_peer_asn[0].updates, 11781)
        self.assertEqual(diff_1.most_upd_peer_asn[0].withdraws, 0)

        self.assertEqual(len(diff_1.most_withd_peer_asn), 0)
        self.assertEqual(diff_1.most_withd_peer_asn, [])

        self.assertEqual(len(diff_1.most_origin_asns), 0)
        self.assertEqual(diff_1.most_origin_asns, [])

        self.assertEqual(diff_1.total_upd, 29688)
        self.assertEqual(diff_1.total_advt, 29396)
        self.assertEqual(diff_1.total_withd, 950)
        self.assertEqual(diff_1.file_list, [])
        self.assertEqual(diff_1.timestamp, "20220501.2305")

    def test_gen_prev_daily_key(self):
        self.assertRaises(ValueError, self.upd_1_stats.gen_prev_daily_key, None)
        self.assertRaises(TypeError, self.upd_1_stats.gen_prev_daily_key, 123)
        ret = self.upd_1_stats.gen_prev_daily_key("20220301")
        self.assertIsInstance(ret, str)
        self.assertEqual(ret, "DAILY:20220228")

    def test_is_empty(self):
        stats = mrt_stats()
        self.assertTrue(stats.is_empty())
        stats.bogon_origin_asns.append(mrt_entry)
        self.assertFalse(stats.is_empty())

    def test_merge(self):
        stats_1 = mrt_stats()
        stats_1.from_file(self.upd_1_json)

        self.assertRaises(ValueError, stats_1.merge, None)
        self.assertRaises(TypeError, stats_1.merge, 123)

        ret = stats_1.merge(self.upd_2_stats)
        self.assertIsInstance(ret, bool)
        self.assertTrue(ret)

        stats_3 = mrt_stats()
        stats_3.from_file(self.upd_3_json)
        stats_3.merge(self.upd_4_stats)

        self.assertEqual(len(stats_1.bogon_origin_asns), 1)
        self.assertEqual(stats_1.bogon_origin_asns[0].advt, 0)
        self.assertEqual(
            stats_1.bogon_origin_asns[0].as_path,
            ["137409", "17494", "137491", "58689", "137464", "65551"]
        )
        self.assertEqual(stats_1.bogon_origin_asns[0].comm_set, [])
        self.assertEqual(
            os.path.basename(stats_1.bogon_origin_asns[0].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(stats_1.bogon_origin_asns[0].next_hop, "27.111.228.145")
        self.assertEqual(stats_1.bogon_origin_asns[0].origin_asns, set(["65551"]))
        self.assertEqual(stats_1.bogon_origin_asns[0].peer_asn, "137409")
        self.assertEqual(stats_1.bogon_origin_asns[0].prefix, "103.109.236.0/24")
        self.assertEqual(stats_1.bogon_origin_asns[0].timestamp, "20220421.0200")
        self.assertEqual(stats_1.bogon_origin_asns[0].updates, 0)
        self.assertEqual(stats_1.bogon_origin_asns[0].withdraws, 0)

        self.assertEqual(len(stats_1.bogon_prefixes), 2)
        self.assertEqual(stats_1.bogon_prefixes[0].advt, 0)
        self.assertEqual(stats_1.bogon_prefixes[0].as_path, ["136168"])
        self.assertEqual(stats_1.bogon_prefixes[0].comm_set, [])
        self.assertEqual(
            os.path.basename(stats_1.bogon_prefixes[0].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(stats_1.bogon_prefixes[0].next_hop, "27.111.228.170")
        self.assertEqual(stats_1.bogon_prefixes[0].origin_asns, set(["136168"]))
        self.assertEqual(stats_1.bogon_prefixes[0].peer_asn, "136168")
        self.assertEqual(stats_1.bogon_prefixes[0].prefix, "100.96.200.3/32")
        self.assertEqual(stats_1.bogon_prefixes[0].timestamp, "20220421.0201")
        self.assertEqual(stats_1.bogon_prefixes[0].updates, 0)
        self.assertEqual(stats_1.bogon_prefixes[0].withdraws, 0)

        self.assertEqual(stats_1.bogon_prefixes[1].advt, 0)
        self.assertEqual(stats_1.bogon_prefixes[1].as_path, ["133210", "6939"])
        self.assertEqual(stats_1.bogon_prefixes[1].comm_set, [])
        self.assertEqual(
            os.path.basename(stats_1.bogon_prefixes[1].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(stats_1.bogon_prefixes[1].next_hop, "27.111.228.81")
        self.assertEqual(stats_1.bogon_prefixes[1].origin_asns, set(["6939"]))
        self.assertEqual(stats_1.bogon_prefixes[1].peer_asn, "133210")
        self.assertEqual(stats_1.bogon_prefixes[1].prefix, "192.88.99.0/24")
        self.assertEqual(stats_1.bogon_prefixes[1].timestamp, "20220501.2309")
        self.assertEqual(stats_1.bogon_prefixes[1].updates, 0)
        self.assertEqual(stats_1.bogon_prefixes[1].withdraws, 0)

        self.assertEqual(len(stats_1.longest_as_path), 1)
        self.assertEqual(stats_1.longest_as_path[0].advt, 0)
        self.assertEqual(
            stats_1.longest_as_path[0].as_path,
            [
                "18106", "23106", "264228", "264228", "264228", "264228",
                "264228", "264228", "264228", "264228", "264228", "264228",
                "264228", "264228", "264228", "264228", "264228", "264228",
                "264228", "264228", "264228", "264228", "264228", "264228",
                "264228", "264228", "264228"
            ]
        )
        self.assertEqual(stats_1.longest_as_path[0].comm_set,["13538:3000"])
        self.assertEqual(
            os.path.basename(stats_1.longest_as_path[0].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(
            stats_1.longest_as_path[0].next_hop,
            ["2001:de8:4::1:8106:1", "fe80::bac2:53ff:fedb:2004"]
        )
        self.assertEqual(stats_1.longest_as_path[0].origin_asns, set(["264228"]))
        self.assertEqual(stats_1.longest_as_path[0].peer_asn, "18106")
        self.assertEqual(stats_1.longest_as_path[0].prefix, "2804:2488::/48")
        self.assertEqual(stats_1.longest_as_path[0].timestamp, "20220421.0200")
        self.assertEqual(stats_1.longest_as_path[0].updates, 0)
        self.assertEqual(stats_1.longest_as_path[0].withdraws, 0)

        self.assertEqual(len(stats_1.longest_comm_set), 4)
        self.assertIsInstance(stats_1.longest_comm_set[0], mrt_entry)
        self.assertEqual(stats_1.longest_comm_set[0].advt, 0)
        self.assertEqual(
            stats_1.longest_comm_set[0].as_path,
            ["133210", "24482", "57463", "61568", "53202", "53202", "53202",
            "53202", "53202", "52543"]
        )
        self.assertEqual(
            stats_1.longest_comm_set[0].comm_set,
            [
                "17:1132", "24482:2", "24482:12040", "24482:12042",
                "53202:1001", "53202:11012", "53202:12010", "53202:14010",
                "53202:14020", "53202:14030", "53202:14040", "53202:14050",
                "53202:14060", "53202:14070", "53202:14080", "53202:14090",
                "53202:14100", "53202:14110", "53202:21012", "57463:57463",
                "61568:110", "61568:120", "61568:150", "61568:160", "61568:170",
                "61568:180", "61568:999", "61568:2080", "61568:2090",
                "61568:65001", "64700:61568", "65400:0", "65400:65400",
                "57463:0:1120", "57463:0:5408", "57463:0:6461", "57463:0:6663",
                "57463:0:6762", "57463:0:6830", "57463:0:6939", "57463:0:8657",
                "57463:0:8757", "57463:0:8763", "57463:0:10906",
                "57463:0:11284", "57463:0:11644", "57463:0:12989",
                "57463:0:13237", "57463:0:14840", "57463:0:20562",
                "57463:0:21574", "57463:0:22356", "57463:0:22381",
                "57463:0:22822", "57463:0:28186", "57463:0:28260",
                "57463:0:28330", "57463:0:28663", "57463:0:32787",
                "57463:0:33891", "57463:0:36351", "57463:0:37100",
                "57463:0:37468", "57463:0:43350", "57463:0:45474",
                "57463:0:52320", "57463:0:52551", "57463:0:52866",
                "57463:0:52937", "57463:0:53162", "57463:0:58453",
                "57463:0:61568", "57463:0:61832", "57463:0:262354",
                "57463:0:262589", "57463:0:262773", "57463:0:262807",
                "57463:0:263009", "57463:0:263276", "57463:0:263324",
                "57463:0:263421", "57463:0:263626", "57463:0:265187",
                "57463:0:267056", "57463:0:267613", "57463:0:268331",
                "57463:0:268696"
            ]
        )
        self.assertEqual(stats_1.longest_comm_set[0].filename, self.upd_1_mrt)
        self.assertEqual(
            stats_1.longest_comm_set[0].next_hop,
            ["2001:de8:4::13:3210:1", "fe80::8ae6:4b00:6c1:6029"]
        )
        self.assertEqual(stats_1.longest_comm_set[0].origin_asns, set(["52543"]))
        self.assertEqual(stats_1.longest_comm_set[0].peer_asn, "133210")
        self.assertEqual(stats_1.longest_comm_set[0].prefix, "2804:e14:8000::/34")
        self.assertEqual(stats_1.longest_comm_set[0].timestamp, "20220421.0201")
        self.assertEqual(stats_1.longest_comm_set[0].updates, 0)
        self.assertEqual(stats_1.longest_comm_set[0].withdraws, 0)

        self.assertIsInstance(stats_1.longest_comm_set[1], mrt_entry)
        self.assertEqual(stats_1.longest_comm_set[1].advt, 0)
        self.assertEqual(
            stats_1.longest_comm_set[1].as_path,
            ["133210", "24482", "57463", "61568", "53202", "53202", "53202",
            "53202", "53202", "52543"]
        )
        self.assertEqual(
            stats_1.longest_comm_set[1].comm_set,
            [
                "17:1132", "24482:2", "24482:12040", "24482:12042",
                "53202:1001", "53202:11012", "53202:12010", "53202:14010",
                "53202:14020", "53202:14030", "53202:14040", "53202:14050",
                "53202:14060", "53202:14070", "53202:14080", "53202:14090",
                "53202:14100", "53202:14110", "53202:21012", "57463:57463",
                "61568:110", "61568:120", "61568:150", "61568:160", "61568:170",
                "61568:180", "61568:999", "61568:2080", "61568:2090",
                "61568:65001", "64700:61568", "65400:0", "65400:65400",
                "57463:0:1120", "57463:0:5408", "57463:0:6461", "57463:0:6663",
                "57463:0:6762", "57463:0:6830", "57463:0:6939", "57463:0:8657",
                "57463:0:8757", "57463:0:8763", "57463:0:10906",
                "57463:0:11284", "57463:0:11644", "57463:0:12989",
                "57463:0:13237", "57463:0:14840", "57463:0:20562",
                "57463:0:21574", "57463:0:22356", "57463:0:22381",
                "57463:0:22822", "57463:0:28186", "57463:0:28260",
                "57463:0:28330", "57463:0:28663", "57463:0:32787",
                "57463:0:33891", "57463:0:36351", "57463:0:37100",
                "57463:0:37468", "57463:0:43350", "57463:0:45474",
                "57463:0:52320", "57463:0:52551", "57463:0:52866",
                "57463:0:52937", "57463:0:53162", "57463:0:58453",
                "57463:0:61568", "57463:0:61832", "57463:0:262354",
                "57463:0:262589", "57463:0:262773", "57463:0:262807",
                "57463:0:263009", "57463:0:263276", "57463:0:263324",
                "57463:0:263421", "57463:0:263626", "57463:0:265187",
                "57463:0:267056", "57463:0:267613", "57463:0:268331",
                "57463:0:268696"
            ]
        )
        self.assertEqual(stats_1.longest_comm_set[1].filename, self.upd_1_mrt)
        self.assertEqual(
            stats_1.longest_comm_set[1].next_hop,
            ["2001:de8:4::13:3210:1", "fe80::8ae6:4b00:6c1:6029"]
        )
        self.assertEqual(stats_1.longest_comm_set[1].origin_asns, set(["52543"]))
        self.assertEqual(stats_1.longest_comm_set[1].peer_asn, "133210")
        self.assertEqual(stats_1.longest_comm_set[1].prefix, "2804:e14:c000::/34")
        self.assertEqual(stats_1.longest_comm_set[1].timestamp, "20220421.0201")
        self.assertEqual(stats_1.longest_comm_set[1].updates, 0)
        self.assertEqual(stats_1.longest_comm_set[1].withdraws, 0)

        self.assertIsInstance(stats_1.longest_comm_set[2], mrt_entry)
        self.assertEqual(stats_1.longest_comm_set[2].advt, 0)
        self.assertEqual(
            stats_1.longest_comm_set[2].as_path,
            ["133210", "24482", "57463", "61568", "53202", "53202", "53202",
            "53202", "53202", "52543"]
        )
        self.assertEqual(
            stats_1.longest_comm_set[2].comm_set,
            [
                "17:1132", "24482:2", "24482:12040", "24482:12042",
                "53202:1001", "53202:11012", "53202:12010", "53202:14010",
                "53202:14020", "53202:14030", "53202:14040", "53202:14050",
                "53202:14060", "53202:14070", "53202:14080", "53202:14090",
                "53202:14100", "53202:14110", "53202:21012", "57463:57463",
                "61568:110", "61568:120", "61568:150", "61568:160", "61568:170",
                "61568:180", "61568:999", "61568:2080", "61568:2090",
                "61568:65001", "64700:61568", "65400:0", "65400:65400",
                "57463:0:1120", "57463:0:5408", "57463:0:6461", "57463:0:6663",
                "57463:0:6762", "57463:0:6830", "57463:0:6939", "57463:0:8657",
                "57463:0:8757", "57463:0:8763", "57463:0:10906",
                "57463:0:11284", "57463:0:11644", "57463:0:12989",
                "57463:0:13237", "57463:0:14840", "57463:0:20562",
                "57463:0:21574", "57463:0:22356", "57463:0:22381",
                "57463:0:22822", "57463:0:28186", "57463:0:28260",
                "57463:0:28330", "57463:0:28663", "57463:0:32787",
                "57463:0:33891", "57463:0:36351", "57463:0:37100",
                "57463:0:37468", "57463:0:43350", "57463:0:45474",
                "57463:0:52320", "57463:0:52551", "57463:0:52866",
                "57463:0:52937", "57463:0:53162", "57463:0:58453",
                "57463:0:61568", "57463:0:61832", "57463:0:262354",
                "57463:0:262589", "57463:0:262773", "57463:0:262807",
                "57463:0:263009", "57463:0:263276", "57463:0:263324",
                "57463:0:263421", "57463:0:263626", "57463:0:265187",
                "57463:0:267056", "57463:0:267613", "57463:0:268331",
                "57463:0:268696"
            ]
        )
        self.assertEqual(stats_1.longest_comm_set[2].filename, self.upd_1_mrt)
        self.assertEqual(
            stats_1.longest_comm_set[2].next_hop,
            ["2001:de8:4::13:3210:1", "fe80::8ae6:4b00:6c1:6029"]
        )
        self.assertEqual(stats_1.longest_comm_set[2].origin_asns, set(["52543"]))
        self.assertEqual(stats_1.longest_comm_set[2].peer_asn, "133210")
        self.assertEqual(stats_1.longest_comm_set[2].prefix, "2804:e14::/34")
        self.assertEqual(stats_1.longest_comm_set[2].timestamp, "20220421.0201")
        self.assertEqual(stats_1.longest_comm_set[2].updates, 0)
        self.assertEqual(stats_1.longest_comm_set[2].withdraws, 0)

        self.assertIsInstance(stats_1.longest_comm_set[3], mrt_entry)
        self.assertEqual(stats_1.longest_comm_set[3].advt, 0)
        self.assertEqual(
            stats_1.longest_comm_set[3].as_path,
            ["133210", "24482", "57463", "61568", "53202", "53202", "53202",
            "53202", "53202", "52543"]
        )
        self.assertEqual(
            stats_1.longest_comm_set[3].comm_set,
            [
                "17:1132", "24482:2", "24482:12040", "24482:12042",
                "53202:1001", "53202:11012", "53202:12010", "53202:14010",
                "53202:14020", "53202:14030", "53202:14040", "53202:14050",
                "53202:14060", "53202:14070", "53202:14080", "53202:14090",
                "53202:14100", "53202:14110", "53202:21012", "57463:57463",
                "61568:110", "61568:120", "61568:150", "61568:160", "61568:170",
                "61568:180", "61568:999", "61568:2080", "61568:2090",
                "61568:65001", "64700:61568", "65400:0", "65400:65400",
                "57463:0:1120", "57463:0:5408", "57463:0:6461", "57463:0:6663",
                "57463:0:6762", "57463:0:6830", "57463:0:6939", "57463:0:8657",
                "57463:0:8757", "57463:0:8763", "57463:0:10906",
                "57463:0:11284", "57463:0:11644", "57463:0:12989",
                "57463:0:13237", "57463:0:14840", "57463:0:20562",
                "57463:0:21574", "57463:0:22356", "57463:0:22381",
                "57463:0:22822", "57463:0:28186", "57463:0:28260",
                "57463:0:28330", "57463:0:28663", "57463:0:32787",
                "57463:0:33891", "57463:0:36351", "57463:0:37100",
                "57463:0:37468", "57463:0:43350", "57463:0:45474",
                "57463:0:52320", "57463:0:52551", "57463:0:52866",
                "57463:0:52937", "57463:0:53162", "57463:0:58453",
                "57463:0:61568", "57463:0:61832", "57463:0:262354",
                "57463:0:262589", "57463:0:262773", "57463:0:262807",
                "57463:0:263009", "57463:0:263276", "57463:0:263324",
                "57463:0:263421", "57463:0:263626", "57463:0:265187",
                "57463:0:267056", "57463:0:267613", "57463:0:268331",
                "57463:0:268696"
            ]
        )
        self.assertEqual(stats_1.longest_comm_set[3].filename, self.upd_1_mrt)
        self.assertEqual(
            stats_1.longest_comm_set[3].next_hop,
            ["2001:de8:4::13:3210:1", "fe80::8ae6:4b00:6c1:6029"]
        )
        self.assertEqual(stats_1.longest_comm_set[3].origin_asns, set(["52543"]))
        self.assertEqual(stats_1.longest_comm_set[3].peer_asn, "133210")
        self.assertEqual(stats_1.longest_comm_set[3].prefix, "2804:e14:4000::/34")
        self.assertEqual(stats_1.longest_comm_set[3].timestamp, "20220421.0201")
        self.assertEqual(stats_1.longest_comm_set[3].updates, 0)
        self.assertEqual(stats_1.longest_comm_set[3].withdraws, 0)

        self.assertEqual(len(stats_1.invalid_len), 8)
        self.assertEqual(stats_1.invalid_len[0].advt, 0)
        self.assertEqual(stats_1.invalid_len[0].as_path, ["199524", "38082"])
        self.assertEqual(stats_1.invalid_len[0].comm_set, [])
        self.assertEqual(
            os.path.basename(stats_1.invalid_len[0].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(stats_1.invalid_len[0].next_hop, ["2001:de8:4::19:9524:1"])
        self.assertEqual(stats_1.invalid_len[0].origin_asns, set(["38082"]))
        self.assertEqual(stats_1.invalid_len[0].peer_asn, "199524")
        self.assertEqual(stats_1.invalid_len[0].prefix, "2405:4000:800:8::/64")
        self.assertEqual(stats_1.invalid_len[0].timestamp, "20220421.0200")
        self.assertEqual(stats_1.invalid_len[0].updates, 0)
        self.assertEqual(stats_1.invalid_len[0].withdraws, 0)

        self.assertEqual(stats_1.invalid_len[1].advt, 0)
        self.assertEqual(
            stats_1.invalid_len[1].as_path,
            ["133210", "4788", "38044", "38044", "23736"]
        )
        self.assertEqual(
            stats_1.invalid_len[1].comm_set,
            [
                "4788:811", "4788:6300", "4788:6310", "4788:16300",
                "4788:23030", "4788:32011"
            ]
        )
        self.assertEqual(
            os.path.basename(stats_1.invalid_len[1].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(
            stats_1.invalid_len[1].next_hop,
            ["2001:de8:4::4788:1", "fe80::8ae6:4b00:6c1:6029"]
        )
        self.assertEqual(stats_1.invalid_len[1].origin_asns, set(["23736"]))
        self.assertEqual(stats_1.invalid_len[1].peer_asn, "133210")
        self.assertEqual(stats_1.invalid_len[1].prefix, "2400:7400:0:105::/64")
        self.assertEqual(stats_1.invalid_len[1].timestamp, "20220421.0201")
        self.assertEqual(stats_1.invalid_len[1].updates, 0)
        self.assertEqual(stats_1.invalid_len[1].withdraws, 0)

        self.assertEqual(stats_1.invalid_len[2].advt, 0)
        self.assertEqual(
            stats_1.invalid_len[2].as_path,
            ["133210", "4788", "38044", "23736"]
        )
        self.assertEqual(
            stats_1.invalid_len[2].comm_set,
            [
                "4788:811", "4788:6300", "4788:6310", "4788:16300",
                "4788:23030", "4788:34002"
            ]
        )
        self.assertEqual(
            os.path.basename(stats_1.invalid_len[2].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(
            stats_1.invalid_len[2].next_hop,
            ["2001:de8:4::4788:1", "fe80::8ae6:4b00:6c1:6029"]
        )
        self.assertEqual(stats_1.invalid_len[2].origin_asns, set(["23736"]))
        self.assertEqual(stats_1.invalid_len[2].peer_asn, "133210")
        self.assertEqual(stats_1.invalid_len[2].prefix, "2400:7400:0:106::/64")
        self.assertEqual(stats_1.invalid_len[2].timestamp, "20220421.0201")
        self.assertEqual(stats_1.invalid_len[2].updates, 0)
        self.assertEqual(stats_1.invalid_len[2].withdraws, 0)

        self.assertEqual(stats_1.invalid_len[3].advt, 0)
        self.assertEqual(stats_1.invalid_len[3].as_path, ["136168"])
        self.assertEqual(stats_1.invalid_len[3].comm_set, [])
        self.assertEqual(
            os.path.basename(stats_1.invalid_len[3].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(stats_1.invalid_len[3].next_hop, "27.111.228.170")
        self.assertEqual(stats_1.invalid_len[3].origin_asns, set(["136168"]))
        self.assertEqual(stats_1.invalid_len[3].peer_asn, "136168")
        self.assertEqual(stats_1.invalid_len[3].prefix, "100.96.200.3/32")
        self.assertEqual(stats_1.invalid_len[3].timestamp, "20220421.0201")
        self.assertEqual(stats_1.invalid_len[3].updates, 0)
        self.assertEqual(stats_1.invalid_len[3].withdraws, 0)

        self.assertEqual(stats_1.invalid_len[4].advt, 0)
        self.assertEqual(
            stats_1.invalid_len[4].as_path, ["133210", "59318", "59318", "15133"]
        )
        self.assertEqual(
            stats_1.invalid_len[4].comm_set, ["15133:4351", "59318:2015"]
        )
        self.assertEqual(
            os.path.basename(stats_1.invalid_len[4].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(
            stats_1.invalid_len[4].next_hop,
            ["2001:de8:4::13:1207:1", "fe80::8ae6:4b00:6c1:6029"]
        )
        self.assertEqual(stats_1.invalid_len[4].origin_asns, set(["15133"]))
        self.assertEqual(stats_1.invalid_len[4].peer_asn, "133210")
        self.assertEqual(stats_1.invalid_len[4].prefix, "2404:b300:33:1::/64")
        self.assertEqual(stats_1.invalid_len[4].timestamp, "20220421.0203")
        self.assertEqual(stats_1.invalid_len[4].updates, 0)
        self.assertEqual(stats_1.invalid_len[4].withdraws, 0)

        self.assertEqual(stats_1.invalid_len[5].advt, 0)
        self.assertEqual(stats_1.invalid_len[5].as_path, ["136168"])
        self.assertEqual(stats_1.invalid_len[5].comm_set, [])
        self.assertEqual(
            os.path.basename(stats_1.invalid_len[5].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(stats_1.invalid_len[5].next_hop, "27.111.228.170")
        self.assertEqual(stats_1.invalid_len[5].origin_asns, set(["136168"]))
        self.assertEqual(stats_1.invalid_len[5].peer_asn, "136168")
        self.assertEqual(stats_1.invalid_len[5].prefix, "123.253.228.188/30")
        self.assertEqual(stats_1.invalid_len[5].timestamp, "20220421.0204")
        self.assertEqual(stats_1.invalid_len[5].updates, 0)
        self.assertEqual(stats_1.invalid_len[5].withdraws, 0)

        self.assertEqual(stats_1.invalid_len[6].advt, 0)
        self.assertEqual(
            stats_1.invalid_len[6].as_path, ["133210", "4788", "54994"]
        )
        self.assertEqual(stats_1.invalid_len[6].comm_set, 
            ["4788:801", "4788:810", "4788:6300", "4788:6310"]
        )
        self.assertEqual(
            os.path.basename(stats_1.invalid_len[6].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(stats_1.invalid_len[6].next_hop,
            ["2001:de8:4::4788:3", "fe80::8ae6:4b00:6c1:6029"]
        )
        self.assertEqual(stats_1.invalid_len[6].origin_asns, set(["54994"]))
        self.assertEqual(stats_1.invalid_len[6].peer_asn, "133210")
        self.assertEqual(stats_1.invalid_len[6].prefix, "2001:e68:20db:10::/64")
        self.assertEqual(stats_1.invalid_len[6].timestamp, "20220501.2306")
        self.assertEqual(stats_1.invalid_len[6].updates, 0)
        self.assertEqual(stats_1.invalid_len[6].withdraws, 0)

        self.assertEqual(stats_1.invalid_len[7].advt, 0)
        self.assertEqual(
            stats_1.invalid_len[7].as_path,
            ["133210", "4788", "54994"]
        )
        self.assertEqual(stats_1.invalid_len[7].comm_set,
            ["4788:801", "4788:810", "4788:6300", "4788:6310"]
        )
        self.assertEqual(
            os.path.basename(stats_1.invalid_len[7].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(stats_1.invalid_len[7].next_hop,
            ["2001:de8:4::4788:3", "fe80::8ae6:4b00:6c1:6029"]
        )
        self.assertEqual(stats_1.invalid_len[7].origin_asns, set(["54994"]))
        self.assertEqual(stats_1.invalid_len[7].peer_asn, "133210")
        self.assertEqual(stats_1.invalid_len[7].prefix, "2001:e68:20db:11::/64")
        self.assertEqual(stats_1.invalid_len[7].timestamp, "20220501.2306")
        self.assertEqual(stats_1.invalid_len[7].updates, 0)
        self.assertEqual(stats_1.invalid_len[7].withdraws, 0)

        self.assertEqual(len(stats_1.most_advt_prefixes), 1)
        self.assertEqual(stats_1.most_advt_prefixes[0].advt, 884)
        self.assertEqual(stats_1.most_advt_prefixes[0].as_path, [])
        self.assertEqual(stats_1.most_advt_prefixes[0].comm_set, [])
        self.assertEqual(
            os.path.basename(stats_1.most_advt_prefixes[0].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(stats_1.most_advt_prefixes[0].next_hop, None)
        self.assertEqual(stats_1.most_advt_prefixes[0].origin_asns, set())
        self.assertEqual(stats_1.most_advt_prefixes[0].peer_asn, None)
        self.assertEqual(stats_1.most_advt_prefixes[0].prefix, "89.30.150.0/23")
        self.assertEqual(stats_1.most_advt_prefixes[0].timestamp, "20220501.2305")
        self.assertEqual(stats_1.most_advt_prefixes[0].updates, 0)
        self.assertEqual(stats_1.most_advt_prefixes[0].withdraws, 0)

        self.assertEqual(len(stats_3.most_bogon_asns), 3)
        self.assertEqual(stats_3.most_bogon_asns[0].advt, 0)
        self.assertEqual(stats_3.most_bogon_asns[0].as_path, ["6939"])
        self.assertEqual(stats_3.most_bogon_asns[0].comm_set, [])
        self.assertEqual(
            os.path.basename(stats_3.most_bogon_asns[0].filename),
            os.path.basename(self.upd_3_mrt)
        )
        self.assertEqual(stats_3.most_bogon_asns[0].next_hop, None)
        self.assertEqual(stats_3.most_bogon_asns[0].origin_asns, set(["23456"]))
        self.assertEqual(stats_3.most_bogon_asns[0].peer_asn, None)
        self.assertEqual(stats_3.most_bogon_asns[0].prefix, None)
        self.assertEqual(
            stats_3.most_bogon_asns[0].timestamp, "20220601.0230"
        )
        self.assertEqual(stats_3.most_bogon_asns[0].updates, 0)
        self.assertEqual(stats_3.most_bogon_asns[0].withdraws, 0)

        self.assertEqual(stats_3.most_bogon_asns[1].advt, 0)
        self.assertEqual(stats_3.most_bogon_asns[1].as_path, ["13999"])
        self.assertEqual(stats_3.most_bogon_asns[1].comm_set, [])
        self.assertEqual(
            os.path.basename(stats_3.most_bogon_asns[1].filename),
            os.path.basename(self.upd_4_mrt)
        )
        self.assertEqual(stats_3.most_bogon_asns[1].next_hop, None)
        self.assertEqual(stats_3.most_bogon_asns[1].origin_asns, set(["65005"]))
        self.assertEqual(stats_3.most_bogon_asns[1].peer_asn, None)
        self.assertEqual(stats_3.most_bogon_asns[1].prefix, None)
        self.assertEqual(
            stats_3.most_bogon_asns[1].timestamp, "20220601.0415"
        )
        self.assertEqual(stats_3.most_bogon_asns[1].updates, 0)
        self.assertEqual(stats_3.most_bogon_asns[1].withdraws, 0)

        self.assertEqual(stats_3.most_bogon_asns[2].advt, 0)
        self.assertEqual(stats_3.most_bogon_asns[2].as_path, ["28210"])
        self.assertEqual(stats_3.most_bogon_asns[2].comm_set, [])
        self.assertEqual(
            os.path.basename(stats_3.most_bogon_asns[2].filename),
            os.path.basename(self.upd_4_mrt)
        )
        self.assertEqual(stats_3.most_bogon_asns[2].next_hop, None)
        self.assertEqual(stats_3.most_bogon_asns[2].origin_asns, set(["65530"]))
        self.assertEqual(stats_3.most_bogon_asns[2].peer_asn, None)
        self.assertEqual(stats_3.most_bogon_asns[2].prefix, None)
        self.assertEqual(
            stats_3.most_bogon_asns[2].timestamp, "20220601.0415"
        )
        self.assertEqual(stats_3.most_bogon_asns[2].updates, 0)
        self.assertEqual(stats_3.most_bogon_asns[2].withdraws, 0)

        self.assertEqual(len(stats_1.most_upd_prefixes), 1)
        self.assertEqual(stats_1.most_upd_prefixes[0].advt, 0)
        self.assertEqual(stats_1.most_upd_prefixes[0].as_path, [])
        self.assertEqual(stats_1.most_upd_prefixes[0].comm_set, [])
        self.assertEqual(
            os.path.basename(stats_1.most_upd_prefixes[0].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(stats_1.most_upd_prefixes[0].next_hop, None)
        self.assertEqual(stats_1.most_upd_prefixes[0].origin_asns, set())
        self.assertEqual(stats_1.most_upd_prefixes[0].peer_asn, None)
        self.assertEqual(stats_1.most_upd_prefixes[0].prefix, "89.30.150.0/23")
        self.assertEqual(stats_1.most_upd_prefixes[0].timestamp, "20220501.2305")
        self.assertEqual(stats_1.most_upd_prefixes[0].updates, 898)
        self.assertEqual(stats_1.most_upd_prefixes[0].withdraws, 0)

        self.assertEqual(len(stats_1.most_withd_prefixes), 1)
        self.assertEqual(stats_1.most_withd_prefixes[0].advt, 0)
        self.assertEqual(stats_1.most_withd_prefixes[0].as_path, [])
        self.assertEqual(stats_1.most_withd_prefixes[0].comm_set, [])
        self.assertEqual(
            os.path.basename(stats_1.most_withd_prefixes[0].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(stats_1.most_withd_prefixes[0].next_hop, None)
        self.assertEqual(stats_1.most_withd_prefixes[0].origin_asns, set())
        self.assertEqual(stats_1.most_withd_prefixes[0].peer_asn, None)
        self.assertEqual(
            stats_1.most_withd_prefixes[0].prefix, "2a01:9e00:4279::/48"
        )
        self.assertEqual(
            stats_1.most_withd_prefixes[0].timestamp, "20220501.2305"
        )
        self.assertEqual(stats_1.most_withd_prefixes[0].updates, 0)
        self.assertEqual(stats_1.most_withd_prefixes[0].withdraws, 89)

        self.assertEqual(len(stats_1.most_advt_origin_asn), 1)
        self.assertEqual(stats_1.most_advt_origin_asn[0].advt, 2628)
        self.assertEqual(stats_1.most_advt_origin_asn[0].as_path, [])
        self.assertEqual(stats_1.most_advt_origin_asn[0].comm_set, [])
        self.assertEqual(
            os.path.basename(stats_1.most_advt_origin_asn[0].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(stats_1.most_advt_origin_asn[0].next_hop, None)
        self.assertEqual(
            stats_1.most_advt_origin_asn[0].origin_asns, set(["20473"])
        )
        self.assertEqual(stats_1.most_advt_origin_asn[0].peer_asn, None)
        self.assertEqual(stats_1.most_advt_origin_asn[0].prefix, None)
        self.assertEqual(
            stats_1.most_advt_origin_asn[0].timestamp, "20220501.2305"
        )
        self.assertEqual(stats_1.most_advt_origin_asn[0].updates, 0)
        self.assertEqual(stats_1.most_advt_origin_asn[0].withdraws, 0)

        self.assertEqual(len(stats_1.most_advt_peer_asn), 1)
        self.assertEqual(stats_1.most_advt_peer_asn[0].advt, 11595)
        self.assertEqual(stats_1.most_advt_peer_asn[0].as_path, [])
        self.assertEqual(stats_1.most_advt_peer_asn[0].comm_set, [])
        self.assertEqual(
            os.path.basename(stats_1.most_advt_peer_asn[0].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(stats_1.most_advt_peer_asn[0].next_hop, None)
        self.assertEqual(stats_1.most_advt_peer_asn[0].origin_asns, set())
        self.assertEqual(stats_1.most_advt_peer_asn[0].peer_asn, "18106")
        self.assertEqual(stats_1.most_advt_peer_asn[0].prefix, None)
        self.assertEqual(stats_1.most_advt_peer_asn[0].timestamp, "20220501.2305")
        self.assertEqual(stats_1.most_advt_peer_asn[0].updates, 0)
        self.assertEqual(stats_1.most_advt_peer_asn[0].withdraws, 0)

        self.assertEqual(len(stats_1.most_upd_peer_asn), 1)
        self.assertEqual(stats_1.most_upd_peer_asn[0].advt, 0)
        self.assertEqual(stats_1.most_upd_peer_asn[0].as_path, [])
        self.assertEqual(stats_1.most_upd_peer_asn[0].comm_set, [])
        self.assertEqual(
            os.path.basename(stats_1.most_upd_peer_asn[0].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(stats_1.most_upd_peer_asn[0].next_hop, None)
        self.assertEqual(stats_1.most_upd_peer_asn[0].origin_asns, set())
        self.assertEqual(stats_1.most_upd_peer_asn[0].peer_asn, "18106")
        self.assertEqual(stats_1.most_upd_peer_asn[0].prefix, None)
        self.assertEqual(stats_1.most_upd_peer_asn[0].timestamp, "20220501.2305")
        self.assertEqual(stats_1.most_upd_peer_asn[0].updates, 11781)
        self.assertEqual(stats_1.most_upd_peer_asn[0].withdraws, 0)

        self.assertEqual(len(stats_1.most_withd_peer_asn), 1)
        self.assertEqual(stats_1.most_withd_peer_asn[0].advt, 0)
        self.assertEqual(stats_1.most_withd_peer_asn[0].as_path, [])
        self.assertEqual(stats_1.most_withd_peer_asn[0].comm_set, [])
        self.assertEqual(
            os.path.basename(stats_1.most_withd_peer_asn[0].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(stats_1.most_withd_peer_asn[0].next_hop, None)
        self.assertEqual(stats_1.most_withd_peer_asn[0].origin_asns, set())
        self.assertEqual(stats_1.most_withd_peer_asn[0].peer_asn, "133210")
        self.assertEqual(stats_1.most_withd_peer_asn[0].prefix, None)
        self.assertEqual(
            stats_1.most_withd_peer_asn[0].timestamp, "20220421.0200"
        )
        self.assertEqual(stats_1.most_withd_peer_asn[0].updates, 0)
        self.assertEqual(stats_1.most_withd_peer_asn[0].withdraws, 193)

        self.assertEqual(len(stats_1.most_origin_asns), 16)
        self.assertEqual(stats_1.most_origin_asns[0].advt, 0)
        self.assertEqual(stats_1.most_origin_asns[0].as_path, [])
        self.assertEqual(stats_1.most_origin_asns[0].comm_set, [])
        self.assertEqual(
            os.path.basename(stats_1.most_origin_asns[0].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(stats_1.most_origin_asns[0].next_hop, None)
        self.assertEqual(
            stats_1.most_origin_asns[0].origin_asns, set(["61424", "58143"])
        )
        self.assertEqual(stats_1.most_origin_asns[0].peer_asn, None)
        self.assertEqual(stats_1.most_origin_asns[0].prefix, "5.35.174.0/24")
        self.assertEqual(stats_1.most_origin_asns[0].timestamp, "20220421.0200")
        self.assertEqual(stats_1.most_origin_asns[0].updates, 0)
        self.assertEqual(stats_1.most_origin_asns[0].withdraws, 0)

        self.assertEqual(stats_1.most_origin_asns[1].advt, 0)
        self.assertEqual(stats_1.most_origin_asns[1].as_path, [])
        self.assertEqual(stats_1.most_origin_asns[1].comm_set, [])
        self.assertEqual(
            os.path.basename(stats_1.most_origin_asns[1].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(stats_1.most_origin_asns[1].next_hop, None)
        self.assertEqual(
            stats_1.most_origin_asns[1].origin_asns, set(["28198", "262375"])
        )
        self.assertEqual(stats_1.most_origin_asns[1].peer_asn, None)
        self.assertEqual(stats_1.most_origin_asns[1].prefix, "177.131.0.0/21")
        self.assertEqual(stats_1.most_origin_asns[1].timestamp, "20220421.0200")
        self.assertEqual(stats_1.most_origin_asns[1].updates, 0)
        self.assertEqual(stats_1.most_origin_asns[1].withdraws, 0)

        self.assertEqual(stats_1.most_origin_asns[2].advt, 0)
        self.assertEqual(stats_1.most_origin_asns[2].as_path, [])
        self.assertEqual(stats_1.most_origin_asns[2].comm_set, [])
        self.assertEqual(
            os.path.basename(stats_1.most_origin_asns[2].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(stats_1.most_origin_asns[2].next_hop, None)
        self.assertEqual(
            stats_1.most_origin_asns[2].origin_asns, set(["396559", "396542"])
        )
        self.assertEqual(stats_1.most_origin_asns[2].peer_asn, None)
        self.assertEqual(stats_1.most_origin_asns[2].prefix, "2620:74:2a::/48")
        self.assertEqual(stats_1.most_origin_asns[2].timestamp, "20220421.0200")
        self.assertEqual(stats_1.most_origin_asns[2].updates, 0)
        self.assertEqual(stats_1.most_origin_asns[2].withdraws, 0)

        self.assertEqual(stats_1.most_origin_asns[3].advt, 0)
        self.assertEqual(stats_1.most_origin_asns[3].as_path, [])
        self.assertEqual(stats_1.most_origin_asns[3].comm_set, [])
        self.assertEqual(
            os.path.basename(stats_1.most_origin_asns[3].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(stats_1.most_origin_asns[3].next_hop, None)
        self.assertEqual(
            stats_1.most_origin_asns[3].origin_asns, set(["138346", "134382"])
        )
        self.assertEqual(stats_1.most_origin_asns[3].peer_asn, None)
        self.assertEqual(stats_1.most_origin_asns[3].prefix, "103.88.233.0/24")
        self.assertEqual(stats_1.most_origin_asns[3].timestamp, "20220421.0200")
        self.assertEqual(stats_1.most_origin_asns[3].updates, 0)
        self.assertEqual(stats_1.most_origin_asns[3].withdraws, 0)

        self.assertEqual(stats_1.most_origin_asns[4].advt, 0)
        self.assertEqual(stats_1.most_origin_asns[4].as_path, [])
        self.assertEqual(stats_1.most_origin_asns[4].comm_set, [])
        self.assertEqual(
            os.path.basename(stats_1.most_origin_asns[4].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(stats_1.most_origin_asns[4].next_hop, None)
        self.assertEqual(
            stats_1.most_origin_asns[4].origin_asns, set(["37154", "7420"])
        )
        self.assertEqual(stats_1.most_origin_asns[4].peer_asn, None)
        self.assertEqual(stats_1.most_origin_asns[4].prefix, "196.46.192.0/19")
        self.assertEqual(stats_1.most_origin_asns[4].timestamp, "20220421.0200")
        self.assertEqual(stats_1.most_origin_asns[4].updates, 0)
        self.assertEqual(stats_1.most_origin_asns[4].withdraws, 0)

        self.assertEqual(stats_1.most_origin_asns[5].advt, 0)
        self.assertEqual(stats_1.most_origin_asns[5].as_path, [])
        self.assertEqual(stats_1.most_origin_asns[5].comm_set, [])
        self.assertEqual(
            os.path.basename(stats_1.most_origin_asns[5].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(stats_1.most_origin_asns[5].next_hop, None)
        self.assertEqual(
            stats_1.most_origin_asns[5].origin_asns, set(["136561", "59362"])
        )
        self.assertEqual(stats_1.most_origin_asns[5].peer_asn, None)
        self.assertEqual(stats_1.most_origin_asns[5].prefix, "123.253.98.0/23")
        self.assertEqual(stats_1.most_origin_asns[5].timestamp, "20220421.0200")
        self.assertEqual(stats_1.most_origin_asns[5].updates, 0)
        self.assertEqual(stats_1.most_origin_asns[5].withdraws, 0)

        self.assertEqual(stats_1.most_origin_asns[6].advt, 0)
        self.assertEqual(stats_1.most_origin_asns[6].as_path, [])
        self.assertEqual(stats_1.most_origin_asns[6].comm_set, [])
        self.assertEqual(
            os.path.basename(stats_1.most_origin_asns[6].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(stats_1.most_origin_asns[6].next_hop, None)
        self.assertEqual(
            stats_1.most_origin_asns[6].origin_asns, set(["132608", "17806"])
        )
        self.assertEqual(stats_1.most_origin_asns[6].peer_asn, None)
        self.assertEqual(stats_1.most_origin_asns[6].prefix, "114.130.38.0/24")
        self.assertEqual(stats_1.most_origin_asns[6].timestamp, "20220421.0200")
        self.assertEqual(stats_1.most_origin_asns[6].updates, 0)
        self.assertEqual(stats_1.most_origin_asns[6].withdraws, 0)

        self.assertEqual(stats_1.most_origin_asns[7].advt, 0)
        self.assertEqual(stats_1.most_origin_asns[7].as_path, [])
        self.assertEqual(stats_1.most_origin_asns[7].comm_set, [])
        self.assertEqual(
            os.path.basename(stats_1.most_origin_asns[7].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(stats_1.most_origin_asns[7].next_hop, None)
        self.assertEqual(
            stats_1.most_origin_asns[7].origin_asns, set(["136907", "55990"])
        )
        self.assertEqual(stats_1.most_origin_asns[7].peer_asn, None)
        self.assertEqual(stats_1.most_origin_asns[7].prefix, "124.71.250.0/24")
        self.assertEqual(stats_1.most_origin_asns[7].timestamp, "20220421.0200")
        self.assertEqual(stats_1.most_origin_asns[7].updates, 0)
        self.assertEqual(stats_1.most_origin_asns[7].withdraws, 0)

        self.assertEqual(stats_1.most_origin_asns[8].advt, 0)
        self.assertEqual(stats_1.most_origin_asns[8].as_path, [])
        self.assertEqual(stats_1.most_origin_asns[8].comm_set, [])
        self.assertEqual(
            os.path.basename(stats_1.most_origin_asns[8].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(stats_1.most_origin_asns[8].next_hop, None)
        self.assertEqual(
            stats_1.most_origin_asns[8].origin_asns, set(["136907", "55990"])
        )
        self.assertEqual(stats_1.most_origin_asns[8].peer_asn, None)
        self.assertEqual(stats_1.most_origin_asns[8].prefix, "139.9.98.0/24")
        self.assertEqual(stats_1.most_origin_asns[8].timestamp, "20220421.0200")
        self.assertEqual(stats_1.most_origin_asns[8].updates, 0)
        self.assertEqual(stats_1.most_origin_asns[8].withdraws, 0)

        self.assertEqual(stats_1.most_origin_asns[9].advt, 0)
        self.assertEqual(stats_1.most_origin_asns[9].as_path, [])
        self.assertEqual(stats_1.most_origin_asns[9].comm_set, [])
        self.assertEqual(
            os.path.basename(stats_1.most_origin_asns[9].filename),
            os.path.basename(self.upd_1_mrt)
        )
        self.assertEqual(stats_1.most_origin_asns[9].next_hop, None)
        self.assertEqual(
            stats_1.most_origin_asns[9].origin_asns, set(["7545", "4739"])
        )
        self.assertEqual(stats_1.most_origin_asns[9].peer_asn, None)
        self.assertEqual(stats_1.most_origin_asns[9].prefix, "203.19.254.0/24")
        self.assertEqual(stats_1.most_origin_asns[9].timestamp, "20220421.0200")
        self.assertEqual(stats_1.most_origin_asns[9].updates, 0)
        self.assertEqual(stats_1.most_origin_asns[9].withdraws, 0)

        self.assertEqual(stats_1.most_origin_asns[10].advt, 0)
        self.assertEqual(stats_1.most_origin_asns[10].as_path, [])
        self.assertEqual(stats_1.most_origin_asns[10].comm_set, [])
        self.assertEqual(
            os.path.basename(stats_1.most_origin_asns[10].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(stats_1.most_origin_asns[10].next_hop, None)
        self.assertEqual(
            stats_1.most_origin_asns[10].origin_asns, set(["271204", "266181"])
        )
        self.assertEqual(stats_1.most_origin_asns[10].peer_asn, None)
        self.assertEqual(stats_1.most_origin_asns[10].prefix, "179.49.190.0/23")
        self.assertEqual(stats_1.most_origin_asns[10].timestamp, "20220501.2305")
        self.assertEqual(stats_1.most_origin_asns[10].updates, 0)
        self.assertEqual(stats_1.most_origin_asns[10].withdraws, 0)

        self.assertEqual(stats_1.most_origin_asns[11].advt, 0)
        self.assertEqual(stats_1.most_origin_asns[11].as_path, [])
        self.assertEqual(stats_1.most_origin_asns[11].comm_set, [])
        self.assertEqual(
            os.path.basename(stats_1.most_origin_asns[11].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(stats_1.most_origin_asns[11].next_hop, None)
        self.assertEqual(
            stats_1.most_origin_asns[11].origin_asns, set(["7487", "54396"])
        )
        self.assertEqual(stats_1.most_origin_asns[11].peer_asn, None)
        self.assertEqual(stats_1.most_origin_asns[11].prefix, "205.197.192.0/21")
        self.assertEqual(stats_1.most_origin_asns[11].timestamp, "20220501.2305")
        self.assertEqual(stats_1.most_origin_asns[11].updates, 0)
        self.assertEqual(stats_1.most_origin_asns[11].withdraws, 0)

        self.assertEqual(stats_1.most_origin_asns[12].advt, 0)
        self.assertEqual(stats_1.most_origin_asns[12].as_path, [])
        self.assertEqual(stats_1.most_origin_asns[12].comm_set, [])
        self.assertEqual(
            os.path.basename(stats_1.most_origin_asns[12].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(stats_1.most_origin_asns[12].next_hop, None)
        self.assertEqual(
            stats_1.most_origin_asns[12].origin_asns, set(["203020", "29802"])
        )
        self.assertEqual(stats_1.most_origin_asns[12].peer_asn, None)
        self.assertEqual(stats_1.most_origin_asns[12].prefix, "206.123.159.0/24")
        self.assertEqual(stats_1.most_origin_asns[12].timestamp, "20220501.2305")
        self.assertEqual(stats_1.most_origin_asns[12].updates, 0)
        self.assertEqual(stats_1.most_origin_asns[12].withdraws, 0)

        self.assertEqual(stats_1.most_origin_asns[13].advt, 0)
        self.assertEqual(stats_1.most_origin_asns[13].as_path, [])
        self.assertEqual(stats_1.most_origin_asns[13].comm_set, [])
        self.assertEqual(
            os.path.basename(stats_1.most_origin_asns[13].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(stats_1.most_origin_asns[13].next_hop, None)
        self.assertEqual(
            stats_1.most_origin_asns[13].origin_asns, set(["52000", "19318"])
        )
        self.assertEqual(stats_1.most_origin_asns[13].peer_asn, None)
        self.assertEqual(stats_1.most_origin_asns[13].prefix, "68.168.210.0/24")
        self.assertEqual(stats_1.most_origin_asns[13].timestamp, "20220501.2305")
        self.assertEqual(stats_1.most_origin_asns[13].updates, 0)
        self.assertEqual(stats_1.most_origin_asns[13].withdraws, 0)

        self.assertEqual(stats_1.most_origin_asns[14].advt, 0)
        self.assertEqual(stats_1.most_origin_asns[14].as_path, [])
        self.assertEqual(stats_1.most_origin_asns[14].comm_set, [])
        self.assertEqual(
            os.path.basename(stats_1.most_origin_asns[14].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(stats_1.most_origin_asns[14].next_hop, None)
        self.assertEqual(
            stats_1.most_origin_asns[14].origin_asns, set(["55020", "137951"])
        )
        self.assertEqual(stats_1.most_origin_asns[14].peer_asn, None)
        self.assertEqual(stats_1.most_origin_asns[14].prefix, "156.241.128.0/22")
        self.assertEqual(stats_1.most_origin_asns[14].timestamp, "20220501.2305")
        self.assertEqual(stats_1.most_origin_asns[14].updates, 0)
        self.assertEqual(stats_1.most_origin_asns[14].withdraws, 0)

        self.assertEqual(stats_1.most_origin_asns[15].advt, 0)
        self.assertEqual(stats_1.most_origin_asns[15].as_path, [])
        self.assertEqual(stats_1.most_origin_asns[15].comm_set, [])
        self.assertEqual(
            os.path.basename(stats_1.most_origin_asns[15].filename),
            os.path.basename(self.upd_2_mrt)
        )
        self.assertEqual(stats_1.most_origin_asns[15].next_hop, None)
        self.assertEqual(
            stats_1.most_origin_asns[15].origin_asns, set(["269208", "268347"])
        )
        self.assertEqual(stats_1.most_origin_asns[15].peer_asn, None)
        self.assertEqual(stats_1.most_origin_asns[15].prefix, "2804:610c::/32")
        self.assertEqual(stats_1.most_origin_asns[15].timestamp, "20220501.2305")
        self.assertEqual(stats_1.most_origin_asns[15].updates, 0)
        self.assertEqual(stats_1.most_origin_asns[15].withdraws, 0)

        self.assertEqual(stats_1.total_upd, 29688)
        self.assertEqual(stats_1.total_advt, 29396)
        self.assertEqual(stats_1.total_withd, 950)
        self.assertEqual(
            sorted([os.path.basename(file) for file in stats_1.file_list]),
            sorted([os.path.basename(self.upd_1_mrt), os.path.basename(self.upd_2_mrt)])
        )
        self.assertEqual(stats_1.timestamp, "20220501.2305")

    def test_to_file(self):
        self.assertRaises(ValueError, self.upd_1_stats.to_file, None)
        self.assertRaises(TypeError, self.upd_1_stats.to_file, 123)
        self.assertRaises(OSError, self.upd_1_stats.to_file, "/2f98h3fwfh4fwp")

        self.upd_1_stats.to_file(self.upd_1_test)
        self.assertTrue(os.path.isfile(self.upd_1_test))

        stats = mrt_stats()
        stats.from_file(self.upd_1_test)
        self.assertTrue(stats.equal_to(self.upd_1_stats))

        os.unlink(self.upd_1_test)

    def test_to_json(self):
        json_str = self.upd_1_stats.to_json()
        self.assertIsInstance(json_str, str)
        self.assertNotEqual(json_str, "")

        self.assertTrue("bogon_origin_asns" in json_str)
        self.assertTrue("bogon_prefixes" in json_str)
        self.assertTrue("longest_as_path" in json_str)
        self.assertTrue("longest_comm_set" in json_str)
        self.assertTrue("invalid_len" in json_str)
        self.assertTrue("most_advt_prefixes" in json_str)
        self.assertTrue("most_bogon_asns" in json_str)
        self.assertTrue("most_upd_prefixes" in json_str)
        self.assertTrue("most_withd_prefixes" in json_str)
        self.assertTrue("most_advt_origin_asn" in json_str)
        self.assertTrue("most_advt_peer_asn" in json_str)
        self.assertTrue("most_upd_peer_asn" in json_str)
        self.assertTrue("most_withd_peer_asn" in json_str)
        self.assertTrue("most_origin_asns" in json_str)
        self.assertTrue("total_upd" in json_str)
        self.assertTrue("total_advt" in json_str)
        self.assertTrue("total_withd" in json_str)
        self.assertTrue("file_list" in json_str)
        self.assertTrue("timestamp" in json_str)

        stats = mrt_stats()
        stats.from_json(json_str)

        self.assertTrue(stats.equal_to(self.upd_1_stats))

    def test_ts_ymd(self):
        self.assertEqual(self.upd_1_stats.ts_ymd(), "20220421")

    def test_ts_ymd_format(self):
        self.assertEqual(self.upd_1_stats.ts_ymd_format(), "2022/04/21")

    def tearDown(self):
        os.remove(self.upd_1_mrt)
        os.remove(self.upd_2_mrt)
        os.remove(self.upd_3_mrt)
        os.remove(self.upd_4_mrt)

if __name__ == '__main__':
    unittest.main()
