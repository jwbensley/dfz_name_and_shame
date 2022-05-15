import json
import os
import re
import sys
import unittest

sys.path.append(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__))
        , "../"
    )
)
from dnas.mrt_stats import mrt_stats
from dnas.mrt_entry import mrt_entry
from dnas.mrt_parser import mrt_parser

class test_mrt_stats(unittest.TestCase):

    def setUp(self):

        # Make full path
        self.upd_1_mrt = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "rcc23.updates.20220421.0200.gz"
        )
        if not os.path.isfile(self.upd_1_mrt):
            raise Exception("Test MRT file is not found: {upd_1_mrt}")
        self.upd_1_stats = mrt_parser.parse_upd_dump(self.upd_1_mrt)

        self.upd_1_json = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "rcc23.updates.20220421.0200.gz.json"
        )
        if not os.path.isfile(self.upd_1_json):
            raise Exception("Test stats json dump is not found: {self.upd_1_json}")

        self.upd_1_test = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "rcc23.updates.20220421.0200.gz.test"
        )

        self.upd_2_mrt = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "rcc23.updates.20220501.2305.gz"
        )
        if not os.path.isfile(self.upd_2_mrt):
            raise Exception("Test MRT file is not found: {self.upd_2_mrt}")
        self.upd_2_stats = mrt_parser.parse_upd_dump(self.upd_2_mrt)

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
        stats_1 = mrt_parser.parse_upd_dump(self.upd_1_mrt)
        stats_2 = mrt_parser.parse_upd_dump(self.upd_2_mrt)

        self.assertIsInstance(stats_1, mrt_stats)
        self.assertIsInstance(stats_2, mrt_stats)

        self.assertRaises(ValueError, stats_1.add, None)
        self.assertRaises(TypeError, stats_1.add, 123)

        ret = stats_1.add(stats_2)
        self.assertIsInstance(ret, bool)
        self.assertTrue(ret)

        self.assertEqual(len(stats_1.bogon_origin_asns), 1)
        self.assertEqual(stats_1.bogon_origin_asns[0].advt, 0)
        self.assertEqual(stats_1.bogon_origin_asns[0].as_path, ["137409", "17494", "137491", "58689", "137464", "65551"])
        self.assertEqual(stats_1.bogon_origin_asns[0].comm_set, [])
        self.assertEqual(
            stats_1.bogon_origin_asns[0].filename,
            self.upd_1_mrt
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
            stats_1.bogon_prefixes[0].filename,
            self.upd_1_mrt
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
        self.assertEqual(
            stats_1.bogon_prefixes[1].comm_set,
            [
                "24115:6939", "24115:24115", "24115:65023",
                "24115:1000:2", "24115:1001:1", "24115:1002:1",
                "24115:1003:1", "24115:1004:6939"
            ]
        )
        self.assertEqual(
            stats_1.bogon_prefixes[1].filename,
            self.upd_2_mrt
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
            stats_1.longest_as_path[0].filename,
            self.upd_1_mrt
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

        self.assertEqual(len(stats_1.longest_comm_set), 1)
        self.assertEqual(stats_1.longest_comm_set[0].advt, 0)
        self.assertEqual(
            stats_1.longest_comm_set[0].as_path,
            ["58952", "39386", "47589", "9155", "13335"]
        )
        self.assertEqual(
            stats_1.longest_comm_set[0].comm_set,
            [
                "13335:10249", "13335:19060", "13335:20510", "13335:20520",
                "24115:1248", "24115:2497", "24115:2518", "24115:3491",
                "24115:3856", "24115:4621", "24115:4628", "24115:4637",
                "24115:4651", "24115:4657", "24115:4739", "24115:4761",
                "24115:4773", "24115:4775", "24115:4788", "24115:4800", 
                "24115:4818", "24115:4826", "24115:4844", "24115:5017",
                "24115:6619", "24115:6648", "24115:7568", "24115:7595",
                "24115:7598", "24115:7632", "24115:7642", "24115:7713",
                "24115:8529", "24115:8757", "24115:8781", "24115:9269",
                "24115:9299", "24115:9304", "24115:9326", "24115:9329",
                "24115:9381", "24115:9498", "24115:9505", "24115:9534",
                "24115:9583", "24115:9658", "24115:9873", "24115:9892",
                "24115:9902", "24115:9924", "24115:9930", "24115:10026",
                "24115:10030", "24115:10089", "24115:10158", "24115:16265",
                "24115:17451", "24115:17494", "24115:17511", "24115:17547",
                "24115:17557", "24115:17639", "24115:17645", "24115:17660",
                "24115:17666", "24115:17676", "24115:17726", "24115:17922",
                "24115:17978", "24115:18001", "24115:18059", "24115:18403",
                "24115:20940", "24115:23576", "24115:23673", "24115:23930",
                "24115:23939", "24115:23944", "24115:23947", "24115:24203",
                "24115:24218", "24115:24482", "24115:24535", "24115:38001",
                "24115:38040", "24115:38082", "24115:38090", "24115:38158",
                "24115:38182", "24115:38193", "24115:38195", "24115:38321",
                "24115:38322", "24115:38466", "24115:38565", "24115:38740",
                "24115:38753", "24115:38757", "24115:38861", "24115:38880",
                "24115:38895", "24115:39386", "24115:45102", "24115:45352",
                "24115:45430", "24115:45474", "24115:45494", "24115:45629",
                "24115:45634", "24115:45706", "24115:45796", "24115:45845",
                "24115:45903", "24115:50010", "24115:54994", "24115:55329",
                "24115:55658", "24115:55685", "24115:55818", "24115:55944",
                "24115:55967", "24115:56258", "24115:56308", "24115:58389",
                "24115:58430", "24115:58436", "24115:58453", "24115:58580",
                "24115:58587", "24115:58599", "24115:58601", "24115:58682",
                "24115:58715", "24115:58717", "24115:58952", "24115:59019",
                "24115:59318", "24115:59605", "24115:63516", "24115:63541",
                "24115:63916", "24115:63927", "24115:63947", "24115:64049",
                "24115:64096", "24115:65023", "24115:1000:2", "24115:1001:1",
                "24115:1002:1", "24115:1003:115", "24115:1004:39386"
            ]
        )
        self.assertEqual(
            stats_1.longest_comm_set[0].filename,
            self.upd_2_mrt
        )
        self.assertEqual(stats_1.longest_comm_set[0].next_hop, "27.111.229.106")
        self.assertEqual(stats_1.longest_comm_set[0].origin_asns, set(["13335"]))
        self.assertEqual(stats_1.longest_comm_set[0].peer_asn, "58952")
        self.assertEqual(stats_1.longest_comm_set[0].prefix, "162.158.59.0/24")
        self.assertEqual(stats_1.longest_comm_set[0].timestamp, "20220501.2308")
        self.assertEqual(stats_1.longest_comm_set[0].updates, 0)
        self.assertEqual(stats_1.longest_comm_set[0].withdraws, 0)

        self.assertEqual(len(stats_1.invalid_len), 8)
        self.assertEqual(stats_1.invalid_len[0].advt, 0)
        self.assertEqual(stats_1.invalid_len[0].as_path, ["199524", "38082"])
        self.assertEqual(stats_1.invalid_len[0].comm_set, [])
        self.assertEqual(
            stats_1.invalid_len[0].filename,
            self.upd_1_mrt
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
                "4788:23030", "4788:32011", "24115:4788", "24115:65023",
                "24115:1000:2", "24115:1001:2", "24115:1002:2", "24115:1003:1",
                "24115:1004:4788"
            ]
        )
        self.assertEqual(
            stats_1.invalid_len[1].filename,
            self.upd_1_mrt
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
                "4788:23030", "4788:34002", "24115:4788", "24115:65023",
                "24115:1000:2", "24115:1001:2", "24115:1002:2", "24115:1003:1",
                "24115:1004:4788"
            ]
        )
        self.assertEqual(
            stats_1.invalid_len[2].filename,
            self.upd_1_mrt
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
            stats_1.invalid_len[3].filename,
            self.upd_1_mrt
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
            stats_1.invalid_len[4].comm_set,
            [
                "15133:4351", "24115:59318", "24115:65023", "59318:2015",
                "24115:1000:2", "24115:1001:1", "24115:1002:1", "24115:1003:33",
                "24115:1004:59318"
            ]
        )
        self.assertEqual(
            stats_1.invalid_len[4].filename,
            self.upd_1_mrt
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
            stats_1.invalid_len[5].filename,
            self.upd_1_mrt
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
            [
                "4788:801", "4788:810", "4788:6300", "4788:6310", "24115:4788",
                "24115:65023", "24115:1000:2", "24115:1001:1", "24115:1002:2",
                "24115:1003:2", "24115:1004:4788"
            ]
        )
        self.assertEqual(
            stats_1.invalid_len[6].filename,
            self.upd_2_mrt
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
            [
                "4788:801", "4788:810", "4788:6300", "4788:6310", "24115:4788",
                "24115:65023", "24115:1000:2", "24115:1001:1", "24115:1002:2",
                "24115:1003:2", "24115:1004:4788"
            ]
        )
        self.assertEqual(
            stats_1.invalid_len[7].filename,
            self.upd_2_mrt
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
        self.assertEqual(stats_1.most_advt_prefixes[0].advt, 1747)
        self.assertEqual(stats_1.most_advt_prefixes[0].as_path, [[]])
        self.assertEqual(stats_1.most_advt_prefixes[0].comm_set, [[]])
        self.assertEqual(
            stats_1.most_advt_prefixes[0].filename,
            self.upd_2_mrt
        )
        self.assertEqual(stats_1.most_advt_prefixes[0].next_hop, None)
        self.assertEqual(stats_1.most_advt_prefixes[0].origin_asns, set())
        self.assertEqual(stats_1.most_advt_prefixes[0].peer_asn, None)
        self.assertEqual(stats_1.most_advt_prefixes[0].prefix, "89.30.150.0/23")
        self.assertEqual(stats_1.most_advt_prefixes[0].timestamp, "20220501.2305")
        self.assertEqual(stats_1.most_advt_prefixes[0].updates, 0)
        self.assertEqual(stats_1.most_advt_prefixes[0].withdraws, 0)

        self.assertEqual(len(stats_1.most_upd_prefixes), 1)
        self.assertEqual(stats_1.most_upd_prefixes[0].advt, 0)
        self.assertEqual(stats_1.most_upd_prefixes[0].as_path, [[]])
        self.assertEqual(stats_1.most_upd_prefixes[0].comm_set, [[]])
        self.assertEqual(
            stats_1.most_upd_prefixes[0].filename,
            self.upd_2_mrt
        )
        self.assertEqual(stats_1.most_upd_prefixes[0].next_hop, None)
        self.assertEqual(stats_1.most_upd_prefixes[0].origin_asns, set())
        self.assertEqual(stats_1.most_upd_prefixes[0].peer_asn, None)
        self.assertEqual(stats_1.most_upd_prefixes[0].prefix, "89.30.150.0/23")
        self.assertEqual(stats_1.most_upd_prefixes[0].timestamp, "20220501.2305")
        self.assertEqual(stats_1.most_upd_prefixes[0].updates, 1782)
        self.assertEqual(stats_1.most_upd_prefixes[0].withdraws, 0)

        self.assertEqual(len(stats_1.most_withd_prefixes), 1)
        self.assertEqual(stats_1.most_withd_prefixes[0].advt, 0)
        self.assertEqual(stats_1.most_withd_prefixes[0].as_path, [[]])
        self.assertEqual(stats_1.most_withd_prefixes[0].comm_set, [[]])
        self.assertEqual(
            stats_1.most_withd_prefixes[0].filename,
            self.upd_2_mrt
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
        self.assertEqual(stats_1.most_advt_origin_asn[0].advt, 5016)
        self.assertEqual(stats_1.most_advt_origin_asn[0].as_path, [[]])
        self.assertEqual(stats_1.most_advt_origin_asn[0].comm_set, [[]])
        self.assertEqual(
            stats_1.most_advt_origin_asn[0].filename,
            self.upd_2_mrt
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
        self.assertEqual(stats_1.most_advt_peer_asn[0].advt, 21592)
        self.assertEqual(stats_1.most_advt_peer_asn[0].as_path, [[]])
        self.assertEqual(stats_1.most_advt_peer_asn[0].comm_set, [[]])
        self.assertEqual(
            stats_1.most_advt_peer_asn[0].filename,
            self.upd_2_mrt
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
        self.assertEqual(stats_1.most_upd_peer_asn[0].as_path, [[]])
        self.assertEqual(stats_1.most_upd_peer_asn[0].comm_set, [[]])
        self.assertEqual(
            stats_1.most_upd_peer_asn[0].filename,
            self.upd_2_mrt
        )
        self.assertEqual(stats_1.most_upd_peer_asn[0].next_hop, None)
        self.assertEqual(stats_1.most_upd_peer_asn[0].origin_asns, set())
        self.assertEqual(stats_1.most_upd_peer_asn[0].peer_asn, "18106")
        self.assertEqual(stats_1.most_upd_peer_asn[0].prefix, None)
        self.assertEqual(stats_1.most_upd_peer_asn[0].timestamp, "20220501.2305")
        self.assertEqual(stats_1.most_upd_peer_asn[0].updates, 21939)
        self.assertEqual(stats_1.most_upd_peer_asn[0].withdraws, 0)

        self.assertEqual(len(stats_1.most_withd_peer_asn), 1)
        self.assertEqual(stats_1.most_withd_peer_asn[0].advt, 0)
        self.assertEqual(stats_1.most_withd_peer_asn[0].as_path, [[]])
        self.assertEqual(stats_1.most_withd_peer_asn[0].comm_set, [[]])
        self.assertEqual(
            stats_1.most_withd_peer_asn[0].filename,
            self.upd_1_mrt
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
        self.assertEqual(stats_1.most_origin_asns[0].as_path, [[]])
        self.assertEqual(stats_1.most_origin_asns[0].comm_set, [[]])
        self.assertEqual(
            stats_1.most_origin_asns[0].filename,
            self.upd_1_mrt
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
        self.assertEqual(stats_1.most_origin_asns[1].as_path, [[]])
        self.assertEqual(stats_1.most_origin_asns[1].comm_set, [[]])
        self.assertEqual(
            stats_1.most_origin_asns[1].filename,
            self.upd_1_mrt
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
        self.assertEqual(stats_1.most_origin_asns[2].as_path, [[]])
        self.assertEqual(stats_1.most_origin_asns[2].comm_set, [[]])
        self.assertEqual(
            stats_1.most_origin_asns[2].filename,
            self.upd_1_mrt
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
        self.assertEqual(stats_1.most_origin_asns[3].as_path, [[]])
        self.assertEqual(stats_1.most_origin_asns[3].comm_set, [[]])
        self.assertEqual(
            stats_1.most_origin_asns[3].filename,
            self.upd_1_mrt
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
        self.assertEqual(stats_1.most_origin_asns[4].as_path, [[]])
        self.assertEqual(stats_1.most_origin_asns[4].comm_set, [[]])
        self.assertEqual(
            stats_1.most_origin_asns[4].filename,
            self.upd_1_mrt
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
        self.assertEqual(stats_1.most_origin_asns[5].as_path, [[]])
        self.assertEqual(stats_1.most_origin_asns[5].comm_set, [[]])
        self.assertEqual(
            stats_1.most_origin_asns[5].filename,
            self.upd_1_mrt
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
        self.assertEqual(stats_1.most_origin_asns[6].as_path, [[]])
        self.assertEqual(stats_1.most_origin_asns[6].comm_set, [[]])
        self.assertEqual(
            stats_1.most_origin_asns[6].filename,
            self.upd_1_mrt
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
        self.assertEqual(stats_1.most_origin_asns[7].as_path, [[]])
        self.assertEqual(stats_1.most_origin_asns[7].comm_set, [[]])
        self.assertEqual(
            stats_1.most_origin_asns[7].filename,
            self.upd_1_mrt
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
        self.assertEqual(stats_1.most_origin_asns[8].as_path, [[]])
        self.assertEqual(stats_1.most_origin_asns[8].comm_set, [[]])
        self.assertEqual(
            stats_1.most_origin_asns[8].filename,
            self.upd_1_mrt
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
        self.assertEqual(stats_1.most_origin_asns[9].as_path, [[]])
        self.assertEqual(stats_1.most_origin_asns[9].comm_set, [[]])
        self.assertEqual(
            stats_1.most_origin_asns[9].filename,
            self.upd_1_mrt
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
        self.assertEqual(stats_1.most_origin_asns[10].as_path, [[]])
        self.assertEqual(stats_1.most_origin_asns[10].comm_set, [[]])
        self.assertEqual(
            stats_1.most_origin_asns[10].filename,
            self.upd_2_mrt
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
        self.assertEqual(stats_1.most_origin_asns[11].as_path, [[]])
        self.assertEqual(stats_1.most_origin_asns[11].comm_set, [[]])
        self.assertEqual(
            stats_1.most_origin_asns[11].filename,
            self.upd_2_mrt
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
        self.assertEqual(stats_1.most_origin_asns[12].as_path, [[]])
        self.assertEqual(stats_1.most_origin_asns[12].comm_set, [[]])
        self.assertEqual(
            stats_1.most_origin_asns[12].filename,
            self.upd_2_mrt
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
        self.assertEqual(stats_1.most_origin_asns[13].as_path, [[]])
        self.assertEqual(stats_1.most_origin_asns[13].comm_set, [[]])
        self.assertEqual(
            stats_1.most_origin_asns[13].filename,
            self.upd_2_mrt
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
        self.assertEqual(stats_1.most_origin_asns[14].as_path, [[]])
        self.assertEqual(stats_1.most_origin_asns[14].comm_set, [[]])
        self.assertEqual(
            stats_1.most_origin_asns[14].filename,
            self.upd_2_mrt
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
        self.assertEqual(stats_1.most_origin_asns[15].as_path, [[]])
        self.assertEqual(stats_1.most_origin_asns[15].comm_set, [[]])
        self.assertEqual(
            stats_1.most_origin_asns[15].filename,
            self.upd_2_mrt
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

        self.assertEqual(stats_1.total_upd, 57245)
        self.assertEqual(stats_1.total_advt, 56752)
        self.assertEqual(stats_1.total_withd, 1837)
        self.assertEqual(stats_1.file_list, [self.upd_1_mrt, self.upd_2_mrt])
        self.assertEqual(stats_1.timestamp, "20220501.2305")

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
        self.assertTrue(self.upd_1_stats)

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

        diff = self.upd_1_stats.get_diff(self.upd_2_stats)
        self.assertIsInstance(diff, mrt_stats)

        self.assertEqual(len(diff.bogon_prefixes), 1)
        self.assertEqual(diff.bogon_prefixes[0].advt, 0)
        self.assertEqual(diff.bogon_prefixes[0].as_path, ["133210", "6939"])
        self.assertEqual(
            diff.bogon_prefixes[0].comm_set,
            [
                "24115:6939", "24115:24115", "24115:65023",
                "24115:1000:2", "24115:1001:1", "24115:1002:1",
                "24115:1003:1", "24115:1004:6939"
            ]
        )
        self.assertEqual(diff.bogon_prefixes[0].filename, self.upd_2_mrt)
        self.assertEqual(diff.bogon_prefixes[0].next_hop, "27.111.228.81")
        self.assertEqual(diff.bogon_prefixes[0].origin_asns, set(["6939"]))
        self.assertEqual(diff.bogon_prefixes[0].peer_asn, "133210")
        self.assertEqual(diff.bogon_prefixes[0].prefix, "192.88.99.0/24")
        self.assertEqual(diff.bogon_prefixes[0].timestamp, "20220501.2309")
        self.assertEqual(diff.bogon_prefixes[0].updates, 0)
        self.assertEqual(diff.bogon_prefixes[0].withdraws, 0)

        self.assertEqual(len(diff.longest_as_path), 1)
        self.assertEqual(diff.longest_as_path[0].advt, 0)
        self.assertEqual(
            diff.longest_as_path[0].as_path,
            [
                "18106", "23106", "264228", "264228", "264228", "264228",
                "264228", "264228", "264228", "264228", "264228", "264228",
                "264228", "264228", "264228", "264228", "264228", "264228",
                "264228", "264228", "264228", "264228", "264228", "264228",
                "264228", "264228", "264228"
            ]
        )
        self.assertEqual(diff.longest_as_path[0].comm_set,["13538:3000"])
        self.assertEqual(diff.longest_as_path[0].filename, self.upd_2_mrt)
        self.assertEqual(
            diff.longest_as_path[0].next_hop,
            ["2001:de8:4::1:8106:1", "fe80::bac2:53ff:fedb:2004"]
        )
        self.assertEqual(diff.longest_as_path[0].origin_asns, set(["264228"]))
        self.assertEqual(diff.longest_as_path[0].peer_asn, "18106")
        self.assertEqual(diff.longest_as_path[0].prefix, "2804:2488::/48")
        self.assertEqual(diff.longest_as_path[0].timestamp, "20220501.2305")
        self.assertEqual(diff.longest_as_path[0].updates, 0)
        self.assertEqual(diff.longest_as_path[0].withdraws, 0)

        self.assertEqual(len(diff.longest_comm_set), 1)
        self.assertEqual(diff.longest_comm_set[0].advt, 0)
        self.assertEqual(
            diff.longest_comm_set[0].as_path,
            ["58952", "39386", "47589", "9155", "13335"]
        )
        self.assertEqual(
            diff.longest_comm_set[0].comm_set,
            [
                "13335:10249", "13335:19060", "13335:20510", "13335:20520",
                "24115:1248", "24115:2497", "24115:2518", "24115:3491",
                "24115:3856", "24115:4621", "24115:4628", "24115:4637",
                "24115:4651", "24115:4657", "24115:4739", "24115:4761",
                "24115:4773", "24115:4775", "24115:4788", "24115:4800", 
                "24115:4818", "24115:4826", "24115:4844", "24115:5017",
                "24115:6619", "24115:6648", "24115:7568", "24115:7595",
                "24115:7598", "24115:7632", "24115:7642", "24115:7713",
                "24115:8529", "24115:8757", "24115:8781", "24115:9269",
                "24115:9299", "24115:9304", "24115:9326", "24115:9329",
                "24115:9381", "24115:9498", "24115:9505", "24115:9534",
                "24115:9583", "24115:9658", "24115:9873", "24115:9892",
                "24115:9902", "24115:9924", "24115:9930", "24115:10026",
                "24115:10030", "24115:10089", "24115:10158", "24115:16265",
                "24115:17451", "24115:17494", "24115:17511", "24115:17547",
                "24115:17557", "24115:17639", "24115:17645", "24115:17660",
                "24115:17666", "24115:17676", "24115:17726", "24115:17922",
                "24115:17978", "24115:18001", "24115:18059", "24115:18403",
                "24115:20940", "24115:23576", "24115:23673", "24115:23930",
                "24115:23939", "24115:23944", "24115:23947", "24115:24203",
                "24115:24218", "24115:24482", "24115:24535", "24115:38001",
                "24115:38040", "24115:38082", "24115:38090", "24115:38158",
                "24115:38182", "24115:38193", "24115:38195", "24115:38321",
                "24115:38322", "24115:38466", "24115:38565", "24115:38740",
                "24115:38753", "24115:38757", "24115:38861", "24115:38880",
                "24115:38895", "24115:39386", "24115:45102", "24115:45352",
                "24115:45430", "24115:45474", "24115:45494", "24115:45629",
                "24115:45634", "24115:45706", "24115:45796", "24115:45845",
                "24115:45903", "24115:50010", "24115:54994", "24115:55329",
                "24115:55658", "24115:55685", "24115:55818", "24115:55944",
                "24115:55967", "24115:56258", "24115:56308", "24115:58389",
                "24115:58430", "24115:58436", "24115:58453", "24115:58580",
                "24115:58587", "24115:58599", "24115:58601", "24115:58682",
                "24115:58715", "24115:58717", "24115:58952", "24115:59019",
                "24115:59318", "24115:59605", "24115:63516", "24115:63541",
                "24115:63916", "24115:63927", "24115:63947", "24115:64049",
                "24115:64096", "24115:65023", "24115:1000:2", "24115:1001:1",
                "24115:1002:1", "24115:1003:115", "24115:1004:39386"
            ]
        )
        self.assertEqual(diff.longest_comm_set[0].filename, self.upd_2_mrt)
        self.assertEqual(diff.longest_comm_set[0].next_hop, "27.111.229.106")
        self.assertEqual(diff.longest_comm_set[0].origin_asns, set(["13335"]))
        self.assertEqual(diff.longest_comm_set[0].peer_asn, "58952")
        self.assertEqual(diff.longest_comm_set[0].prefix, "162.158.59.0/24")
        self.assertEqual(diff.longest_comm_set[0].timestamp, "20220501.2308")
        self.assertEqual(diff.longest_comm_set[0].updates, 0)
        self.assertEqual(diff.longest_comm_set[0].withdraws, 0)

        self.assertEqual(len(diff.invalid_len), 6)
        self.assertEqual(diff.invalid_len[0].advt, 0)
        self.assertEqual(diff.invalid_len[0].as_path, ["199524", "38082"])
        self.assertEqual(diff.invalid_len[0].comm_set, [])
        self.assertEqual(diff.invalid_len[0].filename, self.upd_2_mrt)
        self.assertEqual(diff.invalid_len[0].next_hop, ["2001:de8:4::3:8082:1"])
        self.assertEqual(diff.invalid_len[0].origin_asns, set(["38082"]))
        self.assertEqual(diff.invalid_len[0].peer_asn, "199524")
        self.assertEqual(diff.invalid_len[0].prefix, "2405:4000:800:8::/64")
        self.assertEqual(diff.invalid_len[0].timestamp, "20220501.2305")
        self.assertEqual(diff.invalid_len[0].updates, 0)
        self.assertEqual(diff.invalid_len[0].withdraws, 0)

        self.assertEqual(diff.invalid_len[1].advt, 0)
        self.assertEqual(
            diff.invalid_len[1].as_path,
            ["133210", "59318", "59318", "15133"]
        )
        self.assertEqual(
            diff.invalid_len[1].comm_set,
            [
                "15133:4351", "24115:59318", "24115:65023", "59318:2015",
                "24115:1000:2", "24115:1001:1", "24115:1002:2", "24115:1003:34",
                "24115:1004:59318"
            ]
        )
        self.assertEqual(diff.invalid_len[1].filename, self.upd_2_mrt)
        self.assertEqual(
            diff.invalid_len[1].next_hop,
            ["2001:de8:4::13:1207:1", "fe80::8ae6:4b00:6c1:6029"]
        )
        self.assertEqual(diff.invalid_len[1].origin_asns, set(["15133"]))
        self.assertEqual(diff.invalid_len[1].peer_asn, "133210")
        self.assertEqual(diff.invalid_len[1].prefix, "2404:b300:33:1::/64")
        self.assertEqual(diff.invalid_len[1].timestamp, "20220501.2306")
        self.assertEqual(diff.invalid_len[1].updates, 0)
        self.assertEqual(diff.invalid_len[1].withdraws, 0)

        self.assertEqual(diff.invalid_len[2].advt, 0)
        self.assertEqual(diff.invalid_len[2].as_path, ["133210", "4788", "54994"])
        self.assertEqual(diff.invalid_len[2].comm_set, 
            [
                "4788:801", "4788:810", "4788:6300", "4788:6310", "24115:4788",
                "24115:65023", "24115:1000:2", "24115:1001:1", "24115:1002:2",
                "24115:1003:2", "24115:1004:4788"
            ]
        )
        self.assertEqual(diff.invalid_len[2].filename, self.upd_2_mrt)
        self.assertEqual(diff.invalid_len[2].next_hop,
            ["2001:de8:4::4788:3", "fe80::8ae6:4b00:6c1:6029"]
        )
        self.assertEqual(diff.invalid_len[2].origin_asns, set(["54994"]))
        self.assertEqual(diff.invalid_len[2].peer_asn, "133210")
        self.assertEqual(diff.invalid_len[2].prefix, "2001:e68:20db:10::/64")
        self.assertEqual(diff.invalid_len[2].timestamp, "20220501.2306")
        self.assertEqual(diff.invalid_len[2].updates, 0)
        self.assertEqual(diff.invalid_len[2].withdraws, 0)


        self.assertEqual(diff.invalid_len[3].advt, 0)
        self.assertEqual(
            diff.invalid_len[3].as_path, ["133210", "4788", "54994"]
        )
        self.assertEqual(diff.invalid_len[3].comm_set,
            [
                "4788:801", "4788:810", "4788:6300", "4788:6310", "24115:4788",
                "24115:65023", "24115:1000:2", "24115:1001:1", "24115:1002:2",
                "24115:1003:2", "24115:1004:4788"
            ]
        )
        self.assertEqual(diff.invalid_len[3].filename, self.upd_2_mrt)
        self.assertEqual(diff.invalid_len[3].next_hop,
            ["2001:de8:4::4788:3", "fe80::8ae6:4b00:6c1:6029"]
        )
        self.assertEqual(diff.invalid_len[3].origin_asns, set(["54994"]))
        self.assertEqual(diff.invalid_len[3].peer_asn, "133210")
        self.assertEqual(diff.invalid_len[3].prefix, "2001:e68:20db:11::/64")
        self.assertEqual(diff.invalid_len[3].timestamp, "20220501.2306")
        self.assertEqual(diff.invalid_len[3].updates, 0)
        self.assertEqual(diff.invalid_len[3].withdraws, 0)

        self.assertEqual(diff.invalid_len[4].advt, 0)
        self.assertEqual(
            diff.invalid_len[4].as_path, ["133210", "4788", "38044", "23736"]
        )
        self.assertEqual(
            diff.invalid_len[4].comm_set,
            [
                "4788:811", "4788:6300", "4788:6310", "4788:16300",
                "4788:23030", "4788:34002", "24115:4788", "24115:65023",
                "24115:1000:2", "24115:1001:1", "24115:1002:2", "24115:1003:2",
                "24115:1004:4788"
            ]
        )
        self.assertEqual(diff.invalid_len[4].filename, self.upd_2_mrt)
        self.assertEqual(
            diff.invalid_len[4].next_hop,
            ["2001:de8:4::4788:3", "fe80::8ae6:4b00:6c1:6029"]
        )
        self.assertEqual(diff.invalid_len[4].origin_asns, set(["23736"]))
        self.assertEqual(diff.invalid_len[4].peer_asn, "133210")
        self.assertEqual(diff.invalid_len[4].prefix, "2400:7400:0:106::/64")
        self.assertEqual(diff.invalid_len[4].timestamp, "20220501.2307")
        self.assertEqual(diff.invalid_len[4].updates, 0)
        self.assertEqual(diff.invalid_len[4].withdraws, 0)

        self.assertEqual(diff.invalid_len[5].advt, 0)
        self.assertEqual(
            diff.invalid_len[5].as_path,
            ["133210", "4788", "38044", "38044", "23736"]
        )
        self.assertEqual(
            diff.invalid_len[5].comm_set,
            [
                "4788:811", "4788:6300", "4788:6310", "4788:16300",
                "4788:23030", "4788:32011", "24115:4788", "24115:65023",
                "24115:1000:2", "24115:1001:1", "24115:1002:2", "24115:1003:2",
                "24115:1004:4788"
            ]
        )
        self.assertEqual(diff.invalid_len[5].filename, self.upd_2_mrt)
        self.assertEqual(
            diff.invalid_len[5].next_hop,
            ["2001:de8:4::4788:3", "fe80::8ae6:4b00:6c1:6029"]
        )
        self.assertEqual(diff.invalid_len[5].origin_asns, set(["23736"]))
        self.assertEqual(diff.invalid_len[5].peer_asn, "133210")
        self.assertEqual(diff.invalid_len[5].prefix, "2400:7400:0:105::/64")
        self.assertEqual(diff.invalid_len[5].timestamp, "20220501.2307")
        self.assertEqual(diff.invalid_len[5].updates, 0)
        self.assertEqual(diff.invalid_len[5].withdraws, 0)

        self.assertEqual(len(diff.most_advt_prefixes), 1)
        self.assertEqual(diff.most_advt_prefixes[0].advt, 884)
        self.assertEqual(diff.most_advt_prefixes[0].as_path, [[]])
        self.assertEqual(diff.most_advt_prefixes[0].comm_set, [[]])
        self.assertEqual(diff.most_advt_prefixes[0].filename, self.upd_2_mrt)
        self.assertEqual(diff.most_advt_prefixes[0].next_hop, None)
        self.assertEqual(diff.most_advt_prefixes[0].origin_asns, set())
        self.assertEqual(diff.most_advt_prefixes[0].peer_asn, None)
        self.assertEqual(diff.most_advt_prefixes[0].prefix, "89.30.150.0/23")
        self.assertEqual(diff.most_advt_prefixes[0].timestamp, "20220501.2305")
        self.assertEqual(diff.most_advt_prefixes[0].updates, 0)
        self.assertEqual(diff.most_advt_prefixes[0].withdraws, 0)

        self.assertEqual(len(diff.most_upd_prefixes), 1)
        self.assertEqual(diff.most_upd_prefixes[0].advt, 0)
        self.assertEqual(diff.most_upd_prefixes[0].as_path, [[]])
        self.assertEqual(diff.most_upd_prefixes[0].comm_set, [[]])
        self.assertEqual(diff.most_upd_prefixes[0].filename, self.upd_2_mrt)
        self.assertEqual(diff.most_upd_prefixes[0].next_hop, None)
        self.assertEqual(diff.most_upd_prefixes[0].origin_asns, set())
        self.assertEqual(diff.most_upd_prefixes[0].peer_asn, None)
        self.assertEqual(diff.most_upd_prefixes[0].prefix, "89.30.150.0/23")
        self.assertEqual(diff.most_upd_prefixes[0].timestamp, "20220501.2305")
        self.assertEqual(diff.most_upd_prefixes[0].updates, 898)
        self.assertEqual(diff.most_upd_prefixes[0].withdraws, 0)

        self.assertEqual(len(diff.most_withd_prefixes), 1)
        self.assertEqual(diff.most_withd_prefixes[0].advt, 0)
        self.assertEqual(diff.most_withd_prefixes[0].as_path, [[]])
        self.assertEqual(diff.most_withd_prefixes[0].comm_set, [[]])
        self.assertEqual(diff.most_withd_prefixes[0].filename, self.upd_2_mrt)
        self.assertEqual(diff.most_withd_prefixes[0].next_hop, None)
        self.assertEqual(diff.most_withd_prefixes[0].origin_asns, set())
        self.assertEqual(diff.most_withd_prefixes[0].peer_asn, None)
        self.assertEqual(
            diff.most_withd_prefixes[0].prefix, "2a01:9e00:4279::/48"
        )
        self.assertEqual(
            diff.most_withd_prefixes[0].timestamp, "20220501.2305"
        )
        self.assertEqual(diff.most_withd_prefixes[0].updates, 0)
        self.assertEqual(diff.most_withd_prefixes[0].withdraws, 89)

        self.assertEqual(len(diff.most_advt_origin_asn), 1)
        self.assertEqual(diff.most_advt_origin_asn[0].advt, 2628)
        self.assertEqual(diff.most_advt_origin_asn[0].as_path, [[]])
        self.assertEqual(diff.most_advt_origin_asn[0].comm_set, [[]])
        self.assertEqual(diff.most_advt_origin_asn[0].filename, self.upd_2_mrt)
        self.assertEqual(diff.most_advt_origin_asn[0].next_hop, None)
        self.assertEqual(
            diff.most_advt_origin_asn[0].origin_asns, set(["20473"])
        )
        self.assertEqual(diff.most_advt_origin_asn[0].peer_asn, None)
        self.assertEqual(diff.most_advt_origin_asn[0].prefix, None)
        self.assertEqual(
            diff.most_advt_origin_asn[0].timestamp, "20220501.2305"
        )
        self.assertEqual(diff.most_advt_origin_asn[0].updates, 0)
        self.assertEqual(diff.most_advt_origin_asn[0].withdraws, 0)

        self.assertEqual(len(diff.most_advt_peer_asn), 1)
        self.assertEqual(diff.most_advt_peer_asn[0].advt, 11595)
        self.assertEqual(diff.most_advt_peer_asn[0].as_path, [[]])
        self.assertEqual(diff.most_advt_peer_asn[0].comm_set, [[]])
        self.assertEqual(diff.most_advt_peer_asn[0].filename, self.upd_2_mrt)
        self.assertEqual(diff.most_advt_peer_asn[0].next_hop, None)
        self.assertEqual(diff.most_advt_peer_asn[0].origin_asns, set())
        self.assertEqual(diff.most_advt_peer_asn[0].peer_asn, "18106")
        self.assertEqual(diff.most_advt_peer_asn[0].prefix, None)
        self.assertEqual(diff.most_advt_peer_asn[0].timestamp, "20220501.2305")
        self.assertEqual(diff.most_advt_peer_asn[0].updates, 0)
        self.assertEqual(diff.most_advt_peer_asn[0].withdraws, 0)

        self.assertEqual(len(diff.most_upd_peer_asn), 1)
        self.assertEqual(diff.most_upd_peer_asn[0].advt, 0)
        self.assertEqual(diff.most_upd_peer_asn[0].as_path, [[]])
        self.assertEqual(diff.most_upd_peer_asn[0].comm_set, [[]])
        self.assertEqual(diff.most_upd_peer_asn[0].filename, self.upd_2_mrt)
        self.assertEqual(diff.most_upd_peer_asn[0].next_hop, None)
        self.assertEqual(diff.most_upd_peer_asn[0].origin_asns, set())
        self.assertEqual(diff.most_upd_peer_asn[0].peer_asn, "18106")
        self.assertEqual(diff.most_upd_peer_asn[0].prefix, None)
        self.assertEqual(diff.most_upd_peer_asn[0].timestamp, "20220501.2305")
        self.assertEqual(diff.most_upd_peer_asn[0].updates, 11781)
        self.assertEqual(diff.most_upd_peer_asn[0].withdraws, 0)

        self.assertEqual(len(diff.most_withd_peer_asn), 1)
        self.assertEqual(diff.most_withd_peer_asn[0].advt, 0)
        self.assertEqual(diff.most_withd_peer_asn[0].as_path, [[]])
        self.assertEqual(diff.most_withd_peer_asn[0].comm_set, [[]])
        self.assertEqual(diff.most_withd_peer_asn[0].filename, self.upd_2_mrt)
        self.assertEqual(diff.most_withd_peer_asn[0].next_hop, None)
        self.assertEqual(diff.most_withd_peer_asn[0].origin_asns, set())
        self.assertEqual(diff.most_withd_peer_asn[0].peer_asn, "18106")
        self.assertEqual(diff.most_withd_peer_asn[0].prefix, None)
        self.assertEqual(
            diff.most_withd_peer_asn[0].timestamp, "20220501.2305"
        )
        self.assertEqual(diff.most_withd_peer_asn[0].updates, 0)
        self.assertEqual(diff.most_withd_peer_asn[0].withdraws, 186)

        self.assertEqual(len(diff.most_origin_asns), 9)
        self.assertEqual(diff.most_origin_asns[0].advt, 0)
        self.assertEqual(diff.most_origin_asns[0].as_path, [[]])
        self.assertEqual(diff.most_origin_asns[0].comm_set, [[]])
        self.assertEqual(diff.most_origin_asns[0].filename, self.upd_2_mrt)
        self.assertEqual(diff.most_origin_asns[0].next_hop, None)
        self.assertEqual(
            diff.most_origin_asns[0].origin_asns, set(["28198", "262375"])
        )
        self.assertEqual(diff.most_origin_asns[0].peer_asn, None)
        self.assertEqual(diff.most_origin_asns[0].prefix, "177.131.0.0/21")
        self.assertEqual(diff.most_origin_asns[0].timestamp, "20220501.2305")
        self.assertEqual(diff.most_origin_asns[0].updates, 0)
        self.assertEqual(diff.most_origin_asns[0].withdraws, 0)

        self.assertEqual(diff.most_origin_asns[1].advt, 0)
        self.assertEqual(diff.most_origin_asns[1].as_path, [[]])
        self.assertEqual(diff.most_origin_asns[1].comm_set, [[]])
        self.assertEqual(diff.most_origin_asns[1].filename, self.upd_2_mrt)
        self.assertEqual(diff.most_origin_asns[1].next_hop, None)
        self.assertEqual(
            diff.most_origin_asns[1].origin_asns, set(["271204", "266181"])
        )
        self.assertEqual(diff.most_origin_asns[1].peer_asn, None)
        self.assertEqual(diff.most_origin_asns[1].prefix, "179.49.190.0/23")
        self.assertEqual(diff.most_origin_asns[1].timestamp, "20220501.2305")
        self.assertEqual(diff.most_origin_asns[1].updates, 0)
        self.assertEqual(diff.most_origin_asns[1].withdraws, 0)

        self.assertEqual(diff.most_origin_asns[2].advt, 0)
        self.assertEqual(diff.most_origin_asns[2].as_path, [[]])
        self.assertEqual(diff.most_origin_asns[2].comm_set, [[]])
        self.assertEqual(diff.most_origin_asns[2].filename, self.upd_2_mrt)
        self.assertEqual(diff.most_origin_asns[2].next_hop, None)
        self.assertEqual(
            diff.most_origin_asns[2].origin_asns, set(["396559", "396542"])
        )
        self.assertEqual(diff.most_origin_asns[2].peer_asn, None)
        self.assertEqual(diff.most_origin_asns[2].prefix, "2620:74:2a::/48")
        self.assertEqual(diff.most_origin_asns[2].timestamp, "20220501.2305")
        self.assertEqual(diff.most_origin_asns[2].updates, 0)
        self.assertEqual(diff.most_origin_asns[2].withdraws, 0)

        self.assertEqual(diff.most_origin_asns[3].advt, 0)
        self.assertEqual(diff.most_origin_asns[3].as_path, [[]])
        self.assertEqual(diff.most_origin_asns[3].comm_set, [[]])
        self.assertEqual(diff.most_origin_asns[3].filename, self.upd_2_mrt)
        self.assertEqual(diff.most_origin_asns[3].next_hop, None)
        self.assertEqual(
            diff.most_origin_asns[3].origin_asns, set(["37154", "7420"])
        )
        self.assertEqual(diff.most_origin_asns[3].peer_asn, None)
        self.assertEqual(diff.most_origin_asns[3].prefix, "196.46.192.0/19")
        self.assertEqual(diff.most_origin_asns[3].timestamp, "20220501.2305")
        self.assertEqual(diff.most_origin_asns[3].updates, 0)
        self.assertEqual(diff.most_origin_asns[3].withdraws, 0)

        self.assertEqual(diff.most_origin_asns[4].advt, 0)
        self.assertEqual(diff.most_origin_asns[4].as_path, [[]])
        self.assertEqual(diff.most_origin_asns[4].comm_set, [[]])
        self.assertEqual(diff.most_origin_asns[4].filename, self.upd_2_mrt)
        self.assertEqual(diff.most_origin_asns[4].next_hop, None)
        self.assertEqual(
            diff.most_origin_asns[4].origin_asns, set(["7487", "54396"])
        )
        self.assertEqual(diff.most_origin_asns[4].peer_asn, None)
        self.assertEqual(diff.most_origin_asns[4].prefix, "205.197.192.0/21")
        self.assertEqual(diff.most_origin_asns[4].timestamp, "20220501.2305")
        self.assertEqual(diff.most_origin_asns[4].updates, 0)
        self.assertEqual(diff.most_origin_asns[4].withdraws, 0)

        self.assertEqual(diff.most_origin_asns[5].advt, 0)
        self.assertEqual(diff.most_origin_asns[5].as_path, [[]])
        self.assertEqual(diff.most_origin_asns[5].comm_set, [[]])
        self.assertEqual(diff.most_origin_asns[5].filename, self.upd_2_mrt)
        self.assertEqual(diff.most_origin_asns[5].next_hop, None)
        self.assertEqual(
            diff.most_origin_asns[5].origin_asns, set(["203020", "29802"])
        )
        self.assertEqual(diff.most_origin_asns[5].peer_asn, None)
        self.assertEqual(diff.most_origin_asns[5].prefix, "206.123.159.0/24")
        self.assertEqual(diff.most_origin_asns[5].timestamp, "20220501.2305")
        self.assertEqual(diff.most_origin_asns[5].updates, 0)
        self.assertEqual(diff.most_origin_asns[5].withdraws, 0)

        self.assertEqual(diff.most_origin_asns[6].advt, 0)
        self.assertEqual(diff.most_origin_asns[6].as_path, [[]])
        self.assertEqual(diff.most_origin_asns[6].comm_set, [[]])
        self.assertEqual(diff.most_origin_asns[6].filename, self.upd_2_mrt)
        self.assertEqual(diff.most_origin_asns[6].next_hop, None)
        self.assertEqual(
            diff.most_origin_asns[6].origin_asns, set(["52000", "19318"])
        )
        self.assertEqual(diff.most_origin_asns[6].peer_asn, None)
        self.assertEqual(diff.most_origin_asns[6].prefix, "68.168.210.0/24")
        self.assertEqual(diff.most_origin_asns[6].timestamp, "20220501.2305")
        self.assertEqual(diff.most_origin_asns[6].updates, 0)
        self.assertEqual(diff.most_origin_asns[6].withdraws, 0)

        self.assertEqual(diff.most_origin_asns[7].advt, 0)
        self.assertEqual(diff.most_origin_asns[7].as_path, [[]])
        self.assertEqual(diff.most_origin_asns[7].comm_set, [[]])
        self.assertEqual(diff.most_origin_asns[7].filename, self.upd_2_mrt)
        self.assertEqual(diff.most_origin_asns[7].next_hop, None)
        self.assertEqual(
            diff.most_origin_asns[7].origin_asns, set(["55020", "137951"])
        )
        self.assertEqual(diff.most_origin_asns[7].peer_asn, None)
        self.assertEqual(diff.most_origin_asns[7].prefix, "156.241.128.0/22")
        self.assertEqual(diff.most_origin_asns[7].timestamp, "20220501.2305")
        self.assertEqual(diff.most_origin_asns[7].updates, 0)
        self.assertEqual(diff.most_origin_asns[7].withdraws, 0)

        self.assertEqual(diff.most_origin_asns[8].advt, 0)
        self.assertEqual(diff.most_origin_asns[8].as_path, [[]])
        self.assertEqual(diff.most_origin_asns[8].comm_set, [[]])
        self.assertEqual(diff.most_origin_asns[8].filename, self.upd_2_mrt)
        self.assertEqual(diff.most_origin_asns[8].next_hop, None)
        self.assertEqual(
            diff.most_origin_asns[8].origin_asns, set(["269208", "268347"])
        )
        self.assertEqual(diff.most_origin_asns[8].peer_asn, None)
        self.assertEqual(diff.most_origin_asns[8].prefix, "2804:610c::/32")
        self.assertEqual(diff.most_origin_asns[8].timestamp, "20220501.2305")
        self.assertEqual(diff.most_origin_asns[8].updates, 0)
        self.assertEqual(diff.most_origin_asns[8].withdraws, 0)

        self.assertEqual(diff.total_upd, 29688)
        self.assertEqual(diff.total_advt, 29396)
        self.assertEqual(diff.total_withd, 950)
        self.assertEqual(diff.file_list, [])
        self.assertEqual(diff.timestamp, "")

    def test_get_diff_larger(self):
        self.assertRaises(ValueError, self.upd_1_stats.get_diff_larger, None)
        self.assertRaises(TypeError, self.upd_1_stats.get_diff_larger, 123)

        diff = self.upd_1_stats.get_diff_larger(self.upd_2_stats)
        self.assertIsInstance(diff, mrt_stats)

        self.assertEqual(len(diff.longest_comm_set), 1)
        self.assertEqual(diff.longest_comm_set[0].advt, 0)
        self.assertEqual(
            diff.longest_comm_set[0].as_path,
            ["58952", "39386", "47589", "9155", "13335"]
        )
        self.assertEqual(
            diff.longest_comm_set[0].comm_set,
            [
                "13335:10249", "13335:19060", "13335:20510", "13335:20520",
                "24115:1248", "24115:2497", "24115:2518", "24115:3491",
                "24115:3856", "24115:4621", "24115:4628", "24115:4637",
                "24115:4651", "24115:4657", "24115:4739", "24115:4761",
                "24115:4773", "24115:4775", "24115:4788", "24115:4800", 
                "24115:4818", "24115:4826", "24115:4844", "24115:5017",
                "24115:6619", "24115:6648", "24115:7568", "24115:7595",
                "24115:7598", "24115:7632", "24115:7642", "24115:7713",
                "24115:8529", "24115:8757", "24115:8781", "24115:9269",
                "24115:9299", "24115:9304", "24115:9326", "24115:9329",
                "24115:9381", "24115:9498", "24115:9505", "24115:9534",
                "24115:9583", "24115:9658", "24115:9873", "24115:9892",
                "24115:9902", "24115:9924", "24115:9930", "24115:10026",
                "24115:10030", "24115:10089", "24115:10158", "24115:16265",
                "24115:17451", "24115:17494", "24115:17511", "24115:17547",
                "24115:17557", "24115:17639", "24115:17645", "24115:17660",
                "24115:17666", "24115:17676", "24115:17726", "24115:17922",
                "24115:17978", "24115:18001", "24115:18059", "24115:18403",
                "24115:20940", "24115:23576", "24115:23673", "24115:23930",
                "24115:23939", "24115:23944", "24115:23947", "24115:24203",
                "24115:24218", "24115:24482", "24115:24535", "24115:38001",
                "24115:38040", "24115:38082", "24115:38090", "24115:38158",
                "24115:38182", "24115:38193", "24115:38195", "24115:38321",
                "24115:38322", "24115:38466", "24115:38565", "24115:38740",
                "24115:38753", "24115:38757", "24115:38861", "24115:38880",
                "24115:38895", "24115:39386", "24115:45102", "24115:45352",
                "24115:45430", "24115:45474", "24115:45494", "24115:45629",
                "24115:45634", "24115:45706", "24115:45796", "24115:45845",
                "24115:45903", "24115:50010", "24115:54994", "24115:55329",
                "24115:55658", "24115:55685", "24115:55818", "24115:55944",
                "24115:55967", "24115:56258", "24115:56308", "24115:58389",
                "24115:58430", "24115:58436", "24115:58453", "24115:58580",
                "24115:58587", "24115:58599", "24115:58601", "24115:58682",
                "24115:58715", "24115:58717", "24115:58952", "24115:59019",
                "24115:59318", "24115:59605", "24115:63516", "24115:63541",
                "24115:63916", "24115:63927", "24115:63947", "24115:64049",
                "24115:64096", "24115:65023", "24115:1000:2", "24115:1001:1",
                "24115:1002:1", "24115:1003:115", "24115:1004:39386"
            ]
        )
        self.assertEqual(diff.longest_comm_set[0].filename, self.upd_2_mrt)
        self.assertEqual(diff.longest_comm_set[0].next_hop, "27.111.229.106")
        self.assertEqual(diff.longest_comm_set[0].origin_asns, set(["13335"]))
        self.assertEqual(diff.longest_comm_set[0].peer_asn, "58952")
        self.assertEqual(diff.longest_comm_set[0].prefix, "162.158.59.0/24")
        self.assertEqual(diff.longest_comm_set[0].timestamp, "20220501.2308")
        self.assertEqual(diff.longest_comm_set[0].updates, 0)
        self.assertEqual(diff.longest_comm_set[0].withdraws, 0)


        self.assertEqual(len(diff.most_advt_prefixes), 1)
        self.assertEqual(diff.most_advt_prefixes[0].advt, 884)
        self.assertEqual(diff.most_advt_prefixes[0].as_path, [[]])
        self.assertEqual(diff.most_advt_prefixes[0].comm_set, [[]])
        self.assertEqual(diff.most_advt_prefixes[0].filename, self.upd_2_mrt)
        self.assertEqual(diff.most_advt_prefixes[0].next_hop, None)
        self.assertEqual(diff.most_advt_prefixes[0].origin_asns, set())
        self.assertEqual(diff.most_advt_prefixes[0].peer_asn, None)
        self.assertEqual(diff.most_advt_prefixes[0].prefix, "89.30.150.0/23")
        self.assertEqual(diff.most_advt_prefixes[0].timestamp, "20220501.2305")
        self.assertEqual(diff.most_advt_prefixes[0].updates, 0)
        self.assertEqual(diff.most_advt_prefixes[0].withdraws, 0)

        self.assertEqual(len(diff.most_upd_prefixes), 1)
        self.assertEqual(diff.most_upd_prefixes[0].advt, 0)
        self.assertEqual(diff.most_upd_prefixes[0].as_path, [[]])
        self.assertEqual(diff.most_upd_prefixes[0].comm_set, [[]])
        self.assertEqual(diff.most_upd_prefixes[0].filename, self.upd_2_mrt)
        self.assertEqual(diff.most_upd_prefixes[0].next_hop, None)
        self.assertEqual(diff.most_upd_prefixes[0].origin_asns, set())
        self.assertEqual(diff.most_upd_prefixes[0].peer_asn, None)
        self.assertEqual(diff.most_upd_prefixes[0].prefix, "89.30.150.0/23")
        self.assertEqual(diff.most_upd_prefixes[0].timestamp, "20220501.2305")
        self.assertEqual(diff.most_upd_prefixes[0].updates, 898)
        self.assertEqual(diff.most_upd_prefixes[0].withdraws, 0)

        self.assertEqual(len(diff.most_withd_prefixes), 1)
        self.assertEqual(diff.most_withd_prefixes[0].advt, 0)
        self.assertEqual(diff.most_withd_prefixes[0].as_path, [[]])
        self.assertEqual(diff.most_withd_prefixes[0].comm_set, [[]])
        self.assertEqual(diff.most_withd_prefixes[0].filename, self.upd_2_mrt)
        self.assertEqual(diff.most_withd_prefixes[0].next_hop, None)
        self.assertEqual(diff.most_withd_prefixes[0].origin_asns, set())
        self.assertEqual(diff.most_withd_prefixes[0].peer_asn, None)
        self.assertEqual(
            diff.most_withd_prefixes[0].prefix, "2a01:9e00:4279::/48"
        )
        self.assertEqual(
            diff.most_withd_prefixes[0].timestamp, "20220501.2305"
        )
        self.assertEqual(diff.most_withd_prefixes[0].updates, 0)
        self.assertEqual(diff.most_withd_prefixes[0].withdraws, 89)

        self.assertEqual(len(diff.most_advt_origin_asn), 1)
        self.assertEqual(diff.most_advt_origin_asn[0].advt, 2628)
        self.assertEqual(diff.most_advt_origin_asn[0].as_path, [[]])
        self.assertEqual(diff.most_advt_origin_asn[0].comm_set, [[]])
        self.assertEqual(diff.most_advt_origin_asn[0].filename, self.upd_2_mrt)
        self.assertEqual(diff.most_advt_origin_asn[0].next_hop, None)
        self.assertEqual(
            diff.most_advt_origin_asn[0].origin_asns, set(["20473"])
        )
        self.assertEqual(diff.most_advt_origin_asn[0].peer_asn, None)
        self.assertEqual(diff.most_advt_origin_asn[0].prefix, None)
        self.assertEqual(
            diff.most_advt_origin_asn[0].timestamp, "20220501.2305"
        )
        self.assertEqual(diff.most_advt_origin_asn[0].updates, 0)
        self.assertEqual(diff.most_advt_origin_asn[0].withdraws, 0)

        self.assertEqual(len(diff.most_advt_peer_asn), 1)
        self.assertEqual(diff.most_advt_peer_asn[0].advt, 11595)
        self.assertEqual(diff.most_advt_peer_asn[0].as_path, [[]])
        self.assertEqual(diff.most_advt_peer_asn[0].comm_set, [[]])
        self.assertEqual(diff.most_advt_peer_asn[0].filename, self.upd_2_mrt)
        self.assertEqual(diff.most_advt_peer_asn[0].next_hop, None)
        self.assertEqual(diff.most_advt_peer_asn[0].origin_asns, set())
        self.assertEqual(diff.most_advt_peer_asn[0].peer_asn, "18106")
        self.assertEqual(diff.most_advt_peer_asn[0].prefix, None)
        self.assertEqual(diff.most_advt_peer_asn[0].timestamp, "20220501.2305")
        self.assertEqual(diff.most_advt_peer_asn[0].updates, 0)
        self.assertEqual(diff.most_advt_peer_asn[0].withdraws, 0)

        self.assertEqual(len(diff.most_upd_peer_asn), 1)
        self.assertEqual(diff.most_upd_peer_asn[0].advt, 0)
        self.assertEqual(diff.most_upd_peer_asn[0].as_path, [[]])
        self.assertEqual(diff.most_upd_peer_asn[0].comm_set, [[]])
        self.assertEqual(diff.most_upd_peer_asn[0].filename, self.upd_2_mrt)
        self.assertEqual(diff.most_upd_peer_asn[0].next_hop, None)
        self.assertEqual(diff.most_upd_peer_asn[0].origin_asns, set())
        self.assertEqual(diff.most_upd_peer_asn[0].peer_asn, "18106")
        self.assertEqual(diff.most_upd_peer_asn[0].prefix, None)
        self.assertEqual(diff.most_upd_peer_asn[0].timestamp, "20220501.2305")
        self.assertEqual(diff.most_upd_peer_asn[0].updates, 11781)
        self.assertEqual(diff.most_upd_peer_asn[0].withdraws, 0)

        self.assertEqual(diff.total_upd, 29688)
        self.assertEqual(diff.total_advt, 29396)
        self.assertEqual(diff.total_withd, 950)
        self.assertEqual(diff.file_list, [])
        self.assertEqual(diff.timestamp, "20220501.2305")

        ##diff.print()

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
        stats = mrt_stats()
        stats.from_file(self.upd_1_json)

        self.assertRaises(ValueError, stats.merge, None)
        self.assertRaises(TypeError, stats.merge, 123)

        ret = stats.merge(self.upd_2_stats)
        self.assertIsInstance(ret, bool)
        self.assertTrue(ret)

        self.assertEqual(len(stats.bogon_origin_asns), 1)
        self.assertEqual(stats.bogon_origin_asns[0].advt, 0)
        self.assertEqual(stats.bogon_origin_asns[0].as_path, ["137409", "17494", "137491", "58689", "137464", "65551"])
        self.assertEqual(stats.bogon_origin_asns[0].comm_set, [])
        self.assertEqual(
            stats.bogon_origin_asns[0].filename,
            self.upd_1_mrt
        )
        self.assertEqual(stats.bogon_origin_asns[0].next_hop, "27.111.228.145")
        self.assertEqual(stats.bogon_origin_asns[0].origin_asns, set(["65551"]))
        self.assertEqual(stats.bogon_origin_asns[0].peer_asn, "137409")
        self.assertEqual(stats.bogon_origin_asns[0].prefix, "103.109.236.0/24")
        self.assertEqual(stats.bogon_origin_asns[0].timestamp, "20220421.0200")
        self.assertEqual(stats.bogon_origin_asns[0].updates, 0)
        self.assertEqual(stats.bogon_origin_asns[0].withdraws, 0)

        self.assertEqual(len(stats.bogon_prefixes), 2)
        self.assertEqual(stats.bogon_prefixes[0].advt, 0)
        self.assertEqual(stats.bogon_prefixes[0].as_path, ["136168"])
        self.assertEqual(stats.bogon_prefixes[0].comm_set, [])
        self.assertEqual(
            stats.bogon_prefixes[0].filename,
            self.upd_1_mrt
        )
        self.assertEqual(stats.bogon_prefixes[0].next_hop, "27.111.228.170")
        self.assertEqual(stats.bogon_prefixes[0].origin_asns, set(["136168"]))
        self.assertEqual(stats.bogon_prefixes[0].peer_asn, "136168")
        self.assertEqual(stats.bogon_prefixes[0].prefix, "100.96.200.3/32")
        self.assertEqual(stats.bogon_prefixes[0].timestamp, "20220421.0201")
        self.assertEqual(stats.bogon_prefixes[0].updates, 0)
        self.assertEqual(stats.bogon_prefixes[0].withdraws, 0)

        self.assertEqual(stats.bogon_prefixes[1].advt, 0)
        self.assertEqual(stats.bogon_prefixes[1].as_path, ["133210", "6939"])
        self.assertEqual(
            stats.bogon_prefixes[1].comm_set,
            [
                "24115:6939", "24115:24115", "24115:65023",
                "24115:1000:2", "24115:1001:1", "24115:1002:1",
                "24115:1003:1", "24115:1004:6939"
            ]
        )
        self.assertEqual(
            stats.bogon_prefixes[1].filename,
            self.upd_2_mrt
        )
        self.assertEqual(stats.bogon_prefixes[1].next_hop, "27.111.228.81")
        self.assertEqual(stats.bogon_prefixes[1].origin_asns, set(["6939"]))
        self.assertEqual(stats.bogon_prefixes[1].peer_asn, "133210")
        self.assertEqual(stats.bogon_prefixes[1].prefix, "192.88.99.0/24")
        self.assertEqual(stats.bogon_prefixes[1].timestamp, "20220501.2309")
        self.assertEqual(stats.bogon_prefixes[1].updates, 0)
        self.assertEqual(stats.bogon_prefixes[1].withdraws, 0)

        self.assertEqual(len(stats.longest_as_path), 1)
        self.assertEqual(stats.longest_as_path[0].advt, 0)
        self.assertEqual(
            stats.longest_as_path[0].as_path,
            [
                "18106", "23106", "264228", "264228", "264228", "264228",
                "264228", "264228", "264228", "264228", "264228", "264228",
                "264228", "264228", "264228", "264228", "264228", "264228",
                "264228", "264228", "264228", "264228", "264228", "264228",
                "264228", "264228", "264228"
            ]
        )
        self.assertEqual(stats.longest_as_path[0].comm_set,["13538:3000"])
        self.assertEqual(
            stats.longest_as_path[0].filename,
            self.upd_1_mrt
        )
        self.assertEqual(
            stats.longest_as_path[0].next_hop,
            ["2001:de8:4::1:8106:1", "fe80::bac2:53ff:fedb:2004"]
        )
        self.assertEqual(stats.longest_as_path[0].origin_asns, set(["264228"]))
        self.assertEqual(stats.longest_as_path[0].peer_asn, "18106")
        self.assertEqual(stats.longest_as_path[0].prefix, "2804:2488::/48")
        self.assertEqual(stats.longest_as_path[0].timestamp, "20220421.0200")
        self.assertEqual(stats.longest_as_path[0].updates, 0)
        self.assertEqual(stats.longest_as_path[0].withdraws, 0)

        self.assertEqual(len(stats.longest_comm_set), 1)
        self.assertEqual(stats.longest_comm_set[0].advt, 0)
        self.assertEqual(
            stats.longest_comm_set[0].as_path,
            ["58952", "39386", "47589", "9155", "13335"]
        )
        self.assertEqual(
            stats.longest_comm_set[0].comm_set,
            [
                "13335:10249", "13335:19060", "13335:20510", "13335:20520",
                "24115:1248", "24115:2497", "24115:2518", "24115:3491",
                "24115:3856", "24115:4621", "24115:4628", "24115:4637",
                "24115:4651", "24115:4657", "24115:4739", "24115:4761",
                "24115:4773", "24115:4775", "24115:4788", "24115:4800", 
                "24115:4818", "24115:4826", "24115:4844", "24115:5017",
                "24115:6619", "24115:6648", "24115:7568", "24115:7595",
                "24115:7598", "24115:7632", "24115:7642", "24115:7713",
                "24115:8529", "24115:8757", "24115:8781", "24115:9269",
                "24115:9299", "24115:9304", "24115:9326", "24115:9329",
                "24115:9381", "24115:9498", "24115:9505", "24115:9534",
                "24115:9583", "24115:9658", "24115:9873", "24115:9892",
                "24115:9902", "24115:9924", "24115:9930", "24115:10026",
                "24115:10030", "24115:10089", "24115:10158", "24115:16265",
                "24115:17451", "24115:17494", "24115:17511", "24115:17547",
                "24115:17557", "24115:17639", "24115:17645", "24115:17660",
                "24115:17666", "24115:17676", "24115:17726", "24115:17922",
                "24115:17978", "24115:18001", "24115:18059", "24115:18403",
                "24115:20940", "24115:23576", "24115:23673", "24115:23930",
                "24115:23939", "24115:23944", "24115:23947", "24115:24203",
                "24115:24218", "24115:24482", "24115:24535", "24115:38001",
                "24115:38040", "24115:38082", "24115:38090", "24115:38158",
                "24115:38182", "24115:38193", "24115:38195", "24115:38321",
                "24115:38322", "24115:38466", "24115:38565", "24115:38740",
                "24115:38753", "24115:38757", "24115:38861", "24115:38880",
                "24115:38895", "24115:39386", "24115:45102", "24115:45352",
                "24115:45430", "24115:45474", "24115:45494", "24115:45629",
                "24115:45634", "24115:45706", "24115:45796", "24115:45845",
                "24115:45903", "24115:50010", "24115:54994", "24115:55329",
                "24115:55658", "24115:55685", "24115:55818", "24115:55944",
                "24115:55967", "24115:56258", "24115:56308", "24115:58389",
                "24115:58430", "24115:58436", "24115:58453", "24115:58580",
                "24115:58587", "24115:58599", "24115:58601", "24115:58682",
                "24115:58715", "24115:58717", "24115:58952", "24115:59019",
                "24115:59318", "24115:59605", "24115:63516", "24115:63541",
                "24115:63916", "24115:63927", "24115:63947", "24115:64049",
                "24115:64096", "24115:65023", "24115:1000:2", "24115:1001:1",
                "24115:1002:1", "24115:1003:115", "24115:1004:39386"
            ]
        )
        self.assertEqual(
            stats.longest_comm_set[0].filename,
            self.upd_2_mrt
        )
        self.assertEqual(stats.longest_comm_set[0].next_hop, "27.111.229.106")
        self.assertEqual(stats.longest_comm_set[0].origin_asns, set(["13335"]))
        self.assertEqual(stats.longest_comm_set[0].peer_asn, "58952")
        self.assertEqual(stats.longest_comm_set[0].prefix, "162.158.59.0/24")
        self.assertEqual(stats.longest_comm_set[0].timestamp, "20220501.2308")
        self.assertEqual(stats.longest_comm_set[0].updates, 0)
        self.assertEqual(stats.longest_comm_set[0].withdraws, 0)

        self.assertEqual(len(stats.invalid_len), 8)
        self.assertEqual(stats.invalid_len[0].advt, 0)
        self.assertEqual(stats.invalid_len[0].as_path, ["199524", "38082"])
        self.assertEqual(stats.invalid_len[0].comm_set, [])
        self.assertEqual(
            stats.invalid_len[0].filename,
            self.upd_1_mrt
        )
        self.assertEqual(stats.invalid_len[0].next_hop, ["2001:de8:4::19:9524:1"])
        self.assertEqual(stats.invalid_len[0].origin_asns, set(["38082"]))
        self.assertEqual(stats.invalid_len[0].peer_asn, "199524")
        self.assertEqual(stats.invalid_len[0].prefix, "2405:4000:800:8::/64")
        self.assertEqual(stats.invalid_len[0].timestamp, "20220421.0200")
        self.assertEqual(stats.invalid_len[0].updates, 0)
        self.assertEqual(stats.invalid_len[0].withdraws, 0)

        self.assertEqual(stats.invalid_len[1].advt, 0)
        self.assertEqual(
            stats.invalid_len[1].as_path,
            ["133210", "4788", "38044", "38044", "23736"]
        )
        self.assertEqual(
            stats.invalid_len[1].comm_set,
            [
                "4788:811", "4788:6300", "4788:6310", "4788:16300",
                "4788:23030", "4788:32011", "24115:4788", "24115:65023",
                "24115:1000:2", "24115:1001:2", "24115:1002:2", "24115:1003:1",
                "24115:1004:4788"
            ]
        )
        self.assertEqual(
            stats.invalid_len[1].filename,
            self.upd_1_mrt
        )
        self.assertEqual(
            stats.invalid_len[1].next_hop,
            ["2001:de8:4::4788:1", "fe80::8ae6:4b00:6c1:6029"]
        )
        self.assertEqual(stats.invalid_len[1].origin_asns, set(["23736"]))
        self.assertEqual(stats.invalid_len[1].peer_asn, "133210")
        self.assertEqual(stats.invalid_len[1].prefix, "2400:7400:0:105::/64")
        self.assertEqual(stats.invalid_len[1].timestamp, "20220421.0201")
        self.assertEqual(stats.invalid_len[1].updates, 0)
        self.assertEqual(stats.invalid_len[1].withdraws, 0)

        self.assertEqual(stats.invalid_len[2].advt, 0)
        self.assertEqual(
            stats.invalid_len[2].as_path,
            ["133210", "4788", "38044", "23736"]
        )
        self.assertEqual(
            stats.invalid_len[2].comm_set,
            [
                "4788:811", "4788:6300", "4788:6310", "4788:16300",
                "4788:23030", "4788:34002", "24115:4788", "24115:65023",
                "24115:1000:2", "24115:1001:2", "24115:1002:2", "24115:1003:1",
                "24115:1004:4788"
            ]
        )
        self.assertEqual(
            stats.invalid_len[2].filename,
            self.upd_1_mrt
        )
        self.assertEqual(
            stats.invalid_len[2].next_hop,
            ["2001:de8:4::4788:1", "fe80::8ae6:4b00:6c1:6029"]
        )
        self.assertEqual(stats.invalid_len[2].origin_asns, set(["23736"]))
        self.assertEqual(stats.invalid_len[2].peer_asn, "133210")
        self.assertEqual(stats.invalid_len[2].prefix, "2400:7400:0:106::/64")
        self.assertEqual(stats.invalid_len[2].timestamp, "20220421.0201")
        self.assertEqual(stats.invalid_len[2].updates, 0)
        self.assertEqual(stats.invalid_len[2].withdraws, 0)

        self.assertEqual(stats.invalid_len[3].advt, 0)
        self.assertEqual(stats.invalid_len[3].as_path, ["136168"])
        self.assertEqual(stats.invalid_len[3].comm_set, [])
        self.assertEqual(
            stats.invalid_len[3].filename,
            self.upd_1_mrt
        )
        self.assertEqual(stats.invalid_len[3].next_hop, "27.111.228.170")
        self.assertEqual(stats.invalid_len[3].origin_asns, set(["136168"]))
        self.assertEqual(stats.invalid_len[3].peer_asn, "136168")
        self.assertEqual(stats.invalid_len[3].prefix, "100.96.200.3/32")
        self.assertEqual(stats.invalid_len[3].timestamp, "20220421.0201")
        self.assertEqual(stats.invalid_len[3].updates, 0)
        self.assertEqual(stats.invalid_len[3].withdraws, 0)

        self.assertEqual(stats.invalid_len[4].advt, 0)
        self.assertEqual(
            stats.invalid_len[4].as_path, ["133210", "59318", "59318", "15133"]
        )
        self.assertEqual(
            stats.invalid_len[4].comm_set,
            [
                "15133:4351", "24115:59318", "24115:65023", "59318:2015",
                "24115:1000:2", "24115:1001:1", "24115:1002:1", "24115:1003:33",
                "24115:1004:59318"
            ]
        )
        self.assertEqual(
            stats.invalid_len[4].filename,
            self.upd_1_mrt
        )
        self.assertEqual(
            stats.invalid_len[4].next_hop,
            ["2001:de8:4::13:1207:1", "fe80::8ae6:4b00:6c1:6029"]
        )
        self.assertEqual(stats.invalid_len[4].origin_asns, set(["15133"]))
        self.assertEqual(stats.invalid_len[4].peer_asn, "133210")
        self.assertEqual(stats.invalid_len[4].prefix, "2404:b300:33:1::/64")
        self.assertEqual(stats.invalid_len[4].timestamp, "20220421.0203")
        self.assertEqual(stats.invalid_len[4].updates, 0)
        self.assertEqual(stats.invalid_len[4].withdraws, 0)

        self.assertEqual(stats.invalid_len[5].advt, 0)
        self.assertEqual(stats.invalid_len[5].as_path, ["136168"])
        self.assertEqual(stats.invalid_len[5].comm_set, [])
        self.assertEqual(
            stats.invalid_len[5].filename,
            self.upd_1_mrt
        )
        self.assertEqual(stats.invalid_len[5].next_hop, "27.111.228.170")
        self.assertEqual(stats.invalid_len[5].origin_asns, set(["136168"]))
        self.assertEqual(stats.invalid_len[5].peer_asn, "136168")
        self.assertEqual(stats.invalid_len[5].prefix, "123.253.228.188/30")
        self.assertEqual(stats.invalid_len[5].timestamp, "20220421.0204")
        self.assertEqual(stats.invalid_len[5].updates, 0)
        self.assertEqual(stats.invalid_len[5].withdraws, 0)

        self.assertEqual(stats.invalid_len[6].advt, 0)
        self.assertEqual(
            stats.invalid_len[6].as_path, ["133210", "4788", "54994"]
        )
        self.assertEqual(stats.invalid_len[6].comm_set, 
            [
                "4788:801", "4788:810", "4788:6300", "4788:6310", "24115:4788",
                "24115:65023", "24115:1000:2", "24115:1001:1", "24115:1002:2",
                "24115:1003:2", "24115:1004:4788"
            ]
        )
        self.assertEqual(
            stats.invalid_len[6].filename,
            self.upd_2_mrt
        )
        self.assertEqual(stats.invalid_len[6].next_hop,
            ["2001:de8:4::4788:3", "fe80::8ae6:4b00:6c1:6029"]
        )
        self.assertEqual(stats.invalid_len[6].origin_asns, set(["54994"]))
        self.assertEqual(stats.invalid_len[6].peer_asn, "133210")
        self.assertEqual(stats.invalid_len[6].prefix, "2001:e68:20db:10::/64")
        self.assertEqual(stats.invalid_len[6].timestamp, "20220501.2306")
        self.assertEqual(stats.invalid_len[6].updates, 0)
        self.assertEqual(stats.invalid_len[6].withdraws, 0)

        self.assertEqual(stats.invalid_len[7].advt, 0)
        self.assertEqual(
            stats.invalid_len[7].as_path,
            ["133210", "4788", "54994"]
        )
        self.assertEqual(stats.invalid_len[7].comm_set,
            [
                "4788:801", "4788:810", "4788:6300", "4788:6310", "24115:4788",
                "24115:65023", "24115:1000:2", "24115:1001:1", "24115:1002:2",
                "24115:1003:2", "24115:1004:4788"
            ]
        )
        self.assertEqual(
            stats.invalid_len[7].filename,
            self.upd_2_mrt
        )
        self.assertEqual(stats.invalid_len[7].next_hop,
            ["2001:de8:4::4788:3", "fe80::8ae6:4b00:6c1:6029"]
        )
        self.assertEqual(stats.invalid_len[7].origin_asns, set(["54994"]))
        self.assertEqual(stats.invalid_len[7].peer_asn, "133210")
        self.assertEqual(stats.invalid_len[7].prefix, "2001:e68:20db:11::/64")
        self.assertEqual(stats.invalid_len[7].timestamp, "20220501.2306")
        self.assertEqual(stats.invalid_len[7].updates, 0)
        self.assertEqual(stats.invalid_len[7].withdraws, 0)

        self.assertEqual(len(stats.most_advt_prefixes), 1)
        self.assertEqual(stats.most_advt_prefixes[0].advt, 884)
        self.assertEqual(stats.most_advt_prefixes[0].as_path, [[]])
        self.assertEqual(stats.most_advt_prefixes[0].comm_set, [[]])
        self.assertEqual(
            stats.most_advt_prefixes[0].filename,
            self.upd_2_mrt
        )
        self.assertEqual(stats.most_advt_prefixes[0].next_hop, None)
        self.assertEqual(stats.most_advt_prefixes[0].origin_asns, set())
        self.assertEqual(stats.most_advt_prefixes[0].peer_asn, None)
        self.assertEqual(stats.most_advt_prefixes[0].prefix, "89.30.150.0/23")
        self.assertEqual(stats.most_advt_prefixes[0].timestamp, "20220501.2305")
        self.assertEqual(stats.most_advt_prefixes[0].updates, 0)
        self.assertEqual(stats.most_advt_prefixes[0].withdraws, 0)

        self.assertEqual(len(stats.most_upd_prefixes), 1)
        self.assertEqual(stats.most_upd_prefixes[0].advt, 0)
        self.assertEqual(stats.most_upd_prefixes[0].as_path, [[]])
        self.assertEqual(stats.most_upd_prefixes[0].comm_set, [[]])
        self.assertEqual(
            stats.most_upd_prefixes[0].filename,
            self.upd_2_mrt
        )
        self.assertEqual(stats.most_upd_prefixes[0].next_hop, None)
        self.assertEqual(stats.most_upd_prefixes[0].origin_asns, set())
        self.assertEqual(stats.most_upd_prefixes[0].peer_asn, None)
        self.assertEqual(stats.most_upd_prefixes[0].prefix, "89.30.150.0/23")
        self.assertEqual(stats.most_upd_prefixes[0].timestamp, "20220501.2305")
        self.assertEqual(stats.most_upd_prefixes[0].updates, 898)
        self.assertEqual(stats.most_upd_prefixes[0].withdraws, 0)

        self.assertEqual(len(stats.most_withd_prefixes), 1)
        self.assertEqual(stats.most_withd_prefixes[0].advt, 0)
        self.assertEqual(stats.most_withd_prefixes[0].as_path, [[]])
        self.assertEqual(stats.most_withd_prefixes[0].comm_set, [[]])
        self.assertEqual(
            stats.most_withd_prefixes[0].filename,
            self.upd_2_mrt
        )
        self.assertEqual(stats.most_withd_prefixes[0].next_hop, None)
        self.assertEqual(stats.most_withd_prefixes[0].origin_asns, set())
        self.assertEqual(stats.most_withd_prefixes[0].peer_asn, None)
        self.assertEqual(
            stats.most_withd_prefixes[0].prefix, "2a01:9e00:4279::/48"
        )
        self.assertEqual(
            stats.most_withd_prefixes[0].timestamp, "20220501.2305"
        )
        self.assertEqual(stats.most_withd_prefixes[0].updates, 0)
        self.assertEqual(stats.most_withd_prefixes[0].withdraws, 89)

        self.assertEqual(len(stats.most_advt_origin_asn), 1)
        self.assertEqual(stats.most_advt_origin_asn[0].advt, 2628)
        self.assertEqual(stats.most_advt_origin_asn[0].as_path, [[]])
        self.assertEqual(stats.most_advt_origin_asn[0].comm_set, [[]])
        self.assertEqual(
            stats.most_advt_origin_asn[0].filename,
            self.upd_2_mrt
        )
        self.assertEqual(stats.most_advt_origin_asn[0].next_hop, None)
        self.assertEqual(
            stats.most_advt_origin_asn[0].origin_asns, set(["20473"])
        )
        self.assertEqual(stats.most_advt_origin_asn[0].peer_asn, None)
        self.assertEqual(stats.most_advt_origin_asn[0].prefix, None)
        self.assertEqual(
            stats.most_advt_origin_asn[0].timestamp, "20220501.2305"
        )
        self.assertEqual(stats.most_advt_origin_asn[0].updates, 0)
        self.assertEqual(stats.most_advt_origin_asn[0].withdraws, 0)

        self.assertEqual(len(stats.most_advt_peer_asn), 1)
        self.assertEqual(stats.most_advt_peer_asn[0].advt, 11595)
        self.assertEqual(stats.most_advt_peer_asn[0].as_path, [[]])
        self.assertEqual(stats.most_advt_peer_asn[0].comm_set, [[]])
        self.assertEqual(
            stats.most_advt_peer_asn[0].filename,
            self.upd_2_mrt
        )
        self.assertEqual(stats.most_advt_peer_asn[0].next_hop, None)
        self.assertEqual(stats.most_advt_peer_asn[0].origin_asns, set())
        self.assertEqual(stats.most_advt_peer_asn[0].peer_asn, "18106")
        self.assertEqual(stats.most_advt_peer_asn[0].prefix, None)
        self.assertEqual(stats.most_advt_peer_asn[0].timestamp, "20220501.2305")
        self.assertEqual(stats.most_advt_peer_asn[0].updates, 0)
        self.assertEqual(stats.most_advt_peer_asn[0].withdraws, 0)

        self.assertEqual(len(stats.most_upd_peer_asn), 1)
        self.assertEqual(stats.most_upd_peer_asn[0].advt, 0)
        self.assertEqual(stats.most_upd_peer_asn[0].as_path, [[]])
        self.assertEqual(stats.most_upd_peer_asn[0].comm_set, [[]])
        self.assertEqual(
            stats.most_upd_peer_asn[0].filename,
            self.upd_2_mrt
        )
        self.assertEqual(stats.most_upd_peer_asn[0].next_hop, None)
        self.assertEqual(stats.most_upd_peer_asn[0].origin_asns, set())
        self.assertEqual(stats.most_upd_peer_asn[0].peer_asn, "18106")
        self.assertEqual(stats.most_upd_peer_asn[0].prefix, None)
        self.assertEqual(stats.most_upd_peer_asn[0].timestamp, "20220501.2305")
        self.assertEqual(stats.most_upd_peer_asn[0].updates, 11781)
        self.assertEqual(stats.most_upd_peer_asn[0].withdraws, 0)

        self.assertEqual(len(stats.most_withd_peer_asn), 1)
        self.assertEqual(stats.most_withd_peer_asn[0].advt, 0)
        self.assertEqual(stats.most_withd_peer_asn[0].as_path, [[]])
        self.assertEqual(stats.most_withd_peer_asn[0].comm_set, [[]])
        self.assertEqual(
            stats.most_withd_peer_asn[0].filename,
            self.upd_1_mrt
        )
        self.assertEqual(stats.most_withd_peer_asn[0].next_hop, None)
        self.assertEqual(stats.most_withd_peer_asn[0].origin_asns, set())
        self.assertEqual(stats.most_withd_peer_asn[0].peer_asn, "133210")
        self.assertEqual(stats.most_withd_peer_asn[0].prefix, None)
        self.assertEqual(
            stats.most_withd_peer_asn[0].timestamp, "20220421.0200"
        )
        self.assertEqual(stats.most_withd_peer_asn[0].updates, 0)
        self.assertEqual(stats.most_withd_peer_asn[0].withdraws, 193)

        self.assertEqual(len(stats.most_origin_asns), 16)
        self.assertEqual(stats.most_origin_asns[0].advt, 0)
        self.assertEqual(stats.most_origin_asns[0].as_path, [[]])
        self.assertEqual(stats.most_origin_asns[0].comm_set, [[]])
        self.assertEqual(
            stats.most_origin_asns[0].filename,
            self.upd_1_mrt
        )
        self.assertEqual(stats.most_origin_asns[0].next_hop, None)
        self.assertEqual(
            stats.most_origin_asns[0].origin_asns, set(["61424", "58143"])
        )
        self.assertEqual(stats.most_origin_asns[0].peer_asn, None)
        self.assertEqual(stats.most_origin_asns[0].prefix, "5.35.174.0/24")
        self.assertEqual(stats.most_origin_asns[0].timestamp, "20220421.0200")
        self.assertEqual(stats.most_origin_asns[0].updates, 0)
        self.assertEqual(stats.most_origin_asns[0].withdraws, 0)

        self.assertEqual(stats.most_origin_asns[1].advt, 0)
        self.assertEqual(stats.most_origin_asns[1].as_path, [[]])
        self.assertEqual(stats.most_origin_asns[1].comm_set, [[]])
        self.assertEqual(
            stats.most_origin_asns[1].filename,
            self.upd_1_mrt
        )
        self.assertEqual(stats.most_origin_asns[1].next_hop, None)
        self.assertEqual(
            stats.most_origin_asns[1].origin_asns, set(["28198", "262375"])
        )
        self.assertEqual(stats.most_origin_asns[1].peer_asn, None)
        self.assertEqual(stats.most_origin_asns[1].prefix, "177.131.0.0/21")
        self.assertEqual(stats.most_origin_asns[1].timestamp, "20220421.0200")
        self.assertEqual(stats.most_origin_asns[1].updates, 0)
        self.assertEqual(stats.most_origin_asns[1].withdraws, 0)

        self.assertEqual(stats.most_origin_asns[2].advt, 0)
        self.assertEqual(stats.most_origin_asns[2].as_path, [[]])
        self.assertEqual(stats.most_origin_asns[2].comm_set, [[]])
        self.assertEqual(
            stats.most_origin_asns[2].filename,
            self.upd_1_mrt
        )
        self.assertEqual(stats.most_origin_asns[2].next_hop, None)
        self.assertEqual(
            stats.most_origin_asns[2].origin_asns, set(["396559", "396542"])
        )
        self.assertEqual(stats.most_origin_asns[2].peer_asn, None)
        self.assertEqual(stats.most_origin_asns[2].prefix, "2620:74:2a::/48")
        self.assertEqual(stats.most_origin_asns[2].timestamp, "20220421.0200")
        self.assertEqual(stats.most_origin_asns[2].updates, 0)
        self.assertEqual(stats.most_origin_asns[2].withdraws, 0)

        self.assertEqual(stats.most_origin_asns[3].advt, 0)
        self.assertEqual(stats.most_origin_asns[3].as_path, [[]])
        self.assertEqual(stats.most_origin_asns[3].comm_set, [[]])
        self.assertEqual(
            stats.most_origin_asns[3].filename,
            self.upd_1_mrt
        )
        self.assertEqual(stats.most_origin_asns[3].next_hop, None)
        self.assertEqual(
            stats.most_origin_asns[3].origin_asns, set(["138346", "134382"])
        )
        self.assertEqual(stats.most_origin_asns[3].peer_asn, None)
        self.assertEqual(stats.most_origin_asns[3].prefix, "103.88.233.0/24")
        self.assertEqual(stats.most_origin_asns[3].timestamp, "20220421.0200")
        self.assertEqual(stats.most_origin_asns[3].updates, 0)
        self.assertEqual(stats.most_origin_asns[3].withdraws, 0)

        self.assertEqual(stats.most_origin_asns[4].advt, 0)
        self.assertEqual(stats.most_origin_asns[4].as_path, [[]])
        self.assertEqual(stats.most_origin_asns[4].comm_set, [[]])
        self.assertEqual(
            stats.most_origin_asns[4].filename,
            self.upd_1_mrt
        )
        self.assertEqual(stats.most_origin_asns[4].next_hop, None)
        self.assertEqual(
            stats.most_origin_asns[4].origin_asns, set(["37154", "7420"])
        )
        self.assertEqual(stats.most_origin_asns[4].peer_asn, None)
        self.assertEqual(stats.most_origin_asns[4].prefix, "196.46.192.0/19")
        self.assertEqual(stats.most_origin_asns[4].timestamp, "20220421.0200")
        self.assertEqual(stats.most_origin_asns[4].updates, 0)
        self.assertEqual(stats.most_origin_asns[4].withdraws, 0)

        self.assertEqual(stats.most_origin_asns[5].advt, 0)
        self.assertEqual(stats.most_origin_asns[5].as_path, [[]])
        self.assertEqual(stats.most_origin_asns[5].comm_set, [[]])
        self.assertEqual(
            stats.most_origin_asns[5].filename,
            self.upd_1_mrt
        )
        self.assertEqual(stats.most_origin_asns[5].next_hop, None)
        self.assertEqual(
            stats.most_origin_asns[5].origin_asns, set(["136561", "59362"])
        )
        self.assertEqual(stats.most_origin_asns[5].peer_asn, None)
        self.assertEqual(stats.most_origin_asns[5].prefix, "123.253.98.0/23")
        self.assertEqual(stats.most_origin_asns[5].timestamp, "20220421.0200")
        self.assertEqual(stats.most_origin_asns[5].updates, 0)
        self.assertEqual(stats.most_origin_asns[5].withdraws, 0)

        self.assertEqual(stats.most_origin_asns[6].advt, 0)
        self.assertEqual(stats.most_origin_asns[6].as_path, [[]])
        self.assertEqual(stats.most_origin_asns[6].comm_set, [[]])
        self.assertEqual(
            stats.most_origin_asns[6].filename,
            self.upd_1_mrt
        )
        self.assertEqual(stats.most_origin_asns[6].next_hop, None)
        self.assertEqual(
            stats.most_origin_asns[6].origin_asns, set(["132608", "17806"])
        )
        self.assertEqual(stats.most_origin_asns[6].peer_asn, None)
        self.assertEqual(stats.most_origin_asns[6].prefix, "114.130.38.0/24")
        self.assertEqual(stats.most_origin_asns[6].timestamp, "20220421.0200")
        self.assertEqual(stats.most_origin_asns[6].updates, 0)
        self.assertEqual(stats.most_origin_asns[6].withdraws, 0)

        self.assertEqual(stats.most_origin_asns[7].advt, 0)
        self.assertEqual(stats.most_origin_asns[7].as_path, [[]])
        self.assertEqual(stats.most_origin_asns[7].comm_set, [[]])
        self.assertEqual(
            stats.most_origin_asns[7].filename,
            self.upd_1_mrt
        )
        self.assertEqual(stats.most_origin_asns[7].next_hop, None)
        self.assertEqual(
            stats.most_origin_asns[7].origin_asns, set(["136907", "55990"])
        )
        self.assertEqual(stats.most_origin_asns[7].peer_asn, None)
        self.assertEqual(stats.most_origin_asns[7].prefix, "124.71.250.0/24")
        self.assertEqual(stats.most_origin_asns[7].timestamp, "20220421.0200")
        self.assertEqual(stats.most_origin_asns[7].updates, 0)
        self.assertEqual(stats.most_origin_asns[7].withdraws, 0)

        self.assertEqual(stats.most_origin_asns[8].advt, 0)
        self.assertEqual(stats.most_origin_asns[8].as_path, [[]])
        self.assertEqual(stats.most_origin_asns[8].comm_set, [[]])
        self.assertEqual(
            stats.most_origin_asns[8].filename,
            self.upd_1_mrt
        )
        self.assertEqual(stats.most_origin_asns[8].next_hop, None)
        self.assertEqual(
            stats.most_origin_asns[8].origin_asns, set(["136907", "55990"])
        )
        self.assertEqual(stats.most_origin_asns[8].peer_asn, None)
        self.assertEqual(stats.most_origin_asns[8].prefix, "139.9.98.0/24")
        self.assertEqual(stats.most_origin_asns[8].timestamp, "20220421.0200")
        self.assertEqual(stats.most_origin_asns[8].updates, 0)
        self.assertEqual(stats.most_origin_asns[8].withdraws, 0)

        self.assertEqual(stats.most_origin_asns[9].advt, 0)
        self.assertEqual(stats.most_origin_asns[9].as_path, [[]])
        self.assertEqual(stats.most_origin_asns[9].comm_set, [[]])
        self.assertEqual(
            stats.most_origin_asns[9].filename,
            self.upd_1_mrt
        )
        self.assertEqual(stats.most_origin_asns[9].next_hop, None)
        self.assertEqual(
            stats.most_origin_asns[9].origin_asns, set(["7545", "4739"])
        )
        self.assertEqual(stats.most_origin_asns[9].peer_asn, None)
        self.assertEqual(stats.most_origin_asns[9].prefix, "203.19.254.0/24")
        self.assertEqual(stats.most_origin_asns[9].timestamp, "20220421.0200")
        self.assertEqual(stats.most_origin_asns[9].updates, 0)
        self.assertEqual(stats.most_origin_asns[9].withdraws, 0)

        self.assertEqual(stats.most_origin_asns[10].advt, 0)
        self.assertEqual(stats.most_origin_asns[10].as_path, [[]])
        self.assertEqual(stats.most_origin_asns[10].comm_set, [[]])
        self.assertEqual(
            stats.most_origin_asns[10].filename,
            self.upd_2_mrt
        )
        self.assertEqual(stats.most_origin_asns[10].next_hop, None)
        self.assertEqual(
            stats.most_origin_asns[10].origin_asns, set(["271204", "266181"])
        )
        self.assertEqual(stats.most_origin_asns[10].peer_asn, None)
        self.assertEqual(stats.most_origin_asns[10].prefix, "179.49.190.0/23")
        self.assertEqual(stats.most_origin_asns[10].timestamp, "20220501.2305")
        self.assertEqual(stats.most_origin_asns[10].updates, 0)
        self.assertEqual(stats.most_origin_asns[10].withdraws, 0)

        self.assertEqual(stats.most_origin_asns[11].advt, 0)
        self.assertEqual(stats.most_origin_asns[11].as_path, [[]])
        self.assertEqual(stats.most_origin_asns[11].comm_set, [[]])
        self.assertEqual(
            stats.most_origin_asns[11].filename,
            self.upd_2_mrt
        )
        self.assertEqual(stats.most_origin_asns[11].next_hop, None)
        self.assertEqual(
            stats.most_origin_asns[11].origin_asns, set(["7487", "54396"])
        )
        self.assertEqual(stats.most_origin_asns[11].peer_asn, None)
        self.assertEqual(stats.most_origin_asns[11].prefix, "205.197.192.0/21")
        self.assertEqual(stats.most_origin_asns[11].timestamp, "20220501.2305")
        self.assertEqual(stats.most_origin_asns[11].updates, 0)
        self.assertEqual(stats.most_origin_asns[11].withdraws, 0)

        self.assertEqual(stats.most_origin_asns[12].advt, 0)
        self.assertEqual(stats.most_origin_asns[12].as_path, [[]])
        self.assertEqual(stats.most_origin_asns[12].comm_set, [[]])
        self.assertEqual(
            stats.most_origin_asns[12].filename,
            self.upd_2_mrt
        )
        self.assertEqual(stats.most_origin_asns[12].next_hop, None)
        self.assertEqual(
            stats.most_origin_asns[12].origin_asns, set(["203020", "29802"])
        )
        self.assertEqual(stats.most_origin_asns[12].peer_asn, None)
        self.assertEqual(stats.most_origin_asns[12].prefix, "206.123.159.0/24")
        self.assertEqual(stats.most_origin_asns[12].timestamp, "20220501.2305")
        self.assertEqual(stats.most_origin_asns[12].updates, 0)
        self.assertEqual(stats.most_origin_asns[12].withdraws, 0)

        self.assertEqual(stats.most_origin_asns[13].advt, 0)
        self.assertEqual(stats.most_origin_asns[13].as_path, [[]])
        self.assertEqual(stats.most_origin_asns[13].comm_set, [[]])
        self.assertEqual(
            stats.most_origin_asns[13].filename,
            self.upd_2_mrt
        )
        self.assertEqual(stats.most_origin_asns[13].next_hop, None)
        self.assertEqual(
            stats.most_origin_asns[13].origin_asns, set(["52000", "19318"])
        )
        self.assertEqual(stats.most_origin_asns[13].peer_asn, None)
        self.assertEqual(stats.most_origin_asns[13].prefix, "68.168.210.0/24")
        self.assertEqual(stats.most_origin_asns[13].timestamp, "20220501.2305")
        self.assertEqual(stats.most_origin_asns[13].updates, 0)
        self.assertEqual(stats.most_origin_asns[13].withdraws, 0)

        self.assertEqual(stats.most_origin_asns[14].advt, 0)
        self.assertEqual(stats.most_origin_asns[14].as_path, [[]])
        self.assertEqual(stats.most_origin_asns[14].comm_set, [[]])
        self.assertEqual(
            stats.most_origin_asns[14].filename,
            self.upd_2_mrt
        )
        self.assertEqual(stats.most_origin_asns[14].next_hop, None)
        self.assertEqual(
            stats.most_origin_asns[14].origin_asns, set(["55020", "137951"])
        )
        self.assertEqual(stats.most_origin_asns[14].peer_asn, None)
        self.assertEqual(stats.most_origin_asns[14].prefix, "156.241.128.0/22")
        self.assertEqual(stats.most_origin_asns[14].timestamp, "20220501.2305")
        self.assertEqual(stats.most_origin_asns[14].updates, 0)
        self.assertEqual(stats.most_origin_asns[14].withdraws, 0)

        self.assertEqual(stats.most_origin_asns[15].advt, 0)
        self.assertEqual(stats.most_origin_asns[15].as_path, [[]])
        self.assertEqual(stats.most_origin_asns[15].comm_set, [[]])
        self.assertEqual(
            stats.most_origin_asns[15].filename,
            self.upd_2_mrt
        )
        self.assertEqual(stats.most_origin_asns[15].next_hop, None)
        self.assertEqual(
            stats.most_origin_asns[15].origin_asns, set(["269208", "268347"])
        )
        self.assertEqual(stats.most_origin_asns[15].peer_asn, None)
        self.assertEqual(stats.most_origin_asns[15].prefix, "2804:610c::/32")
        self.assertEqual(stats.most_origin_asns[15].timestamp, "20220501.2305")
        self.assertEqual(stats.most_origin_asns[15].updates, 0)
        self.assertEqual(stats.most_origin_asns[15].withdraws, 0)

        self.assertEqual(stats.total_upd, 29688)
        self.assertEqual(stats.total_advt, 29396)
        self.assertEqual(stats.total_withd, 950)
        self.assertEqual(stats.file_list, [self.upd_1_mrt, self.upd_2_mrt])
        self.assertEqual(stats.timestamp, "20220501.2305")

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

if __name__ == '__main__':
    unittest.main()
