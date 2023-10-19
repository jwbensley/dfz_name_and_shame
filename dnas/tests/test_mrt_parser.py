import os
import shutil
import sys
import typing
import unittest

sys.path.append(
    os.path.join(os.path.dirname(os.path.realpath(__file__)), "../")
)
from dnas.config import config
from dnas.mrt_archives import mrt_archives
from dnas.mrt_entry import mrt_entry
from dnas.mrt_parser import mrt_parser
from dnas.mrt_stats import mrt_stats


class test_mrt_parser(unittest.TestCase):
    cfg = config()

    def setUp(self: "test_mrt_parser") -> None:
        """
        Copy the test files to the location they would be in,
        if we had downloaded them from the public archives:
        """

        self.upd_1_fn = "rcc23.updates.20220421.0200.gz"
        self.upd_2_fn = "rcc23.updates.20220501.2305.gz"
        self.upd_3_fn = "sydney.updates.20220601.0415.bz2"
        self.json_3_fn = "sydney.updates.20220601.0415.bz2.json"
        self.upd_4_fn = "rcc01.updates.20100827.0840.gz"

        self.upd_1_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "RCC23/",
            self.upd_1_fn,
        )
        if not os.path.isfile(self.upd_1_path):
            raise Exception(f"Test MRT file is not found: {self.upd_1_path}")

        self.upd_2_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "RCC23/",
            self.upd_2_fn,
        )
        if not os.path.isfile(self.upd_2_path):
            raise Exception(f"Test MRT file is not found: {self.upd_2_path}")

        self.upd_3_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "SYDNEY/",
            self.upd_3_fn,
        )
        if not os.path.isfile(self.upd_3_path):
            raise Exception(f"Test MRT file is not found: {self.upd_3_path}")

        self.json_3_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            "SYDNEY/",
            self.json_3_fn,
        )
        if not os.path.isfile(self.json_3_path):
            raise Exception(f"Test JSON file is not found: {self.json_3_path}")

        self.upd_4_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)), "RCC1/", self.upd_4_fn
        )
        if not os.path.isfile(self.upd_4_path):
            raise Exception(f"Test MRT file is not found: {self.upd_4_path}")

        mrt_a = mrt_archives()
        for arch in mrt_a.archives:
            if arch.NAME == "UNIT_TEST_RCC_23":
                os.makedirs(arch.MRT_DIR, exist_ok=True)
                self.upd_1_mrt = os.path.join(arch.MRT_DIR, self.upd_1_fn)
                self.upd_2_mrt = os.path.join(arch.MRT_DIR, self.upd_2_fn)
                self.upd_4_mrt = os.path.join(arch.MRT_DIR, self.upd_4_fn)
            if arch.NAME == "UNIT_TEST_RV_SYDNEY":
                os.makedirs(arch.MRT_DIR, exist_ok=True)
                self.upd_3_mrt = os.path.join(arch.MRT_DIR, self.upd_3_fn)

        shutil.copy2(self.upd_1_path, self.upd_1_mrt)
        shutil.copy2(self.upd_2_path, self.upd_2_mrt)
        shutil.copy2(self.upd_3_path, self.upd_3_mrt)
        shutil.copy2(self.upd_4_path, self.upd_4_mrt)

    def test_init(self: "test_mrt_parser"):
        """
        The mrt_parser class contains only static methods for now, so check
        that nothing is returned and nothing is raised.
        """
        asserted = False
        try:
            mrt_p = mrt_parser()
        except:
            asserted = True
        self.assertEqual(type(mrt_p), mrt_parser)
        self.assertEqual(asserted, False)

    def test_parse_upd_dump(self: "test_mrt_parser") -> None:
        """
        Throughout this function the MRT file being parsed is alternating,
        because we want to hit all code paths, no one MRT file contains the
        "perfect storm" of UPDATES to hit all code paths.
        """

        mrt_p = mrt_parser()

        self.assertRaises(ValueError, mrt_p.parse_upd_dump, None)
        self.assertRaises(TypeError, mrt_p.parse_upd_dump, 123)

        upd_1_stats = mrt_p.parse_upd_dump(self.upd_1_mrt)
        self.assertIsInstance(upd_1_stats, mrt_stats)
        upd_2_stats = mrt_p.parse_upd_dump(self.upd_2_mrt)
        self.assertIsInstance(upd_2_stats, mrt_stats)
        upd_3_stats = mrt_p.parse_upd_dump(self.upd_3_mrt)
        self.assertIsInstance(upd_3_stats, mrt_stats)
        test_3_stats = mrt_stats()
        test_3_stats.from_file(self.json_3_path)
        self.assertIsInstance(test_3_stats, mrt_stats)
        upd_4_stats = mrt_p.parse_upd_dump(self.upd_4_mrt)
        self.assertIsInstance(upd_4_stats, mrt_stats)

        self.assertIsInstance(upd_1_stats.bogon_origin_asns, list)
        self.assertEqual(len(upd_1_stats.bogon_origin_asns), 1)
        self.assertIsInstance(upd_1_stats.bogon_origin_asns[0], mrt_entry)
        self.assertEqual(upd_1_stats.bogon_origin_asns[0].advt, 0)
        self.assertEqual(
            upd_1_stats.bogon_origin_asns[0].as_path,
            ["137409", "17494", "137491", "58689", "137464", "65551"],
        )
        self.assertEqual(upd_1_stats.bogon_origin_asns[0].comm_set, [])
        self.assertEqual(
            upd_1_stats.bogon_origin_asns[0].filename, self.upd_1_mrt
        )
        self.assertEqual(
            upd_1_stats.bogon_origin_asns[0].next_hop, "27.111.228.145"
        )
        self.assertEqual(
            upd_1_stats.bogon_origin_asns[0].origin_asns, set(["65551"])
        )
        self.assertEqual(upd_1_stats.bogon_origin_asns[0].peer_asn, "137409")
        self.assertEqual(
            upd_1_stats.bogon_origin_asns[0].prefix, "103.109.236.0/24"
        )
        self.assertEqual(
            upd_1_stats.bogon_origin_asns[0].timestamp, "20220421.0200"
        )
        self.assertEqual(upd_1_stats.bogon_origin_asns[0].updates, 0)
        self.assertEqual(upd_1_stats.bogon_origin_asns[0].withdraws, 0)

        self.assertIsInstance(upd_2_stats.bogon_prefixes, list)
        self.assertEqual(len(upd_2_stats.bogon_prefixes), 1)
        self.assertIsInstance(upd_2_stats.bogon_prefixes[0], mrt_entry)
        self.assertEqual(upd_2_stats.bogon_prefixes[0].advt, 0)
        self.assertEqual(
            upd_2_stats.bogon_prefixes[0].as_path, ["133210", "6939"]
        )
        self.assertEqual(upd_2_stats.bogon_prefixes[0].comm_set, [])
        self.assertEqual(
            upd_2_stats.bogon_prefixes[0].filename, self.upd_2_mrt
        )
        self.assertEqual(
            upd_2_stats.bogon_prefixes[0].next_hop, "27.111.228.81"
        )
        self.assertEqual(
            upd_2_stats.bogon_prefixes[0].origin_asns, set(["6939"])
        )
        self.assertEqual(upd_2_stats.bogon_prefixes[0].peer_asn, "133210")
        self.assertEqual(
            upd_2_stats.bogon_prefixes[0].prefix, "192.88.99.0/24"
        )
        self.assertEqual(
            upd_2_stats.bogon_prefixes[0].timestamp, "20220501.2309"
        )
        self.assertEqual(upd_2_stats.bogon_prefixes[0].updates, 0)
        self.assertEqual(upd_2_stats.bogon_prefixes[0].withdraws, 0)

        self.assertIsInstance(upd_1_stats.highest_med_prefixes, list)
        self.assertEqual(len(upd_1_stats.highest_med_prefixes), 1)
        self.assertIsInstance(upd_1_stats.highest_med_prefixes[0], mrt_entry)
        self.assertEqual(upd_1_stats.highest_med_prefixes[0].advt, 0)
        self.assertEqual(
            upd_1_stats.highest_med_prefixes[0].as_path,
            ["16509"],
        )
        self.assertEqual(upd_1_stats.highest_med_prefixes[0].comm_set, [])
        self.assertEqual(
            upd_1_stats.highest_med_prefixes[0].filename, self.upd_1_mrt
        )
        self.assertEqual(upd_1_stats.highest_med_prefixes[0].med, 1000)
        self.assertEqual(
            upd_1_stats.highest_med_prefixes[0].next_hop,
            "27.111.228.87",
        )
        self.assertEqual(
            upd_1_stats.highest_med_prefixes[0].origin_asns, set(["16509"])
        )
        self.assertEqual(upd_1_stats.highest_med_prefixes[0].peer_asn, "16509")
        self.assertEqual(
            upd_1_stats.highest_med_prefixes[0].prefix, "130.137.111.0/24"
        )
        self.assertEqual(
            upd_1_stats.highest_med_prefixes[0].unknown_attrs, set()
        )
        self.assertEqual(
            upd_1_stats.highest_med_prefixes[0].timestamp, "20220421.0200"
        )
        self.assertEqual(upd_1_stats.highest_med_prefixes[0].updates, 0)
        self.assertEqual(upd_1_stats.highest_med_prefixes[0].withdraws, 0)

        self.assertIsInstance(upd_2_stats.longest_as_path, list)
        self.assertEqual(len(upd_2_stats.longest_as_path), 1)
        self.assertIsInstance(upd_2_stats.longest_as_path[0], mrt_entry)
        self.assertEqual(upd_2_stats.longest_as_path[0].advt, 0)
        self.assertEqual(
            upd_2_stats.longest_as_path[0].as_path,
            [
                "18106",
                "23106",
                "264228",
                "264228",
                "264228",
                "264228",
                "264228",
                "264228",
                "264228",
                "264228",
                "264228",
                "264228",
                "264228",
                "264228",
                "264228",
                "264228",
                "264228",
                "264228",
                "264228",
                "264228",
                "264228",
                "264228",
                "264228",
                "264228",
                "264228",
                "264228",
                "264228",
            ],
        )
        self.assertEqual(
            upd_2_stats.longest_as_path[0].comm_set, ["13538:3000"]
        )
        self.assertEqual(
            upd_2_stats.longest_as_path[0].filename, self.upd_2_mrt
        )
        self.assertEqual(
            upd_2_stats.longest_as_path[0].next_hop,
            ["2001:de8:4::1:8106:1", "fe80::bac2:53ff:fedb:2004"],
        )
        self.assertEqual(
            upd_2_stats.longest_as_path[0].origin_asns, set(["264228"])
        )
        self.assertEqual(upd_2_stats.longest_as_path[0].peer_asn, "18106")
        self.assertEqual(
            upd_2_stats.longest_as_path[0].prefix, "2804:2488::/48"
        )
        self.assertEqual(
            upd_2_stats.longest_as_path[0].timestamp, "20220501.2305"
        )
        self.assertEqual(upd_2_stats.longest_as_path[0].updates, 0)
        self.assertEqual(upd_2_stats.longest_as_path[0].withdraws, 0)

        self.assertIsInstance(upd_2_stats.longest_comm_set, list)
        self.assertEqual(len(upd_2_stats.longest_comm_set), 4)
        self.assertIsInstance(upd_2_stats.longest_comm_set[0], mrt_entry)
        self.assertEqual(upd_2_stats.longest_comm_set[0].advt, 0)
        self.assertEqual(
            upd_2_stats.longest_comm_set[0].as_path,
            ["18106", "57463", "61568", "268267"],
        )
        self.assertEqual(
            upd_2_stats.longest_comm_set[0].comm_set,
            [
                "13538:3000",
                "57463:0:1120",
                "57463:0:5408",
                "57463:0:6461",
                "57463:0:6663",
                "57463:0:6762",
                "57463:0:6830",
                "57463:0:6939",
                "57463:0:8657",
                "57463:0:8757",
                "57463:0:8763",
                "57463:0:10906",
                "57463:0:11284",
                "57463:0:11644",
                "57463:0:12989",
                "57463:0:13237",
                "57463:0:14840",
                "57463:0:20562",
                "57463:0:21574",
                "57463:0:22356",
                "57463:0:22381",
                "57463:0:22822",
                "57463:0:28186",
                "57463:0:28260",
                "57463:0:28330",
                "57463:0:28663",
                "57463:0:32787",
                "57463:0:33891",
                "57463:0:36351",
                "57463:0:37100",
                "57463:0:37468",
                "57463:0:43350",
                "57463:0:45474",
                "57463:0:52320",
                "57463:0:52551",
                "57463:0:52866",
                "57463:0:52937",
                "57463:0:53162",
                "57463:0:58453",
                "57463:0:61568",
                "57463:0:61832",
                "57463:0:262354",
                "57463:0:262589",
                "57463:0:262773",
                "57463:0:262807",
                "57463:0:263009",
                "57463:0:263276",
                "57463:0:263324",
                "57463:0:263421",
                "57463:0:263626",
                "57463:0:265187",
                "57463:0:267056",
                "57463:0:267613",
                "57463:0:268331",
                "57463:0:268696",
            ],
        )
        self.assertEqual(
            upd_2_stats.longest_comm_set[0].filename, self.upd_2_mrt
        )
        self.assertEqual(
            upd_2_stats.longest_comm_set[0].next_hop,
            ["2001:de8:4::1:8106:1", "fe80::bac2:53ff:fedb:2004"],
        )
        self.assertEqual(
            upd_2_stats.longest_comm_set[0].origin_asns, set(["268267"])
        )
        self.assertEqual(upd_2_stats.longest_comm_set[0].peer_asn, "18106")
        self.assertEqual(
            upd_2_stats.longest_comm_set[0].prefix, "2804:4e88::/34"
        )
        self.assertEqual(
            upd_2_stats.longest_comm_set[0].timestamp, "20220501.2305"
        )
        self.assertEqual(upd_2_stats.longest_comm_set[0].updates, 0)
        self.assertEqual(upd_2_stats.longest_comm_set[0].withdraws, 0)

        self.assertIsInstance(upd_2_stats.longest_comm_set[1], mrt_entry)
        self.assertEqual(upd_2_stats.longest_comm_set[1].advt, 0)
        self.assertEqual(
            upd_2_stats.longest_comm_set[1].as_path,
            ["18106", "57463", "61568", "265080", "270793"],
        )
        self.assertEqual(
            upd_2_stats.longest_comm_set[1].comm_set,
            [
                "13538:3000",
                "57463:0:1120",
                "57463:0:5408",
                "57463:0:6461",
                "57463:0:6663",
                "57463:0:6762",
                "57463:0:6830",
                "57463:0:6939",
                "57463:0:8657",
                "57463:0:8757",
                "57463:0:8763",
                "57463:0:10906",
                "57463:0:11284",
                "57463:0:11644",
                "57463:0:12989",
                "57463:0:13237",
                "57463:0:14840",
                "57463:0:20562",
                "57463:0:21574",
                "57463:0:22356",
                "57463:0:22381",
                "57463:0:22822",
                "57463:0:28186",
                "57463:0:28260",
                "57463:0:28330",
                "57463:0:28663",
                "57463:0:32787",
                "57463:0:33891",
                "57463:0:36351",
                "57463:0:37100",
                "57463:0:37468",
                "57463:0:43350",
                "57463:0:45474",
                "57463:0:52320",
                "57463:0:52551",
                "57463:0:52866",
                "57463:0:52937",
                "57463:0:53162",
                "57463:0:58453",
                "57463:0:61568",
                "57463:0:61832",
                "57463:0:262354",
                "57463:0:262589",
                "57463:0:262773",
                "57463:0:262807",
                "57463:0:263009",
                "57463:0:263276",
                "57463:0:263324",
                "57463:0:263421",
                "57463:0:263626",
                "57463:0:265187",
                "57463:0:267056",
                "57463:0:267613",
                "57463:0:268331",
                "57463:0:268696",
            ],
        )
        self.assertEqual(
            upd_2_stats.longest_comm_set[1].filename, self.upd_2_mrt
        )
        self.assertEqual(
            upd_2_stats.longest_comm_set[1].next_hop,
            ["2001:de8:4::1:8106:1", "fe80::bac2:53ff:fedb:2004"],
        )
        self.assertEqual(
            upd_2_stats.longest_comm_set[1].origin_asns, set(["270793"])
        )
        self.assertEqual(upd_2_stats.longest_comm_set[1].peer_asn, "18106")
        self.assertEqual(
            upd_2_stats.longest_comm_set[1].prefix, "2804:7180:100::/40"
        )
        self.assertEqual(
            upd_2_stats.longest_comm_set[1].timestamp, "20220501.2305"
        )
        self.assertEqual(upd_2_stats.longest_comm_set[1].updates, 0)
        self.assertEqual(upd_2_stats.longest_comm_set[1].withdraws, 0)

        self.assertIsInstance(upd_2_stats.longest_comm_set[2], mrt_entry)
        self.assertEqual(upd_2_stats.longest_comm_set[2].advt, 0)
        self.assertEqual(
            upd_2_stats.longest_comm_set[2].as_path,
            ["18106", "57463", "61568", "265080", "270793"],
        )
        self.assertEqual(
            upd_2_stats.longest_comm_set[2].comm_set,
            [
                "13538:3000",
                "57463:0:1120",
                "57463:0:5408",
                "57463:0:6461",
                "57463:0:6663",
                "57463:0:6762",
                "57463:0:6830",
                "57463:0:6939",
                "57463:0:8657",
                "57463:0:8757",
                "57463:0:8763",
                "57463:0:10906",
                "57463:0:11284",
                "57463:0:11644",
                "57463:0:12989",
                "57463:0:13237",
                "57463:0:14840",
                "57463:0:20562",
                "57463:0:21574",
                "57463:0:22356",
                "57463:0:22381",
                "57463:0:22822",
                "57463:0:28186",
                "57463:0:28260",
                "57463:0:28330",
                "57463:0:28663",
                "57463:0:32787",
                "57463:0:33891",
                "57463:0:36351",
                "57463:0:37100",
                "57463:0:37468",
                "57463:0:43350",
                "57463:0:45474",
                "57463:0:52320",
                "57463:0:52551",
                "57463:0:52866",
                "57463:0:52937",
                "57463:0:53162",
                "57463:0:58453",
                "57463:0:61568",
                "57463:0:61832",
                "57463:0:262354",
                "57463:0:262589",
                "57463:0:262773",
                "57463:0:262807",
                "57463:0:263009",
                "57463:0:263276",
                "57463:0:263324",
                "57463:0:263421",
                "57463:0:263626",
                "57463:0:265187",
                "57463:0:267056",
                "57463:0:267613",
                "57463:0:268331",
                "57463:0:268696",
            ],
        )
        self.assertEqual(
            upd_2_stats.longest_comm_set[2].filename, self.upd_2_mrt
        )
        self.assertEqual(
            upd_2_stats.longest_comm_set[2].next_hop,
            ["2001:de8:4::1:8106:1", "fe80::bac2:53ff:fedb:2004"],
        )
        self.assertEqual(
            upd_2_stats.longest_comm_set[2].origin_asns, set(["270793"])
        )
        self.assertEqual(upd_2_stats.longest_comm_set[2].peer_asn, "18106")
        self.assertEqual(
            upd_2_stats.longest_comm_set[2].prefix, "2804:7180::/40"
        )
        self.assertEqual(
            upd_2_stats.longest_comm_set[2].timestamp, "20220501.2305"
        )
        self.assertEqual(upd_2_stats.longest_comm_set[2].updates, 0)
        self.assertEqual(upd_2_stats.longest_comm_set[2].withdraws, 0)

        self.assertIsInstance(upd_2_stats.longest_comm_set[3], mrt_entry)
        self.assertEqual(upd_2_stats.longest_comm_set[3].advt, 0)
        self.assertEqual(
            upd_2_stats.longest_comm_set[3].as_path,
            [
                "18106",
                "57463",
                "61568",
                "264293",
                "267429",
                "267956",
                "267956",
                "267956",
                "267956",
                "267956",
                "267956",
                "268182",
            ],
        )
        self.assertEqual(
            upd_2_stats.longest_comm_set[3].comm_set,
            [
                "13538:3000",
                "57463:0:1120",
                "57463:0:5408",
                "57463:0:6461",
                "57463:0:6663",
                "57463:0:6762",
                "57463:0:6830",
                "57463:0:6939",
                "57463:0:8657",
                "57463:0:8757",
                "57463:0:8763",
                "57463:0:10906",
                "57463:0:11284",
                "57463:0:11644",
                "57463:0:12989",
                "57463:0:13237",
                "57463:0:14840",
                "57463:0:20562",
                "57463:0:21574",
                "57463:0:22356",
                "57463:0:22381",
                "57463:0:22822",
                "57463:0:28186",
                "57463:0:28260",
                "57463:0:28330",
                "57463:0:28663",
                "57463:0:32787",
                "57463:0:33891",
                "57463:0:36351",
                "57463:0:37100",
                "57463:0:37468",
                "57463:0:43350",
                "57463:0:45474",
                "57463:0:52320",
                "57463:0:52551",
                "57463:0:52866",
                "57463:0:52937",
                "57463:0:53162",
                "57463:0:58453",
                "57463:0:61568",
                "57463:0:61832",
                "57463:0:262354",
                "57463:0:262589",
                "57463:0:262773",
                "57463:0:262807",
                "57463:0:263009",
                "57463:0:263276",
                "57463:0:263324",
                "57463:0:263421",
                "57463:0:263626",
                "57463:0:265187",
                "57463:0:267056",
                "57463:0:267613",
                "57463:0:268331",
                "57463:0:268696",
            ],
        )
        self.assertEqual(
            upd_2_stats.longest_comm_set[3].filename, self.upd_2_mrt
        )
        self.assertEqual(
            upd_2_stats.longest_comm_set[3].next_hop,
            ["2001:de8:4::1:8106:1", "fe80::bac2:53ff:fedb:2004"],
        )
        self.assertEqual(
            upd_2_stats.longest_comm_set[3].origin_asns, set(["268182"])
        )
        self.assertEqual(upd_2_stats.longest_comm_set[3].peer_asn, "18106")
        self.assertEqual(
            upd_2_stats.longest_comm_set[3].prefix, "2804:5950::/33"
        )
        self.assertEqual(
            upd_2_stats.longest_comm_set[3].timestamp, "20220501.2305"
        )
        self.assertEqual(upd_2_stats.longest_comm_set[3].updates, 0)
        self.assertEqual(upd_2_stats.longest_comm_set[3].withdraws, 0)

        self.assertIsInstance(upd_1_stats.invalid_len, list)
        self.assertEqual(len(upd_1_stats.invalid_len), 6)
        self.assertIsInstance(upd_1_stats.invalid_len[0], mrt_entry)
        self.assertEqual(upd_1_stats.invalid_len[0].advt, 0)
        self.assertEqual(
            upd_1_stats.invalid_len[0].as_path, ["199524", "38082"]
        )
        self.assertEqual(upd_1_stats.invalid_len[0].comm_set, [])
        self.assertEqual(upd_1_stats.invalid_len[0].filename, self.upd_1_mrt)
        self.assertEqual(
            upd_1_stats.invalid_len[0].next_hop, ["2001:de8:4::19:9524:1"]
        )
        self.assertEqual(
            upd_1_stats.invalid_len[0].origin_asns, set(["38082"])
        )
        self.assertEqual(upd_1_stats.invalid_len[0].peer_asn, "199524")
        self.assertEqual(
            upd_1_stats.invalid_len[0].prefix, "2405:4000:800:8::/64"
        )
        self.assertEqual(upd_1_stats.invalid_len[0].timestamp, "20220421.0200")
        self.assertEqual(upd_1_stats.invalid_len[0].updates, 0)
        self.assertEqual(upd_1_stats.invalid_len[0].withdraws, 0)

        self.assertEqual(upd_1_stats.invalid_len[1].advt, 0)
        self.assertEqual(
            upd_1_stats.invalid_len[1].as_path,
            ["133210", "4788", "38044", "38044", "23736"],
        )
        self.assertEqual(
            upd_1_stats.invalid_len[1].comm_set,
            [
                "4788:811",
                "4788:6300",
                "4788:6310",
                "4788:16300",
                "4788:23030",
                "4788:32011",
            ],
        )
        self.assertEqual(upd_1_stats.invalid_len[1].filename, self.upd_1_mrt)
        self.assertEqual(
            upd_1_stats.invalid_len[1].next_hop,
            ["2001:de8:4::4788:1", "fe80::8ae6:4b00:6c1:6029"],
        )
        self.assertEqual(
            upd_1_stats.invalid_len[1].origin_asns, set(["23736"])
        )
        self.assertEqual(upd_1_stats.invalid_len[1].peer_asn, "133210")
        self.assertEqual(
            upd_1_stats.invalid_len[1].prefix, "2400:7400:0:105::/64"
        )
        self.assertEqual(upd_1_stats.invalid_len[1].timestamp, "20220421.0201")
        self.assertEqual(upd_1_stats.invalid_len[1].updates, 0)
        self.assertEqual(upd_1_stats.invalid_len[1].withdraws, 0)

        self.assertEqual(upd_1_stats.invalid_len[2].advt, 0)
        self.assertEqual(
            upd_1_stats.invalid_len[2].as_path,
            ["133210", "4788", "38044", "23736"],
        )
        self.assertEqual(
            upd_1_stats.invalid_len[2].comm_set,
            [
                "4788:811",
                "4788:6300",
                "4788:6310",
                "4788:16300",
                "4788:23030",
                "4788:34002",
            ],
        )
        self.assertEqual(upd_1_stats.invalid_len[2].filename, self.upd_1_mrt)
        self.assertEqual(
            upd_1_stats.invalid_len[2].next_hop,
            ["2001:de8:4::4788:1", "fe80::8ae6:4b00:6c1:6029"],
        )
        self.assertEqual(
            upd_1_stats.invalid_len[2].origin_asns, set(["23736"])
        )
        self.assertEqual(upd_1_stats.invalid_len[2].peer_asn, "133210")
        self.assertEqual(
            upd_1_stats.invalid_len[2].prefix, "2400:7400:0:106::/64"
        )
        self.assertEqual(upd_1_stats.invalid_len[2].timestamp, "20220421.0201")
        self.assertEqual(upd_1_stats.invalid_len[2].updates, 0)
        self.assertEqual(upd_1_stats.invalid_len[2].withdraws, 0)

        self.assertEqual(upd_1_stats.invalid_len[3].advt, 0)
        self.assertEqual(upd_1_stats.invalid_len[3].as_path, ["136168"])
        self.assertEqual(upd_1_stats.invalid_len[3].comm_set, [])
        self.assertEqual(upd_1_stats.invalid_len[3].filename, self.upd_1_mrt)
        self.assertEqual(upd_1_stats.invalid_len[3].next_hop, "27.111.228.170")
        self.assertEqual(
            upd_1_stats.invalid_len[3].origin_asns, set(["136168"])
        )
        self.assertEqual(upd_1_stats.invalid_len[3].peer_asn, "136168")
        self.assertEqual(upd_1_stats.invalid_len[3].prefix, "100.96.200.3/32")
        self.assertEqual(upd_1_stats.invalid_len[3].timestamp, "20220421.0201")
        self.assertEqual(upd_1_stats.invalid_len[3].updates, 0)
        self.assertEqual(upd_1_stats.invalid_len[3].withdraws, 0)

        self.assertEqual(upd_1_stats.invalid_len[4].advt, 0)
        self.assertEqual(
            upd_1_stats.invalid_len[4].as_path,
            ["133210", "59318", "59318", "15133"],
        )
        self.assertEqual(
            upd_1_stats.invalid_len[4].comm_set, ["15133:4351", "59318:2015"]
        )
        self.assertEqual(upd_1_stats.invalid_len[4].filename, self.upd_1_mrt)
        self.assertEqual(
            upd_1_stats.invalid_len[4].next_hop,
            ["2001:de8:4::13:1207:1", "fe80::8ae6:4b00:6c1:6029"],
        )
        self.assertEqual(
            upd_1_stats.invalid_len[4].origin_asns, set(["15133"])
        )
        self.assertEqual(upd_1_stats.invalid_len[4].peer_asn, "133210")
        self.assertEqual(
            upd_1_stats.invalid_len[4].prefix, "2404:b300:33:1::/64"
        )
        self.assertEqual(upd_1_stats.invalid_len[4].timestamp, "20220421.0203")
        self.assertEqual(upd_1_stats.invalid_len[4].updates, 0)
        self.assertEqual(upd_1_stats.invalid_len[4].withdraws, 0)

        self.assertEqual(upd_1_stats.invalid_len[5].advt, 0)
        self.assertEqual(upd_1_stats.invalid_len[5].as_path, ["136168"])
        self.assertEqual(upd_1_stats.invalid_len[5].comm_set, [])
        self.assertEqual(upd_1_stats.invalid_len[5].filename, self.upd_1_mrt)
        self.assertEqual(upd_1_stats.invalid_len[5].next_hop, "27.111.228.170")
        self.assertEqual(
            upd_1_stats.invalid_len[5].origin_asns, set(["136168"])
        )
        self.assertEqual(upd_1_stats.invalid_len[5].peer_asn, "136168")
        self.assertEqual(
            upd_1_stats.invalid_len[5].prefix, "123.253.228.188/30"
        )
        self.assertEqual(upd_1_stats.invalid_len[5].timestamp, "20220421.0204")
        self.assertEqual(upd_1_stats.invalid_len[5].updates, 0)
        self.assertEqual(upd_1_stats.invalid_len[5].withdraws, 0)

        self.assertIsInstance(upd_2_stats.most_advt_prefixes, list)
        self.assertEqual(len(upd_2_stats.most_advt_prefixes), 1)
        self.assertIsInstance(upd_2_stats.most_advt_prefixes[0], mrt_entry)
        self.assertEqual(upd_2_stats.most_advt_prefixes[0].advt, 884)
        self.assertEqual(upd_2_stats.most_advt_prefixes[0].as_path, [])
        self.assertEqual(upd_2_stats.most_advt_prefixes[0].comm_set, [])
        self.assertEqual(
            upd_2_stats.most_advt_prefixes[0].filename, self.upd_2_mrt
        )
        self.assertEqual(upd_2_stats.most_advt_prefixes[0].next_hop, "")
        self.assertEqual(upd_2_stats.most_advt_prefixes[0].origin_asns, set())
        self.assertEqual(upd_2_stats.most_advt_prefixes[0].peer_asn, "")
        self.assertEqual(
            upd_2_stats.most_advt_prefixes[0].prefix, "89.30.150.0/23"
        )
        self.assertEqual(
            upd_2_stats.most_advt_prefixes[0].timestamp, "20220501.2305"
        )
        self.assertEqual(upd_2_stats.most_advt_prefixes[0].updates, 0)
        self.assertEqual(upd_2_stats.most_advt_prefixes[0].withdraws, 0)

        self.assertIsInstance(upd_3_stats.most_bogon_asns, list)
        self.assertEqual(len(upd_3_stats.most_bogon_asns), 2)
        for i in range(0, len(upd_3_stats.most_bogon_asns)):
            print(f"comparing {i}")
            upd_3_stats.most_bogon_asns[i].print()
            test_3_stats.most_bogon_asns[i].print()
            self.assertTrue(
                upd_3_stats.most_bogon_asns[i].equal_to(
                    test_3_stats.most_bogon_asns[i], meta=True
                )
            )

        self.assertIsInstance(upd_2_stats.most_upd_prefixes, list)
        self.assertEqual(len(upd_2_stats.most_upd_prefixes), 1)
        self.assertIsInstance(upd_2_stats.most_upd_prefixes[0], mrt_entry)
        self.assertEqual(upd_2_stats.most_upd_prefixes[0].advt, 0)
        self.assertEqual(upd_2_stats.most_upd_prefixes[0].as_path, [])
        self.assertEqual(upd_2_stats.most_upd_prefixes[0].comm_set, [])
        self.assertEqual(
            upd_2_stats.most_upd_prefixes[0].filename, self.upd_2_mrt
        )
        self.assertEqual(upd_2_stats.most_upd_prefixes[0].next_hop, "")
        self.assertEqual(upd_2_stats.most_upd_prefixes[0].origin_asns, set())
        self.assertEqual(upd_2_stats.most_upd_prefixes[0].peer_asn, "")
        self.assertEqual(
            upd_2_stats.most_upd_prefixes[0].prefix, "89.30.150.0/23"
        )
        self.assertEqual(
            upd_2_stats.most_upd_prefixes[0].timestamp, "20220501.2305"
        )
        self.assertEqual(upd_2_stats.most_upd_prefixes[0].updates, 898)
        self.assertEqual(upd_2_stats.most_upd_prefixes[0].withdraws, 0)

        self.assertIsInstance(upd_2_stats.most_withd_prefixes, list)
        self.assertEqual(len(upd_2_stats.most_withd_prefixes), 1)
        self.assertIsInstance(upd_2_stats.most_withd_prefixes[0], mrt_entry)
        self.assertEqual(upd_2_stats.most_withd_prefixes[0].advt, 0)
        self.assertEqual(upd_2_stats.most_withd_prefixes[0].as_path, [])
        self.assertEqual(upd_2_stats.most_withd_prefixes[0].comm_set, [])
        self.assertEqual(
            upd_2_stats.most_withd_prefixes[0].filename, self.upd_2_mrt
        )
        self.assertEqual(upd_2_stats.most_withd_prefixes[0].next_hop, "")
        self.assertEqual(upd_2_stats.most_withd_prefixes[0].origin_asns, set())
        self.assertEqual(upd_2_stats.most_withd_prefixes[0].peer_asn, "")
        self.assertEqual(
            upd_2_stats.most_withd_prefixes[0].prefix, "2a01:9e00:4279::/48"
        )
        self.assertEqual(
            upd_2_stats.most_withd_prefixes[0].timestamp, "20220501.2305"
        )
        self.assertEqual(upd_2_stats.most_withd_prefixes[0].updates, 0)
        self.assertEqual(upd_2_stats.most_withd_prefixes[0].withdraws, 89)

        self.assertIsInstance(upd_2_stats.most_advt_origin_asn, list)
        self.assertEqual(len(upd_2_stats.most_advt_origin_asn), 1)
        self.assertIsInstance(upd_2_stats.most_advt_origin_asn[0], mrt_entry)
        self.assertEqual(upd_2_stats.most_advt_origin_asn[0].advt, 2628)
        self.assertEqual(upd_2_stats.most_advt_origin_asn[0].as_path, [])
        self.assertEqual(upd_2_stats.most_advt_origin_asn[0].comm_set, [])
        self.assertEqual(
            upd_2_stats.most_advt_origin_asn[0].filename, self.upd_2_mrt
        )
        self.assertEqual(upd_2_stats.most_advt_origin_asn[0].next_hop, "")
        self.assertEqual(
            upd_2_stats.most_advt_origin_asn[0].origin_asns, set(["20473"])
        )
        self.assertEqual(upd_2_stats.most_advt_origin_asn[0].peer_asn, "")
        self.assertEqual(upd_2_stats.most_advt_origin_asn[0].prefix, "")
        self.assertEqual(
            upd_2_stats.most_advt_origin_asn[0].timestamp, "20220501.2305"
        )
        self.assertEqual(upd_2_stats.most_advt_origin_asn[0].updates, 0)
        self.assertEqual(upd_2_stats.most_advt_origin_asn[0].withdraws, 0)

        self.assertIsInstance(upd_2_stats.most_advt_peer_asn, list)
        self.assertEqual(len(upd_2_stats.most_advt_peer_asn), 1)
        self.assertIsInstance(upd_2_stats.most_advt_peer_asn[0], mrt_entry)
        self.assertEqual(upd_2_stats.most_advt_peer_asn[0].advt, 11595)
        self.assertEqual(upd_2_stats.most_advt_peer_asn[0].as_path, [])
        self.assertEqual(upd_2_stats.most_advt_peer_asn[0].comm_set, [])
        self.assertEqual(
            upd_2_stats.most_advt_peer_asn[0].filename, self.upd_2_mrt
        )
        self.assertEqual(upd_2_stats.most_advt_peer_asn[0].next_hop, "")
        self.assertEqual(upd_2_stats.most_advt_peer_asn[0].origin_asns, set())
        self.assertEqual(upd_2_stats.most_advt_peer_asn[0].peer_asn, "18106")
        self.assertEqual(upd_2_stats.most_advt_peer_asn[0].prefix, "")
        self.assertEqual(
            upd_2_stats.most_advt_peer_asn[0].timestamp, "20220501.2305"
        )
        self.assertEqual(upd_2_stats.most_advt_peer_asn[0].updates, 0)
        self.assertEqual(upd_2_stats.most_advt_peer_asn[0].withdraws, 0)

        self.assertIsInstance(upd_2_stats.most_upd_peer_asn, list)
        self.assertEqual(len(upd_2_stats.most_upd_peer_asn), 1)
        self.assertIsInstance(upd_2_stats.most_upd_peer_asn[0], mrt_entry)
        self.assertEqual(upd_2_stats.most_upd_peer_asn[0].advt, 0)
        self.assertEqual(upd_2_stats.most_upd_peer_asn[0].as_path, [])
        self.assertEqual(upd_2_stats.most_upd_peer_asn[0].comm_set, [])
        self.assertEqual(
            upd_2_stats.most_upd_peer_asn[0].filename, self.upd_2_mrt
        )
        self.assertEqual(upd_2_stats.most_upd_peer_asn[0].next_hop, "")
        self.assertEqual(upd_2_stats.most_upd_peer_asn[0].origin_asns, set())
        self.assertEqual(upd_2_stats.most_upd_peer_asn[0].peer_asn, "18106")
        self.assertEqual(upd_2_stats.most_upd_peer_asn[0].prefix, "")
        self.assertEqual(
            upd_2_stats.most_upd_peer_asn[0].timestamp, "20220501.2305"
        )
        self.assertEqual(upd_2_stats.most_upd_peer_asn[0].updates, 11781)
        self.assertEqual(upd_2_stats.most_upd_peer_asn[0].withdraws, 0)

        self.assertIsInstance(upd_2_stats.most_withd_peer_asn, list)
        self.assertEqual(len(upd_2_stats.most_withd_peer_asn), 1)
        self.assertIsInstance(upd_2_stats.most_withd_peer_asn[0], mrt_entry)
        self.assertEqual(upd_2_stats.most_withd_peer_asn[0].advt, 0)
        self.assertEqual(upd_2_stats.most_withd_peer_asn[0].as_path, [])
        self.assertEqual(upd_2_stats.most_withd_peer_asn[0].comm_set, [])
        self.assertEqual(
            upd_2_stats.most_withd_peer_asn[0].filename, self.upd_2_mrt
        )
        self.assertEqual(upd_2_stats.most_withd_peer_asn[0].next_hop, "")
        self.assertEqual(upd_2_stats.most_withd_peer_asn[0].origin_asns, set())
        self.assertEqual(upd_2_stats.most_withd_peer_asn[0].peer_asn, "18106")
        self.assertEqual(upd_2_stats.most_withd_peer_asn[0].prefix, "")
        self.assertEqual(
            upd_2_stats.most_withd_peer_asn[0].timestamp, "20220501.2305"
        )
        self.assertEqual(upd_2_stats.most_withd_peer_asn[0].updates, 0)
        self.assertEqual(upd_2_stats.most_withd_peer_asn[0].withdraws, 186)

        self.assertIsInstance(upd_2_stats.most_origin_asns, list)
        self.assertEqual(len(upd_2_stats.most_origin_asns), 9)
        self.assertIsInstance(upd_2_stats.most_origin_asns[0], mrt_entry)
        self.assertEqual(upd_2_stats.most_origin_asns[0].advt, 0)
        self.assertEqual(upd_2_stats.most_origin_asns[0].as_path, [])
        self.assertEqual(upd_2_stats.most_origin_asns[0].comm_set, [])
        self.assertEqual(
            upd_2_stats.most_origin_asns[0].filename, self.upd_2_mrt
        )
        self.assertEqual(upd_2_stats.most_origin_asns[0].next_hop, "")
        self.assertEqual(
            upd_2_stats.most_origin_asns[0].origin_asns,
            set(["28198", "262375"]),
        )
        self.assertEqual(upd_2_stats.most_origin_asns[0].peer_asn, "")
        self.assertEqual(
            upd_2_stats.most_origin_asns[0].prefix, "177.131.0.0/21"
        )
        self.assertEqual(
            upd_2_stats.most_origin_asns[0].timestamp, "20220501.2305"
        )
        self.assertEqual(upd_2_stats.most_origin_asns[0].updates, 0)
        self.assertEqual(upd_2_stats.most_origin_asns[0].withdraws, 0)

        self.assertEqual(upd_2_stats.most_origin_asns[1].advt, 0)
        self.assertEqual(upd_2_stats.most_origin_asns[1].as_path, [])
        self.assertEqual(upd_2_stats.most_origin_asns[1].comm_set, [])
        self.assertEqual(
            upd_2_stats.most_origin_asns[1].filename, self.upd_2_mrt
        )
        self.assertEqual(upd_2_stats.most_origin_asns[1].next_hop, "")
        self.assertEqual(
            upd_2_stats.most_origin_asns[1].origin_asns,
            set(["271204", "266181"]),
        )
        self.assertEqual(upd_2_stats.most_origin_asns[1].peer_asn, "")
        self.assertEqual(
            upd_2_stats.most_origin_asns[1].prefix, "179.49.190.0/23"
        )
        self.assertEqual(
            upd_2_stats.most_origin_asns[1].timestamp, "20220501.2305"
        )
        self.assertEqual(upd_2_stats.most_origin_asns[1].updates, 0)
        self.assertEqual(upd_2_stats.most_origin_asns[1].withdraws, 0)

        self.assertEqual(upd_2_stats.most_origin_asns[2].advt, 0)
        self.assertEqual(upd_2_stats.most_origin_asns[2].as_path, [])
        self.assertEqual(upd_2_stats.most_origin_asns[2].comm_set, [])
        self.assertEqual(
            upd_2_stats.most_origin_asns[2].filename, self.upd_2_mrt
        )
        self.assertEqual(upd_2_stats.most_origin_asns[2].next_hop, "")
        self.assertEqual(
            upd_2_stats.most_origin_asns[2].origin_asns,
            set(["396559", "396542"]),
        )
        self.assertEqual(upd_2_stats.most_origin_asns[2].peer_asn, "")
        self.assertEqual(
            upd_2_stats.most_origin_asns[2].prefix, "2620:74:2a::/48"
        )
        self.assertEqual(
            upd_2_stats.most_origin_asns[2].timestamp, "20220501.2305"
        )
        self.assertEqual(upd_2_stats.most_origin_asns[2].updates, 0)
        self.assertEqual(upd_2_stats.most_origin_asns[2].withdraws, 0)

        self.assertEqual(upd_2_stats.most_origin_asns[3].advt, 0)
        self.assertEqual(upd_2_stats.most_origin_asns[3].as_path, [])
        self.assertEqual(upd_2_stats.most_origin_asns[3].comm_set, [])
        self.assertEqual(
            upd_2_stats.most_origin_asns[3].filename, self.upd_2_mrt
        )
        self.assertEqual(upd_2_stats.most_origin_asns[3].next_hop, "")
        self.assertEqual(
            upd_2_stats.most_origin_asns[3].origin_asns, set(["7420", "37154"])
        )
        self.assertEqual(upd_2_stats.most_origin_asns[3].peer_asn, "")
        self.assertEqual(
            upd_2_stats.most_origin_asns[3].prefix, "196.46.192.0/19"
        )
        self.assertEqual(
            upd_2_stats.most_origin_asns[3].timestamp, "20220501.2305"
        )
        self.assertEqual(upd_2_stats.most_origin_asns[3].updates, 0)
        self.assertEqual(upd_2_stats.most_origin_asns[3].withdraws, 0)

        self.assertEqual(upd_2_stats.most_origin_asns[4].advt, 0)
        self.assertEqual(upd_2_stats.most_origin_asns[4].as_path, [])
        self.assertEqual(upd_2_stats.most_origin_asns[4].comm_set, [])
        self.assertEqual(
            upd_2_stats.most_origin_asns[4].filename, self.upd_2_mrt
        )
        self.assertEqual(upd_2_stats.most_origin_asns[4].next_hop, "")
        self.assertEqual(
            upd_2_stats.most_origin_asns[4].origin_asns, set(["7487", "54396"])
        )
        self.assertEqual(upd_2_stats.most_origin_asns[4].peer_asn, "")
        self.assertEqual(
            upd_2_stats.most_origin_asns[4].prefix, "205.197.192.0/21"
        )
        self.assertEqual(
            upd_2_stats.most_origin_asns[4].timestamp, "20220501.2305"
        )
        self.assertEqual(upd_2_stats.most_origin_asns[4].updates, 0)
        self.assertEqual(upd_2_stats.most_origin_asns[4].withdraws, 0)

        self.assertEqual(upd_2_stats.most_origin_asns[5].advt, 0)
        self.assertEqual(upd_2_stats.most_origin_asns[5].as_path, [])
        self.assertEqual(upd_2_stats.most_origin_asns[5].comm_set, [])
        self.assertEqual(
            upd_2_stats.most_origin_asns[5].filename, self.upd_2_mrt
        )
        self.assertEqual(upd_2_stats.most_origin_asns[5].next_hop, "")
        self.assertEqual(
            upd_2_stats.most_origin_asns[5].origin_asns,
            set(["203020", "29802"]),
        )
        self.assertEqual(upd_2_stats.most_origin_asns[5].peer_asn, "")
        self.assertEqual(
            upd_2_stats.most_origin_asns[5].prefix, "206.123.159.0/24"
        )
        self.assertEqual(
            upd_2_stats.most_origin_asns[5].timestamp, "20220501.2305"
        )
        self.assertEqual(upd_2_stats.most_origin_asns[5].updates, 0)
        self.assertEqual(upd_2_stats.most_origin_asns[5].withdraws, 0)

        self.assertEqual(upd_2_stats.most_origin_asns[6].advt, 0)
        self.assertEqual(upd_2_stats.most_origin_asns[6].as_path, [])
        self.assertEqual(upd_2_stats.most_origin_asns[6].comm_set, [])
        self.assertEqual(
            upd_2_stats.most_origin_asns[6].filename, self.upd_2_mrt
        )
        self.assertEqual(upd_2_stats.most_origin_asns[6].next_hop, "")
        self.assertEqual(
            upd_2_stats.most_origin_asns[6].origin_asns,
            set(["52000", "19318"]),
        )
        self.assertEqual(upd_2_stats.most_origin_asns[6].peer_asn, "")
        self.assertEqual(
            upd_2_stats.most_origin_asns[6].prefix, "68.168.210.0/24"
        )
        self.assertEqual(
            upd_2_stats.most_origin_asns[6].timestamp, "20220501.2305"
        )
        self.assertEqual(upd_2_stats.most_origin_asns[6].updates, 0)
        self.assertEqual(upd_2_stats.most_origin_asns[6].withdraws, 0)

        self.assertEqual(upd_2_stats.most_origin_asns[7].advt, 0)
        self.assertEqual(upd_2_stats.most_origin_asns[7].as_path, [])
        self.assertEqual(upd_2_stats.most_origin_asns[7].comm_set, [])
        self.assertEqual(
            upd_2_stats.most_origin_asns[7].filename, self.upd_2_mrt
        )
        self.assertEqual(upd_2_stats.most_origin_asns[7].next_hop, "")
        self.assertEqual(
            upd_2_stats.most_origin_asns[7].origin_asns,
            set(["55020", "137951"]),
        )
        self.assertEqual(upd_2_stats.most_origin_asns[7].peer_asn, "")
        self.assertEqual(
            upd_2_stats.most_origin_asns[7].prefix, "156.241.128.0/22"
        )
        self.assertEqual(
            upd_2_stats.most_origin_asns[7].timestamp, "20220501.2305"
        )
        self.assertEqual(upd_2_stats.most_origin_asns[7].updates, 0)
        self.assertEqual(upd_2_stats.most_origin_asns[7].withdraws, 0)

        self.assertEqual(upd_2_stats.most_origin_asns[8].advt, 0)
        self.assertEqual(upd_2_stats.most_origin_asns[8].as_path, [])
        self.assertEqual(upd_2_stats.most_origin_asns[8].comm_set, [])
        self.assertEqual(
            upd_2_stats.most_origin_asns[8].filename, self.upd_2_mrt
        )
        self.assertEqual(upd_2_stats.most_origin_asns[8].next_hop, "")
        self.assertEqual(
            upd_2_stats.most_origin_asns[8].origin_asns,
            set(["269208", "268347"]),
        )
        self.assertEqual(upd_2_stats.most_origin_asns[8].peer_asn, "")
        self.assertEqual(
            upd_2_stats.most_origin_asns[8].prefix, "2804:610c::/32"
        )
        self.assertEqual(
            upd_2_stats.most_origin_asns[8].timestamp, "20220501.2305"
        )
        self.assertEqual(upd_2_stats.most_origin_asns[8].updates, 0)
        self.assertEqual(upd_2_stats.most_origin_asns[8].withdraws, 0)

        self.assertIsInstance(upd_4_stats.most_unknown_attrs, list)
        self.assertEqual(len(upd_4_stats.most_unknown_attrs), 1)
        self.assertEqual(upd_4_stats.most_unknown_attrs[0].advt, 0)
        self.assertEqual(
            upd_4_stats.most_unknown_attrs[0].as_path, ["286", "1103", "12654"]
        )
        self.assertEqual(
            upd_4_stats.most_unknown_attrs[0].comm_set,
            ["286:80", "286:800", "286:3031", "286:4001"],
        )
        self.assertEqual(
            os.path.basename(upd_4_stats.most_unknown_attrs[0].filename),
            os.path.basename(self.upd_4_mrt),
        )
        self.assertEqual(
            upd_4_stats.most_unknown_attrs[0].next_hop, "195.66.224.54"
        )
        self.assertEqual(
            upd_4_stats.most_unknown_attrs[0].origin_asns, set(["12654"])
        )
        self.assertEqual(upd_4_stats.most_unknown_attrs[0].peer_asn, "286")
        self.assertEqual(
            upd_4_stats.most_unknown_attrs[0].prefix, "93.175.144.0/24"
        )
        self.assertEqual(
            upd_4_stats.most_unknown_attrs[0].timestamp, "20100827.0842"
        )
        self.assertEqual(upd_4_stats.most_unknown_attrs[0].updates, 0)
        self.assertEqual(upd_4_stats.most_unknown_attrs[0].withdraws, 0)
        self.assertEqual(
            upd_4_stats.most_unknown_attrs[0].unknown_attrs, set([99])
        )

        self.assertIsInstance(upd_2_stats.total_upd, int)
        self.assertEqual(upd_2_stats.total_upd, 29688)
        self.assertIsInstance(upd_2_stats.total_advt, int)
        self.assertEqual(upd_2_stats.total_advt, 29396)
        self.assertIsInstance(upd_2_stats.total_withd, int)
        self.assertEqual(upd_2_stats.total_withd, 950)
        self.assertIsInstance(upd_2_stats.file_list, list)
        self.assertEqual(upd_2_stats.file_list, [self.upd_2_mrt])
        self.assertIsInstance(upd_2_stats.timestamp, str)
        self.assertEqual(upd_2_stats.timestamp, "20220501.2305")


if __name__ == "__main__":
    unittest.main()
