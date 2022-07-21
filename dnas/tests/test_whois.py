import os
import sys
import unittest

sys.path.append(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__))
        , "../"
    )
)
from dnas.whois import whois

class test_whois(unittest.TestCase):

    def test_as_lookup(self):
        self.assertRaises(ValueError, whois.as_lookup, -123)
        self.assertRaises(TypeError, whois.as_lookup, "abc")
        self.assertIsInstance(whois.as_lookup(41695), str)

        # Bogon ASN
        self.assertEqual("", whois.as_lookup(65000))

        # ASN which redirects to a private whois server
        #self.assertEqual("", whois.as_lookup(8100))
        #print(whois.as_lookup(8100))
        # ^ they've gone public / fixed the redirect so no longer a valid test.

        # Whois entry which will decode using utf-8
        self.assertEqual("VOSTRON-AS", whois.as_lookup(41695))

        # Whois entry which will decode using ISO-8859-1
        self.assertEqual("Linkever", whois.as_lookup(38336))

if __name__ == '__main__':
    unittest.main()
