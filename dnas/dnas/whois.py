import logging
import os
import subprocess
import typing

from dnas.bogon_asn import bogon_asn

class whois:

    @staticmethod
    def as_lookup(asn: int = -1) -> str:
        if asn < 0:
            raise ValueError(
                f"Missing required arguments: asn={asn}"
            )

        if type(asn) != int:
            raise TypeError(
                f"asn is not string: {type(asn)}"
            )

        if bogon_asn.is_bogon(asn):
            return ""

        cmd = ["whois", f"AS{str(asn)}"]

        # Some WHOIS records redirect to private unreachable whois servers
        try:
            ret = subprocess.check_output(cmd)
        except subprocess.CalledProcessError as e:
            if "returned non-zero exit status 2" in str(e):
                return ""
            else:
                raise

        try:
            output = ret.decode("utf-8")
        except UnicodeDecodeError:
            try:
                output = ret.decode("utf-16")
            except UnicodeDecodeError:
                try:
                    output = ret.decode("utf-32")
                except UnicodeDecodeError:
                    try:
                        output = ret.decode("ISO-8859-1")
                    except UnicodeDecodeError:
                        logging.error("Unable to decode WHOIS output: {cmd}")
                        return ""

        as_name = ""
        for line in output.split("\n"):
            if "as-name:" in line:
                tmp = line.split()[-1]
                if tmp.strip():
                    as_name = tmp
                    break
        if as_name:
            return as_name

        for line in output.split("\n"):
            if "owner:" in line:
                tmp = ' '.join(line.split()[1:])
                if tmp.strip():
                    as_name = tmp
                    break
        if as_name:
            return as_name

        for line in output.split("\n"):
            if "ASName:" in line:
                tmp = ' '.join(line.split()[1:])
                if tmp.strip():
                    as_name = tmp
                    break
        if as_name:
            return as_name

        for line in output.split("\n"):
            if "OrgName:" in line:
                tmp = ' '.join(line.split()[1:])
                if tmp.strip():
                    as_name = tmp
                    break

        return as_name
