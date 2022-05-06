import logging
import os
import subprocess
import typing

class whois:

    @staticmethod
    def as_lookup(as_num: int = -1) -> str:
        if as_num < 0:
            raise ValueError(
                f"Missing required arguments: as_num={as_num}"
            )

        if type(as_num) != int:
            raise TypeError(
                f"as_num is not string: {type(as_num)}"
            )

        asn = "AS" + str(as_num)
        cmd = ["whois", f"{asn}"]
        ret = subprocess.check_output(cmd)

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
