import os
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
        cmd = f"whois {asn}" #" | grep -m 1 as-name:"
        output = os.popen(cmd).read()

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
        if as_name:
            return as_name

        return as_name
