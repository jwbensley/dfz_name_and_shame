
class whois:

    @staticmethod
    def as_lookup(asn):
        if not asn:
            return False
        if not isinstance(asn, int):
            return False

        asn = "AS" + str(asn)
        cmd = f"whois {asn}"#" | grep -m 1 as-name:"
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
