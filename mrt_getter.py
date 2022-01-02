

def download_mrt(filename, url):

    with open(filename, "wb") as f:
        print(f"Downloading {url} to {filename}")
        try:
            req = requests.get(url, stream=True)
        except requests.exceptions.ConnectionError as e:
            print(f"Couldn't connect to MRT server: {e}")
            f.close()
            return False

        file_len = req.headers['Content-length']

        if req.status_code != 200:
            print(f"HTTP error: {req.status_code}")
            print(req.url)
            print(req.text)
            f.write(req.content)
            f.close()
            return False

        if file_len is None:
            print(f"Missing file length!")
            print(req.url)
            print(req.text)
            f.write(req.content)
            f.close()
            return False

        file_len = int(file_len)
        rcvd = 0
        print(f"File size is {file_len/1024/1024:.7}MBs")
        progress = 0
        for chunk in req.iter_content(chunk_size=1024):

            if req.status_code != 200:
                print(f"HTTP error: {req.status_code}")
                print(req.url)
                print(req.text)
                f.write(req.content)
                f.close()
                return False

            rcvd += len(chunk)
            f.write(chunk)
            f.flush()

            if rcvd == file_len:
                print(f"Downloaded {rcvd}/{file_len} ({(rcvd/file_len)*100}%)")
            elif ((rcvd/file_len)*100)//10 > progress:
                print(f"\rDownloaded {rcvd}/{file_len} ({(rcvd/file_len)*100:.3}%)", end="\r")
                progress = ((rcvd/file_len)*100)//10

    return True


def test_mrt_rib(data):

    print(f"Started test_mrt_rib() with {len(data)} entries")

    i = 0
    for entry in data:
        if (entry.data["type"][0] != mrtparse.MRT_T['TABLE_DUMP_V2']):
            print(f"Not TABLE_DUMP_V2")
            print(entry.data)
            return i
        
        # RIB dumps can contain both AFIs (v4 and v6)
        if (entry.data["subtype"][0] != mrtparse.TD_V2_ST['PEER_INDEX_TABLE'] and
            entry.data["subtype"][0] != mrtparse.TD_V2_ST['RIB_IPV4_UNICAST'] and
            entry.data["subtype"][0] != mrtparse.TD_V2_ST['RIB_IPV6_UNICAST']):
            print(f"Not PEER_INDEX_TABLE or RIB_IPV4_UNICAST or RIB_IPV6_UNICAST")
            print(entry.data)
            return i

        i+= 1

    return i

def test_mrt_updates(data):

    print(f"Started test_mrt_updates() with {len(data)} entries")

    i = 0
    for entry in data:
        if (entry.data["type"][0] != mrtparse.MRT_T['BGP4MP_ET']):
            print(f"Not BGP4MP_ET")
            print(entry.data)
            return i
        
        # RIB dumps can contain both AFIs (v4 and v6)
        if (entry.data["subtype"][0] != mrtparse.BGP4MP_ST['BGP4MP_MESSAGE_AS4'] and
            entry.data["subtype"][0] != mrtparse.BGP4MP_ST['BGP4MP_MESSAGE']):
            print(f"Not BGP4MP_MESSAGE or BGP4MP_MESSAGE_AS4")
            print(entry.data)
            return i

        i+= 1

    return i

def get_mrt_size(filename):

    i = 0
    for entry in mrtparse.Reader(filename):
        i += 1

    return i
