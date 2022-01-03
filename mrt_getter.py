import datetime
import os
import requests

class mrt_getter:
    """
    Class which can be used to get MRT files from public sources.
    """

    # Locations of popular MRT dump archives:

    """
    Route-Views London
    RIB dumps are every 2 hours
    RIB dump example: http://archive.routeviews.org/route-views.linx/bgpdata/2021.12/RIBS/rib.20211222.0600.bz2
    UPDATE dumps are ever 15 minutes
    UPDATE dump example: http://archive.routeviews.org/route-views.linx/bgpdata/2021.12/UPDATES/updates.20211222.0600.bz2
    """
    RV_LINX = "http://archive.routeviews.org/route-views.linx/bgpdata/"

    """
    Route-Views Sydnex
    RIB dumps are every 2 hours
    RIB dump example: http://archive.routeviews.org/route-views.sydney/bgpdata/2021.12/RIBS/rib.20211201.0600.bz2
    UPDATE dumps are ever 15 minutes
    UPDATE dump example: http://archive.routeviews.org/route-views.sydney/bgpdata/2021.12/UPDATES/updates.20211201.0030.bz2
    """
    RV_SYDNEY = "http://archive.routeviews.org/route-views.sydney/bgpdata/"


    """
    RRC23 Singapore
    RIB dumps are every 8 hours
    RIB dump example: https://data.ris.ripe.net/rrc23/2021.12/bview.20211206.1600.gz
    UPDATE dumps are ever 5 minutes
    UPDATE dump example: https://data.ris.ripe.net/rrc23/2021.12/updates.20211231.2335.gz
    """
    RCC_23 = "https://data.ris.ripe.net/rrc23/"

    """
    RRC24 LACNIC
    RIB dumps are every 8 hours
    RIB dump example: https://data.ris.ripe.net/rrc24/2021.12/bview.20211208.1600.gz
    UPDATE dumps are ever 5 minutes
    UPDATE dump example: https://data.ris.ripe.net/rrc24/2021.12/updates.20211231.1245.gz
    """
    RCC_24 = "https://data.ris.ripe.net/rrc24/"

    @staticmethod
    def get_latest_rv():

        downloaded = []

        hours = int(datetime.datetime.strftime(datetime.datetime.now(), "%H"))
        if hours % 2 != 0:
            hours = 3
        else:
            hours = 2
        h_delta = datetime.timedelta(hours=hours)

        latest_ym = datetime.datetime.strftime(datetime.datetime.now()-h_delta,"%Y.%m")
        latest_ymd_hm = datetime.datetime.strftime(datetime.datetime.now()-h_delta,"%Y%m%d.%H00")

        latest_linx_rib = mrt_getter.RV_LINX + latest_ym + "/RIBS/rib." + latest_ymd_hm + ".bz2"
        filename = "/tmp/" + os.path.basename(latest_linx_rib)
        mrt_getter.download_mrt(filename=filename, url=latest_linx_rib)
        downloaded.append(latest_linx_rib)

        update_base = datetime.datetime.strptime(latest_ymd_hm, "%Y%m%d.%H00")
        for i in range(0, 8):
            m_delta = datetime.timedelta(minutes=(i*15))
            update_ymd_hm = datetime.datetime.strftime(update_base+m_delta,"%Y%m%d.%H%M")

            latest_linx_upd = mrt_getter.RV_LINX + latest_ym + "/UPDATES/updates." + update_ymd_hm + ".bz2"
            filename = "/tmp/" + os.path.basename(latest_linx_upd)
            mrt_getter.download_mrt(filename=filename, url=latest_linx_upd)
            downloaded.append(filename)

        return downloaded

    @staticmethod
    def download_mrt(filename=None, url=None, debug=False):
        """
        Download an MRT file from the given url,
        and save it as the given filename.
        """

        if not url:
            raise ValueError("Missing ULR")

        if not filename:
            filename = os.path.basename(url)

        with open(filename, "wb") as f:
            print(f"Downloading {url} to {filename}")
            try:
                req = requests.get(url, stream=True)
            except requests.exceptions.ConnectionError as e:
                if debug:
                    print(f"Couldn't connect to MRT server: {e}")
                f.close()
                raise requests.exceptions.ConnectionError

            file_len = req.headers['Content-length']

            if req.status_code != 200:
                if debug:
                    print(f"HTTP error: {req.status_code}")
                    print(req.url)
                    print(req.text)
                f.write(req.content)
                f.close()
                req.raise_for_status()

            if file_len is None:
                if debug:
                    print(req.url)
                    print(req.text)
                f.write(req.content)
                f.close()
                raise ValueError("Missing file length!")

            file_len = int(file_len)
            rcvd = 0
            print(f"File size is {file_len/1024/1024:.7}MBs")
            progress = 0
            for chunk in req.iter_content(chunk_size=1024):

                if req.status_code != 200:
                    if debug:
                        print(f"HTTP error: {req.status_code}")
                        print(req.url)
                        print(req.text)
                    f.write(req.content)
                    f.close()
                    req.raise_for_status()

                rcvd += len(chunk)
                f.write(chunk)
                f.flush()

                if rcvd == file_len:
                    print(f"Downloaded {rcvd}/{file_len} ({(rcvd/file_len)*100}%)")
                elif ((rcvd/file_len)*100)//10 > progress:
                    print(f"\rDownloaded {rcvd}/{file_len} ({(rcvd/file_len)*100:.3}%)", end="\r")
                    progress = ((rcvd/file_len)*100)//10
