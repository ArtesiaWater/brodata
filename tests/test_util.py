import os
import brodata


def test_read_zipfile():
    fname = os.path.join("data", "r-calje@artesia-water-nl_2024-06-04-12-35-07.zip")
    data = brodata.util.read_zipfile(fname)
