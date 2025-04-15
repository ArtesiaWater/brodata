import os

import brodata


def test_read_zipfile():
    fname = os.path.join(
        "tests", "data", "r-calje@artesia-water-nl_2024-06-04-12-35-07.zip"
    )
    brodata.util.read_zipfile(fname)
