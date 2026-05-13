# %%
import urllib
import requests
import pytest

import brodata


def test_get_bronhouders():
    try:
        brodata.bro.get_bronhouders()
    except Exception as e:
        allow_network_fail(e)


def test_get_brondocumenten_per_bronhouder():
    try:
        brodata.bro.get_brondocumenten_per_bronhouder()
    except Exception as e:
        allow_network_fail(e)


def allow_network_fail(e):
    allowed = (
        urllib.error.URLError,
        requests.ConnectionError,
        requests.exceptions.RequestException,
        FileNotFoundError,
    )
    if isinstance(e, allowed):
        pytest.skip(f"Network unavailable: {e}")
    raise


def test_get_kvk_df():
    brodata.bro.get_kvk_df()
