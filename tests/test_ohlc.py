import requests
from datetime import date, datetime
import unittest
import pandas as pd
from persistent_ohlc_client import PersistentOhlcClient
import json

urlbase = "http://127.0.0.1:8002"
macd_columns = set(
    [
        "macd_signal",
        "macd_macd",
        "macd_cycle",
        "macd_histogram",
        "macd_above_signal",
        "macd_crossover",
    ]
)
fake_symbol = "some fake symbol"
fake_ta = "some fake TA"


class TestClient(unittest.TestCase):
    def test_count(self):
        count = 10
        client = PersistentOhlcClient()
        query = client.get_ohlc("BTC-USD", count=count)
        self.assertEqual(len(query), count)

    def test_bad_symbol(self):
        client = PersistentOhlcClient()
        error_triggered = False
        try:
            query = client.get_ohlc(fake_symbol)
        except requests.exceptions.HTTPError as e:
            json_response = json.loads(e.response.text)
            if "detail" in json_response.keys():
                if json_response["detail"] == f"Symbol {fake_symbol} not found":
                    error_triggered = True

        self.assertEqual(True, error_triggered)

        with self.assertRaises(requests.exceptions.HTTPError):
            query = client.get_ohlc(fake_symbol)

    def test_good_symbol(self):
        client = PersistentOhlcClient()
        query = client.get_ohlc("BTC-USD")
        self.assertTrue(len(query) > 0)

    def test_ta_str(self):
        client = PersistentOhlcClient()
        query = client.get_ohlc("BTC-USD", ta="MacdTA")

        diff_len = len(macd_columns.difference(query.columns))
        self.assertEquals(diff_len, 0)

    def test_ta_macd(self):
        client = PersistentOhlcClient()
        query = client.get_ohlc("BTC-USD", ta=["MacdTA"])
        diff_len = len(macd_columns.difference(query.columns))
        self.assertEquals(diff_len, 0)

    def test_ta_fake(self):
        client = PersistentOhlcClient()
        error_triggered = False
        try:
            query = client.get_ohlc("BTC-USD", ta=fake_ta)
        except requests.exceptions.HTTPError as e:
            json_response = json.loads(e.response.text)
            if "detail" in json_response.keys():
                if json_response["detail"] == f"TA function {fake_ta} was not found":
                    error_triggered = True

        self.assertEqual(True, error_triggered)
