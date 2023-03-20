import requests
from datetime import date, datetime
from app import main, exceptions, schemas
import unittest
import pandas as pd

urlbase = "http://127.0.0.1:8001"


class TestEndpoints(unittest.TestCase):
    def test_intervals(self):
        url = f"{urlbase}"
        return requests.get(url)

    def test_get_data(self):
        url = f"{urlbase}/nyse/symbol/AAPL/ohlc/5m"
        result = requests.get(url)
        print("banan")


class TestFunctions(unittest.TestCase):
    def test_market_open_at_closed_date_closed_hours(self):
        date = "2023-03-04 23:09:32.578174+00:00"
        interval = "5m"
        self.assertEqual(
            main._market_open_at("nyse", date),
            False,
        )

    def test_market_open_at_closed_date_open_hours(self):
        date = "2023-03-04 18:09:32.578174+00:00"
        interval = "5m"
        self.assertEqual(
            main._market_open_at("nyse", date),
            False,
        )

    def test_market_open_at_open_date_open_hours(self):
        date = "2023-03-02 18:09:32.578174+00:00"
        interval = "5m"
        self.assertEqual(
            main._market_open_at("nyse", date),
            True,
        )

    def test_market_open_at_open_date_closed_hours(self):
        date = "2023-03-02 23:09:32.578174+00:00"
        interval = "5m"
        self.assertEqual(
            main._market_open_at("nyse", date),
            False,
        )

    def test_validate_date_good_date(self):
        interval_settings = schemas.Interval(
            interval=300, interval_name="5m", max_history_days=300
        )
        date = "2023-03-04 12:09:32.578174+11:00"
        market = "nyse"
        validated_date = main.validate_date(
            interval_settings=interval_settings, start_date=date, market=market
        )
        self.assertEqual(
            validated_date,
            datetime.fromisoformat(date).astimezone(),
        )

    def test_validate_date_bad_date(self):
        date = "a2023-03-04 12:09:32.578174+11:00"
        interval = "5m"
        try:
            main.validate_date(date, interval)
        except exceptions.InvalidDate as e:
            caught_error = True
        else:
            caught_error = False

        self.assertTrue(caught_error)

    def test_validate_date_bad_interval(self):
        date = "2023-03-04 12:09:32.578174+11:00"
        interval = "banana"
        try:
            main.validate_date(date, interval)
        except exceptions.InvalidIntervalError as e:
            caught_error = True
        else:
            caught_error = False

        self.assertTrue(caught_error)

    def test_validate_date_good_interval(self):
        date = "2023-03-04 12:09:32.578174+11:00"
        interval = "1m"
        try:
            main.validate_date(date, interval)
        except exceptions.InvalidIntervalError as e:
            caught_error = True
        else:
            caught_error = False

        self.assertFalse(caught_error)

    def test_validate_date_none_specified(self):
        date = "max"
        interval = "5m"

        try:
            main.validate_date(date, interval)
        except Exception as e:
            caught_error = True
        else:
            caught_error = False
        self.assertFalse(caught_error)

    def test_snap_interval_5m_market_closed(self):
        date = "2023-03-04 12:09:32.578174+11:00"
        interval_str = "5m"
        expected = pd.Timestamp("2023-03-04 08:00:00+1100")

        snapped = main._snap_to_interval(date, interval_str, "nyse")

        self.assertEqual(snapped, expected)

    def test_snap_interval_5m_market_open(self):
        date = "2023-03-02 06:09:32.578174+11:00"
        interval_str = "5m"
        expected = pd.Timestamp("2023-03-02 06:05:00+1100")

        snapped = main._snap_to_interval(date, interval_str, "nyse")

        self.assertEqual(snapped, expected)
