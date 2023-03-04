import requests
from datetime import date, datetime
from app import main, exceptions
import unittest

urlbase = "http://127.0.0.1:8001/symbol/"


class TestEndpoints(unittest.TestCase):
    def test_intervals(self):
        url = f"{urlbase}"
        return requests.get(url)

    def test_data(self):
        url = f"{urlbase}5m/1INCH"
        result = requests.get(url)
        print("banan")


class TestFunctions(unittest.TestCase):
    def test_validate_date_good_date(self):
        date = "2023-03-04 12:09:32.578174+11:00"
        interval = "5m"
        self.assertEqual(
            main.validate_date(date, interval),
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
