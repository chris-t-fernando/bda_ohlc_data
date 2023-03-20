import requests

import unittest

urlbase = "http://127.0.0.1:8001/symbol/"


url = f"{urlbase}AAPL/ohlc/5m"
url = f"http://127.0.0.1:8001/nyse/symbol/AAPL/hours?open_at=2023-03-15T20%3A42%3A23.408391&clock_id=banana"
result = requests.get(url)
print("aba")
