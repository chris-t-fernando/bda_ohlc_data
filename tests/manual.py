import requests

import unittest

urlbase = "http://127.0.0.1:8001/symbol/"


url = f"{urlbase}5m/1INCH"
result = requests.get(url)
print("aba")
