import time
from urllib.parse import quote_plus
import pandas as pd
import requests


def get_ohlc(symbol, since=None):
    if since:
        since_str = f"?since={quote_plus(since)}"
    else:
        since_str = ""
    urlbase = "http://127.0.0.1:8002"
    url = f"{urlbase}/symbols/{symbol}/ohlc/5m{since_str}"
    start = time.time()
    ret_get = requests.get(url)
    print(f"GET_OHLC RAN FOR {time.time()-start}")
    return ret_get


def do_run():
    symbols = ["BTC-USD", "ETH-USD", "MATIC-USD", "AAVE-USD", "COMP-USD", "AVAX-USD"]

    print("bootstrapping...")
    for s in symbols:
        print(f"Starting {s}")
        ex_now = time.time()
        endpoint_response = get_ohlc(s)
        if endpoint_response.status_code == 404:
            print(f"\tSymbol {s} was not found, bailing")
            continue

        print(f"\tGot {s} CSV in {(time.time() - ex_now):.3f}")
        ex_now = time.time()
        in_df = pd.read_json(endpoint_response.text)
        print(f"\tGot {s} DF len {len(in_df)} in {(time.time() - ex_now):.3f}")
    print("done bootstrapping")

    print("refreshing...")
    for s in symbols:
        print(f"Starting {s}")
        ex_now = time.time()
        endpoint_response = get_ohlc(s, since="2023-03-24 01:04:31.850056+11:00")
        if endpoint_response.status_code == 404:
            print(f"\tSymbol {s} was not found, bailing")
            continue

        print(f"\tGot {s} CSV in {(time.time() - ex_now):.3f}")
        ex_now = time.time()
        in_df = pd.read_json(endpoint_response.text)
        print(f"\tGot {s} DF len {len(in_df)} in {(time.time() - ex_now):.3f}")
    print("done refreshing")

    return "Done"


if __name__ == "__main__":
    do_run()
