from fastapi import FastAPI, HTTPException, Query, Response
from typing import Annotated, List
import uvicorn
import json
from parameter_store import S3
from parameter_store import exceptions as p_exceptions
import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
import redis
from symbol_cache import Symbol, MacdTA
import time

# from . import crud, deps, models, schemas, security
# from app
import sys

if "unittest" in sys.modules.keys():
    from . import schemas, config, exceptions
else:
    import schemas, config, exceptions

# redit config
pool = redis.ConnectionPool(host="localhost", port=6379, db=0, decode_responses=True)

pool = redis.ConnectionPool(
    host="host.docker.internal", port=6379, db=0, decode_responses=True
)
redis = redis.Redis(connection_pool=pool)

intervals = [
    schemas.Interval(interval=60, interval_name="1m", max_history_days=6),
    schemas.Interval(interval=300, interval_name="5m", max_history_days=59),
    schemas.Interval(interval=3600, interval_name="1h", max_history_days=500),
    schemas.Interval(interval=86400, interval_name="1d", max_history_days=2000),
]
"""
/symbol/
/symbol/{interval}/{symbol}
/symbol/{interval}/{symbol}?start=
/symbol/{interval}/{symbol}?end=
/symbol/{interval}/{symbol}?start= &end=


requests.get(/symbol/5m/ACN?clock=banana1)
"""


app = FastAPI()
s3 = S3("mfers-tabot")

active_symbols = dict()


@app.get("/symbols/{symbol}/ohlc/{interval}")
def get_market_hours(
    symbol: str,
    interval: str,
    start=None,
    end=None,
    ta: List[str] = Query(None),
):
    ex_now = time.time()
    if symbol not in active_symbols.keys():
        active_symbols[symbol] = dict()

    if interval not in active_symbols[symbol].keys():
        active_symbols[symbol][interval] = Symbol(symbol, interval=interval)

    if ta:
        for algo in ta:
            active_symbols[symbol][interval].ohlc.apply_ta(algo)

    ret_df = active_symbols[symbol][interval].ohlc.bars
    if start:
        ret_df = ret_df.loc[ret_df.index >= start]

    if end:
        ret_df = ret_df.loc[ret_df.index <= end]

    response = Response(
        ret_df.to_json(date_format="iso"), media_type="application/json"
    )
    print(f"\tJSON LOAD IN {time.time() - ex_now}")
    return response

    # js_return = json.loads(ret_df.to_json(date_format="iso"))
    # print(f"\tJSON LOAD IN {time.time() - ex_now}")
    # return js_return


@app.get("/symbols/{symbol}/info/{interval}/next_tick", response_model=dict)
def get_tick(symbol: str, interval: str, when: datetime.datetime = None):
    if symbol not in active_symbols.keys():
        active_symbols[symbol] = dict()

    if interval not in active_symbols[symbol].keys():
        active_symbols[symbol][interval] = Symbol(symbol, interval=interval)

    df = active_symbols[symbol][interval].ohlc.bars
    return_dict = {"next_timestamp": None, "ready_in_seconds": None}

    if when:
        trimmed = df.loc[df.index <= when]
        last_timestamp = trimmed.index[-1]
        # loc = df.index.get_loc(last_timestamp)
        if last_timestamp < df.index[-1]:
            loc = df.index.get_loc(last_timestamp)
            return_dict["next_timestamp"] = df.index[loc + 1]
            return_dict["ready_in_seconds"] = 0
            return return_dict

    # need to calculate when the next
    return_dict["ready_in_seconds"] = active_symbols[symbol][interval].ohlc.get_pause()
    last_timestamp = df.index[-1]
    interval_seconds = [i.interval for i in intervals if i.interval_name == interval][0]
    next_timestamp = last_timestamp + relativedelta(seconds=interval_seconds)
    return_dict["next_timestamp"] = next_timestamp

    return return_dict


@app.get("/{market}/hours", response_model=dict)
def get_market_hours(market: str, clock_id=None):
    if market in active_symbols.keys():
        return {"market": market, "is_open": "open"}
    active_symbols[market] = "def"
    return {"market": market, "is_open": "def"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8002)
