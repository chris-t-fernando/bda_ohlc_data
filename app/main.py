from fastapi import FastAPI, HTTPException
import uvicorn
import json
from parameter_store import S3
from parameter_store import exceptions as p_exceptions
import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from bda_clock import BdaClock, NoSuchClockError
import redis
from symbol import SymbolData
import pandas_market_calendars as mcal
import pytz
import time
from io import StringIO

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


def csv_to_df(csv_str: str):
    csv = StringIO(csv_str)
    df = pd.read_csv(csv, sep=",", index_col=0, parse_dates=True)
    return df


def validate_date(
    interval_settings: schemas.Interval,
    market: str,
    start_date: str = None,
    end_date: str = None,
):
    ex_now = time.time()

    date = start_date if start_date else end_date
    if not start_date and not end_date:
        raise ValueError(
            f"Cannot run validate_date function without specifying either start_date or end_date"
        )
    if start_date and end_date:
        raise ValueError(
            f"Cannot run validate_date function specifying both start_date or end_date"
        )

    # get the max allowable history first
    if date == "max":
        real_now = datetime.datetime.now().astimezone()
        if start_date:
            date_obj = real_now - relativedelta(days=interval_settings.max_history_days)

        elif end_date:
            date_obj = real_now

    else:
        # specified a date, so try convert it to a real datetime
        try:
            date_obj = datetime.datetime.fromisoformat(date)
        except ValueError as e:
            raise exceptions.InvalidDate(
                f"Specified start date {date} is not a valid ISO formatted date"
            )

        naive = date_obj.tzinfo is None or date_obj.tzinfo.utcoffset(date_obj) is None
        if naive:
            raise exceptions.InvalidDate(
                f"Specified start date {date} is timezone naive"
            )
    print(f"30 {time.time() - ex_now}")
    ex_now = time.time()
    # now align it to the nearest interval
    snapped_date = _snap_to_interval(str(date_obj), interval_settings, market=market)
    print(f"31 {time.time() - ex_now}")
    ex_now = time.time()
    return snapped_date


def _get_interval_settings(interval):
    try:
        interval_object = [x for x in list_intervals() if x.interval_name == interval][
            0
        ]
    except IndexError as e:
        raise exceptions.InvalidIntervalError(
            f"Specified interval {interval} is not supported"
        )
    return interval_object


def _validate_start(
    start: datetime.datetime, end: datetime.datetime, now: datetime.datetime
):
    if end > start:
        raise exceptions.EndBeforeStartError(f"Start date")
    return True


def _get_s3_data(interval_name: str, symbol: str):
    path = config.data_path_template.substitute(interval=interval_name, symbol=symbol)
    try:
        existing_data = s3.get(path)
    except p_exceptions.NoSuchKeyException as e:
        return None

    # do something smart with imported data to validate it
    df = csv_to_df(existing_data)
    return df


def _snap_to_interval(
    timestamp: datetime.datetime,
    interval_settings: schemas.Interval,
    market: str,
    forward=False,
):
    interval_name = interval_settings.interval_name
    interval_seconds = interval_settings.interval
    timestamp_obj = datetime.datetime.fromisoformat(timestamp)

    # now align it to the nearest interval
    out_date = timestamp_obj.replace(second=0, microsecond=0)
    #    out_date = out_date.replace(microsecond=0)
    if interval_name == "1m":
        ...
    elif interval_name == "5m":
        mod_minute = out_date.minute % 5
        out_date = out_date.replace(minute=out_date.minute - mod_minute)
    elif interval_name == "1h":
        out_date = out_date.replace(minute=0)
    elif interval_name == "1d":
        out_date = out_date.replace(minute=0, hour=0)

    ex_now = time.time()
    # is the market open at this time?
    if _market_open_at(market, str(out_date)):
        print(f"40 {time.time() - ex_now}")
        ex_now = time.time()
        return out_date
    else:
        last_day = _market_last_open_day(market, str(out_date))
        print(f"41 {time.time() - ex_now}")
        ex_now = time.time()
        last_interval = last_day.market_close
        # convert this to the original timezone
        last_interval_tz = last_interval.astimezone(timestamp_obj.tzname())
        delta = relativedelta(seconds=interval_seconds)
        last_record_before_close = last_interval_tz - delta
        return last_record_before_close


def _market_last_open_day(market: str, when: str):
    ex_now = time.time()
    market_handle = mcal.get_calendar(market.upper())
    print(f"50 {time.time() - ex_now}")
    ex_now = time.time()
    try:
        when_obj = datetime.datetime.fromisoformat(when)
    except Exception as e:
        raise HTTPException(str(e))

    when_obj_tz = when_obj.astimezone(pytz.utc)
    open_window = when_obj_tz - relativedelta(days=4)
    market_hours = market_handle.schedule(start_date=open_window, end_date=when_obj_tz)
    print(f"51 {time.time() - ex_now}")
    ex_now = time.time()
    return market_hours.loc[market_hours.market_open < when_obj].iloc[-1]
    # return market_hours.iloc[-1]


def _market_open_at(market: str, open_at: str):
    ex_now = time.time()
    market_handle = mcal.get_calendar(market.upper())
    print(f"20 {time.time() - ex_now}")
    ex_now = time.time()

    try:
        open_at_obj = datetime.datetime.fromisoformat(open_at)
        print(f"21 {time.time() - ex_now}")
        ex_now = time.time()
    except Exception as e:
        raise HTTPException(str(e))

    open_at_market_tz = open_at_obj.astimezone(pytz.utc)
    market_hours = market_handle.schedule(
        start_date=open_at_market_tz, end_date=open_at_market_tz
    )
    print(f"22 {time.time() - ex_now}")
    ex_now = time.time()

    if len(market_hours) == 0:
        return False

    if open_at_market_tz < market_hours.market_open[0]:
        return False

    if open_at_market_tz > market_hours.market_close[0]:
        return False

    return True


def _make_clock(clock_id: BdaClock = None):
    # first up work out what the current time is meant to be
    if clock_id == "realtime" or not clock_id:
        now = datetime.datetime.now().astimezone()
    else:
        clock = BdaClock(clock_id)
        now = clock.now


@app.get("/symbol/", response_model=schemas.Intervals)
def list_intervals():
    intervals = [
        schemas.Interval(interval=60, interval_name="1m", max_history_days=6),
        schemas.Interval(interval=300, interval_name="5m", max_history_days=59),
        schemas.Interval(interval=3600, interval_name="1h", max_history_days=500),
        schemas.Interval(interval=86400, interval_name="1d", max_history_days=2000),
    ]

    return intervals


@app.get("/{market}/hours", response_model=schemas.Symbol)
def get_market_hours(market: str, clock_id=None):
    # first up work out what the current time is meant to be
    if clock_id:
        now = BdaClock(clock_id).now
    else:
        now = datetime.datetime.now().astimezone()

    open = _market_open_at(market=market, open_at=str(now))
    return {"market": market, "is_open": open}


@app.get("/{market}/symbol/{symbol}/ohlc/{interval}", response_model=dict)
def get_data(
    market: str, interval: str, symbol: str, start="max", end="max", clock_id=None
):
    ex_now = time.time()
    # get interval settings like max range
    interval_settings = _get_interval_settings(interval)
    print(f"1  {time.time() - ex_now}")
    ex_now = time.time()
    # now we have some data - check if we have all the data
    try:
        start_date = validate_date(
            interval_settings=interval_settings, start_date=start, market=market
        )
        print(f"2  {time.time() - ex_now}")
        ex_now = time.time()
        end_date = validate_date(
            interval_settings=interval_settings, end_date=end, market=market
        )
        print(f"3  {time.time() - ex_now}")
        ex_now = time.time()

    except Exception as e:
        raise

    if start_date > end_date:
        raise exceptions.EndBeforeStartError

    # first up work out what the current time is meant to be
    if clock_id:
        print(f"4  {time.time() - ex_now}")
        ex_now = time.time()
        now = BdaClock(clock_id).now
    else:
        now = datetime.datetime.now().astimezone()
        print(f"5  {time.time() - ex_now}")
        ex_now = time.time()

    # now see if we have any of this data already cached in redis
    redis_key = f"ohlc:{interval_settings.interval_name}:{symbol}"
    print(f"6  {time.time() - ex_now}")
    ex_now = time.time()
    print(f"7  {time.time() - ex_now}")
    ex_now = time.time()
    redis_cached_data = redis.get(redis_key)
    print(f"8  {time.time() - ex_now}")
    ex_now = time.time()
    if redis_cached_data:
        # cast redis to dataframe
        df = csv_to_df(redis_cached_data)
        print(f"9  {time.time() - ex_now}")
        ex_now = time.time()
    else:
        # if not, see if we have it in s3
        df = _get_s3_data(interval_settings.interval_name, symbol)
        print(f"10 {time.time() - ex_now}")
        ex_now = time.time()

    # df will either be None (if not cached in redis or S3) or a dataframe with the data we have
    # if start is not in data
    #    and if start is within fetchable len
    #         then yes
    #    else if retrievable len is beyond start of stored data (so you can at least get some)
    #         then yes
    #    else
    #         then no
    # else no
    # if end is not in data
    #    and if end is within fetchable len
    #         then yes
    #    else if retrievable len is beyond the end of stored data
    #         then yes
    #    else
    #         then no
    fetch_data = False
    if df is None:
        fetch_data = True
    elif start_date not in df.index:
        max_start = datetime.datetime.now().astimezone() - relativedelta(
            days=interval_settings.max_history_days
        )
        max_start = validate_date(
            interval_settings=interval_settings,
            market=market,
            start_date=max_start.isoformat(),
        )
        print(f"11 {time.time() - ex_now}")
        ex_now = time.time()
        if start_date >= max_start:
            fetch_data = True
        elif max_start < df.index.min():
            fetch_data = True

    latest_record = _snap_to_interval(now.isoformat(), interval_settings, market=market)
    print(f"12 {time.time() - ex_now}")
    ex_now = time.time()
    if latest_record > df.index.max():
        # there is more to get
        fetch_data = True

    # TODO merge bars - some logic about handling gaps
    if fetch_data:
        # grab data from yf, put it in s3, load it in to redis
        fetched = SymbolData(yf_symbol=symbol, interval=interval_settings.interval_name)
        print(f"13 {time.time() - ex_now}")
        ex_now = time.time()
        csv = fetched.bars.to_csv()
        print(f"14 {time.time() - ex_now}")
        ex_now = time.time()
        s3_path = config.data_path_template.substitute(
            interval=interval_settings.interval_name, symbol=symbol
        )
        s3.put(path=s3_path, value=csv)
        print(f"15 {time.time() - ex_now}")
        ex_now = time.time()
        redis.set(redis_key, csv)
        print(f"16 {time.time() - ex_now}")
        ex_now = time.time()

    # trim down to just what was requested
    df_trim = df.loc[(df.index >= start_date) & (df.index <= end_date)]
    df_str = df_trim.to_json(date_format="iso")
    df_json = json.loads(df_str)
    print(f"17 {time.time() - ex_now}")
    ex_now = time.time()
    return df_json


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
