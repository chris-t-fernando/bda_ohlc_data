from fastapi import FastAPI
import uvicorn
import json
from parameter_store import S3
from parameter_store import exceptions as p_exceptions
import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from bda_clock import BdaClock

# from symbol import Symbol, SymbolData, InvalidQuantity, InvalidPrice


# from . import crud, deps, models, schemas, security
# from app
import sys

if "unittest" in sys.modules.keys():
    from . import schemas, config, exceptions
else:
    import schemas, config, exceptions


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
    from io import StringIO

    csv = StringIO(csv_str)
    df = pd.read_csv(csv, sep=",", index_col=0, parse_dates=True)
    return df


def validate_date(date, interval_settings: schemas.Interval):
    # get the max allowable history first

    if date == "max":
        # default, so go back as far as we can from the real today
        real_now = datetime.datetime.now().astimezone()
        date_obj = real_now - relativedelta(days=interval_settings.max_history_days)
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

    return date_obj


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


def _get_cached_data(interval_str: int, symbol: str):
    path = config.data_path_template.substitute(interval=interval_str, symbol=symbol)
    try:
        existing_data = s3.get(path)
    except p_exceptions.NoSuchKeyException as e:
        existing_data = s3.get(config.df_template)

    # do something smart with imported data to validate it
    df = csv_to_df(existing_data)
    return df


@app.get("/symbol/", response_model=schemas.Intervals)
def list_intervals():
    intervals = [
        schemas.Interval(interval=60, interval_name="1m", max_history_days=6),
        schemas.Interval(interval=300, interval_name="5m", max_history_days=59),
        schemas.Interval(interval=3600, interval_name="1h", max_history_days=500),
        schemas.Interval(interval=86400, interval_name="1d", max_history_days=2000),
    ]

    return intervals


@app.get("/symbol/{interval}/{symbol}", response_model=schemas.Intervals)
def get_data(interval: str, symbol: str, start="max", end="max", clock_id="realtime"):
    # get interval settings like max range
    interval_settings = _get_interval_settings(interval)

    # now we have some data - check if we have all the data
    try:
        start_date = validate_date(start, interval_settings)
        end_date = validate_date(end, interval_settings)
    except Exception as e:
        raise

    if start_date > end_date:
        raise exceptions.EndBeforeStartError

    # first up work out what the current time is meant to be
    if clock_id != "realtime":
        clock = BdaClock(clock_id)
        now = clock.now
    else:
        now = datetime.datetime.now().astimezone()

    # now cast the start and end to datetimes and do basic validation of them as parameters
    try:
        start_date = validate_date(start, interval_settings)
    except Exception as e:
        raise

    # now see if we have any of this data already cached
    df = _get_cached_data(interval_settings.interval_name, symbol)

    # can we expect more data?
    last = df.index.max()
    next = last + relativedelta(seconds=interval_settings.interval)
    if next < now:
        # there is more data
        ...
        # TODO need to hook this up to redis so that there is statefulness of the data

    print("ba")

    zz = s3.put("abc", "abc")
    return


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
