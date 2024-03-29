import requests
from urllib.parse import quote_plus
import pandas as pd
from datetime import datetime
from string import Template
import pandas as pd
import json
from dateutil.relativedelta import relativedelta


class BacktestClock:
    _now: datetime
    interval: int
    start: datetime

    def __init__(self, now: datetime, interval: int = 300):
        self.interval = interval
        self.now = now
        self.start = now

    def reset(self):
        self.now = self.start

    def tick(self):
        delta = relativedelta(seconds=self.interval)
        self.now = self.now + delta

    @property
    def now(self):
        return self._now

    @now.setter
    def now(self, new_now):
        self._now = new_now


class PersistentOhlcClient:
    http_endpoint: str
    ohlc_path: str = "/symbols/${symbol}/ohlc/${interval}?${start}&${end}"
    symbol_tick: str = "/symbols/${symbol}/info/${interval}/next_tick?${when}"
    clock = None

    def __init__(
        self,
        http_endpoint: str = "http://127.0.0.1:8002",
        clock=None,
        default_interval="5m",
    ):
        self.http_endpoint = http_endpoint
        self.template_ohlc_path = Template(self.ohlc_path)
        self.template_symbol_tick = Template(self.symbol_tick)
        self.clock = clock
        self.default_interval = default_interval

    def get_ohlc(
        self,
        symbol: str,
        interval: str = None,
        start: datetime = None,
        end: datetime = None,
        count: int = None,
        ta: list = None,
    ):
        if not interval:
            interval = self.default_interval

        start = f"start={quote_plus(start)}" if start is not None else ""
        end = f"end={quote_plus(end)}" if end is not None else ""

        actual_path = self.template_ohlc_path.substitute(
            symbol=symbol, interval=interval, start=start, end=end
        )

        algos = ""
        if ta:
            # because i am lazy
            if type(ta) != list:
                ta = [ta]

            for algo in ta:
                algos += f"&ta={algo}"

        count = f"&count={count}" if count is not None else ""

        url = f"{self.http_endpoint}{actual_path}{algos}{count}"
        try:
            raw_response = requests.get(url)
            raw_response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise

        in_df = pd.read_json(raw_response.text)

        if self.clock:
            in_df = in_df.loc[in_df.index <= self.clock.now]

        return in_df

    def get_tick(self, symbol: str, interval: str = None, when: datetime = None):
        if not interval:
            interval = self.default_interval

        if when:
            when_obj = datetime.fromisoformat(when)
            if when_obj > self.clock.now:
                when = str(self.clock.now)

            when = f"when={quote_plus(when)}"

        else:
            when = ""

        actual_path = self.template_symbol_tick.substitute(
            symbol=symbol, interval=interval, when=when
        )
        url = f"{self.http_endpoint}{actual_path}"
        raw_response = requests.get(url)
        json_response = json.loads(raw_response.text)
        return json_response


if __name__ == "__main__":
    clock = BacktestClock(datetime.fromisoformat("2023-03-21 01:04:31.850056+11:00"))
    c = PersistentOhlcClient(clock=clock)
    # z = c.get_ohlc("BTC-USD", "5m")
    y = c.get_ohlc(
        "BTC-USD",
        "5m",
        start="2023-03-24 01:04:31.850056+11:00",
        end="2023-03-24 06:04:31.850056+11:00",
        ta=["MacdTA", "abc"],
    )
    a = c.get_tick("BTC-USD", "5m")
    b = c.get_tick(
        "BTC-USD",
        "5m",
        when="2023-03-24 06:04:31.850056+11:00",
    )
    clock.tick()
    b = c.get_tick(
        "BTC-USD",
        "5m",
        when="2023-03-24 06:04:31.850056+11:00",
    )
    print("abc")
