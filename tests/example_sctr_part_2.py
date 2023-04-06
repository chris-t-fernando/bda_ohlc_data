from persistent_ohlc_client import PersistentOhlcClient
import numpy as np
import pandas_ta as ta
from ta.momentum import PercentagePriceOscillator
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd
from monkey_back import BackTestAPI

from abc import ABC, abstractmethod
from typing import Set
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

client = PersistentOhlcClient()
interval = "1d"


class TimeManagerNotStartedError(Exception):
    ...


class ITimeManager(ABC):
    back_test: bool = False

    @abstractmethod
    def __init__(self, interval: int = 300) -> None:
        ...

    # @abstractmethod
    def add_symbol(self, symbol: str) -> bool:
        ...

    @abstractmethod
    def add_symbols(self, symbols: set) -> bool:
        ...

    @abstractmethod
    def start(self):
        ...

    @property
    @abstractmethod
    def first(self):
        ...

    @property
    @abstractmethod
    def last(self):
        ...

    @property
    @abstractmethod
    def now(self):
        ...

    @now.setter
    @abstractmethod
    def now(self, new_date):
        ...

    @abstractmethod
    def tick(self):
        ...


class BackTestTimeManager(ITimeManager):
    _symbols: Set
    _date: pd.Timestamp
    tick_padding: int = 90
    now: pd.Timestamp
    first: pd.Timestamp
    _cached_first: pd.Timestamp
    _cached_first_valid = False
    last: pd.Timestamp
    _cached_last: pd.Timestamp
    _cached_last_valid: False
    tick_ttl: int
    back_test: bool = True

    def __init__(self, interval: str = "5m") -> None:
        self._symbols = set()
        self._date = None
        self._interval_name = interval
        if interval == "1d":
            self._interval = 60 * 60 * 24
        elif interval == "5m":
            self._interval = 300
        else:
            raise RuntimeError

        self._delta = relativedelta(seconds=self._interval)

        self._symbols = set()

    def start(self):
        self.now = self.first

    def add_symbols(self, symbols: Set[str]) -> bool:
        symbol_set = set(symbols)
        self._symbols = self._symbols | symbol_set
        self._cached_first_valid = False
        self._cached_last_valid = False

    @property
    def first(self) -> pd.Timestamp:
        if self._cached_first_valid:
            return self._cached_first

        if len(self._symbols) == 0:
            raise RuntimeError("No symbols added yet")

        earliest = None
        for symbol in self._symbols:
            this_date = client.get_ohlc(symbol, interval, count=-10).index[0]
            if not earliest:
                earliest = this_date
            elif this_date > earliest:
                earliest = this_date

        # earliest is now the LATEST first record
        # pad it out for SMA etc
        padding = self._interval * 100
        padded_earliest = earliest + relativedelta(seconds=padding)

        self._cached_first = padded_earliest
        self._cached_first_valid = True
        return padded_earliest

    @property
    def last(self) -> pd.Timestamp:
        if self._cached_last_valid:
            return self._cached_last

        if len(self._symbols) == 0:
            raise RuntimeError("No symbols added yet")

        latest = None
        for symbol in self._symbols:
            this_date = client.get_ohlc(symbol, interval, count=10).index[-1]

            if not latest:
                latest = this_date
            elif this_date < latest:
                latest = this_date

        self._cached_last = latest
        self._cached_last_valid = True
        return latest

    @property
    def now(self) -> pd.Timestamp:
        if not self._date:
            # hackity hack
            # self._date = next(iter(self._symbols)).ohlc.bars.iloc[-1].name
            raise TimeManagerNotStartedError(
                f"Current date not set. Have you called start() yet?"
            )

        return self._date

    @now.setter
    def now(self, new_date: pd.Timestamp) -> None:
        if new_date < self.first:
            raise KeyError(
                f"New date {new_date} is earlier than earliest date {self.first}"
            )
        if new_date > self.last:
            raise KeyError(f"New date {new_date} is after latest date {self.last}")

        self._date = new_date

    def tick(self) -> pd.Timestamp:
        # TODO raise exception or something if we try to tick in to the future
        self.now = self.now + self._delta
        return self.now

    @property
    def tick_ttl(self) -> int:
        if self.now == self.latest:
            raise KeyError(f"Backtesting - already at last row")

        return 0


symbols = [
    "BTC-USD",
    "AVAX-USD",
    "ETH-USD",
    "DOT-USD",
    "MATIC-USD",
    "AAVE-USD",
    "CRV-USD",
    "XRP-USD",
    "MKR-USD",
    "DOGE-USD",
    "BNB-USD",
    "ATOM-USD",
    "SHIB-USD",
]
symbols = [
    "ABNB",
    "ADBE",
    "ADI",
    "ADP",
    "ADSK",
    "AEP",
    "ALGN",
    "AMAT",
    "AMD",
    "AMGN",
    "ANSS",
    "ASML",
    "ATVI",
    "AVGO",
    "AZN",
    "BIIB",
    "BKNG",
    "BKR",
    "CDNS",
    "CEG",
    "CHTR",
    "CMCSA",
    "COST",
    "CPRT",
    "CRWD",
    "CSCO",
    "CSGP",
    "CSX",
    "CTAS",
    "CTSH",
    "DDOG",
    "DLTR",
    "DXCM",
    "EA",
    "EBAY",
    "ENPH",
    "EXC",
    "FANG",
    "FAST",
    "FISV",
    "FTNT",
    "GFS",
    "GILD",
    "GOOG",
    "GOOGL",
    "HON",
    "IDXX",
    "ILMN",
    "INTC",
    "INTU",
    "ISRG",
    "JD",
    "KDP",
    "KHC",
    "KLAC",
    "LCID",
    "LRCX",
    "LULU",
    "MAR",
    "MCHP",
    "MDLZ",
    "MELI",
    "META",
    "MNST",
    "MRNA",
    "MRVL",
    "MSFT",
    "MU",
    "NFLX",
    "NVDA",
    "NXPI",
    "ODFL",
    "ORLY",
    "PANW",
    "PAYX",
    "PCAR",
    "PDD",
    "PEP",
    "PYPL",
    "QCOM",
    "REGN",
    "RIVN",
    "ROST",
    "SBUX",
    "SGEN",
    "SIRI",
    "SNPS",
    "TEAM",
    "TMUS",
    "TXN",
    "VRSK",
    "VRTX",
    "WBA",
    "WBD",
    "WDAY",
    "XEL",
    "ZM",
    "ZS",
]
for s in symbols:
    client.get_ohlc(s, interval="1d")

idx = pd.IndexSlice
buy_value = 2000
tm = BackTestTimeManager(interval="1d")
tm.add_symbols(symbols)

broker = BackTestAPI(
    time_manager=tm, back_testing=True, buy_metric="Close", sell_metric="High"
)

sctr_data = pd.read_csv(
    "nndf.csv", header=[0, 1], skipinitialspace=True, index_col=0, parse_dates=[0]
)

tm.now = sctr_data.index[1]
top_25 = round(len(symbols) * 0.4)

active_positions = {}
gains = 0
yesterday_top_25 = None

# while tm.now < sctr_data.index[-1]:
for now in sctr_data.index:
    tm.now = now
    yesterday_iloc = sctr_data.index.get_loc(now) - 1
    yesterday = sctr_data.index[yesterday_iloc]
    if yesterday_top_25 is None:
        yesterday_top_25 = (
            sctr_data.loc[yesterday, idx[:, "rank"]]
            .loc[lambda x: x <= top_25]
            .loc[slice(None), "rank"]
        )
    else:
        yesterday_top_25 = today_top_25

    today_top_25 = (
        sctr_data.loc[now, idx[:, "rank"]]
        .loc[lambda x: x <= top_25]
        .loc[slice(None), "rank"]
    )

    yesterday_symbols = set(yesterday_top_25.keys())
    today_symbols = set(today_top_25.keys())

    exited = yesterday_symbols.difference(today_symbols)
    entered = today_symbols.difference(yesterday_symbols)

    positions = broker.list_positions()
    for s in exited:
        if s == "KLAC":
            ...
        for p in positions:
            if p.symbol == s:
                # got one that has exited
                # is it in the money?
                this_close = sctr_data.loc[tm.now, idx[s, "close"]]
                current_value = p.quantity * this_close
                buy_value = active_positions[s].filled_total_value
                itm = True if current_value > buy_value else False

                sell_order = broker.sell_order_market(
                    s, units=active_positions[s].filled_unit_quantity
                )
                updated_order = broker.get_order(sell_order.order_id)

                text = (
                    "WINNER"
                    if buy_value < updated_order.filled_total_value
                    else "LOSER"
                )
                diff = updated_order.filled_total_value - buy_value
                gains += diff
                print(
                    f"{tm.now} {s} {text} by {diff:.2f} - bought for {buy_value:.2f} and exited for {updated_order.filled_total_value:.2f} Gains stand at {gains:.2f}"
                )
                ...

    # process sells first
    highest_ranked = None
    for s in entered:
        if not highest_ranked:
            highest_ranked = s
        else:
            if today_top_25[s] < today_top_25[highest_ranked]:
                highest_ranked = s

    if highest_ranked:
        this_close = sctr_data.loc[tm.now, idx[highest_ranked, "close"]]
        buy_vol = round(buy_value / this_close)
        buy_vol = 1 if buy_vol == 0 else buy_vol
        active_positions[highest_ranked] = broker.buy_order_market(
            highest_ranked, units=buy_vol
        )
        pos = today_top_25[highest_ranked]
        print(f"{tm.now} {highest_ranked} Bought in position {pos:.0f}")

    # process sells now

...
