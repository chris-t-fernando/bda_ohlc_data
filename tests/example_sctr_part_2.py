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


crytpo_symbols = [
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
    "ADA-USD",
    "SOL-USD",
    "LTC-USD",
    "TRX-USD",
    "DAI-USD",
    "WBTC-USD",
    "LINK-USD",
    "LEO-USD",
    "XMR-USD",
    "ETC-USD",
    "TON-USD",
    "OKB-USD",
]

nyse_symbols = [
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

asx_symbols = [
    "A2M.AX",
    "AAA.AX",
    "ABC.AX",
    "ABP.AX",
    "AFI.AX",
    "AGL.AX",
    "AIA.AX",
    "ALD.AX",
    "ALL.AX",
    "ALQ.AX",
    "ALU.AX",
    "ALX.AX",
    "AMC.AX",
    "AMP.AX",
    "ANN.AX",
    "ANZ.AX",
    "APA.AX",
    "APE.AX",
    "APX.AX",
    "ARB.AX",
    "ARG.AX",
    "ASX.AX",
    "AWC.AX",
    "AZJ.AX",
    "BAP.AX",
    "BEN.AX",
    "BGA.AX",
    "BHP.AX",
    "BKW.AX",
    "BLD.AX",
    "BOQ.AX",
    "BPT.AX",
    "BRG.AX",
    "BSL.AX",
    "BWP.AX",
    "BXB.AX",
    "CAR.AX",
    "CBA.AX",
    "CCP.AX",
    "CDA.AX",
    "CGF.AX",
    "CHC.AX",
    "CHN.AX",
    "CIA.AX",
    "CLW.AX",
    "CMW.AX",
    "CNU.AX",
    "COH.AX",
    "COL.AX",
    "CPU.AX",
    "CQR.AX",
    "CSL.AX",
    "CSR.AX",
    "CTD.AX",
    "CWY.AX",
    "DEG.AX",
    "DHG.AX",
    "DMP.AX",
    "DOW.AX",
    "DRR.AX",
    "DXS.AX",
    "EBO.AX",
    "ELD.AX",
    "EML.AX",
    "EVN.AX",
    "EVT.AX",
    "FBU.AX",
    "FLT.AX",
    "FMG.AX",
    "FPH.AX",
    "GMG.AX",
    "GNE.AX",
    "GOZ.AX",
    "GPT.AX",
    "HLS.AX",
    "HVN.AX",
    "IAG.AX",
    "IEL.AX",
    "IFL.AX",
    "IFT.AX",
    "IGO.AX",
    "ILU.AX",
    "IOO.AX",
    "IOZ.AX",
    "IPL.AX",
    "IRE.AX",
    "IVV.AX",
    "JBH.AX",
    "JHX.AX",
    "LFG.AX",
    "LFS.AX",
    "LLC.AX",
    "LNK.AX",
    "LYC.AX",
    "MEZ.AX",
    "MFG.AX",
    "MGF.AX",
    "MGOC.AX",
    "MGR.AX",
    "MIN.AX",
    "MP1.AX",
    "MPL.AX",
    "MQG.AX",
    "MTS.AX",
    "NAB.AX",
    "NCM.AX",
    "NEC.AX",
    "NHF.AX",
    "NIC.AX",
    "NSR.AX",
    "NST.AX",
    "NUF.AX",
    "NWL.AX",
    "NXT.AX",
    "ORA.AX",
    "ORG.AX",
    "ORI.AX",
    "OZL.AX",
    "PBH.AX",
    "PLS.AX",
    "PME.AX",
    "PMGOLD.AX",
    "PMV.AX",
    "PNI.AX",
    "PNV.AX",
    "PPT.AX",
    "PTM.AX",
    "QAN.AX",
    "QBE.AX",
    "QUB.AX",
    "REA.AX",
    "REH.AX",
    "RHC.AX",
    "RIO.AX",
    "RMD.AX",
    "RRL.AX",
    "RWC.AX",
    "S32.AX",
    "SCG.AX",
    "SDF.AX",
    "SEK.AX",
    "SGM.AX",
    "SGP.AX",
    "SGR.AX",
    "SHL.AX",
    "SKC.AX",
    "SNZ.AX",
    "SOL.AX",
    "SPK.AX",
    "STO.AX",
    "STW.AX",
    "SUL.AX",
    "SUN.AX",
    "SVW.AX",
    "TAH.AX",
    "TCL.AX",
    "TLS.AX",
    "TNE.AX",
    "TPG.AX",
    "TWE.AX",
    "TYR.AX",
    "VAP.AX",
    "VAS.AX",
    "VCX.AX",
    "VEA.AX",
    "VEU.AX",
    "VGS.AX",
    "VTS.AX",
    "VUK.AX",
    "WAM.AX",
    "WBC.AX",
    "WEB.AX",
    "WES.AX",
    "WOR.AX",
    "WOW.AX",
    "WPR.AX",
    "WTC.AX",
    "XRO.AX",
    "YAL.AX",
    "ZIM.AX",
]

symbols = asx_symbols

buy_model = "highest"
winner_count = 0
loser_count = 0
for s in symbols:
    client.get_ohlc(s, interval="1d")

idx = pd.IndexSlice
buy_value = 2000
tm = BackTestTimeManager(interval="1d")
tm.add_symbols(symbols)

broker = BackTestAPI(
    time_manager=tm, back_testing=True, buy_metric="Close", sell_metric="Close"
)

sctr_data = pd.read_csv(
    "nndf.csv", header=[0, 1], skipinitialspace=True, index_col=0, parse_dates=[0]
)

tm.now = sctr_data.index[1]
top_25 = round(len(symbols) * 0.4)

active_positions = {}
total_gains = 0
yesterday_top_25 = None

# while tm.now < sctr_data.index[-1]:
for now in sctr_data.index:
    tm.now = now
    yesterday_iloc = sctr_data.index.get_loc(now) - 1
    if yesterday_iloc == -1:
        continue
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
                sell_order = broker.get_order(sell_order.order_id)

                text = (
                    "WINNER" if buy_value < sell_order.filled_total_value else "LOSER"
                )
                if buy_value < sell_order.filled_total_value:
                    winner_count += 1
                else:
                    loser_count += 1

                diff = sell_order.filled_total_value - buy_value
                total_gains += diff

                bought_on = active_positions[s].create_time
                held_days = (now - bought_on).days
                pct = (sell_order.filled_total_value / buy_value) * 100 - 100
                pct_win = winner_count / (winner_count + loser_count) * 100

                print(
                    f"{tm.now} {s} {text} ${diff=:.2f} or {pct:.1f}% {buy_value=:.2f} {bought_on=} {held_days=} {sell_order.filled_total_value=:.2f} {total_gains=:.2f} {winner_count=} {loser_count=} {pct_win=:.0f}%"
                )
                ...

    if buy_model == "highest":
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

    if buy_model == "velocity":
        if len(entered) == 0:
            continue
        velocity = sctr_data.loc[now, idx[list(entered), "velocity"]]
        fastest_symbol = velocity.idxmin()[0]

        this_close = sctr_data.loc[tm.now, idx[fastest_symbol, "close"]]
        buy_vol = round(buy_value / this_close)
        buy_vol = 1 if buy_vol == 0 else buy_vol
        active_positions[fastest_symbol] = broker.buy_order_market(
            fastest_symbol, units=buy_vol
        )
        velocity = sctr_data.loc[now, idx[fastest_symbol, "velocity"]]

        print(f"{tm.now} {fastest_symbol} Bought in {velocity=:.0f}")

        """
        found = False
        fastest_ranked = None
        # there's so a better way to do this but whatever
        symbol_list = (
            sctr_data.loc[yesterday, idx[:, "rank"]]
            .loc[lambda x: x <= top_25]
            .loc[slice(None), "rank"]
        ).keys()
        for tmp_symbol in symbol_list:
            sctr_data[idx[s, "velocy"]] = (
                sctr_data[idx[tmp_symbol, "rank"]].shift()
                - sctr_data[idx[tmp_symbol, "rank"]]
            )

        for s in entered:
            if not fastest_ranked:
                fastest_ranked = s
            else:
                for key, vel in velocity.items():
                    position = sctr_data.loc[
                        now, idx[slice(None), "rank"]
                    ].sort_values()[key]
                    if position <= top_25:
                        print("found")
                        this_close = sctr_data.loc[tm.now, idx[key, "close"]]
                        buy_vol = round(buy_value / this_close[0])
                        buy_vol = 1 if buy_vol == 0 else buy_vol
                        active_positions[key[0]] = broker.buy_order_market(
                            key[0], units=buy_vol
                        )
                        pos = today_top_25[key[0]]
                        yday_pos = sctr_data.loc[
                            yesterday, idx[slice(None), "rank"]
                        ].sort_values()[key]
                        this_vel = yday_pos - pos

                        print(
                            f"{tm.now} {key[0]} Bought in. Velocity {this_vel} today pos {pos:.0f}, yday pos {yday_pos}"
                        )
                        found = True
                        break
            if found:
                break
        """
    if buy_model == "3up":
        # if its entered
        # if its been on 2 up
        # the fastest of these
        increase = sctr_data.loc[now, idx[list(entered), "consecutive_increase"]].loc[
            lambda x: x == True
        ]
        if len(increase):
            increase_keys = list(
                increase.loc[slice(None), "consecutive_increase"].keys()
            )

            velocity = (
                sctr_data.loc[now, idx[list(entered), "velocity"]]
                .sort_values()
                .loc[increase_keys]
            )
            buy_symbol = velocity.idxmin()[0]
            this_close = sctr_data.loc[now, idx[buy_symbol, "close"]]
            buy_vol = round(buy_value / this_close)
            buy_vol = 1 if buy_vol == 0 else buy_vol
            active_positions[buy_symbol] = broker.buy_order_market(
                buy_symbol, units=buy_vol
            )
            this_pos = sctr_data.loc[now, idx[buy_symbol, "rank"]]

            print(
                f"{tm.now} {buy_symbol} Bought in at {this_pos=:.0f} {velocity.min()=:.0f}"
            )

...
