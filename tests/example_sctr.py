from persistent_ohlc_client import PersistentOhlcClient
import numpy as np
import pandas_ta as ta
from ta.momentum import PercentagePriceOscillator
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pandas as pd


def calculate_trend(values):
    if len(values) == 0:
        return 0, 0

    x = np.arange(1, len(values) + 1, 1)
    y = np.array(values)
    #  Handle nan values
    x_new = x[~np.isnan(y)]
    y_new = y[~np.isnan(y)]
    m, c = np.polyfit(x_new, y_new, 1)
    return m, c


def calculate_ppo_hist_slope(df):
    df["PPO_HIST_SLOPE"] = 0

    for i, row in df.iterrows():
        # for index in range(0, len(df)):
        index = df.index.get_loc(i)
        if index <= 2:
            continue
        ppo_hist_lb = df["PPO_HIST"].values[index - 3 : index]
        check_nan = np.isnan(ppo_hist_lb)
        if True in check_nan:
            continue
        m, c = calculate_trend(ppo_hist_lb)
        i = df.index[index]
        df.at[i, "PPO_HIST_SLOPE"] = m

    # my func
    # df.A - df.A.shift(1)
    # use apply for calc trend using shift to get last 3

    return df


def calculate_indicators(df):
    #  Long-term
    df["EMA_200"] = ta.ema(df["Close"], length=200)
    df["EMA_200_CLOSE_PC"] = (df["Close"] / df["EMA_200"]) * 100
    df["ROC_125"] = ta.momentum.roc(close=df["Close"], window=125)
    #  Mid-term
    df["EMA_50"] = ta.ema(df["Close"], length=50)
    df["EMA_50_CLOSE_PC"] = (df["Close"] / df["EMA_50"]) * 100
    df["ROC_20"] = ta.momentum.roc(close=df["Close"], window=20)
    # Short-term
    ppo_ind = PercentagePriceOscillator(
        close=df["Close"], window_slow=26, window_fast=12, window_sign=9
    )
    df["PPO"] = ppo_ind.ppo()
    df["PPO_EMA_9"] = ta.ema(df["PPO"], length=9)
    df["PPO_HIST"] = df["Close"] - df["PPO_EMA_9"]
    #  Calculate PPO histogram slope
    df = calculate_ppo_hist_slope(df)
    df["RSI"] = ta.momentum.rsi(df["Close"], window=14)
    return df


def calculate_weights(df):
    #  Long-term
    df["EMA_200_CLOSE_PC_WEIGHTED"] = df["EMA_200_CLOSE_PC"] * 0.3
    df["ROC_125_WEIGHTED"] = df["ROC_125"] * 0.3
    #  Mid-term
    df["EMA_50_CLOSE_PC_WEIGHTED"] = df["EMA_50_CLOSE_PC"] * 0.15
    df["ROC_20_WEIGHTED"] = df["ROC_20"] * 0.15
    #  Short-term
    df["RSI_WEIGHTED"] = df["RSI"] * 0.05
    df["PPO_HIST_SLOPE_WEIGHTED"] = 0
    df.loc[df["PPO_HIST_SLOPE"] < -1, "PPO_HIST_SLOPE_WEIGHTED"] = 0
    df.loc[df["PPO_HIST_SLOPE"] >= -1, "PPO_HIST_SLOPE_WEIGHTED"] = (
        (df["PPO_HIST_SLOPE"] + 1) * 50 * 0.05
    )
    df.loc[df["PPO_HIST_SLOPE"] > 1, "PPO_HIST_SLOPE_WEIGHTED"] = 5
    return df


def calculate_sctr(df):
    df["IND_SCORE"] = (
        df["EMA_200_CLOSE_PC_WEIGHTED"]
        + df["ROC_125_WEIGHTED"]
        + df["EMA_50_CLOSE_PC_WEIGHTED"]
        + df["ROC_20_WEIGHTED"]
        + df["RSI_WEIGHTED"]
        + df["PPO_HIST_SLOPE_WEIGHTED"]
    )
    return df


def fetch_price_data(symbols: str):
    return_data = {}
    for symbol in symbols:
        return_data[symbol] = c.get_ohlc(
            symbol, interval="1d"
        )  # SymbolData(symbol, "1d").bars
    return return_data


c = PersistentOhlcClient()

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

data = fetch_price_data(symbols)
end_date = data[symbols[0]].iloc[-1].name
start_date = end_date - relativedelta(days=250)
assess_date = start_date

sctr_df = pd.DataFrame(columns=["datetime", "symbol", "IND_SCORE"])

# while assess_date <= end_date:
for s, price_df in data.items():
    price_df = calculate_indicators(price_df)
    price_df = calculate_weights(price_df)
    price_df = calculate_sctr(price_df)

    while assess_date <= end_date:
        if assess_date in price_df.index:
            # print(f"{assess_date}")
            row = pd.DataFrame(
                {
                    "datetime": assess_date,
                    "symbol": [s],
                    "IND_SCORE": [price_df["IND_SCORE"].loc[assess_date]],
                    "rank": -1,
                }
            )
            sctr_df = pd.concat([sctr_df, row], axis=0, ignore_index=True)
        else:
            wd = assess_date.weekday()
            if wd != 5 and wd != 6:
                ...
        # can't do it this way because of goddamn daylight savings
        # assess_date += relativedelta(days=1)
        this_iloc = price_df.index.get_loc(assess_date)
        next_iloc = this_iloc + 1
        try:
            assess_date = price_df.index[next_iloc]
        except:
            if assess_date + relativedelta(days=1) > end_date:
                break
            continue
    assess_date = start_date

col_index = pd.MultiIndex.from_product(
    [symbols, ["sctr"]], names=["symbol", "sctr_col"]
)

# create the data frame structure
ndf = pd.DataFrame(index=sctr_df.datetime.unique(), columns=col_index)

# populate the data frame structure - there's 100% a better way than this but whatevs
for s in sctr_df.symbol.unique():
    ndf.loc[:, (s, "sctr")] = sctr_df["IND_SCORE"].loc[sctr_df.symbol == s].tolist()
    ndf.loc[:, (s, "close")] = data[s].Close[ndf.index]

# used for selecting 2nd level columns
idx = pd.IndexSlice
ranks = ndf.loc[slice(None), idx[:, "sctr"]].rank(axis=1, ascending=False)
ranks = ranks.rename(columns={"sctr": "rank"})
nndf = ndf.join(ranks)
nndf = nndf.reindex(sorted(nndf.columns), axis=1)
nndf = nndf.dropna()
nndf.to_csv("nndf.csv")
