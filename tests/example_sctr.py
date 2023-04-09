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
    # "ABC.AX",
    "ABP.AX",
    "AFI.AX",
    "AGL.AX",
    "AIA.AX",
    "ALD.AX",
    "ALL.AX",
    "ALQ.AX",
    "ALU.AX",
    "ALX.AX",
    # "AMC.AX",
    "AMP.AX",
    # "ANN.AX",
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

data = fetch_price_data(symbols)
end_date = data[symbols[0]].iloc[-1].name
start_date = end_date - relativedelta(days=250)

sctr_df = pd.DataFrame(columns=["datetime", "symbol", "IND_SCORE"])

# while assess_date <= end_date:
for s, price_df in data.items():
    price_df = calculate_indicators(price_df)
    price_df = calculate_weights(price_df)
    price_df = calculate_sctr(price_df)

    start_idx = price_df.index.get_indexer([start_date], method="nearest")[0]
    end_idx = price_df.index.get_indexer([end_date], method="nearest")[0]

    for idx in range(start_idx, end_idx):
        assess_date = price_df.index[idx]
        row = pd.DataFrame(
            {
                "datetime": assess_date,
                "symbol": [s],
                "IND_SCORE": [price_df["IND_SCORE"].loc[assess_date]],
                "rank": -1,
            }
        )
        sctr_df = pd.concat([sctr_df, row], axis=0, ignore_index=True)


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
velocity = ranks.loc[slice(None), idx[:, "rank"]].rank(
    axis=1, ascending=False
).shift() - ranks.loc[slice(None), idx[:, "rank"]].rank(axis=1, ascending=False)
velocity = velocity.rename(columns={"rank": "velocity"})

one_day_increase = (
    ndf.loc[slice(None), idx[:, "close"]]
    - ndf.loc[slice(None), idx[:, "close"]].shift()
    > 0
)
one_day_increase = one_day_increase.rename(columns={"close": "one_day_increase"})
two_day_increase = (
    ndf.loc[slice(None), idx[:, "close"]].shift()
    - ndf.loc[slice(None), idx[:, "close"]].shift(2)
    > 0
)
two_day_increase = two_day_increase.rename(columns={"close": "two_day_increase"})


nndf = ndf.join(ranks)
nndf = nndf.join(velocity)
nndf = nndf.join(one_day_increase)
nndf = nndf.join(two_day_increase)

consecutive_increase = np.logical_and(
    nndf.loc[:, idx[:, "one_day_increase"]], nndf.loc[:, idx[:, "two_day_increase"]]
)
consecutive_increase = consecutive_increase.rename(
    columns={"one_day_increase": "consecutive_increase"}
)
nndf = nndf.join(consecutive_increase)

nndf = nndf.reindex(sorted(nndf.columns), axis=1)
nndf = nndf.dropna()
nndf.to_csv("nndf.csv")
