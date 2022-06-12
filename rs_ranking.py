import sys
import pandas as pd
import numpy as np
import json
import os
from datetime import date
from scipy.stats import linregress
import yaml
from rs_data import TD_API, cfg, read_json
from functools import reduce

DIR = os.path.dirname(os.path.realpath(__file__))

pd.set_option('display.max_rows', None)
pd.set_option('display.width', None)
pd.set_option('display.max_columns', None)

try:
    with open('config.yaml', 'r') as stream:
        config = yaml.safe_load(stream)
except FileNotFoundError:
    config = None
except yaml.YAMLError as exc:
        print(exc)

PRICE_DATA = os.path.join(DIR, "data", "price_history.json")
MIN_PERCENTILE = cfg("MIN_PERCENTILE")
POS_COUNT_TARGET = cfg("POSITIONS_COUNT_TARGET")
REFERENCE_TICKER = cfg("REFERENCE_TICKER")
ALL_STOCKS = cfg("USE_ALL_LISTED_STOCKS")
TICKER_INFO_FILE = os.path.join(DIR, "data_persist", "ticker_info.json")
TICKER_INFO_DICT = read_json(TICKER_INFO_FILE)

TITLE_RANK = "Rank"
TITLE_TICKER = "Ticker"
TITLE_TICKERS = "Tickers"
TITLE_SECTOR = "Sector"
TITLE_INDUSTRY = "Industry"
TITLE_UNIVERSE = "Universe" if not ALL_STOCKS else "Exchange"
TITLE_PERCENTILE = "Percentile"
TITLE_1M = "1 Month Ago"
TITLE_3M = "3 Months Ago"
TITLE_6M = "6 Months Ago"
TITLE_RS = "Relative Strength"

if not os.path.exists('output'):
    os.makedirs('output')

def relative_strength(closes: pd.Series, closes_ref: pd.Series):
    rs_stock = strength(closes)
    rs_ref = strength(closes_ref)
    rs = (1 + rs_stock) / (1 + rs_ref) * 100
    rs = int(rs*100) / 100 # round to 2 decimals
    return rs

def strength(closes: pd.Series):
    """Calculates the performance of the last year (most recent quarter is weighted double)"""
    try:
        quarters1 = quarters_perf(closes, 1)
        quarters2 = quarters_perf(closes, 2)
        quarters3 = quarters_perf(closes, 3)
        quarters4 = quarters_perf(closes, 4)
        return 0.4*quarters1 + 0.2*quarters2 + 0.2*quarters3 + 0.2*quarters4
    except:
        return 0

def quarters_perf(closes: pd.Series, n):
    length = min(len(closes), n*int(252/4))
    prices = closes.tail(length)
    pct_chg = prices.pct_change().dropna()
    perf_cum = (pct_chg + 1).cumprod() - 1
    return perf_cum.tail(1).item()


def rankings():
    """Returns a dataframe with percentile rankings for relative strength"""
    json = read_json(PRICE_DATA)
    relative_strengths = []
    ranks = []
    industries = {}
    ind_ranks = []
    stock_rs = {}
    ref = json[REFERENCE_TICKER]
    for ticker in json:
        if not cfg("SP500") and json[ticker]["universe"] == "S&P 500":
            continue
        if not cfg("SP400") and json[ticker]["universe"] == "S&P 400":
            continue
        if not cfg("SP600") and json[ticker]["universe"] == "S&P 600":
            continue
        if not cfg("NQ100") and json[ticker]["universe"] == "Nasdaq 100":
            continue
        try:
            closes = list(map(lambda candle: candle["close"], json[ticker]["candles"]))
            closes_ref = list(map(lambda candle: candle["close"], ref["candles"]))
            industry = TICKER_INFO_DICT[ticker]["info"]["industry"] if json[ticker]["industry"] == "unknown" else json[ticker]["industry"]
            sector = TICKER_INFO_DICT[ticker]["info"]["sector"] if json[ticker]["sector"] == "unknown" else json[ticker]["sector"]
            if len(closes) >= 6*20 and industry != "n/a" and len(industry.strip()) > 0:
                closes_series = pd.Series(closes)
                closes_ref_series = pd.Series(closes_ref)
                rs = relative_strength(closes_series, closes_ref_series)
                month = 20
                tmp_percentile = 100
                rs1m = relative_strength(closes_series.head(-1*month), closes_ref_series.head(-1*month))
                rs3m = relative_strength(closes_series.head(-3*month), closes_ref_series.head(-3*month))
                rs6m = relative_strength(closes_series.head(-6*month), closes_ref_series.head(-6*month))

                # if rs is too big assume there is faulty price data
                if rs < 600:
                    # stocks output
                    ranks.append(len(ranks)+1)
                    relative_strengths.append((0, ticker, sector, industry, json[ticker]["universe"], rs, tmp_percentile, rs1m, rs3m, rs6m))
                    stock_rs[ticker] = rs

                    # industries output
                    if industry not in industries:
                        industries[industry] = {
                            "info": (0, industry, sector, 0, 99, 1, 3, 6),
                            TITLE_RS: [],
                            TITLE_1M: [],
                            TITLE_3M: [],
                            TITLE_6M: [],
                            TITLE_TICKERS: []
                        }
                        ind_ranks.append(len(ind_ranks)+1)
                    industries[industry][TITLE_RS].append(rs)
                    industries[industry][TITLE_1M].append(rs1m)
                    industries[industry][TITLE_3M].append(rs3m)
                    industries[industry][TITLE_6M].append(rs6m)
                    industries[industry][TITLE_TICKERS].append(ticker)
        except KeyError:
            print(f'Ticker {ticker} has corrupted data.')
    dfs = []
    suffix = ''

    # stocks
    df = pd.DataFrame(relative_strengths, columns=[TITLE_RANK, TITLE_TICKER, TITLE_SECTOR, TITLE_INDUSTRY, TITLE_UNIVERSE, TITLE_RS, TITLE_PERCENTILE, TITLE_1M, TITLE_3M, TITLE_6M])
    df[TITLE_PERCENTILE] = pd.qcut(df[TITLE_RS], 100, labels=False, duplicates="drop")
    df[TITLE_1M] = pd.qcut(df[TITLE_1M], 100, labels=False, duplicates="drop")
    df[TITLE_3M] = pd.qcut(df[TITLE_3M], 100, labels=False, duplicates="drop")
    df[TITLE_6M] = pd.qcut(df[TITLE_6M], 100, labels=False, duplicates="drop")
    df = df.sort_values(([TITLE_RS]), ascending=False)
    df[TITLE_RANK] = ranks
    out_tickers_count = 0
    for index, row in df.iterrows():
        if row[TITLE_PERCENTILE] >= MIN_PERCENTILE:
            out_tickers_count = out_tickers_count + 1
    df = df.head(out_tickers_count)

    df.to_csv(os.path.join(DIR, "output", f'rs_stocks{suffix}.csv'), index = False)
    dfs.append(df)

    # industries
    def getDfView(industry_entry):
        return industry_entry["info"]
    def sum(a,b):
        return a+b
    def getRsAverage(industries, industry, column):
        rs = reduce(sum, industries[industry][column])/len(industries[industry][column])
        rs = int(rs*100) / 100 # round to 2 decimals
        return rs
    def rs_for_stock(ticker):
        return stock_rs[ticker]
    def getTickers(industries, industry):
        return ",".join(sorted(industries[industry][TITLE_TICKERS], key=rs_for_stock, reverse=True))

    # remove industries with only one stock
    filtered_industries = filter(lambda i: len(i[TITLE_TICKERS]) > 1, list(industries.values()))
    df_industries = pd.DataFrame(map(getDfView, filtered_industries), columns=[TITLE_RANK, TITLE_INDUSTRY, TITLE_SECTOR, TITLE_RS, TITLE_PERCENTILE, TITLE_1M, TITLE_3M, TITLE_6M])
    df_industries[TITLE_RS] = df_industries.apply(lambda row: getRsAverage(industries, row[TITLE_INDUSTRY], TITLE_RS), axis=1)
    df_industries[TITLE_1M] = df_industries.apply(lambda row: getRsAverage(industries, row[TITLE_INDUSTRY], TITLE_1M), axis=1)
    df_industries[TITLE_3M] = df_industries.apply(lambda row: getRsAverage(industries, row[TITLE_INDUSTRY], TITLE_3M), axis=1)
    df_industries[TITLE_6M] = df_industries.apply(lambda row: getRsAverage(industries, row[TITLE_INDUSTRY], TITLE_6M), axis=1)
    df_industries[TITLE_PERCENTILE] = pd.qcut(df_industries[TITLE_RS], 100, labels=False, duplicates="drop")
    df_industries[TITLE_1M] = pd.qcut(df_industries[TITLE_1M], 100, labels=False, duplicates="drop")
    df_industries[TITLE_3M] = pd.qcut(df_industries[TITLE_3M], 100, labels=False, duplicates="drop")
    df_industries[TITLE_6M] = pd.qcut(df_industries[TITLE_6M], 100, labels=False, duplicates="drop")
    df_industries[TITLE_TICKERS] = df_industries.apply(lambda row: getTickers(industries, row[TITLE_INDUSTRY]), axis=1)
    df_industries = df_industries.sort_values(([TITLE_RS]), ascending=False)
    ind_ranks = ind_ranks[:len(df_industries)]
    df_industries[TITLE_RANK] = ind_ranks

    df_industries.to_csv(os.path.join(DIR, "output", f'rs_industries{suffix}.csv'), index = False)
    dfs.append(df_industries)

    return dfs


def main(skipEnter = False):
    ranks = rankings()
    print(ranks[0])
    print("***\nYour 'rs_stocks.csv' is in the output folder.\n***")
    if not skipEnter and cfg("EXIT_WAIT_FOR_ENTER"):
        input("Press Enter key to exit...")

if __name__ == "__main__":
    main()
