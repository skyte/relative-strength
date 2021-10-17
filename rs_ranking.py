import sys
import pandas as pd
import numpy as np
import json
import os
from datetime import date
from scipy.stats import linregress
import yaml
from rs_data import cfg

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

TITLE_RANK = "Rank"
TITLE_TICKER = "Ticker"
TITLE_SECTOR = "Sector"
TITLE_UNIVERSE = "Universe"
TITLE_PERCENTILE = "Percentile"
TITLE_1M = "1 Month Ago"
TITLE_3M = "3 Months Ago"
TITLE_6M = "6 Months Ago"
TITLE_RS = "Relative Strength"

if not os.path.exists('output'):
    os.makedirs('output')

def read_json(json_file):
    with open(json_file, "r") as fp:
        return json.load(fp)

def relative_strength(closes: pd.Series, closes_ref: pd.Series):
    rs_stock = strength(closes)
    rs_ref = strength(closes_ref)
    rs = (rs_stock/rs_ref - 1) * 100
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
            if closes:
                closes_series = pd.Series(closes)
                closes_ref_series = pd.Series(closes_ref)
                rs = relative_strength(closes_series, closes_ref_series)
                month = 20
                tmp_percentile = 100
                rs1m = relative_strength(closes_series.head(-1*month), closes_ref_series.head(-1*month))
                rs3m = relative_strength(closes_series.head(-3*month), closes_ref_series.head(-3*month))
                rs6m = relative_strength(closes_series.head(-6*month), closes_ref_series.head(-6*month))
                ranks.append(len(ranks)+1)
                relative_strengths.append((0, ticker, json[ticker]["sector"], json[ticker]["universe"], rs, tmp_percentile, rs1m, rs3m, rs6m))
        except KeyError:
            print(f'Ticker {ticker} has corrupted data.')
    dfs = []
    suffix = ''
    df = pd.DataFrame(relative_strengths, columns=[TITLE_RANK, TITLE_TICKER, TITLE_SECTOR, TITLE_UNIVERSE, TITLE_RS, TITLE_PERCENTILE, TITLE_1M, TITLE_3M, TITLE_6M])
    df[TITLE_PERCENTILE] = pd.qcut(df[TITLE_RS], 100, labels=False)
    df[TITLE_1M] = pd.qcut(df[TITLE_1M], 100, labels=False)
    df[TITLE_3M] = pd.qcut(df[TITLE_3M], 100, labels=False)
    df[TITLE_6M] = pd.qcut(df[TITLE_6M], 100, labels=False)
    df = df.sort_values(([TITLE_RS]), ascending=False)
    df[TITLE_RANK] = ranks
    out_tickers_count = 0
    for index, row in df.iterrows():
        if row[TITLE_PERCENTILE] >= MIN_PERCENTILE:
            out_tickers_count = out_tickers_count + 1
    df = df.head(out_tickers_count)

    df.to_csv(os.path.join(DIR, "output", f'rs_stocks{suffix}.csv'), index = False)

    dfs.append(df)

    return dfs


def main():
    ranks = rankings()
    print(ranks[0])
    print("***\nYour 'rs_stocks.csv' is in the output folder.\n***")
    if cfg("EXIT_WAIT_FOR_ENTER"):
        input("Press Enter key to exit...")

if __name__ == "__main__":
    main()
