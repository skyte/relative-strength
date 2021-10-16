#!/usr/bin/env python
import requests
import json
import time
import bs4 as bs
import datetime as dt
import os
import pandas_datareader.data as web
import pickle
import requests
import yaml
import yfinance as yf
import pandas as pd
import dateutil.relativedelta
import numpy as np

from datetime import date
from datetime import datetime

DIR = os.path.dirname(os.path.realpath(__file__))

if not os.path.exists(os.path.join(DIR, 'data')):
    os.makedirs(os.path.join(DIR, 'data'))
if not os.path.exists(os.path.join(DIR, 'tmp')):
    os.makedirs(os.path.join(DIR, 'tmp'))

try:
    with open(os.path.join(DIR, 'config_private.yaml'), 'r') as stream:
        private_config = yaml.safe_load(stream)
except FileNotFoundError:
    private_config = None
except yaml.YAMLError as exc:
        print(exc)

try:
    with open('config.yaml', 'r') as stream:
        config = yaml.safe_load(stream)
except FileNotFoundError:
    config = None
except yaml.YAMLError as exc:
        print(exc)

def cfg(key):
    try:
        return private_config[key]
    except:
        try:
            return config[key]
        except:
            return None

API_KEY = cfg("API_KEY")
TD_API = cfg("TICKERS_API")
PRICE_DATA_OUTPUT = os.path.join(DIR, "data", "price_history.json")
REFERENCE_TICKER = cfg("REFERENCE_TICKER")
DATA_SOURCE = cfg("DATA_SOURCE")

def getSecurities(url, tickerPos = 1, tablePos = 1, sectorPosOffset = 1, universe = "N/A"):
    resp = requests.get(url)
    soup = bs.BeautifulSoup(resp.text, 'lxml')
    table = soup.findAll('table', {'class': 'wikitable sortable'})[tablePos-1]
    secs = {}
    for row in table.findAll('tr')[tablePos:]:
        sec = {}
        sec["ticker"] = row.findAll('td')[tickerPos-1].text.strip()
        sec["sector"] = row.findAll('td')[tickerPos-1+sectorPosOffset].text.strip()
        sec["universe"] = universe
        secs[sec["ticker"]] = sec
    with open(os.path.join(DIR, "tmp", "tickers.pickle"), "wb") as f:
        pickle.dump(secs, f)
    return secs

def get_resolved_securities():
    ref_ticker = {"ticker": REFERENCE_TICKER, "sector": "Reference", "universe": "Reference"}
    tickers = {REFERENCE_TICKER: ref_ticker}
    if cfg("NQ100"):
        tickers.update(getSecurities('https://en.wikipedia.org/wiki/Nasdaq-100', 2, 3, universe="Nasdaq 100"))
    if cfg("SP500"):
        tickers.update(getSecurities('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies', sectorPosOffset=3, universe="S&P 500"))
    if cfg("SP400"):
        tickers.update(getSecurities('https://en.wikipedia.org/wiki/List_of_S%26P_400_companies', 2, universe="S&P 400"))
    if cfg("SP600"):
        tickers.update(getSecurities('https://en.wikipedia.org/wiki/List_of_S%26P_600_companies', 2, universe="S&P 600"))
    return tickers

SECURITIES = get_resolved_securities().values()


def create_price_history_file(tickers_dict):
    with open(PRICE_DATA_OUTPUT, "w") as fp:
        json.dump(tickers_dict, fp)

def enrich_ticker_data(ticker_response, security):
    ticker_response["sector"] = security["sector"]
    ticker_response["universe"] = security["universe"]

def tda_params(apikey, period_type="year", period=1, frequency_type="daily", frequency=1):
    """Returns tuple of api get params. Uses clenow default values."""
    return (
           ("apikey", apikey),
           ("periodType", period_type),
           ("period", period),
           ("frequencyType", frequency_type),
           ("frequency", frequency)
    )

def print_data_progress(ticker, universe, idx, securities, error_text, elapsed_s, remaining_s):
    dt_ref = datetime.fromtimestamp(0)
    dt_e = datetime.fromtimestamp(elapsed_s)
    elapsed = dateutil.relativedelta.relativedelta (dt_e, dt_ref)
    if remaining_s and not np.isnan(remaining_s):
        dt_r = datetime.fromtimestamp(remaining_s)
        remaining = dateutil.relativedelta.relativedelta (dt_r, dt_ref)
        remaining_string = f'{remaining.minutes}m {remaining.seconds}s'
    else:
        remaining_string = "?"
    print(f'{ticker} from {universe}{error_text} ({idx+1} / {len(securities)}). Elapsed: {elapsed.minutes}m {elapsed.seconds}s. Remaining: {remaining_string}.')

def get_remaining_seconds(all_load_times, idx, len):
    load_time_ma = pd.Series(all_load_times).rolling(np.minimum(idx+1, 25)).mean().tail(1).item()
    remaining_seconds = (len - idx) * load_time_ma
    return remaining_seconds

def load_prices_from_tda(securities):
    print("*** Loading Stocks from TD Ameritrade ***")
    headers = {"Cache-Control" : "no-cache"}
    params = tda_params(API_KEY)
    tickers_dict = {}
    start = time.time()
    load_times = []

    for idx, sec in enumerate(securities):
        r_start = time.time()
        response = requests.get(
                TD_API % sec["ticker"],
                params=params,
                headers=headers
        )
        now = time.time()
        current_load_time = now - r_start
        load_times.append(current_load_time)
        remaining_seconds = get_remaining_seconds(load_times, idx, len(securities))
        ticker_data = response.json()
        enrich_ticker_data(ticker_data, sec)
        tickers_dict[sec["ticker"]] = ticker_data
        error_text = f' Error with code {response.status_code}' if response.status_code != 200 else ''
        print_data_progress(sec["ticker"], sec["universe"], idx, securities, error_text, now - start, remaining_seconds)

    create_price_history_file(tickers_dict)


def get_yf_data(security, start_date, end_date):
        escaped_ticker = security["ticker"].replace(".","-")
        df = yf.download(escaped_ticker, start=start_date, end=end_date)
        yahoo_response = df.to_dict()
        timestamps = list(yahoo_response["Open"].keys())
        timestamps = list(map(lambda timestamp: int(timestamp.timestamp()), timestamps))
        opens = list(yahoo_response["Open"].values())
        closes = list(yahoo_response["Close"].values())
        lows = list(yahoo_response["Low"].values())
        highs = list(yahoo_response["High"].values())
        volumes = list(yahoo_response["Volume"].values())
        ticker_data = {}
        candles = []

        for i in range(0, len(opens)):
            candle = {}
            candle["open"] = opens[i]
            candle["close"] = closes[i]
            candle["low"] = lows[i]
            candle["high"] = highs[i]
            candle["volume"] = volumes[i]
            candle["datetime"] = timestamps[i]
            candles.append(candle)

        ticker_data["candles"] = candles
        enrich_ticker_data(ticker_data, security)
        return ticker_data

def load_prices_from_yahoo(securities):
    print("*** Loading Stocks from Yahoo Finance ***")
    today = date.today()
    start = time.time()
    start_date = today - dt.timedelta(days=1*365)
    tickers_dict = {}
    load_times = []
    for idx, security in enumerate(securities):
        r_start = time.time()
        ticker_data = get_yf_data(security, start_date, today)
        now = time.time()
        current_load_time = now - r_start
        load_times.append(current_load_time)
        remaining_seconds = remaining_seconds = get_remaining_seconds(load_times, idx, len(securities))
        print_data_progress(security["ticker"], security["universe"], idx, securities, "", time.time() - start, remaining_seconds)
        tickers_dict[security["ticker"]] = ticker_data
    create_price_history_file(tickers_dict)

def save_data(source, securities):
    if source == "YAHOO":
        load_prices_from_yahoo(securities)
    elif source == "TD_AMERITRADE":
        load_prices_from_tda(securities)


def main():
    save_data(DATA_SOURCE, SECURITIES)

if __name__ == "__main__":
    main()
