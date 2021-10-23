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
import re
from ftplib import FTP
from io import StringIO

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

def read_json(json_file):
    with open(json_file, "r", encoding="utf-8") as fp:
        return json.load(fp)

API_KEY = cfg("API_KEY")
TD_API = "https://api.tdameritrade.com/v1/marketdata/%s/pricehistory"
PRICE_DATA_FILE = os.path.join(DIR, "data", "price_history.json")
REFERENCE_TICKER = cfg("REFERENCE_TICKER")
DATA_SOURCE = cfg("DATA_SOURCE")
ALL_STOCKS = cfg("USE_ALL_LISTED_STOCKS")
TICKER_INFO_FILE = os.path.join(DIR, "data_persist", "ticker_info.json")
TICKER_INFO_DICT = read_json(TICKER_INFO_FILE)
REF_TICKER = {"ticker": REFERENCE_TICKER, "sector": "--- Reference ---", "industry": "--- Reference ---", "universe": "--- Reference ---"}

UNKNOWN = "unknown"

def get_securities(url, ticker_pos = 1, table_pos = 1, sector_offset = 1, industry_offset = 1, universe = "N/A"):
    resp = requests.get(url)
    soup = bs.BeautifulSoup(resp.text, 'lxml')
    table = soup.findAll('table', {'class': 'wikitable sortable'})[table_pos-1]
    secs = {}
    for row in table.findAll('tr')[table_pos:]:
        sec = {}
        sec["ticker"] = row.findAll('td')[ticker_pos-1].text.strip()
        sec["sector"] = row.findAll('td')[ticker_pos-1+sector_offset].text.strip()
        sec["industry"] = row.findAll('td')[ticker_pos-1+sector_offset+industry_offset].text.strip()
        sec["universe"] = universe
        secs[sec["ticker"]] = sec
    with open(os.path.join(DIR, "tmp", "tickers.pickle"), "wb") as f:
        pickle.dump(secs, f)
    return secs

def get_resolved_securities():
    tickers = {REFERENCE_TICKER: REF_TICKER}
    if ALL_STOCKS:
        return get_tickers_from_nasdaq(tickers)
    else:
        return get_tickers_from_wikipedia(tickers)

def get_tickers_from_wikipedia(tickers):
    if cfg("NQ100"):
        tickers.update(get_securities('https://en.wikipedia.org/wiki/Nasdaq-100', 2, 3, universe="Nasdaq 100"))
    if cfg("SP500"):
        tickers.update(get_securities('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies', sector_offset=3, universe="S&P 500"))
    if cfg("SP400"):
        tickers.update(get_securities('https://en.wikipedia.org/wiki/List_of_S%26P_400_companies', 2, universe="S&P 400"))
    if cfg("SP600"):
        tickers.update(get_securities('https://en.wikipedia.org/wiki/List_of_S%26P_600_companies', 2, universe="S&P 600"))
    return tickers

def exchange_from_symbol(symbol):
    if symbol == "Q":
        return "NASDAQ"
    if symbol == "A":
        return "NYSE MKT"
    if symbol == "N":
       return "NYSE"
    if symbol == "P":
       return "NYSE ARCA"
    if symbol == "Z":
       return "BATS"
    if symbol == "V":
       return "IEXG"
    return "n/a"

def get_tickers_from_nasdaq(tickers):
    filename = "nasdaqtraded.txt"
    ticker_column = 1
    etf_column = 5
    exchange_column = 3
    test_column = 7
    ftp = FTP('ftp.nasdaqtrader.com')
    ftp.login()
    ftp.cwd('SymbolDirectory')
    lines = StringIO()
    ftp.retrlines('RETR '+filename, lambda x: lines.write(str(x)+'\n'))
    ftp.quit()
    lines.seek(0)
    results = lines.readlines()

    for entry in results:
        sec = {}
        values = entry.split('|')
        ticker = values[ticker_column]
        if re.match(r'^[A-Z]+$', ticker) and values[etf_column] == "N" and values[test_column] == "N":
            sec["ticker"] = ticker
            sec["sector"] = UNKNOWN
            sec["industry"] = UNKNOWN
            sec["universe"] = exchange_from_symbol(values[exchange_column])
            tickers[sec["ticker"]] = sec

    return tickers

SECURITIES = get_resolved_securities().values()

def write_to_file(dict, file):
    with open(file, "w", encoding='utf8') as fp:
        json.dump(dict, fp, ensure_ascii=False)

def write_price_history_file(tickers_dict):
    write_to_file(tickers_dict, PRICE_DATA_FILE)

def write_ticker_info_file(info_dict):
    write_to_file(info_dict, TICKER_INFO_FILE)

def enrich_ticker_data(ticker_response, security):
    ticker_response["sector"] = security["sector"]
    ticker_response["industry"] = security["industry"]
    ticker_response["universe"] = security["universe"]

def tda_params(apikey, period_type="year", period=2, frequency_type="daily", frequency=1):
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
        remaining_string = f'{remaining.hours}h {remaining.minutes}m {remaining.seconds}s'
    else:
        remaining_string = "?"
    print(f'{ticker} from {universe}{error_text} ({idx+1} / {len(securities)}). Elapsed: {elapsed.hours}h {elapsed.minutes}m {elapsed.seconds}s. Remaining: {remaining_string}.')

def get_remaining_seconds(all_load_times, idx, len):
    load_time_ma = pd.Series(all_load_times).rolling(np.minimum(idx+1, 25)).mean().tail(1).item()
    remaining_seconds = (len - idx) * load_time_ma
    return remaining_seconds

def escape_ticker(ticker):
    return ticker.replace(".","-")

def get_info_from_dict(dict, key):
    value = dict[key] if key in dict else "n/a"
    # fix unicode
    # value = value.replace("\u2014", " ")
    return value

def load_ticker_info(ticker, info_dict):
    escaped_ticker = escape_ticker(ticker)
    info = yf.Ticker(escaped_ticker)
    ticker_info = {
        "info": {
            "industry": get_info_from_dict(info.info, "industry"),
            "sector": get_info_from_dict(info.info, "sector")
        }
    }
    info_dict[ticker] = ticker_info

def load_prices_from_tda(securities, api_key):
    print("*** Loading Stocks from TD Ameritrade ***")
    headers = {"Cache-Control" : "no-cache"}
    params = tda_params(api_key)
    tickers_dict = {}
    start = time.time()
    load_times = []
    new_entries = 0

    for idx, sec in enumerate(securities):
        ticker = sec["ticker"]
        r_start = time.time()
        response = requests.get(
                TD_API % ticker,
                params=params,
                headers=headers
        )
        ticker_data = response.json()
        if not ticker in TICKER_INFO_DICT:
            new_entries = new_entries + 1
            load_ticker_info(ticker, TICKER_INFO_DICT)
            if new_entries % 25 == 0:
                write_ticker_info_file(TICKER_INFO_DICT)
        ticker_data["industry"] = TICKER_INFO_DICT[ticker]["info"]["industry"]
        now = time.time()
        current_load_time = now - r_start
        load_times.append(current_load_time)
        remaining_seconds = get_remaining_seconds(load_times, idx, len(securities))
        enrich_ticker_data(ticker_data, sec)
        tickers_dict[sec["ticker"]] = ticker_data
        error_text = f' Error with code {response.status_code}' if response.status_code != 200 else ''
        print_data_progress(sec["ticker"], sec["universe"], idx, securities, error_text, now - start, remaining_seconds)

    write_price_history_file(tickers_dict)


def get_yf_data(security, start_date, end_date):
        ticker_data = {}
        ticker = security["ticker"]
        escaped_ticker = escape_ticker(ticker)
        df = yf.download(escaped_ticker, start=start_date, end=end_date)
        yahoo_response = df.to_dict()
        timestamps = list(yahoo_response["Open"].keys())
        timestamps = list(map(lambda timestamp: int(timestamp.timestamp()), timestamps))
        opens = list(yahoo_response["Open"].values())
        closes = list(yahoo_response["Close"].values())
        lows = list(yahoo_response["Low"].values())
        highs = list(yahoo_response["High"].values())
        volumes = list(yahoo_response["Volume"].values())
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
    start_date = today - dt.timedelta(days=1*365+183) # 183 = 6 months
    tickers_dict = {}
    load_times = []
    for idx, security in enumerate(securities):
        ticker = security["ticker"]
        r_start = time.time()
        ticker_data = get_yf_data(security, start_date, today)
        # if not ticker in TICKER_INFO_DICT:
        #     load_ticker_info(ticker, TICKER_INFO_DICT)
        # ticker_data["industry"] = TICKER_INFO_DICT[ticker]["info"]["industry"]
        now = time.time()
        current_load_time = now - r_start
        load_times.append(current_load_time)
        remaining_seconds = remaining_seconds = get_remaining_seconds(load_times, idx, len(securities))
        print_data_progress(ticker, security["universe"], idx, securities, "", time.time() - start, remaining_seconds)
        tickers_dict[ticker] = ticker_data
    write_price_history_file(tickers_dict)

def save_data(source, securities, api_key):
    if source == "YAHOO":
        load_prices_from_yahoo(securities)
    elif source == "TD_AMERITRADE":
        load_prices_from_tda(securities, api_key)


def main(forceTDA = False, api_key = API_KEY):
    dataSource = DATA_SOURCE if not forceTDA else "TD_AMERITRADE"
    save_data(dataSource, SECURITIES, api_key)
    write_ticker_info_file(TICKER_INFO_DICT)

if __name__ == "__main__":
    main()
