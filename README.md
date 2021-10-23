# relative-strength
IBD Style Relative Strength Percentile Ranking of Stocks (i.e. 0-100 Score).  
I also made a TradingView indicator, but it cannot give you the percentile ranking, it just shows you the Relative Strength: https://www.tradingview.com/script/SHE1xOMC-Relative-Strength-IBD-Style/

## Daily Generated Outputs
Stocks: https://github.com/skyte/rs-log/blob/main/output/rs_stocks.csv  
Industries: https://github.com/skyte/rs-log/blob/main/output/rs_industries.csv  
  

## Known Issues
Unfortunately the close prices loaded from the price history API are not always split adjusted. So if a stock had a split recently there is a chance the relative strength value will be wrong...
## Calculation
Yearly performance of stock (most recent quarter is weighted double) divided by yearly performance of reference index (`SPY` by default).
  

## Considered Stocks
Tickers from ftp://ftp.nasdaqtrader.com/symboldirectory/nasdaqtraded.txt disregarding ETFs and all stocks where the industry and sector information couldn't be retrieved from yahoo finance.
## How To Run

### Run EXE

1. Open the latest successful run here: https://github.com/skyte/relative-strength/actions/workflows/exe.yml
2. Download `exe-package` at the bottom (need to be logged in into github)
3. Exctract the `relative-strength` folder and enter it
   - If needed open `config.yaml` and put in your preferences 
4. Run `relative-strength.exe`

### Run Python Script

1. Open `config.yaml` and put in your preferences 
2. Install requirements: `python -m pip install -r requirements.txt`
3. Run `relative-strength.py`

#### Separate Steps

Instead of running `relative-strength.py` you can also:

1. Run `rs_data.py` to aggregate the price data
2. Run `rs_ranking.py` to calculate the relative strength rankings



### \*\*\* Output \*\*\*

- in the `output` folder you will find:
  - the list of ranked stocks: `rs_stocks.csv`
  - the list of ranked industries: `rs_industries.csv`


## Config

#### Private File

You can create a `config_private.yaml` next to `config.yaml` and overwrite some parameters like `API_KEY`. That way you don't get conflicts when pulling a new version.

#### Data Sources

Can be switched with the field `DATA_SOURCE`

##### Yahoo Finance

(Benchmark: Loads 1500 Stocks in 20m)

- Is default, no config necessary.

##### TD Ameritrade

(Benchmark: Loads 1500 Stocks in 18m)

1. Create TDAmeritrade Developer Account and App
2. Put in your `API_KEY` in `config.yaml` and change `DATA_SOURCE`.
