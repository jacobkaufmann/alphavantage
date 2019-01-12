import sys

import csv
import json
import requests
from io import StringIO

import time

import numpy as np
import pandas as pd


intervals = {
    "1min",
    "5min",
    "15min",
    "30min",
    "60min",
    "daily",
    "weekly",
    "monthly"
}
series_types = {"close", "open", "high", "low"}
data_formats = {"json", "csv"}
output_sizes = {"full", "compact"}


class Client(object):
    """Python client for Alpha Vantage API
    """

    def __init__(self, token, max_attempts: int=3):
        """Initializes the client

        Args:
            max_attempts: Integer, maximum number of requests made to API in
                in case of failure.
        """

        self.apikey = token
        self.base_url = "https://www.alphavantage.co/query?"
        self.max_attempts = max_attempts

    def process_csv(self, r):
        decoded = r.content.decode("utf-8")
        data = pd.read_csv(StringIO(decoded))
        columns = data.columns.values
        if columns[0] != "time":
            columns[0] = "time"
        data.columns = columns
        data = data.set_index("time")
        return data

    def process_json(self, r):
        decoded = r.json()
        return decoded

    def ts_daily(self,
                 symbol: str,
                 adjusted: bool=True,
                 output_size: str="full",
                 data_format: str="csv"):
        """Time series data for a particular equity

        Args:
            symbol: String, symbol specifying equity
            adjusted: bool,  whether to use adjusted prices
            output_size: String, size of time series data
            data_format: String, format of response
        """
        assert(output_size in output_sizes)
        assert(data_format in data_formats)
        func = "TIME_SERIES_DAILY"
        if adjusted:
            func = func + "_ADJUSTED"
        params = {
            "function": func,
            "symbol": symbol.upper(),
            "outputsize": output_size,
            "datatype": data_format,
            "apikey": self.apikey
        }
        r = requests.get(self.base_url, params, stream=True)
        r.raise_for_status()

        if data_format == "csv":
            return self.process_csv(r)
        else:
            return self.process_json(r)

    def sma(self,
            symbol: str,
            interval: str="daily",
            time_period: int=15,
            series_type: str="close",
            data_format: str="csv"):
        """Simple moving average

        Args:
            symbol: String, symbol specifying equity
            interval: String, interval between consequitive data points
            time_period: Integer, number of data points to collect
            series_type: String, desired price type (open, high, low, close)
            data_format: String, format of response
        """
        assert(interval in intervals)
        assert(series_type in series_types)
        assert(data_format in data_formats)
        params = {
            "function": "SMA",
            "symbol": symbol.upper(),
            "interval": interval,
            "time_period": time_period,
            "series_type": series_type,
            "datatype": data_format,
            "apikey": self.apikey
        }
        r = requests.get(self.base_url, params, stream=True)
        r.raise_for_status()
        if data_format == "csv":
            return self.process_csv(r)
        else:
            return self.process_json(r)

    def ema(self,
            symbol: str,
            interval: str="daily",
            time_period: int=15,
            series_type: str="close",
            data_format: str="csv"):
        """Exponential moving average

        Args:
            symbol: String, symbol specifying equity
            interval: String, interval between consequitive data points
            time_period: Integer, number of data points to collect
            series_type: String, desired price type (open, high, low, close)
            data_format: String, format of response
        """
        assert(interval in intervals)
        assert(series_type in series_types)
        assert(data_format in data_formats)
        params = {
            "function": "EMA",
            "symbol": symbol.upper(),
            "interval": interval,
            "time_period": time_period,
            "series_type": series_type,
            "datatype": data_format,
            "apikey": self.apikey
        }
        r = requests.get(self.base_url, params, stream=True)
        r.raise_for_status()
        if data_format == "csv":
            return self.process_csv(r)
        else:
            return self.process_json(r)

    def wma(self,
            symbol: str,
            interval: str="daily",
            time_period: int=15,
            series_type: str="close",
            data_format: str="csv"):
        """Weighted moving average

        Args:
            symbol: String, symbol specifying equity
            interval: String, interval between consequitive data points
            time_period: Integer, number of data points to collect
            series_type: String, desired price type (open, high, low, close)
            data_format: String, format of response
        """
        assert(interval in intervals)
        assert(series_type in series_types)
        assert(data_format in data_formats)
        params = {
            "function": "WMA",
            "symbol": symbol.upper(),
            "interval": interval,
            "time_period": time_period,
            "series_type": series_type,
            "datatype": data_format,
            "apikey": self.apikey
        }
        r = requests.get(self.base_url, params)
        r.raise_for_status()
        if data_format == "csv":
            return self.process_csv(r)
        else:
            return self.process_json(r)

    def macd(self,
             symbol: str,
             interval: str="daily",
             series_type: str="close",
             data_format: str="csv",
             fast: int=12,
             slow: int=26,
             signal: int=9):
        """MACD (Moving Average Convergence Divergence)

        Args:
            symbol: String, symbol specifying equity
            interval: String, interval between consequitive data points
            series_type: String, desired price type (open, high, low, close)
            data_format: String, format of response
            fast: Integer, fast period
            slow: Integer, slow period
            signal: Integer, signal period
        """
        assert(interval in intervals)
        assert(series_type in series_types)
        assert(data_format in data_formats)
        params = {
            "function": "MACD",
            "symbol": symbol.upper(),
            "interval": interval,
            "fastperiod": fast,
            "slowperiod": slow,
            "signalperiod": signal,
            "series_type": series_type,
            "datatype": data_format,
            "apikey": self.apikey
        }
        r = requests.get(self.base_url, params, stream=True)
        r.raise_for_status()
        if data_format == "csv":
            return self.process_csv(r)
        else:
            return self.process_json(r)

    def stoch(self,
              symbol: str,
              interval: str="daily",
              data_format: str="csv",
              fastk: int=5,
              slowk: int=3,
              slowd: int=3):
        """Stochastic Oscillator

        Args:
            symbol: String, symbol specifying equity
            interval: String, interval between consequitive data points
            data_format: String, format of response
            fastk: Integer, fast k period
            slowk: Integer, slow k period
            slowd: Integer, slow d period
        """
        assert(interval in intervals)
        assert(data_format in data_formats)
        params = {
            "function": "STOCH",
            "symbol": symbol.upper(),
            "interval": interval,
            "fastkperiod": fastk,
            "slowkperiod": slowk,
            "slowdperiod": slowd,
            "datatype": data_format,
            "apikey": self.apikey
        }
        r = requests.get(self.base_url, params, stream=True)
        r.raise_for_status()
        if data_format == "csv":
            return self.process_csv(r)
        else:
            return self.process_json(r)

    def rsi(self,
            symbol: str,
            interval: str="daily",
            time_period: int=14,
            series_type: str="close",
            data_format: str="csv"):
        """RSI (Relative Strength Index)

        Args:
            symbol: String, symbol specifying equity
            interval: String, interval between consequitive data points
            time_period: Integer, number of data points used in calculations
            series_type: String, desired price type (open, high, low, close)
            data_format: String, format of response
        """
        assert(interval in intervals)
        assert(series_type in series_types)
        assert(data_format in data_formats)
        params = {
            "function": "RSI",
            "symbol": symbol.upper(),
            "interval": interval,
            "time_period": time_period,
            "series_type": series_type,
            "datatype": data_format,
            "apikey": self.apikey
        }
        r = requests.get(self.base_url, params, stream=True)
        r.raise_for_status()
        if data_format == "csv":
            return self.process_csv(r)
        else:
            return self.process_json(r)

    def momentum(self,
                 symbol: str,
                 interval: str="daily",
                 time_period: int=14,
                 series_type: str="close",
                 data_format: str="csv"):
        """Momentum

        Args:
            symbol: String, symbol specifying equity
            interval: String, interval between consequitive data points
            time_period: Integer, number of data points used in calculations
            series_type: String, desired price type (open, high, low, close)
            data_format: String, format of response
        """
        assert(interval in intervals)
        assert(series_type in series_types)
        assert(data_format in data_formats)
        params = {
            "function": "MOM",
            "symbol": symbol.upper(),
            "interval": interval,
            "time_period": time_period,
            "series_type": series_type,
            "datatype": data_format,
            "apikey": self.apikey
        }
        r = requests.get(self.base_url, params, stream=True)
        r.raise_for_status()
        if data_format == "csv":
            return self.process_csv(r)
        else:
            return self.process_json(r)

    def ppo(self,
            symbol: str,
            interval: str="daily",
            series_type: str="close",
            data_format: str="csv",
            fast: int=12,
            slow: int=26):
        """PPO (Percentage Price Oscillator)

        Args:
            symbol: String, symbol specifying equity
            interval: String, interval between consequitive data points
            series_type: String, desired price type (open, high, low, close)
            data_format: String, format of response
            fast: Integer, fast period
            slow: Integer, slow period
        """
        assert(interval in intervals)
        assert(series_type in series_types)
        assert(data_format in data_formats)
        params = {
            "function": "PPO",
            "symbol": symbol.upper(),
            "interval": interval,
            "fastperiod": fast,
            "slowperiod": slow,
            "series_type": series_type,
            "datatype": data_format,
            "apikey": self.apikey
        }
        r = requests.get(self.base_url, params, stream=True)
        r.raise_for_status()
        if data_format == "csv":
            return self.process_csv(r)
        else:
            return self.process_json(r)

    def ad(self, symbol: str, interval: str="daily", data_format: str="csv"):
        """Chaikin A/D (Accumulation/Distribution) line

        Args:
            symbol: String, symbol specifying equity
            interval: String, interval between consequitive data points
            data_format: String, format of response
        """
        assert(interval in intervals)
        assert(data_format in data_formats)
        params = {
            "function": "AD",
            "symbol": symbol.upper(),
            "interval": interval,
            "datatype": data_format,
            "apikey": self.apikey
        }
        r = requests.get(self.base_url, params, stream=True)
        r.raise_for_status()
        if data_format == "csv":
            return self.process_csv(r)
        else:
            return self.process_json(r)

    def adx(self,
            symbol: str,
            interval: str="daily",
            time_period: int=14,
            data_format: str="csv"):
        """ADX (Average Directional Index)

        Args:
            symbol: String, symbol specifying equity
            interval: String, interval between consequitive data points
            time_period: Integer, number of data points used in calculations
            data_format: String, format of response
        """
        assert(interval in intervals)
        assert(data_format in data_formats)
        params = {
            "function": "ADX",
            "symbol": symbol.upper(),
            "interval": interval,
            "time_period": str(time_period),
            "datatype": data_format,
            "apikey": self.apikey
        }
        r = requests.get(self.base_url, params, stream=True)
        r.raise_for_status()
        if data_format == "csv":
            return self.process_csv(r)
        else:
            return self.process_json(r)

    def cci(self,
            symbol: str,
            interval: str="daily",
            time_period: int=20,
            data_format: str="csv"):
        """CCI (Commodity Channel Index)

        Args:
            symbol: String, symbol specifying equity
            interval: String, interval between consequitive data points
            time_period: Integer, number of data points used in calculations
            data_format: String, format of response
        """
        assert(interval in intervals)
        assert(data_format in data_formats)
        params = {
            "function": "CCI",
            "symbol": symbol.upper(),
            "interval": interval,
            "time_period": str(time_period),
            "datatype": data_format,
            "apikey": self.apikey
        }
        r = requests.get(self.base_url, params, stream=True)
        r.raise_for_status()
        if data_format == "csv":
            return self.process_csv(r)
        else:
            return self.process_json(r)

    def aroon(self,
              symbol: str,
              interval: str="daily",
              time_period: int=20,
              data_format: str="csv"):
        """Aroon

        Args:
            symbol: String, symbol specifying equity
            interval: String, interval between consequitive data points
            time_period: Integer, number of data points used in calculations
            data_format: String, format of response
        """
        assert(interval in intervals)
        assert(data_format in data_formats)
        params = {
            "function": "AROON",
            "symbol": symbol.upper(),
            "interval": interval,
            "time_period": str(time_period),
            "datatype": data_format,
            "apikey": self.apikey
        }
        r = requests.get(self.base_url, params, stream=True)
        r.raise_for_status()
        if data_format == "csv":
            return self.process_csv(r)
        else:
            return self.process_json(r)

    def aroon_osc(self,
                  symbol: str,
                  interval: str="daily",
                  time_period: int=20,
                  data_format: str="csv"):
        """Aroon Oscillator

        Args:
            symbol: String, symbol specifying equity
            interval: String, interval between consequitive data points
            time_period: Integer, number of data points used in calculations
            data_format: String, format of response
        """
        assert(interval in intervals)
        assert(data_format in data_formats)
        params = {
            "function": "AROONOSC",
            "symbol": symbol.upper(),
            "interval": interval,
            "time_period": str(time_period),
            "datatype": data_format,
            "apikey": self.apikey
        }
        r = requests.get(self.base_url, params, stream=True)
        r.raise_for_status()
        if data_format == "csv":
            return self.process_csv(r)
        else:
            return self.process_json(r)

    def ultimate_osc(self,
                     symbol: str,
                     interval: str="daily",
                     time_period1: int=7,
                     time_period2: int=14,
                     time_period3: int=28,
                     data_format: str="csv"):
        """Ultimate Oscillator

        Args:
            symbol: String, symbol specifying equity
            interval: String, interval between consequitive data points
            time_period1: Integer, first time period
            time_period2: Integer, second time period
            time_period3: Integer, third time period
            data_format: String, format of response
        """
        assert(interval in intervals)
        assert(data_format in data_formats)
        params = {
            "function": "ULTOSC",
            "symbol": symbol.upper(),
            "interval": interval,
            "timeperiod1": str(time_period1),
            "timeperiod2": str(time_period2),
            "timeperiod3": str(time_period3),
            "datatype": data_format,
            "apikey": self.apikey
        }
        r = requests.get(self.base_url, params, stream=True)
        r.raise_for_status()
        if data_format == "csv":
            return self.process_csv(r)
        else:
            return self.process_json(r)

    def hilbert_transform_sine(self,
                               symbol: str,
                               interval: str="daily",
                               series_type: str="close",
                               data_format: str="csv"):
        """Hilbert Transform Sine Wave

        Args:
            symbol: String, symbol specifying equity
            interval: String, interval between consequitive data points
            series_type: String, desired price type in the time series
            data_format: String, format of response
        """
        assert(interval in intervals)
        assert(series_type in series_types)
        assert(data_format in data_formats)
        params = {
            "function": "HT_SINE",
            "symbol": symbol.upper(),
            "interval": interval,
            "series_type": series_type,
            "datatype": data_format,
            "apikey": self.apikey
        }
        r = requests.get(self.base_url, params, stream=True)
        r.raise_for_status()
        if data_format == "csv":
            return self.process_csv(r)
        else:
            return self.process_json(r)

    def hilbert_transform_trendmode(self,
                                    symbol: str,
                                    interval: str="daily",
                                    series_type: str="close",
                                    data_format: str="csv"):
        """Hilbert Transform Trend vs Cycle Mode

        Args:
            symbol: String, symbol specifying equity
            interval: String, interval between consequitive data points
            series_type: String, desired price type in the time series
            data_format: String, format of response
        """
        assert(interval in intervals)
        assert(series_type in series_types)
        assert(data_format in data_formats)
        params = {
            "function": "HT_TRENDMODE",
            "symbol": symbol.upper(),
            "interval": interval,
            "series_type": series_type,
            "datatype": data_format,
            "apikey": self.apikey
        }
        r = requests.get(self.base_url, params, stream=True)
        r.raise_for_status()
        if data_format == "csv":
            return self.process_csv(r)
        else:
            return self.process_json(r)

    def hilbert_transform_dcperiod(self,
                                   symbol: str,
                                   interval: str="daily",
                                   series_type: str="close",
                                   data_format: str="csv"):
        """Hilbert Transform Dominant Cycle Period

        Args:
            symbol: String, symbol specifying equity
            interval: String, interval between consequitive data points
            series_type: String, desired price type in the time series
            data_format: String, format of response
        """
        assert(interval in intervals)
        assert(series_type in series_types)
        assert(data_format in data_formats)
        params = {
            "function": "HT_DCPERIOD",
            "symbol": symbol.upper(),
            "interval": interval,
            "series_type": series_type,
            "datatype": data_format,
            "apikey": self.apikey
        }
        r = requests.get(self.base_url, params, stream=True)
        r.raise_for_status()
        if data_format == "csv":
            return self.process_csv(r)
        else:
            return self.process_json(r)
