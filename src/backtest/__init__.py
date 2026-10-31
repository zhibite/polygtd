"""In-process historical backtest for the BTC 15m strategy.

Data: future.news (event + token kline), Polymarket data API (per-second
fills) and Binance 1m klines. The dashboard page is `/backtest`; the job
manager is `src.backtest.runner`. This package never places orders and
must not be imported by the execution path.
"""
