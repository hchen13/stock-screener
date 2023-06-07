import logging
from datetime import datetime

import pandas as pd

from market import best_host
from market.consts import Stock
from market.tdx_core import update_candlesticks, get_stock_list, update_supply, get_recent_trading_day, \
    _get_recent_trading_day, api, update_stock_candlesticks, tdx_kline_type


def update_market(interval: str='1d'):
    stock_list = get_stock_list()
    update_candlesticks(interval)
    for stock in stock_list:
        update_supply(stock)


if __name__ == '__main__':
    pd.set_option("display.max_columns", None)
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(levelname)s %(filename)s:%(lineno)s %(message)s',
        force=True
    )
    update_market()
    # with api.connect(best_host[1], best_host[2]):
    #     update_stock_candlesticks(Stock("002273", "SZ"), "1d")
    #     update_stock_candlesticks(Stock("002273", "SZ"), "1d")