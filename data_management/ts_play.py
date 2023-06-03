import os

import tushare
from dotenv import load_dotenv

from data_management.consts import Stock
from utils import get_recent_trading_day

load_dotenv()
TOKEN = os.environ.get("TUSHARE_TOKEN")

tushare.set_token(TOKEN)
pro = tushare.pro_api()


def update_supply(stock: Stock):
    fields = "total_share, float_share, turn_over, trade_date"
    end_date = get_recent_trading_day().strftime("%Y%m%d")
    print(end_date)
    # data = pro.bak_daily(ts_code=stock.ts_code, start_date="19980101", end_date=end_date, fields=fields)
    # data.to_csv("002273supply.csv", index=False)


if __name__ == '__main__':
    stock = Stock("002273", "SZ")
    update_supply(stock)