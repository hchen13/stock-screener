import asyncio
import os
from functools import wraps

import nest_asyncio
import pandas as pd
from aioinflux import InfluxDBClient
from dotenv import load_dotenv

from data_management.consts import Stock
from utils import check_environment

load_dotenv()

INFLUX_HOST = os.getenv("INFLUX_HOST")
INFLUX_PORT = int(os.getenv("INFLUX_PORT"))
INFLUX_USER = os.getenv("INFLUX_USER")
INFLUX_PASS = os.getenv("INFLUX_PASS")
INFLUX_DB = os.getenv("INFLUX_DB")

client = InfluxDBClient(
    host=INFLUX_HOST,
    port=INFLUX_PORT,
    username=INFLUX_USER,
    password=INFLUX_PASS,
    database=INFLUX_DB,
    output="dataframe",
)


def jupyter_compatible(coro):
    current_env = check_environment()
    @wraps(coro)
    def inner(*args, **kwargs):
        if current_env == 'jupyter':
            nest_asyncio.apply()
            loop = asyncio.get_event_loop()
            return loop.run_until_complete(coro(*args, **kwargs))
        return asyncio.get_event_loop().run_until_complete(coro(*args, **kwargs))

    return inner


@jupyter_compatible
async def get_recent_candlestick_record(symbol, interval):
    query = f"""
    select * from OHLCV where symbol = '{symbol}' and interval = '{interval}' order by time desc limit 1 
    """
    result = await client.query(query)
    if isinstance(result, pd.DataFrame):
        return result.iloc[0]
    return None


@jupyter_compatible
async def write_candlesticks(stock: Stock, interval, candlesticks: pd.DataFrame):
    data_points = []
    for i, row in candlesticks.iterrows():
        tags = {
            "symbol": stock.symbol,
            "interval": interval,
            "code": stock.code,
            "exchange": stock.market.name,
        }
        fields = {
            "open": row.open,
            "high": row.high,
            "low": row.low,
            "close": row.close,
            "volume": row.vol,
            "amount": row.amount,
        }
        data_point = {
            "measurement": "OHLCV",
            "tags": tags,
            "fields": fields,
            "time": row.datetime,
        }
        data_points.append(data_point)
    await client.write(data_points)


@jupyter_compatible
async def write_supply(stock: Stock, total_supply, circulating_supply, trading_date):
    tags = {
        "symbol": stock.symbol,
        "code": stock.code,
        "exchange": stock.market.name,
    }
    fields = {
        "total_supply": total_supply,
        "circulating_supply": circulating_supply,
    }
    data_point = {
        "measurement": "supply",
        "tags": tags,
        "fields": fields,
        "time": trading_date,
    }
    await client.write(data_point)
