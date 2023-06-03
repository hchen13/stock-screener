import logging
from datetime import timedelta, datetime
from pathlib import Path

import pandas as pd
from pytdx.config.hosts import hq_hosts
from pytdx.hq import TdxHq_API
from pytdx.params import TDXParams
from pytdx.util.best_ip import ping
from tqdm import tqdm

from data_management.consts import Stock, MARKET
from data_management.influx_engine import get_recent_candlestick_record, write_candlesticks
from utils import tz_cn, get_recent_trading_day


def find_best_host():
    print("===== Finding best host =====")
    best = timedelta(seconds=10)
    for name, ip, port in hq_hosts:
        print(f"Testing {name}")
        r = ping(ip, port)
        if r < best:
            best = r
            best_name, best_host, best_port = name, ip, port
        if r < timedelta(seconds=0.1):
            print(f"Host {name} Latency: {r}\n===========================\n")
            return name, ip, port
    return best_name, best_host, best_port


api = TdxHq_API()
best_host = find_best_host()
_stocklist_save_path = Path(__file__).parent / 'stocklist.tdx.csv'
tdx_kline_type = {
    '1m': TDXParams.KLINE_TYPE_1MIN,
    '5m': TDXParams.KLINE_TYPE_5MIN,
    '15m': TDXParams.KLINE_TYPE_15MIN,
    '1h': TDXParams.KLINE_TYPE_1HOUR,
    '1d': TDXParams.KLINE_TYPE_DAILY,
}


def parse_code_type(code: str, market: MARKET):
    code = str(code)
    if market == MARKET.SZ:
        if code[:2] in ['00', '30', '02']:
            return 'stock'
        elif code[:2] in ['39']:
            return 'index'
        elif code[:2] in ['15', '16']:
            return 'etf'
        elif code[0:3] in ['101', '104', '105', '106', '107', '108', '109',
                            '111', '112', '114', '115', '116', '117', '118', '119',
                            '123', '127', '128',
                            '131', '139', ]:
            return 'bond'
        elif code[0:3] in ['120', '121', '122', '124', '125', '126', '130', '132', '133', '134', '135', '136', '137',
                           '138', '140', '141', '142', '143', '144', '145', '146', '147', '148', '150', '151',
                           '152', '153', '154', '155', '156', '157', '158', '159', '160', '161', '162', '163',
                           '164', '165', '166', '167', '168', '169', '170', '171', '172', '173', '174', '175',
                           '176', '177', '178', '179', '180', '181', '182', '183', '184', '185', '186', '187',
                           '188', '189', '190', '191', '192', '193', '194', '195', '196', '197', '198', '199']:
            return 'future'
        elif code[:2] in ['20']:
            return 'stock-b'
        return 'unknown'
    elif market == MARKET.SH:
        if code[0] == '6':
            return 'stock'
        elif code[:3] in ['000', '880']:
            return 'index'
        elif code[:2] in ['51', '58']:
            return 'etf'
        elif str(code)[0:3] in ['102', '110', '113', '120', '122', '124',
                                '130', '132', '133', '134', '135', '136',
                                '140', '141', '143', '144', '147', '148']:
            return 'bond'
        else:
            return 'undefined'


def update_security_list():
    with api.connect(best_host[1], best_host[2]):
        data = pd.concat([
            pd.concat(
                [
                    api.to_df(api.get_security_list(market.value, i)).assign(exchange=market.name)
                    for i in range(0, api.get_security_count(market.value), 1000)
                ],
                axis=0, sort=False
            ) for market in MARKET.__members__.values()],
            axis=0,
            sort=False
        )
        data = data.drop_duplicates()
        data = data.loc[:, ['code', 'volunit', 'decimal_point', 'name', 'pre_close', 'exchange']].set_index(['code', 'exchange'], drop=False)
        sz = data.query('exchange == "SZ"')
        sh = data.query('exchange == "SH"')
        sz = sz.assign(sec=sz.code.apply(lambda x: parse_code_type(x, MARKET.SZ)))
        sh = sh.assign(sec=sh.code.apply(lambda x: parse_code_type(x, MARKET.SH)))
        data = pd.concat([sz, sh], axis=0, sort=False).query('sec == "stock"').sort_index()
    # convert the `code` column to string type and save `data` to csv
    data['code'] = data['code'].astype(str)
    data.to_csv(str(_stocklist_save_path), index=False)


def get_stock_list():
    update_security_list()
    # read the csv file as dataframe and retain the `code` column as string type
    raw = pd.read_csv(str(_stocklist_save_path), dtype={'code': str})
    return Stock.from_tdx_dataframe(raw)


def update_candlesticks(interval: str='1d'):
    stock_list = get_stock_list()
    with api.connect(best_host[1], best_host[2]):
        for stock in tqdm(stock_list):
            update_stock_candlesticks(stock, interval=interval)


def update_stock_candlesticks(stock: Stock, interval: str= '1d'):
    tick = datetime.now()
    recent_candle: pd.Series = get_recent_candlestick_record(stock.symbol, interval)
    read_elapse = datetime.now() - tick
    if recent_candle is None:
        recent_datetime = datetime(1998, 1, 1, 0, 0, 0, tzinfo=tz_cn)
    else:
        recent_datetime = recent_candle.name
    if recent_datetime >= get_recent_trading_day():
        logging.debug(f"{stock.symbol} {interval} 已经是最新数据，无需更新")
        return
    logging.debug(f"更新 {stock.symbol} {interval} 数据，最近更新时间为 {recent_datetime}")
    ohlcv = []
    pointer = 0
    tick = datetime.now()
    while True:
        data = api.get_security_bars(tdx_kline_type[interval], stock.market.value, stock.symbol, pointer, 800)
        ohlcv += data
        data = api.to_df(data)
        if data.empty:
            break
        pointer += len(data)
        min_datetime = datetime.strptime(data.iloc[0].datetime, "%Y-%m-%d %H:%M").replace(tzinfo=tz_cn)
        if min_datetime <= recent_datetime:
            break
    ohlcv = api.to_df(ohlcv)
    ohlcv['datetime'] = ohlcv['datetime'].apply(lambda s: datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=tz_cn))
    # sort ohlcv by datetime field
    ohlcv = ohlcv.sort_values(by='datetime', ascending=True)
    # drop columns except datetime, open, high, low, close, vol, amount
    ohlcv = ohlcv.loc[:, ['datetime', 'open', 'high', 'low', 'close', 'vol', 'amount']]
#         save the ohlcv dataframe to influxdb
    download_elapse = datetime.now() - tick
    tick = datetime.now()
    write_candlesticks(stock, interval, ohlcv)
    write_elapse = datetime.now() - tick
    logging.debug(f"读取历史数据耗时 {read_elapse}, 下载数据耗时 {download_elapse}, 写入数据耗时 {write_elapse}")


if __name__ == '__main__':
    pd.set_option("display.max_columns", None)
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s %(filename)s:%(lineno)s %(message)s',
        force=True
    )
    update_candlesticks(interval='1d')
