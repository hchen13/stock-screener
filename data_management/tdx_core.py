import logging
import os
import re
from datetime import datetime
from pathlib import Path

import pandas as pd
from pytdx.crawler.history_financial_crawler import HistoryFinancialListCrawler, HistoryFinancialCrawler
from pytdx.hq import TdxHq_API
from pytdx.params import TDXParams
from tqdm import tqdm

from data_management import best_host
from data_management.consts import Stock, MARKET
from data_management.influx_engine import get_recent_candlestick_record, write_candlesticks, write_supply
from utils import tz_cn, get_recent_trading_day

api = TdxHq_API()

_stocklist_save_path = Path(__file__).parent / '_cache' / 'stocklist.tdx.csv'
financial_list_checksum_path = Path(__file__).parent / '_cache' / 'financial.checksum.csv'
financial_archive_dir = Path(__file__).parent / '_cache' / 'financials'
financial_archive_dir.mkdir(exist_ok=True, parents=True)


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
        data_list = api.get_security_bars(tdx_kline_type[interval], stock.market.value, stock.symbol, pointer, 800)
        data = api.to_df(data_list)
        if len(data) == 0 or data is None or data.empty or data_list is None or len(data_list) == 0:
            break
        ohlcv += data_list
        pointer += len(data)
        min_datetime = datetime.strptime(data.iloc[0].datetime, "%Y-%m-%d %H:%M").replace(tzinfo=tz_cn)
        if min_datetime <= recent_datetime:
            break
    ohlcv = api.to_df(ohlcv)
    try:
        ohlcv['datetime'] = ohlcv['datetime'].apply(lambda s: datetime.strptime(s, "%Y-%m-%d %H:%M").replace(tzinfo=tz_cn))
    except Exception as e:
        logging.error(f"Error {e}: stock = {stock} ohlcv = {ohlcv}")
        return
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


def update_supply(stock: Stock):
    """ 获取股票当前流通股本和总股本并更新数据 """
    with api.connect(best_host[1], best_host[2]):
        data = api.to_df(api.get_finance_info(stock.market.value, stock.symbol))
        print(data)
        circulating_supply = data.iloc[0].liutongguben
        total_supply = data.iloc[0].zongguben
        print(f"{total_supply:,} {circulating_supply:,}")
        recent_trading_day = get_recent_trading_day(offline=False)
        write_supply(stock, total_supply, circulating_supply, recent_trading_day)


def parse_financial_date(filename: str):
    pattern = "(?<=gpcw)\d{8}"
    matches = re.findall(pattern, filename)
    date_str = matches[0]
    financial_datetime = datetime.strptime(date_str, "%Y%m%d").replace(hour=15, tzinfo=tz_cn)
    return financial_datetime


def is_financial_updated(filename, hash):
    if os.path.exists(str(financial_list_checksum_path)):
        checksum = pd.read_csv(str(financial_list_checksum_path))
        record = checksum[checksum['filename'] == filename]
        if len(record) != 1:
            return False
        recorded_hash = record.iloc[0]['hash']
        logging.debug(f"Financial file {filename} recorded hash and current hash: \n"
                      f"{recorded_hash} {hash}\n"
                      f"diff = {recorded_hash == hash}")
        return recorded_hash != hash
        # return (record['hash'] != hash).all().item()
    return True


def save_supply_history(financials, stocks, financial_datetime):
    for stock in stocks:
        if stock.code != "002273":
            continue
        try:
            report = financials.loc[stock.code]
        except KeyError:
            continue
        total_supply = report.loc['col238']
        circulating_supply = report.loc['col266']
        if circulating_supply == 0 and total_supply != 0:
            circulating_supply = total_supply
        write_supply(stock, total_supply, circulating_supply, financial_datetime)
        break


def update_supply_history():
    """ 通过下载历史财务数据补充过去股本数据 """
    list_crawler = HistoryFinancialListCrawler()
    financial_list = pd.DataFrame(list_crawler.fetch_and_parse())
    data_crawler = HistoryFinancialCrawler()
    stocks = get_stock_list()
    for i, (idx, row) in enumerate(financial_list.iterrows()):
        logging.info(f"正在处理历史财务数据文件({i + 1}/{len(financial_list)}): {row.filename}")
        financial_datetime = parse_financial_date(row.filename)
        if financial_datetime > datetime.now(tz_cn):
            continue
        if not is_financial_updated(row.filename, row.hash):
            continue
        download_path = financial_archive_dir / row.filename
        financials = data_crawler.fetch_and_parse(
            filename=row.filename,
            download_path=str(download_path),
            filesize=row.filesize
        )
        financials = data_crawler.to_df(financials)
        if financials is None:
            continue
        save_supply_history(financials, stocks=stocks, financial_datetime=financial_datetime)
    financial_list.to_csv(str(financial_list_checksum_path), index=False)


if __name__ == '__main__':
    pd.set_option("display.max_columns", None)
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s %(levelname)s %(filename)s:%(lineno)s %(message)s',
        force=True
    )
    stock = Stock("688420", 'SH')
    # update_supply(stock)
    update_supply_history()