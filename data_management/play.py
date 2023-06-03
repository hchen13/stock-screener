from datetime import timedelta

import pandas as pd
from pytdx.config.hosts import hq_hosts
from pytdx.hq import TdxHq_API
from pytdx.params import TDXParams
from pytdx.util.best_ip import ping

from data_management.consts import MARKET

pd.set_option("display.max_columns", None)

api = TdxHq_API()

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

best_host = find_best_host()

# vol = 24640080
# circulating_supply = 1355112500
# print(vol / circulating_supply)

with api.connect(best_host[1], best_host[2]):
    res = api.get_security_bars(TDXParams.KLINE_TYPE_DAILY, MARKET.SZ.value, "002273", 0, 10)
    res = api.to_df(res)
    print(res)

    # res = api.get_finance_info(MARKET.SZ.value, "002273")
    # res = api.to_df(res)
    # print(res)
    # print(res.iloc[0].liutongguben)

    # res = api.get_company_info_category(TDXParams.MARKET_SZ, "002273")
    # res = api.to_df(res)
    # print(res)
    # for i, row in res.iterrows():
    #     print(f"====== {row['name']} ======")
    #     info = api.get_company_info_content(TDXParams.MARKET_SZ, "002273", row.filename, row.start, row.length)
    #     print(info)
