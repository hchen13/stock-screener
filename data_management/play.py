from datetime import timedelta, datetime

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

n = 10
with api.connect(best_host[1], best_host[2]):
    tick = datetime.now()
    for _ in range(n):
        res = api.get_security_bars(TDXParams.KLINE_TYPE_DAILY, MARKET.SZ.value, "002273", 0, 800)
    tdx_elapse = datetime.now() - tick
    tdx_res = api.to_df(res)


import tushare

TOKEN = "fee4be37c99f362790f575eb3bf8d6d87262a9923982b7fd3e3d6cc5"

tushare.set_token(TOKEN)
pro = tushare.pro_api()

tick = datetime.now()
for _ in range(n):
    ts_res = tushare.pro_bar(ts_code="002273.SZ", start_date="20220602", end_date="20230602")
ts_elapse = datetime.now() - tick
print(f"tdx: {tdx_elapse / n}, ts: {ts_elapse / n}")
