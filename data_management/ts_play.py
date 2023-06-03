from datetime import datetime

import tushare
from matplotlib import pyplot as plt

TOKEN = "fee4be37c99f362790f575eb3bf8d6d87262a9923982b7fd3e3d6cc5"

tushare.set_token(TOKEN)
pro = tushare.pro_api()


security_list = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
print(security_list)
security_list.to_csv("security_list.csv", index=False)
