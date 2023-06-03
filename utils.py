from datetime import timedelta, timezone, datetime

from IPython import get_ipython


def check_environment():
    try:
        ipython = get_ipython()
        if "IPKernelApp" in ipython.config or "ZMQInteractiveShell" in str(type(ipython)):
            return "jupyter"
        return "IPython terminal"
    except:
        return "terminal"


tz_cn = timezone(timedelta(hours=8))
tz_utc = timezone(timedelta(hours=0))


def get_recent_trading_day(check_date: datetime=None) -> datetime:
    ''' 获取最近一个交易日 '''
    if check_date is None:
        check_date = datetime.now(tz_cn)

    # tushare API改为需要2000积分才能使用，暂时改为简单判断
    # df = pro.trade_cal(
    #     exchange="SSE",
    #     start_date=check_date.strftime("%Y%m%d"),
    #     end_date=check_date.strftime("%Y%m%d")
    # )
    # logging.debug(df)
    # if df.iloc[0]['is_open'] == 1 and check_date.hour >= 15:
    #     return check_date.date()
    # return datetime.datetime.strptime(df.iloc[0]['pretrade_date'], "%Y%m%d").date()

    if check_date.weekday() in [5, 6]:
        return get_recent_trading_day(check_date - timedelta(days=1))
    if check_date.hour >= 15:
        return check_date.replace(hour=15, minute=0, second=0, microsecond=0)
    return (check_date - timedelta(days=1)).replace(hour=15, minute=0, second=0, microsecond=0)
