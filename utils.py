from datetime import timedelta, timezone, datetime

from IPython import get_ipython
from pytdx.hq import TdxHq_API


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


def get_recent_trading_day(check_date: datetime=None, offline=False) -> datetime:
    ''' 获取最近一个交易日 '''
    if check_date is None:
        check_date = datetime.now(tz_cn)

    if offline:
        # 如果是离线模式，简单通过是否为周末判断
        if check_date.weekday() in [5, 6]:
            return get_recent_trading_day(
                check_date.replace(hour=15, minute=0, second=0, microsecond=0) - timedelta(days=1), offline=offline)
        if check_date.hour >= 15:
            return check_date.replace(hour=15, minute=0, second=0, microsecond=0)
        return (check_date - timedelta(days=1)).replace(hour=15, minute=0, second=0, microsecond=0)

    # 如果是在线模式，通过通达信获取上证指数行情，找到最近的交易日
    api = TdxHq_API()
    from data_management.tdx_core import best_host
    with api.connect(best_host[1], best_host[2]):
        ex, code = 1, "000001"
        pointer = 0
        while True:
            res = api.to_df(api.get_index_bars(9, ex, code, pointer, 800))
            if res.empty:
                break
            res["datetime"] = res["datetime"].apply(
                lambda dt_str: datetime.strptime(dt_str, "%Y-%m-%d %H:%M").replace(tzinfo=tz_cn)
            )
            # find the first row whose datetime is less than or equal to check_date
            # and return the date of the previous row
            try:
                return res[res["datetime"] <= check_date].iloc[-1]["datetime"]
            except IndexError:
                pass
            pointer += 800

