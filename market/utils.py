from datetime import timedelta, timezone, datetime

from IPython import get_ipython
from pytdx.hq import TdxHq_API

from market import best_host


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
