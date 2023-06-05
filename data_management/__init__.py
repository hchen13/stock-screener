from datetime import timedelta

from pytdx.config.hosts import hq_hosts
from pytdx.util.best_ip import ping


def find_best_host():
    print("===== Finding best host =====")
    best = timedelta(seconds=10)
    for name, ip, port in hq_hosts:
        print(f"Testing {name}")
        r = ping(ip, port)
        if r < best:
            best = r
            best_name, best_host, best_port = name, ip, port
        if r < timedelta(seconds=0.2):
            print(f"Host {name} Latency: {r}\n===========================\n")
            return name, ip, port
    return best_name, best_host, best_port


best_host = find_best_host()