from retrying import retry
import time


def retry_if_result_none(result):
    print(result)
    return 1


@retry(stop_max_attempt_number=3, retry_on_result=retry_if_result_none)
def a():
    for i in range(1, 20):
        if i == 5:
            print("gg")
    time.sleep(2)
    return "hah"


def ip_change():
    # 隧道的host与端口
    # 隧道服务器
    tunnel_host = "tps161.kdlapi.com"
    tunnel_port = "15818"

    # 隧道id和密码
    tid = "t18449818935473"
    password = "jg4cg2j9"

    proxies = {
        "http": "http://%s:%s@%s:%s/" % (tid, password, tunnel_host, tunnel_port),
        "https": "http://%s:%s@%s:%s/" % (tid, password, tunnel_host, tunnel_port)
    }
    return proxies


res = ip_change()
print(res)
