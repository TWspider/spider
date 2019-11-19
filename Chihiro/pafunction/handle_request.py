import random
import time


class RequestHandle(object):
    def __init__(self):
        self.USERAGENT_LIST = [
            "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:30.0) Gecko/20100101 Firefox/30.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/537.75.14",
            "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Win64; x64; Trident/6.0)",
            'Mozilla/5.0 (Windows; U; Windows NT 5.1; it; rv:1.8.1.11) Gecko/20071127 Firefox/2.0.0.11',
            'Opera/9.25 (Windows NT 5.1; U; en)',
            'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)',
            'Mozilla/5.0 (compatible; Konqueror/3.5; Linux) KHTML/3.5.5 (like Gecko) (Kubuntu)',
        ]

    def get_header(self):
        headers = {
            'User-Agent': random.choice(self.USERAGENT_LIST),
        }
        return headers

    def get_cookies(self):
        t = int(time.time())
        cookies = '_ga=GA1.2.206133092.1570515306; yfx_c_g_u_id_10000001=_ck19100814150715757814535041135; yfx_key_10000001=%25e6%2588%2591%25e7%2588%25b1%25e6%2588%2591%25e5%25ae%25b6; yfx_mr_f_n_10000001=baidu%3A%3Amarket_type_ppzq%3A%3A%3A%3Abaidu_ppc%3A%3A%25e6%2588%2591%25e7%2588%25b1%25e6%2588%2591%25e5%25ae%25b6%3A%3A%3A%3A%25E6%25A0%2587%25E9%25A2%2598%3A%3Asp0.baidu.com%3A%3A%3A%3A%3A%3A%25E5%25B7%25A6%25E4%25BE%25A7%25E6%25A0%2587%25E9%25A2%2598%3A%3A%25E6%25A0%2587%25E9%25A2%2598%3A%3A179%3A%3Apmf_from_adv%3A%3Ash.5i5j.com%2F; yfx_mr_n_10000001=baidu%3A%3Amarket_type_ppzq%3A%3A%3A%3Abaidu_ppc%3A%3A%25e6%2588%2591%25e7%2588%25b1%25e6%2588%2591%25e5%25ae%25b6%3A%3A%3A%3A%25E6%25A0%2587%25E9%25A2%2598%3A%3Asp0.baidu.com%3A%3A%3A%3A%3A%3A%25E5%25B7%25A6%25E4%25BE%25A7%25E6%25A0%2587%25E9%25A2%2598%3A%3A%25E6%25A0%2587%25E9%25A2%2598%3A%3A179%3A%3Apmf_from_adv%3A%3Ash.5i5j.com%2F; smidV2=20191008141508bf80bc8aac216b0c6563b998e96f665600d9e27936cc54430; _wjstatis=409b6d2b-9154-71d9-f33b-c2878d14b63f; __TD_deviceId=50SR09R8NFGOL3V4; zufang_BROWSES=43459385%2C43227996; ershoufang_BROWSES=43466268%2C500331774%2C40149742%2C38622365%2C500053479%2C43538691%2C500187116%2C43546959%2C43526851%2C42879295%2C40579143%2C43391615; PHPSESSID=lnlqevd7gpq56lei1q8qq04a46; _gid=GA1.2.18787267.1572334418; Hm_lvt_94ed3d23572054a86ed341d64b267ec6=1570529110,1570587978,1571208748,1572334425; _Jo0OQK=50188F7A26A7CF7086D77A999941F28C683A10BE7DEEA0274EAA32212134FFC370580654710FA91E77B3DBE1E5A5D4E68336FED3A336801E16A92AC947E67811672A3A6B6373DCEA275A28E4E02FA79F6B8A28E4E02FA79F6B8832E09A7BEBE53E1A31E6B115E0A3E43GJ1Z1NQ==; isClose=yes; domain=sh; yfx_f_l_v_t_10000001=f_t_1570515307574__r_t_1572403385601__v_t_1572412624629__r_c_9; Hm_lpvt_94ed3d23572054a86ed341d64b267ec6={}'.format(
            str(t))
        headers_i5j = {'Cookie': cookies}
        return headers_i5j

    def get_ipaddress(self):
        pass


if __name__ == '__main__':
    handle_r = RequestHandle()
    headers = handle_r.get_header()
    print(headers)
