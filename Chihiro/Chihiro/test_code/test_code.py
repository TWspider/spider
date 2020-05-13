# coding=gbk
from selenium import webdriver
import time
from selenium.webdriver.chrome.options import Options
from scrapy.selector import Selector
from selenium.webdriver.common.by import By
import requests


def t1():
    chrome_options = Options()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--headless')

    # chrome_options.add_argument('blink-settings=imagesEnabled=false')
    # chrome_options.add_argument('--disable-gpu')

    driver = webdriver.Chrome(chrome_options=chrome_options)
    driver.get("https://weixin.sogou.com/")
    # driver.find_element_by_xpath("//input[@id='query']").send_keys("太平洋房屋")
    driver.find_element_by_xpath("//input[@class='swz']").click()
    res_txt = driver.page_source
    res = Selector(text=res_txt)
    # source_url_list = res.xpath("//div[@class='txt-box']/h3/a/@href").extract()

    # print(res)
    # driver.find_element_by_xpath("//div[@class='txt-box']/h3/a").click()
    # print(driver.find_element_by_xpath("//div[@class='txt-box']/h3/a").get(0))
    # print(driver.find_element(By.XPATH("//div[@class='txt-box']/h3/a")))
    # print(driver.title)
    time.sleep(3)
    driver.close()


'''
g85WbNOTtWi1cyaAkFk-4IszmrLHFBAaXknh*XhBnzUPR2cLf-sdYFXvRSKywHZU3sIzb-cLBIYZNWqAu0NURLvBNO2*3ExxDmUmy5a1eKRfHANkzw9HPemyOayzKK-Y

g85WbNOTtWi1cyaAkFk-4IszmrLHFBAaXknh*XhBnzUPR2cLf-sdYFXvRSKywHZU3sIzb-cLBIYZNWqAu0NURLvBNO2*3ExxDmUmy5a1eKRfHANkzw9HPemyOayzKK-Y
'''


def t2():
    import pyodbc
    import pandas as pd
    from sqlalchemy import create_engine
    host = '10.10.202.13'
    user = 'bigdata_user'
    password = 'ulyhx3rxqhtw'
    database = 'TWSpider'

    engine_res = create_engine(
        'mssql+pyodbc://{0}:{1}@{2}/{3}?driver=SQL Server Native Client 11.0'.format(user, password, host,
                                                                                     database),
        fast_executemany=True)
    sl = "select content from News_Params where type={}"
    searchword_list = pd.read_sql(
        sl.format(1),
        engine_res)
    print(searchword_list.loc[:, "content"].tolist())
    print(type(searchword_list.loc[:, "content"].tolist()))


import random


class T():
    def __init__(self):
        self.test = self.t()

    def a(self):
        print(self.test)

    @classmethod
    def t(cls):
        b = random.randint(1, 23)
        return b


unrelated_keyword = '篱笆社区|分享新浪微博|引用只看此人|下一页第页确定|注册日期|第\d+楼|被.+编辑过'

import requests
import re
from scrapy.selector import Selector


def t_req_content():
    headers_baidu = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Cache-Control": "max-age=0",
        "Connection": "keep-alive",
        # "Host": "www.baidu.com",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.122 Safari/537.36",
    }
    url = "http://www.baidu.com/link?url=Zhy6tjzBb2gc71LlGaX6ULs6DfySEhWBEofPm_ux4lil0uQr_Pn_NpJQ98GXg9EwN9PgfYdAAkoAHkZiuagAh_"
    # url = "http://www.fangchan.com/news/132/2019-07-09/6554250444678696994.html"

    res = requests.get(url=url, headers=headers_baidu)
    res_text = res.content.decode("utf8", 'ignore')
    res_xml = Selector(text=res_text)

    res = res_xml.xpath("//div").xpath("string(.)").extract()
    res_line = []
    for i in res:
        res_clean = re.sub(r"\s+", '\n', i)
        res_clean = re.sub(r"[^0-9a-zA-Z\u4e00-\u9fa5\.%]+", '', res_clean)
        res_split = []
        for item in res_clean.split('\n'):
            if len(item) < 10:
                continue

            if re.search(unrelated_keyword, item):
                continue

            if re.search('[\u4e00-\u9fa5]', item):
                res_split.append(item)

        res_line.extend(res_split)

    res_line = set(res_line)
    print(res_line)


def t3():
    '''
    导入数据到表内
    :return:
    '''
    import pandas as pd
    from sqlalchemy.types import NVARCHAR, INT
    from sqlalchemy import create_engine
    host = '10.10.202.13'
    database = 'TWSpider'
    user = 'bigdata_user'
    password = 'ulyhx3rxqhtw'
    engine_word = create_engine(
        'mssql+pymssql://{}:{}@{}/{}'.format(user, password, host, database))
    ls = [
        "不.*找.*太平洋",
        "不要.*太平洋",
        "别找.*太平洋",
        "压价.*太平洋",
        "吐槽.*太平洋",
        "呵呵.*太平洋",
        "垃圾.*太平洋",
        "太平洋.*不.*怎么样",
        "太平洋.*不要脸",
        "太平洋.*做的出",
        "太平洋.*伎俩",
        "太平洋.*保平安",
        "太平洋.*值.*[么吗]",
        "太平洋.*假",
        "太平洋.*假",
        "太平洋.*傻逼",
        "太平洋.*公司.*问题",
        "太平洋.*名声.*差",
        "太平洋.*呵呵",
        "太平洋.*嚣张",
        "太平洋.*垃圾",
        "太平洋.*奇葩",
        "太平洋.*套路",
        "太平洋.*威胁",
        "太平洋.*屁",
        "太平洋.*恶劣",
        "太平洋.*打压",
        "太平洋.*打架",
        "太平洋.*托",
        "太平洋.*扮演",
        "太平洋.*投诉",
        "太平洋.*折磨",
        "太平洋.*报警",
        "太平洋.*招数",
        "太平洋.*整改",
        "太平洋.*无耻",
        "太平洋.*智障",
        "太平洋.*最差",
        "太平洋.*最没有",
        "太平洋.*有问题",
        "太平洋.*服务.*差",
        "太平洋.*毫无底线",
        "太平洋.*水分.*大",
        "太平洋.*没一个",
        "太平洋.*没完没了",
        "太平洋.*法院",
        "太平洋.*洗地",
        "太平洋.*洗脑",
        "太平洋.*流氓",
        "太平洋.*涉事",
        "太平洋.*烂",
        "太平洋.*盯.*[上我]",
        "太平洋.*立案",
        "太平洋.*素质",
        "太平洋.*纠纷",
        "太平洋.*罚",
        "太平洋.*虚假",
        "太平洋.*虚报",
        "太平洋.*要钱",
        "太平洋.*谨慎",
        "太平洋.*负面",
        "太平洋.*责令",
        "太平洋.*跟踪",
        "太平洋.*轰炸",
        "太平洋.*辱骂",
        "太平洋.*这素质",
        "太平洋.*退回",
        "太平洋.*退款",
        "太平洋.*造假",
        "太平洋.*陌生号码",
        "太平洋.*风波",
        "太平洋.*骗",
        "太平洋.*麻烦",
        "太平洋.*黑",
        "恐怖.*太平洋",
        "扰乱.*市场.*太平洋",
        "投诉.*太平洋",
        "最差.*太平洋",
        "游说.*太平洋",
        "虚假.*太平洋",
        "调查.*太平洋",
        "责令.*太平洋",
        "远离.*太平洋",
        "违法.*太平洋",
        "骗.*太平洋",
        "黑.*太平洋"
    ]
    ls_type = len(ls) * [1]
    df = pd.DataFrame({"type": ls_type, "content": ls})
    # df.to_sql("News_Params", if_exists="append", index=False, con=engine_word)
    print(df)


def t4():
    s = "s<so>ogao"
    ls = s.split("<so>")
    print(ls)


def sk():
    pass


def t5(obj):
    from urllib import parse
    import hashlib
    url = "http://api.map.baidu.com/place/v2/search?query=" + obj + "&region=289&city_limit=true&output=json&ak=1wqVM7ZDWaTLkqBacqKhO15W3zl7zB24&scope=1&page_size=20&page_num=1"
    resp1 = requests.get(url)
    resp1_str = resp1.json()
    res = len(resp1_str.get("results"))
    print(res)
    return resp1_str


def t6():
    url = "https://sh.centanet.com/xiaoqu/xq-pebawewaws"
    headers = {
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Accept-Encoding': '*/*',
        'accept-language': 'gzip',
        'content-type': 'charset=utf8',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.88 Safari/537.36',
        'Cookie': "gr_user_id=8cfc279d-37f1-4214-a9c4-3bc18729c385; grwng_uid=96cd6356-a60d-4b6b-ab22-361f1d1d839a; Y190cmFja2lk=73a9977002ec49b68dcc0fdde38ccc17; acw_tc=65597d1615705879540916094ee97fc1cd6e308264fee86e78dba6a105; Hm_lvt_219872fb6de637cac5884769682da5ad=1570587955,1571127896,1572328984; gioClientCookie=d29e91b3-d697-4177-a822-38ffe0ab608d; _pk_ref.10.5e68=%5B%22%22%2C%22%22%2C1572418167%2C%22http%3A%2F%2Fdefault.centanet.com%2F%22%5D; _pk_ses.10.5e68=*;_pk_id.10.5e68=91dbe8d1a8824461.1570587955.18.1572419485.1572328990.; dft034f=Wu%2FMiTlp99s0GdWA9dWmKg__; Hm_lpvt_219872fb6de637cac5884769682da5ad=1572419485",
    }
    res = requests.get(url=url, headers=headers)
    res_extract = res.content.decode("utf8", 'ignore')
    # res.encoding = "UTF8"
    print(res_extract)

def t7():
    a =None
    if "" in a:
        print(a)


if __name__ == "__main__":
    # t_req_content()
    # res = t5("链家地产(高")
    # print(res)
    t7()

'''
yum install -y chkconfig python bind-utils psmisc libxslt zlib sqlite cyrus-sasl-plain cyrus-sasl-qssapi fuse fuse-libs redhat-lsb
'''
