from selenium import webdriver
import time
from selenium.webdriver.chrome.options import Options
from scrapy.selector import Selector
from selenium.webdriver.common.by import By


def t1():
    chrome_options = Options()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--headless')

    # chrome_options.add_argument('blink-settings=imagesEnabled=false')
    # chrome_options.add_argument('--disable-gpu')

    driver = webdriver.Chrome(chrome_options=chrome_options)
    driver.get("https://weixin.sogou.com/")
    driver.find_element_by_xpath("//input[@id='query']").send_keys("太平洋房屋")
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


def t_req_content():
    import requests
    from lxml import etree
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
    print(res_text)
    res_xml = etree.HTML(res_text)
    res = res_xml.xpath("//div")
    for i in res:
        res1 = i.xpath("string(.)").replace(" ", '').replace("\n", '')
        if res1 != '' or res1 != None:
            pass
            # print(res1)


def t_req_content_1():
    # -*- coding: utf-8 -*-
    pass


t_req_content()
'''
yum install -y chkconfig python bind-utils psmisc libxslt zlib sqlite cyrus-sasl-plain cyrus-sasl-qssapi fuse fuse-libs redhat-lsb
'''
