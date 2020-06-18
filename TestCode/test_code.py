# coding=gbk
from selenium import webdriver
import time
import re
from selenium.webdriver.chrome.options import Options
from scrapy.selector import Selector
from selenium.webdriver.common.by import By
import requests


def req_chrome():
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
    source_url_list = res.xpath("//div[@class='txt-box']/h3/a/@href").extract()

    print(res)
    driver.find_element_by_xpath("//div[@class='txt-box']/h3/a").click()
    print(driver.find_element_by_xpath("//div[@class='txt-box']/h3/a").get(0))
    print(driver.find_element(By.XPATH("//div[@class='txt-box']/h3/a")))
    print(driver.title)
    time.sleep(3)
    driver.close()


def sql_pandas():
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

    ]
    ls_type = len(ls) * [1]
    df = pd.DataFrame({"type": ls_type, "content": ls})
    # df.to_sql("News_Params", if_exists="append", index=False, con=engine_word)
    print(df)


def req(i):
    from fake_useragent import UserAgent
    import json
    ua = UserAgent()
    try:
        headers = ua.random
        # print(ua.random)
    except:
        pass
    url = "http://www.taiwu.com/siteapi/outnetfront/api/property/PropertyListController/getPropertySecondHandList"
    # req = requests.session()
    headers = {
        "Accept": "application/json, text/plain, */*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Content-Length": "435",
        "Content-Type": "application/json;charset=UTF-8",
        "Cookie": "Hm_lvt_bf7258513fd26ee6d3231554baf1a331=1590384930; Hm_lpvt_bf7258513fd26ee6d3231554baf1a331=1590384980",
        "Host": "www.taiwu.com",
        "istravel": '0',
        "Origin": "http://www.taiwu.com",
        "Proxy-Connection": "keep-alive",
        "ref": '0',
        "Referer": "http://www.taiwu.com/ershoufang/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36",
    }
    data = {
        "areaCode": "",
        "averagePrice": [],
        "buildingAge": [],
        "buildingDirectionCode": [],
        "elevatorExist": [],
        "fixTypeCode": [],
        "gongge": 1,
        "intentionalMetroLineCode": "",
        "layerHighLowTypeCode": [],
        "leaseMode": [],
        "leasePriceCode": [],
        "pageNum": 2,
        "pageSize": 1000,
        "propertyLabelFlg": [],
        "propertySquareCode": [],
        "propertySquareSort": None,
        "releaseDateSort": None,
        "roomNumberType": [],
        "roomTypeCode": [],
        "sellPriceCode": [],
        "sellPriceSort": None,
        "unitPriceSort": None,
    }
    data = json.dumps(data)
    # print(data)
    res = requests.post(url=url, headers=headers, data=data)
    # cookies = req.cookies
    print(res)
    res_extract = res.content.decode("utf8", 'ignore')

    # res.encoding = "UTF8"
    # print(cookies)
    return res_extract


def sql():
    from sqlalchemy.types import NVARCHAR, INT, DATETIME
    import pandas as pd
    import pyodbc
    import pymssql
    from sqlalchemy import create_engine
    dtype = {
        "id_third": INT,
        "id_tw": INT,
    }
    host = '10.10.202.13'
    user = 'bigdata_user'
    password = 'ulyhx3rxqhtw'
    database = 'TWSpider'
    with pymssql.connect(host=host, database=database,
                         user=user, password=password, charset="utf8") as conn:
        with conn.cursor() as cursor:
            cursor.execute("select count(*) as n from house_rank")
            res = cursor.fetchone()[0]
            if res:
                print(res)
    # df = pd.DataFrame(columns=['one', 'two', 'three'])
    # print(df)
    # df.to_sql("test", con=engine_res, if_exists="replace", index=False,
    #           dtype=dtype)


def item():
    s = 3
    it = (i for i in range(1, s))
    for i in it:
        s += 1
        print(i)

    # flag_end = len(self.crawler.engine.slot.scheduler)
    # t = self
    # print(dir(t))
    # print(t)
    # item = {}
    # yield item


if __name__ == "__main__":
    pass
    # from concurrent import futures
    #
    # with futures.ThreadPoolExecutor(max_workers=17) as executor:
    #     result = executor.map(req, range(1, 17))
    #     for res in result:
    #         pass
    #         # print(res)
    # print("-----------")
    # for i in range(1, 11):
    #     req(1)
    item()

'''
yum install -y chkconfig python bind-Utils psmisc libxslt zlib sqlite cyrus-sasl-plain cyrus-sasl-qssapi fuse fuse-libs redhat-lsb
'''
