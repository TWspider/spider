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


t2()

'''

yum install -y chkconfig python bind-utils psmisc libxslt zlib sqlite cyrus-sasl-plain cyrus-sasl-qssapi fuse fuse-libs redhat-lsb
'''
