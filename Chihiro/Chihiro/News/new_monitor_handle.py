import requests
import random
import time
import datetime
import re
import hashlib
import pymssql
from urllib import parse
from scrapy.selector import Selector
import pandas as pd
from sqlalchemy.types import NVARCHAR, INT
from sqlalchemy import create_engine
from gne import GeneralNewsExtractor
from retrying import retry, RetryError


class News:
    def __init__(self):
        '''
        招聘、买房、卖房、形象
        搜索词：0
        相关词：1
        负面词：2
        白名单：3
        '''

        self.delay_random_interval = [2, 3]
        self.req = requests.session()
        self.worddict = {}
        self.host = '10.10.202.13'
        self.database = 'TWSpider'
        self.user = 'bigdata_user'
        self.password = 'ulyhx3rxqhtw'
        self.engine_word = create_engine(
            'mssql+pymssql://{}:{}@{}/{}'.format(self.user, self.password, self.host, self.database))
        self.connect = pymssql.connect(host=self.host, database=self.database,
                                       user=self.user, password=self.password, charset="utf8")  # 建立连接
        self.inserttime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self.connect.cursor() as cur:
            cur.execute(
                "select title,source from News where DATEDIFF(d,[inserttime],GETDATE())=0".format(self.inserttime))
            self.title_source = cur.fetchall()
        self.sql_insert = '''Insert into News(Source,SearchWord,Located,NewUrl,Title,NewLabel,MonitorWord,PublishTime,TitleId,InsertTime) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

        self.sql_content_list = "select content from News_Params where type={}"
        # 搜索词0
        self.searchword_list = pd.read_sql(
            self.sql_content_list.format(0),
            self.engine_word).loc[:, "content"].tolist()
        # 相关词1
        self.related_list = pd.read_sql(
            self.sql_content_list.format(1),
            self.engine_word).loc[:, "content"].tolist()
        # 负面词2
        '''
        加班
        '''
        self.monitorword_list = pd.read_sql(
            self.sql_content_list.format(2),
            self.engine_word).loc[:, "content"].tolist()
        # 白名单3
        self.white_list = pd.read_sql(
            self.sql_content_list.format(3),
            self.engine_word).loc[:, "content"].tolist()
        # 黑名单4
        self.black_list = [

        ]

        self.set_list = set()
        self.ANTONYM = ['不']
        self.KEY_WORDS_RE = '|'.join(self.monitorword_list)
        self.ANTONYM_RE = '|'.join(self.ANTONYM)
        self.STOP = '[,，。?!，．？！]'
        self.headers_sogou = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'zh-CN,zh;q=0.9',
            'Connection': 'keep-alive',
            'Host': 'weixin.sogou.com',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'same-origin',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.116 Safari/537.36',
        }
        self.headers_baidu = {
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

    def start(self):
        pass