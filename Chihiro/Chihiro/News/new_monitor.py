# coding=gbk
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


def ip_change():
    # �����host��˿�
    # ���������
    tunnel_host = "tps161.kdlapi.com"
    tunnel_port = "15818"

    # ���id������
    tid = "t18449818935473"
    password = "jg4cg2j9"

    proxies = {
        "http": "http://%s:%s@%s:%s/" % (tid, password, tunnel_host, tunnel_port),
        "https": "http://%s:%s@%s:%s/" % (tid, password, tunnel_host, tunnel_port)
    }
    return proxies


def req_exception_handle(result):
    flag = result.content.decode("utf8", 'ignore')
    if "�쳣����" in flag:
        print("ip���⣬�������ԡ�����")
        # ����
        # time.sleep(random.uniform(1, 3))
        return 1
    else:
        # ����
        return 0


class News:
    def __init__(self):
        '''
        ��Ƹ���򷿡�����������
        '''

        self.proxies = ip_change()
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
                                       user=self.user, password=self.password, charset="utf8")  # ��������
        self.inserttime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self.connect.cursor() as cur:
            cur.execute(
                "select title,source from News where DATEDIFF(d,[inserttime],GETDATE())=0".format(self.inserttime))
            self.title_source = cur.fetchall()
        self.sql_insert = '''Insert into News(Source,SearchWord,Located,NewUrl,Title,NewLabel,MonitorWord,PublishTime,TitleId,InsertTime) values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        # ������
        self.searchword_list = [
            '̫ƽ����',
            '̫ƽ�󷿲�',
            '̫ƽ���н�',
            '̫����',
            '̫�ݼ���',
            'ݼӢ�ز�',
            'tpy����',
            'tpy�н�',
            'tpy����',
            '̫ƽ�����н�',
        ]
        # 1��������
        self.white_list = [
            "̫ƽ����", '̫ƽ�����', '̫ƽ�����', '̫ƽ������', '̫ƽ������', '̫ƽ�����',
            '������̫ƽ��',
            '̫ƽ���װ', '̫ƽ��֤ȯ', '�׿�һ�а�����ƫ��', '�������', '���Բ��',
            '�����н�',
            '�Ӷ�����һ�����ƭ',
        ]
        # 2��������
        self.black_list = [

        ]
        self.related_list = [
            "̫ƽ��",
            "tpy",
            "TPY",
            "̫��",
            "ݼӢ�ز�",
        ]
        # 2�������
        '''
        �Ӱ�
        '''
        self.monitorword_list = [
            "�巣", "����", "��Ƿ����", "�Ӱ��", "������",
            "ɧ��", "�Բ��", "������", "̫ĳ��", "̫ĳ����",
            "���н�", "�����н�", '��',
            "��̸", "��·", "�ٿͻ�", "����", "����",
            "ƭ��", '����', "����", '����', '��ƭ', '����',
            '����', '��ƭ', 'ƭ����', '�����ը'
        ]
        self.set_list = set()
        self.ANTONYM = ['��']
        self.KEY_WORDS_RE = '|'.join(self.monitorword_list)
        self.ANTONYM_RE = '|'.join(self.ANTONYM)
        self.STOP = '[,����?!��������]'
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

    @retry(stop_max_attempt_number=5, retry_on_result=req_exception_handle)
    def req_delay(self, **kwargs):
        '''
        retry_on_result������ֵֻ��Ϊbool��ָ����������True��������.
        stop_max_attempt_number�������������׳�RetryError[Attempts: 3, Value: hah]
        :param kwargs:
        :return:
        '''
        time.sleep(random.uniform(self.delay_random_interval[0], self.delay_random_interval[1]))
        # print(kwargs)
        response = self.req.get(**kwargs, timeout=5, proxies=self.proxies)
        return response

    def verdict_content(self, content):
        len_white_list = len(self.white_list) - 1
        res = []
        for i in self.related_list:
            if i in content:
                # ���
                flag = 0
                for index, j in enumerate(self.white_list):
                    if j in content:
                        flag += 1
                    elif flag == 0 and len_white_list == index:
                        for k in self.monitorword_list:
                            if k in content:
                                res.append(k)
        return res

    def news_analysis(self, content):

        """�ı��޹أ���ΪTrue"""
        if not bool(re.search('̫.��|ݼӢ', content)):
            return False

        paragraph = re.split(r'\r|\n|<p>', content)
        paragraph = [item for item in paragraph if re.search('̫.��|ݼӢ', item)]

        lines = re.split(self.STOP, ''.join(paragraph))
        lines = [item for item in lines if re.search(self.KEY_WORDS_RE, item)]

        antonym = [item for item in lines if re.search(self.ANTONYM_RE, item)]

        if len(antonym) > len(lines) / 2:
            return False
        return True

    def extract_current_url_sogou(self, searchword, page):
        url_listpage = 'https://weixin.sogou.com/weixin?query={}&type=2&page={}&ie=utf8'.format(
            parse.quote(searchword), str(page))
        headers = {
            'Referer': url_listpage
        }
        headers.update(self.headers_sogou)
        req_kw = {
            "url": url_listpage, "headers": headers
        }
        res = self.req_delay(**req_kw)
        res_text = res.text
        # cookies = res.headers["Set-Cookie"]
        url_list_handle = Selector(text=res_text)
        url_list = url_list_handle.xpath("//div[@class='txt-box']/h3/a/@href").extract()
        return url_list

    def get_real_url_sogou(self, url):
        b = int(random.random() * 100) + 1
        a = url.find("url=")
        url = url + "&k=" + str(b) + "&h=" + url[a + 4 + 21 + b: a + 4 + 21 + b + 1]
        return url

    def item_sogou(self, searchword, page, located, url, item):
        #
        page_url = 'https://weixin.sogou.com/weixin?query={}&type=2&page={}&ie=utf8'.format(
            parse.quote(searchword), str(page))
        headers = {
            'Referer': page_url,
        }
        headers.update(self.headers_sogou)
        req_kw = {
            "url": page_url, "headers": headers
        }
        self.req_delay(**req_kw)

        # 1
        url = "https://weixin.sogou.com" + url
        real_handle_url = self.get_real_url_sogou(url)
        req_kw1 = {
            "url": real_handle_url, "headers": self.headers_sogou
        }
        res1 = self.req_delay(**req_kw1)

        # 2
        fragments = re.findall("url \+= '(.*?)'", res1.text, re.S)
        newurl = ''
        for j in fragments:
            newurl += j
        headers2 = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "zh-CN,zh;q=0.9,en;q=0.8",
            "cache-control": "max-age=0",
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.116 Safari/537.36',
        }
        req_kw2 = {
            "url": newurl,
            "headers": headers2
        }
        res2 = self.req_delay(**req_kw2)
        res2_text = res2.text
        res2_handle = Selector(text=res2_text)

        # title
        title = res2_handle.xpath('//meta[@property="og:title"]/@content').extract_first()
        try:
            content = res2_handle.xpath("//div[@id='js_content']").xpath("string(.)").extract_first()
            content = content.strip()
        except Exception as e:
            content = None
        item.update(
            {
                "title": title,
                "content": content,
                'newurl': newurl,
                'searchword': searchword,
                'located': located,
            }
        )
        return item

    def item_baidu(self, searchword, page):
        url = 'http://www.baidu.com.cn/s?wd=' + parse.quote(searchword) + '&pn={}'.format(str(page))
        req_kw = {
            "url": url,
            'headers': self.headers_baidu
        }
        res = self.req_delay(**req_kw)
        res_handle = Selector(text=res.text)
        ls = res_handle.xpath("//div[@id='content_left']/div/h3/a/@href").extract()
        return ls

    def item_baidu_1(self, searchword, located, url,
                     item):
        req_kw = {
            "url": url,
            'headers': self.headers_baidu
        }
        res = self.req_delay(**req_kw)
        res_extract = res.content.decode("utf-8", 'ignore')
        res_handle = Selector(text=res_extract)
        title = res_handle.xpath("//title/text()").extract_first()
        # extractor = GeneralNewsExtractor()
        # result = extractor.extract(res_extract)
        content = res_extract
        item.update(
            {
                "title": title,
                "content": content,
                'newurl': url,
                'searchword': searchword,
                'located': located,

            }
        )
        return item

    def req_baidu(self):
        self.delay_random_interval = [0, 1]
        for searchword in self.searchword_list:
            start_page = 1
            for page in range(start_page, 11):
                try:
                    url_list = self.item_baidu(searchword=searchword, page=page)
                    for located, url in enumerate(url_list):
                        item = {}
                        located = (page - 1) * 10 + located + 1
                        try:
                            # ��ȡ��Ϣ
                            item = self.item_baidu_1(searchword=searchword, located=located, url=url,
                                                     item=item)
                            print(searchword, located)
                            print(item)
                            # sql_baidu
                            item.update({"source": "�ٶ���ҳ"})
                            title = item.get("title")
                            flag_set = (searchword, title)
                            if flag_set not in self.set_list:
                                searchword_title = searchword + title
                                titleid = hashlib.md5(searchword_title.encode(encoding='UTF-8')).hexdigest()
                                item.update({"titleid": titleid})
                                self.sql(item)
                                self.set_list.add(flag_set)
                        except Exception as e:
                            if isinstance(e, RetryError):
                                print("����ʧ��:{}".format(url))
                            else:
                                print(e, e.__traceback__.tb_lineno)
                except Exception as e:
                    if isinstance(e, RetryError):
                        print("��{}ҳ����ʧ��".format(page))
                    else:
                        print(e, e.__traceback__.tb_lineno)

    def req_sogou(self):
        self.delay_random_interval = [1, 2]
        for searchword in self.searchword_list:
            start_page = 1
            for page in range(start_page, 11):
                # ��ȡ����url
                self.req.cookies.clear()
                try:
                    url_list = self.extract_current_url_sogou(searchword=searchword, page=page)
                    for located, url in enumerate(url_list):
                        item = {}
                        located = (page - 1) * 10 + located + 1
                        try:
                            # ��ȡ��Ϣ
                            self.req.cookies.clear()
                            item = self.item_sogou(searchword=searchword, page=page, located=located, url=url,
                                                   item=item)
                            print(searchword, located)
                            print(item)
                            item.update({"source": "�ѹ�΢��"})
                            # sql_sogou
                            title = item.get("title")
                            flag_set = (searchword, title)
                            if flag_set not in self.set_list:
                                searchword_title = searchword + title
                                titleid = hashlib.md5(searchword_title.encode(encoding='UTF-8')).hexdigest()
                                item.update({"titleid": titleid})
                                self.sql(item)
                                self.set_list.add(flag_set)
                        except Exception as e:
                            if isinstance(e, RetryError):
                                print("����ʧ��:{}".format(url))
                            else:
                                print(e, e.__traceback__.tb_lineno)
                except Exception as e:
                    if isinstance(e, RetryError):
                        print("��{}ҳ����ʧ��".format(page))
                    else:
                        print(e, e.__traceback__.tb_lineno)

    def wordcloud_pd_handle(self):
        ls = [{"source": i[0], "searchword": j[0], "monitorword": pd.value_counts(j[1].split(",")).index,
               "wordcount": pd.value_counts(j[1].split(",")).values}
              for i in self.worddict.items() for j in
              i[1].items()]
        res = pd.DataFrame()
        for k in ls:
            dict_word1 = pd.DataFrame(k)
            res = res.append(dict_word1, ignore_index=True)
        dtype = {
            "source": NVARCHAR(255),
            "searchword": NVARCHAR(255),
            "monitorword": NVARCHAR(255),
            "wordcount": INT,
        }
        res.to_sql("wordcloud", con=self.engine_word, if_exists="replace", index=False, dtype=dtype)

    def wordcloud_dict_handle(self, source, searchword, monitorword):
        if source in self.worddict.keys():
            searchword_dict = self.worddict.get(source)
            if searchword in searchword_dict.keys():
                # �������
                monitorword_str = searchword_dict.get(searchword)
                monitorword_str += "," + monitorword
                searchword_dict.update({searchword: monitorword_str})
            else:
                searchword_dict.update({searchword: monitorword})
        else:
            self.worddict.update({source: {searchword: monitorword}})

    def sql(self, item):
        '''
        SearchWord,Page,Located,NewUrl,Title,NewLabel,MonitorWord,InsertTime,
        :param item:
        :return:
        trueΪ����
        '''
        content = item.get("content")
        monitorword = self.verdict_content(content)
        monitorword = list(set(monitorword))
        # flag_verdict = self.news_analysis(content)
        if monitorword:
            with self.connect.cursor() as cur:
                monitorword = ','.join(monitorword)
                item.update({
                    "monitorword": monitorword
                })
                inserttime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                flag_repeat = (item.get("title"), item.get("source"))
                if flag_repeat not in self.title_source:
                    # cur.execute("Insert into Content(newurl, title, content) values(%s,%s,%s)",
                    #             (item.get("newurl"), item.get("title"), content))
                    '''
                    Source,SearchWord,Located,NewUrl,Title,NewLabel,MonitorWord,PublishTime,TitleId,InsertTime
                    '''
                    source = item.get("source")
                    searchword = item.get("searchword")
                    cur.execute(self.sql_insert, (
                        source,
                        searchword,
                        item.get("located"),
                        item.get("newurl"),
                        item.get("title"),
                        item.get("newlabel"),
                        item.get("monitorword"),
                        item.get('publishtime'),
                        item.get('titleid'),
                        inserttime
                    ))
                    self.connect.commit()
                    self.wordcloud_dict_handle(source=source, searchword=searchword, monitorword=monitorword)


if __name__ == '__main__':
    '''
    �����ʣ� 
        ̫ƽ���ݡ�̫ƽ�󷿲���̫ƽ���н顢̫������̫�ݼ��š�ݼӢ�ز�
    ������
        ��Ƹ��"̫ƽ���巣", "̫ƽ�󷣿�", "̫ƽ����Ƿ����", "̫ƽ��Ӱ�", "̫ƽ���ݼӰ�", "̫ƽ���н", "̫ƽ��Ͷ��", "̫ƽ���ְ", "̫ƽ�󲻿���", "̫ƽ����ְ", "̫ƽ��ӵ�", 
        �򷿣�"̫ƽ����ƭ������",
        ������ "̫ƽ����ƭ�ͻ�",
        ����"tpy���н�", "tpyɧ��", "tpy�Բ��", "tpy��·", "tpy�н��", "tpyӶ��", "tpy������", "tpy��ƭ������", "̫ĳ��", "̫ĳ����", "̫ƽ����н�","̫ƽ��ɧ��", "̫ƽ��Բ��","��ɽݼӢ","̫ƽ���ƭ��", "̫ƽ��Ϸ��", "�����н�̫ƽ��", "̫ƽ���н��",
                 "̫ƽ���̸", "̫ƽ��VIP��", "̫ƽ�����", "̫ƽ����·",  "̫ƽ��ٿͻ�", "̫ƽ�����", "̫ƽ�󱨸�", "�Ϻ�ݼӢ�ز�", "ݼӢ�ز�",
                "̫ƽ���ƭ��", "̫ƽ��Ϸ��", "�����н�̫ƽ��", "̫ƽ���н��", 
    '''
    news = News()
    '''
    pass1��{"�ѹ���ҳ":{"̫ƽ����":["����",'��·','����']},}
    pass2��sql����
    '''

    from apscheduler.schedulers.blocking import BlockingScheduler

    scheduler = BlockingScheduler()
    # ����������д��ǰ
    def start():
        news.req_sogou()
        news.req_baidu()
        news.wordcloud_pd_handle()

    scheduler.add_job(start, 'interval', days=1, start_date='2020-04-08 00:00:00', misfire_grace_time=10)
    scheduler.start()


'''

docker run -id --hostname=quickstart.cloudera --net=bridge --privileged=true -p 8020:8020 -p 7180:7180 -p 21050:21050 -p 8890:8890 -p 10002:10002 -p 25010:25010 -p 25020:25020 -p 18088:18088 -p 8088:8088 -p 19888:19888 -p 7187:7187 -p 11000:11000 -t -p 8888:8888 --name=mycdh3 cloudera/quickstart /usr/bin/docker-quickstart
 ${
 if(len(TitleId) == 0,
 " ",
 "and TitleId in (SELECT VALUE FROM STRING_SPLIT('"+ TitleId +"',','))")
 }
'''
