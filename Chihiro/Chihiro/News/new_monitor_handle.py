'''


负负面词
http://www.baidu.com/link?url=9QZOcp_QDR6HTMh_2P1_7pPqFmt7SLKZ6yt_dj4LRIm__3mowelotbQakFSVR-NQyKqiok7EMktHL45S1KZqLNrdasPPHtYeBB-QIJ5G5dC
偏见：不靠谱？其实经纪人们很专业的
一套1000万的房子，你收我30万的中介费？你们这是抢劫吧
http://www.baidu.com/link?url=PgNxLCd6c3uXMfxPkM0dCvYcc9lPnw3OLpwC2AA_KhgKFoU-FTKJYLAsk70PO5X04WaAIKQyoump5UBuZ2biO_
没听说过，你想干嘛？Q房，家家顺，中原这些可以，珠三角地区我可以帮你打理房产
楼上的真的是无脑黑，房屋中介这行你不诚信能活下去？太平洋在上海能生存22年说明有实力也有口碑
太平洋房屋中介非常不靠谱，不仅是中介费要的最高，后期会给你增加各种费用，让买家卖家都进退不得，我就是因为不熟悉中介的事白白被坑了2万多，希望不要再有人上当
不靠谱
https://mp.weixin.qq.com/s/WtGe7SnxmIGgnm-mzMBxJA
不靠谱
http://mp.weixin.qq.com/s?src=11&timestamp=1584526692&ver=2224&signature=RNfYGC25Kpvow2xaNS6GqwTckr6*WoD5nkxD0YQ6igUQ-nX-RGI3pcGLxVlJ7t1mzk0*QOX5DLMTJ6TAGwhMx1bqv929uNlQj7P7SskRKE1Mq28TEstPD4-FTb2v699e&new=1
吃差价
http://www.baidu.com/link?url=Fk_R3B05tv2ct3MsBy2j8j8s46qTkASBFEtAdJyeAxq_LW7b-gZJlNHggV-VlE9ZPtAw-5ZXS8mq8V6q7ajm7K
加班
https://mp.weixin.qq.com/s/I-G8qet0jlJxs_ik3RrtYA
佣金
http://www.baidu.com/link?url=HJPmabFETfcooRqn_GY1xdPfK-yPtg48RnKzEPHScLhNxZH4tkhdXhEbshwi1GHDn85m1IQJU1zlbcuv66EMbq
体罚,加班,辞职,翻脸
https://mp.weixin.qq.com/s/Ip9zMFu8HLr-Efhh6ywn3w
坑、套路（房产行业问答）
坑和tpy在不同的问题上
https://www.bilibili.com/video/av74299919
坑爹

'''


import requests
import random
import time
import datetime
import re
import pymssql
from urllib import parse
from scrapy.selector import Selector

from gne import GeneralNewsExtractor
from retrying import retry, RetryError


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


def req_exception_handle(result):
    flag = result.content.decode("utf8")
    if "异常访问" in flag:
        print("ip被封，正在重试。。。")
        # 重试
        # time.sleep(random.uniform(1, 3))
        return 1
    else:
        # 跳出
        return 0


class News:
    def __init__(self):
        '''
        招聘、买房、卖房、形象
        '''
        self.proxies = ip_change()
        self.delay_random_interval = [2, 3]
        self.req = requests.session()
        self.connect = pymssql.connect(host='10.10.202.13', database='TWSpider',
                                       user='bigdata_user', password='ulyhx3rxqhtw', charset="utf8")  # 建立连接
        self.inserttime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        with self.connect.cursor() as cur:
            cur.execute(
                "select title,source from News where DATEDIFF(d,[inserttime],GETDATE())=0".format(self.inserttime))
            self.title_source = cur.fetchall()
        self.sql_insert = '''Insert into News(Source,SearchWord,Page,Located,NewUrl,Title,NewLabel,MonitorWord,InsertTime) values(%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        # 搜索词
        self.searchword_list = [
            '太平洋房屋',
            '太平洋房产',
            '太平洋中介',
            '太屋网',
            '太屋集团',
            '菁英地产'
        ]
        # 1、太平洋
        self.related_not_list = [
            "太平洋保险", '太平洋金融', '太平洋理财', '太平洋人寿', '太平洋汽车', '太平洋霸主', '爱茉莉太平洋'
        ]
        self.related_list = [
            "太平洋",
            "tpy",
            "太屋",
            "菁英地产",
        ]
        # 2、负面词
        '''
        加班
        '''
        self.monitorword_list = [
            "体罚", "罚款", "拖欠工资", "加班", "投诉", "不靠谱",
            "骚扰", "吃差价", "恶作剧", "太某屋", "太某洋房屋",
            "黑中介", "无良中介", '坑',
            "假谈", "套路", "假客户", "二卡", "报复",
            "骗子", '恶劣', "忽悠", '翻脸', '欺骗', '辱骂',
            '处罚'
        ]
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
        retry_on_result：返回值只能为bool，指定函数返回True，则重试.
        stop_max_attempt_number：超过最大次数抛出RetryError[Attempts: 3, Value: hah]
        :param kwargs:
        :return:
        '''
        time.sleep(random.uniform(self.delay_random_interval[0], self.delay_random_interval[1]))
        # print(kwargs)
        response = self.req.get(**kwargs, timeout=5, proxies=self.proxies)
        return response

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
        # result = extractor.extract(html=res3_text)
        item.update(
            {
                "title": title,
                "content": content,
                'newurl': newurl,
                'searchword': searchword,
                'page': page,
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

    def item_baidu_1(self, searchword, page, located, url,
                     item):
        req_kw = {
            "url": url,
            'headers': self.headers_baidu
        }
        res = self.req_delay(**req_kw)
        res_text = res.text
        res_extract = res.content.decode("utf-8")
        res_handle = Selector(text=res_text)
        title = res_handle.xpath("//title/text()").extract_first()
        extractor = GeneralNewsExtractor()
        result = extractor.extract(res_extract)
        content = result.get("content")
        item.update(
            {
                "title": title,
                "content": content,
                'newurl': url,
                'searchword': searchword,
                'page': page,
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
                        try:
                            # 获取信息
                            item = self.item_baidu_1(searchword=searchword, page=page, located=located, url=url,
                                                     item=item)
                            print(searchword, page, located)
                            print(item)
                            # sql_baidu
                            item.update({"source": "百度网页"})
                            self.sql(item)
                        except Exception as e:
                            if isinstance(e, RetryError):
                                print("重试失败:{}".format(url))
                            else:
                                print(e, e.__traceback__.tb_lineno)
                except Exception as e:
                    if isinstance(e, RetryError):
                        print("第{}页重试失败".format(page))
                    else:
                        print(e, e.__traceback__.tb_lineno)

    def req_sogou(self):
        self.delay_random_interval = [1, 2]
        for searchword in self.searchword_list:
            start_page = 1
            for page in range(start_page, 11):
                # 获取单条url
                self.req.cookies.clear()
                try:
                    url_list = self.extract_current_url_sogou(searchword=searchword, page=page)
                    for located, url in enumerate(url_list):
                        item = {}
                        try:
                            # 获取信息
                            self.req.cookies.clear()
                            item = self.item_sogou(searchword=searchword, page=page, located=located, url=url,
                                                   item=item)
                            print(searchword, page, located)
                            print(item)
                            item.update({"source": "搜狗微信"})
                            # sql_sogou
                            self.sql(item)
                        except Exception as e:
                            if isinstance(e, RetryError):
                                print("重试失败:{}".format(url))
                            else:
                                print(e, e.__traceback__.tb_lineno)
                except Exception as e:
                    if isinstance(e, RetryError):
                        print("第{}页重试失败".format(page))
                    else:
                        print(e, e.__traceback__.tb_lineno)

    def sql(self, item):
        '''
        SearchWord,Page,Located,NewUrl,Title,NewLabel,MonitorWord,InsertTime,
        :param item:
        :return:
        true为负面
        '''
        content = item.get("content")
        monitorword = [k for i in self.related_list if i in content for j in self.related_not_list if j not in content
                       for k in self.monitorword_list if k in content]
        monitorword = list(set(monitorword))
        if monitorword:
            pass


if __name__ == '__main__':
    '''
    关键词： 
        太平洋房屋、太平洋房产、太平洋中介、太屋网、太屋集团、菁英地产
    监测包：
        招聘："太平洋体罚", "太平洋罚款", "太平洋拖欠工资", "太平洋加班", "太平洋房屋加班", "太平洋底薪", "太平洋投诉", "太平洋辞职", "太平洋不靠谱", "太平洋离职", "太平洋坑爹", 
        买房："太平洋欺骗消费者",
        卖房： "太平洋欺骗客户",
        形象："tpy黑中介", "tpy骚扰", "tpy吃差价", "tpy套路", "tpy中介费", "tpy佣金", "tpy恶作剧", "tpy欺骗消费者", "太某屋", "太某洋房屋", "太平洋黑中介","太平洋骚扰", "太平洋吃差价","宝山菁英","太平洋大骗子", "太平洋戏精", "无良中介太平洋", "太平洋中介费",
                 "太平洋假谈", "太平洋VIP房", "太平洋独家", "太平洋套路",  "太平洋假客户", "太平洋二卡", "太平洋报复", "上海菁英地产", "菁英地产",
                "太平洋大骗子", "太平洋戏精", "无良中介太平洋", "太平洋中介费", 
    '''
    news = News()
    '''
    pass1：req_exception_handle，ip更换
    pass2：sql部分
    '''
    # 请求间隔长的写在前
    news.req_sogou()
    news.req_baidu()
'''

docker run -id --hostname=quickstart.cloudera --net=bridge --privileged=true -p 8020:8020 -p 7180:7180 -p 21050:21050 -p 8890:8890 -p 10002:10002 -p 25010:25010 -p 25020:25020 -p 18088:18088 -p 8088:8088 -p 19888:19888 -p 7187:7187 -p 11000:11000 -t -p 8888:8888 --name=mycdh3 cloudera/quickstart /usr/bin/docker-quickstart

'''




