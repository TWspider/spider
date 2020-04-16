# -*- coding: utf-8 -*-
import datetime
import json
import time

import scrapy

from dafangya.config import dfy_headers
from dafangya.logic import get_sqlserver_conn, get_redis_conn
from dafangya.logic import get_home_data, save_redis, get_plate_data

# 创建数据库连接
from lxml import etree
from scrapy_redis.spiders import RedisSpider

conn, cur = get_sqlserver_conn()
# 将数据库里的数据添加到redis中，做缓存
redis_conn = get_redis_conn()
save_redis(cur, redis_conn)
# 用来判断房源是否已经存在于数据库中
cur.execute("select HouseUrl FROM ThirdHouseResource where Resource='大房鸭' ")
sql_url_set = set([url_tuple[0] for url_tuple in cur.fetchall()])
# 过滤网页中重复的记录
web_url_set = set()


class DfySpider(RedisSpider):
    name = 'dfy'
    allowed_domains = ['www.dafangya.com']
    # start_urls = ['https://www.dafangya.com']
    redis_key = 'myspider:start_urls'

    def parse(self, response):
        base_url = 'https://www.dafangya.com/api/v2/search/list?level=12&ot=0&pnr=-1%7C-1&bnr=-1%7C-1&tnr=-1%7C-1&fr=-1%7C-1&bar=-1%7C-1&pdr=-1&pr=-1%7C-1&ar=-1%7C-1&dt=-1&hf=&hut=&ll=&sort=houseFrom%2Casc&sort=publishDate%2Cdesc&sort=auto&size=1000&page={}&q=1&ele=&latL=31.043706&lonL=121.376391&latR=31.408334&lonR=121.564963'
        # 总套数
        total_num = response.xpath('//span[@id="onlinenum"]/text()').extract_first()
        total_page = int(total_num) // 1000
        for i in range(total_page):
            next_url = base_url.format(i)
            yield scrapy.Request(url=next_url, callback=self.second_parse, headers=dfy_headers, dont_filter=True)

    def second_parse(self, response):
        obj = json.loads(response.text)
        data = get_home_data(obj)
        for index, url in enumerate(data['house_url']):
            # TODO 新增判断网页重复
            if url not in web_url_set:
                if url not in sql_url_set:
                    item = {}
                    item['city'] = data['city'][index]
                    item['area'] = data['area'][index]
                    item['road'] = data['road'][index]
                    item['house_url'] = data['house_url'][index]
                    item['build_time'] = data['build_time'][index]
                    item['release_time'] = time.strftime("%Y-%m-%d",
                                                         time.localtime(int(str(data['release_time'][index])[:-3])))
                    item['addr'] = data['addr'][index]
                    item['floor'] = data['floor'][index]
                    item['total_price'] = data['total_price'][index]
                    item['build_size'] = data['build_size'][index]
                    item['community'] = data['community'][index]
                    item['elevator'] = data['elevator'][index]
                    item['resource_status'] = data['resource_status'][index]
                    try:
                        next_url = "https://www.dafangya.com" + url
                        web_url_set.add(url)  # TODO 测试一下
                    except:
                        continue
                    yield scrapy.Request(url=next_url,
                                         callback=self.third_parse,
                                         meta={'item': item}, dont_filter=True)
                else:
                    resource_status = data['resource_status'][index]
                    sql_resource_status = redis_conn.hget(url, 'house_status').decode()
                    if resource_status != None and sql_resource_status == '可售':
                        id = redis_conn.hget(url, 'id').decode()
                        update_sql = "UPDATE ThirdHouseResource SET HouseStatus='已售',UpdateTime='{}' WHERE id='{}'".format(
                            '已售',
                            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            id)
                        cur.execute(update_sql)
                        cur.commit()
            else:
                print('已存在')

    def third_parse(self, response):
        if response.status == 200:
            tree = etree.HTML(response.text)
            item = response.meta['item']
            # 获取次页数据
            item = get_plate_data(tree, item)
            if item != None:
                yield item
