# -*- coding: utf-8 -*-
import random
from io import BytesIO
from html.parser import HTMLParser
import datetime
import pymssql
from fontTools.ttLib.ttFont import TTFont
import base64
from copy import deepcopy
import scrapy
import re
import json
from scrapy import Item, Field

from scrapy.loader import ItemLoader
from ..items import ChihiroItem
import pandas as pd
from sqlalchemy import create_engine
import datetime

class Chihiro(scrapy.Spider):
    name = 'anjuke_esf'
    start_urls = ['https://m.anjuke.com/sh/sale/?from=anjuke_home/']
    PropertyCity = '上海'
    Resource = '安居客'
    RentalStatus = 0
    HouseStatus = '可售'
    ua = [
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36 OPR/26.0.1656.60",
        "Opera/8.0 (Windows NT 5.1; U; en)",
        "Mozilla/5.0 (Windows NT 5.1; U; en; rv:1.8.1) Gecko/20061208 Firefox/2.0.0 Opera 9.50",
        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; en) Opera 9.50",
        "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:34.0) Gecko/20100101 Firefox/34.0",
        "Mozilla/5.0 (X11; U; Linux x86_64; zh-CN; rv:1.9.2.10) Gecko/20100922 Ubuntu/10.10 (maverick) Firefox/3.6.10",
        "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
        "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11",
        "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.16 (KHTML, like Gecko) Chrome/10.0.648.133 Safari/534.16"
    ]
    custom_settings = {
        # 基本参数
        "BOT_NAME": name,
        "PropertyCity": PropertyCity,
        "Resource": Resource,
        "RentalStatus": RentalStatus,
        "HouseStatus": HouseStatus,
        # 请求参数
        "DEFAULT_REQUEST_HEADERS": {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "zh-CN,zh;q=0.9",
            "cookie": "sessid=8D7C8A6B-7779-548B-094C-C3CB5F0FDB9B; aQQ_ajkguid=70F5E5F1-F40E-DBBB-AAAB-7094AB594957; lps=http%3A%2F%2Fwww.anjuke.com%2F%3Fpi%3DPZ-baidu-pc-all-biaoti%7Chttps%3A%2F%2Fsp0.baidu.com%2F9q9JcDHa2gU2pMbgoY3K%2Fadrc.php%3Ft%3D06KL00c00f7Hj1f0qh-h00PpAsj8o0dX00000cM5yNC00000vb3aI6.THvs_oeHEtY0UWdBmy-bIfK15ynLPycdrjDYnj0sPWbsPyD0IHYYP1DLrjKjPYNjfHTsP1-APYnLnY7AwRm4wjRkPHPKPsK95gTqFhdWpyfqn1nznHmdn1RYnausThqbpyfqnHmhIAYqniuB5HD0uHdCIZwsT1CEQLILIz49UhGdpvR8mvqVQ1qspHdfyBdBmy-bIidsmzd9UAsVmh-9ULwG0APzm1Ykn1T%26tpl%3Dtpl_11534_21264_17382%26l%3D1515835273%26attach%3Dlocation%253D%2526linkName%253D%2525E6%2525A0%252587%2525E5%252587%252586%2525E5%2525A4%2525B4%2525E9%252583%2525A8-%2525E6%2525A0%252587%2525E9%2525A2%252598-%2525E4%2525B8%2525BB%2525E6%2525A0%252587%2525E9%2525A2%252598%2526linkText%253D%2525E5%2525AE%252589%2525E5%2525B1%252585%2525E5%2525AE%2525A2-%2525E5%252585%2525A8%2525E6%252588%2525BF%2525E6%2525BA%252590%2525E7%2525BD%252591%2525EF%2525BC%25258C%2525E6%252596%2525B0%2525E6%252588%2525BF%252520%2525E4%2525BA%25258C%2525E6%252589%25258B%2525E6%252588%2525BF%252520%2525E6%25258C%252591%2525E5%2525A5%2525BD%2525E6%252588%2525BF%2525E4%2525B8%25258A%2525E5%2525AE%252589%2525E5%2525B1%252585%2525E5%2525AE%2525A2%2525EF%2525BC%252581%2526xp%253Did%28%252522m3321653540_canvas%252522%29%25252FDIV%25255B1%25255D%25252FDIV%25255B1%25255D%25252FDIV%25255B1%25255D%25252FDIV%25255B1%25255D%25252FDIV%25255B1%25255D%25252FH2%25255B1%25255D%25252FA%25255B1%25255D%2526linkType%253D%2526checksum%253D136%26ie%3Dutf-8%26f%3D3%26tn%3Dbaidu%26wd%3D%25E5%25AE%2589%25E5%25B1%2585%25E5%25AE%25A2%26oq%3Drequest.Meta%28%252526%25252339%25253BPATH_INFO%252526%25252339%25253B%29%26rqlang%3Dcn%26inputT%3D2090%26prefixsug%3Danjuk%252520%26rsp%3D0; twe=2; _ga=GA1.2.1604733208.1577954544; _gid=GA1.2.1092178387.1577954544; 58tj_uuid=51406026-f2b6-4de0-91af-54157a7a0ded; als=0; ctid=11; app_cv=unknown; ajk_member_captcha=8fb607d69420cc2464ed565602a83ac9; propertys=wbxvnz-q3icbr_wahiii-q3h3fz_; init_refer=; new_uv=3; wmda_uuid=0aaf8e2fdb9279beff99f55df08af07e; wmda_new_uuid=1; wmda_session_id_6145577459763=1578022398335-f7b1c845-ab2e-d042; new_session=0; __xsptplus8=8.3.1578022400.1578022400.1%232%7Csp0.baidu.com%7C%7C%7C%25E5%25AE%2589%25E5%25B1%2585%25E5%25AE%25A2%7C%23%23OQs5xYd2JmNoiUmQhFEjUrv8pyVQyBuv%23; ctid=11; wmda_session_id_8146401978551=1578022442566-b5bbfe2e-e184-a9fe; wmda_visited_projects=%3B6145577459763%3B8146401978551; xzfzqtoken=QDXyvFW1tRBHVmNf7xSnO2diT3DEFMjMpy7MFGE4cMs48QX6fjVimh7a%2BJi8ZkG8in35brBb%2F%2FeSODvMgkQULA%3D%3D",
            # "referer": "https://shanghai.anjuke.com/sale/p3/",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "user-agent": random.choice(ua),
            # "x-requested-with": "XMLHttpRequest",
            "Host": "m.anjuke.com"
        },
        "DOWNLOAD_DELAY": 0.2,
        "CONCURRENT_REQUESTS": 5,
        "RETRY_HTTP_CODES": [403, 502],
        "RETRY_TIMES": 1,
        "DOWNLOADER_MIDDLEWARES": {
            'Chihiro.middleware_request.IpAgent_Middleware': 543,
        },
        # 清洗参数
        "SPIDER_MIDDLEWARES": {
            # 'Chihiro.middleware_item.ChihiroSpiderMiddleware': 500,
        },
        # 业务参数
        # "ITEM_PIPELINES": {
        #     'Chihiro.middleware_sql.ChihiroPipeline': 300,
        # },
        # 错误记录
        # ERROR_RECORD = True
        # 日志
        "LOG_LEVEL": 'INFO',
        "LOG_FILE": "anjuke.txt"
    }

    xx = {'shanghaizhoubian': '上海周边',
          'jiading': '嘉定',
          'pudong': '浦东',
          'hongkou': '虹口',
          'minhang': '闵行',
          'baoshan': '宝山',
          'qingpu': '青浦',
          'putuo': '普陀',
          'huangpu': '黄浦',
          'chongming': '崇明',
          'fengxian': '奉贤',
          'jinshan': '金山',
          'changning': '长宁',
          'jingan': '静安',
          'yangpu': '杨浦',
          'songjiang': '松江',
          'xuhui': '徐汇'}

    def __init__(self):
        host = '10.10.202.13'
        user = 'bigdata_user'
        password = 'ulyhx3rxqhtw'

        # host = '10.55.5.7'
        # user = 'tw_user'
        # password = '123456'

        database = 'TWSpider'
        self.scaned_url_list = []
        self.engine_third_house = create_engine(
            'mssql+pymssql://{}:{}@{}/{}'.format(user, password, host, database))
        self.sql_select = pd.read_sql(
            "select HouseUrl,HouseStatus from ThirdHouseResource where Resource='%s' and RentalStatus = %s" % (
                self.Resource, self.RentalStatus),
            self.engine_third_house)
        self.url_list = self.sql_select["HouseUrl"].tolist()

    def parse(self, response):
        href_list = response.xpath(
            '//div[@class="none js-block-list block-list"]//div[@class="options-list-item "]//a/@href')
        for href in href_list:
            area_url = href.extract()
            if re.findall('shanghaizhoubian', area_url):
                print('上海周边', area_url)
            else:
                for page in range(1, 60):
                    next_url = area_url + '?page=' + str(page)
                    area = next_url.split('sale/')[1].split('-q')[0]
                    print(next_url, '======================', area)
                    item = Item()
                    item.fields["AreaName"] = Field()
                    item["AreaName"] = self.xx[area]
                    yield scrapy.Request(next_url, callback=self.second_parse,
                                         dont_filter=True, meta={'item': item})
                # break
            # break

    def second_parse(self, response):
        item = response.meta['item']
        text = response.text
        with open('1.html', 'w+', encoding='utf-8')as fp:
            fp.write(response.text)
        flag = re.findall('<div class="noresult-img"></div>', text)
        if not flag:
            # print('非末页', response.url, flag)
            item_url_list = response.xpath('//a[@class="base house-item fix-clear"]/@href')
            for item_url in item_url_list:
                url = item_url.extract()
                yield scrapy.Request(url, callback=self.third_parse, meta={'item': item})
                # break
        else:
            print('末页', response.url, flag)

    def third_parse(self, response):
        with open('3.html', 'w+', encoding='utf-8')as fp:
            fp.write(response.text)
        item = response.meta['item']
        # try:
        house_url = response.url.split('/?isauction=')[0]
        item.fields["HouseUrl"] = Field()
        item.fields["HouseDesc"] = Field()
        item.fields["TotalPrice"] = Field()
        item.fields["HouseType"] = Field()
        item.fields["BuildingSquare"] = Field()
        item.fields["PubCompany"] = Field()
        item.fields["Agent"] = Field()
        item.fields["PropertyCommunity"] = Field()
        item.fields["PropertyAddress"] = Field()
        item.fields["PriceUnit"] = Field()
        item.fields["TotalFloor"] = Field()
        item.fields["Floor"] = Field()
        item.fields["HouseDirection"] = Field()

        item['HouseUrl'] = house_url
        try:
            item['HouseDesc'] = response.xpath('//div[@class="house-address"]/text()').extract_first().strip()
        except:
            item['HouseDesc'] = None

        try:
            item['TotalPrice'] = response.xpath(
                '//div[@class="house-data"]/span[@class="fl"]/text()').extract_first().strip()
        except:
            item['TotalPrice'] = None

        try:
            item['HouseType'] = response.xpath(
                '//div[@class="house-data"]/span[last()]/text()').extract_first().strip()
        except:
            item['HouseType'] = None

        try:
            item['HouseDesc'] = response.xpath('//div[@class="house-address"]/text()').extract_first().strip()
        except:
            item['HouseDesc'] = None

        try:
            item['BuildingSquare'] = response.xpath(
                '//div[@class="house-data"]/span[@class="fr"]/text()').extract_first().strip()
        except:
            item['BuildingSquare'] = None

        try:

            item['PubCompany'] = response.xpath(
                '//li[@class="broker-info-item textflow"][1]/text()').extract_first().strip()
        except:
            item['PubCompany'] = None

        try:
            item['Agent'] = response.xpath('//span[@class="broker-name textflow"]/text()').extract_first().strip()
        except:
            item['Agent'] = None
        try:
            item['PropertyCommunity'] = response.xpath(
                '//li[@class="info-long-item"]/a[@class="to-other-link"]/text()').extract_first().strip()
        except:
            item['PropertyCommunity'] = None
        try:
            PropertyAddress = response.xpath('//li[@class="info-long-item"][2]/text()').extract_first().strip()
            item['PropertyAddress'] = re.findall('\（(.*?)\）', PropertyAddress)[0]
        except:
            item['PropertyAddress'] = None

        try:
            item['PriceUnit'] = response.xpath(
                '//ul[@class="info-list"]//li[1]/text()').extract_first().strip()
        except:
            item['PriceUnit'] = None
        try:
            floor_desc = response.xpath('//ul[@class="info-list"]//li[3]/text()').extract_first().strip()
            item['TotalFloor'] = re.findall('共(\d+)层', floor_desc)[0]
        except:
            item['TotalFloor'] = None
        try:
            item['Floor'] = re.findall('(\w+)[\(\（]', floor_desc)[0]
        except:
            item['Floor'] = 0
        try:
            item['HouseDirection'] = response.xpath('//ul[@class="info-list"]//li[2]/text()').extract_first().strip()
        except:
            item['HouseDirection'] = None
        if self.is_finished():
            self.do_final()
        yield item

    def is_finished(self):
        if self.crawler.engine.downloader.active:
            return False
        if self.crawler.engine.slot.start_requests is not None:
            return False
        if self.crawler.engine.slot.scheduler.has_pending_requests():
            return False
        return True

    def do_final(self):
        '''
        更新可售为已售、可租为已租
        :param cursor:
        :param spider:
        :return:
        '''
        housing_trade_list = [x for x in self.url_list if x not in self.scaned_url_list]
        for housing_url in housing_trade_list:
            yield scrapy.Request(url=housing_url, callback=self.house_status_handle)

    def house_status_handle(self, response):
        # 验证码
        url = response.url
        item = {}
        flag = response.xpath("//div[@class='house-address']/text()").extract_first()
        if flag:
            pass
        else:
            item["flag_remaining"] = True
            item["HouseUrl"] = url
            yield item