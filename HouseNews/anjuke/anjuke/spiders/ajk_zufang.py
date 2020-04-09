# -*- coding: utf-8 -*-

import scrapy
from lxml import etree
import datetime
import random
import base64
from fontTools.ttLib.ttFont import TTFont
from io import BytesIO
import re



class AjkSpider(scrapy.Spider):
    name = 'ajk_zufang'
    # allowed_domains = ['bannan.anjuke.com']
    start_urls = ['https://sh.zu.anjuke.com/?from=navigation']
    custom_settings = {
        # "RETRY_HTTP_CODES": [403, 302, 502],
        # "RETRY_TIMES": 10,
    }

    def __init__(self):
        self.ua = [
            "Mozilla/5.0 (Windows NT 6.3; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36",
            "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:30.0) Gecko/20100101 Firefox/30.0",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/537.75.14",
            "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Win64; x64; Trident/6.0)",
            'Mozilla/5.0 (Windows; U; Windows NT 5.1; it; rv:1.8.1.11) Gecko/20071127 Firefox/2.0.0.11',
            'Opera/9.25 (Windows NT 5.1; U; en)',
            'Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)',
            'Lynx/2.8.5rel.1 libwww-FM/2.14 SSL-MM/1.4.1 GNUTLS/1.2.9',
        ]
        self.count = 0

    def get_headers(self):
        '''
        :return:
        '''
        headers = {
            "accept": "*/*",
            "cache-control": "max-age=0",
            "accept-encoding": "gzip, deflate, br",
            "accept-language": "zh-CN,zh;q=0.9",
            # "cookie": "sessid=D3FA154C-F7E1-52F2-676C-1DC012BC9B96; aQQ_ajkguid=709D6AAF-571A-D14C-F96F-013A77B8F0CC; lps=http%3A%2F%2Fwww.anjuke.com%2F%3Fpi%3DPZ-baidu-pc-all-biaoti%7Chttps%3A%2F%2Fsp0.baidu.com%2F9q9JcDHa2gU2pMbgoY3K%2Fadrc.php%3Ft%3D06KL00c00f7Hj1f0qh-h00PpAsKDXfwX00000cM5yNC00000uN-d_g.THvs_oeHEtY0UWdBmy-bIy9EUyNxTAT0T1dBuWbLmWc3Pj0snADdmW0k0ZRqPb7DwH0kPWIDPHI7wDRLPbNafbP7fbcswDF7nHNan1c0mHdL5iuVmv-b5Hn1nWDvPHndPj0hTZFEuA-b5HDvFMwV5HDhmWYk0ARqpZwYTZnlQzqLILT8my4JIyV-QhPEUitOTAbqR7CVmh7GuZRVTAnVmyk_QyFGmyqYpfKWThnqnWn3n1T%26tpl%3Dtpl_11534_21264_17382%26l%3D1515835273%26attach%3Dlocation%253D%2526linkName%253D%2525E6%2525A0%252587%2525E5%252587%252586%2525E5%2525A4%2525B4%2525E9%252583%2525A8-%2525E6%2525A0%252587%2525E9%2525A2%252598-%2525E4%2525B8%2525BB%2525E6%2525A0%252587%2525E9%2525A2%252598%2526linkText%253D%2525E5%2525AE%252589%2525E5%2525B1%252585%2525E5%2525AE%2525A2-%2525E5%252585%2525A8%2525E6%252588%2525BF%2525E6%2525BA%252590%2525E7%2525BD%252591%2525EF%2525BC%25258C%2525E6%252596%2525B0%2525E6%252588%2525BF%252520%2525E4%2525BA%25258C%2525E6%252589%25258B%2525E6%252588%2525BF%252520%2525E6%25258C%252591%2525E5%2525A5%2525BD%2525E6%252588%2525BF%2525E4%2525B8%25258A%2525E5%2525AE%252589%2525E5%2525B1%252585%2525E5%2525AE%2525A2%2525EF%2525BC%252581%2526xp%253Did%28%252522m3321653540_canvas%252522%29%25252FDIV%25255B1%25255D%25252FDIV%25255B1%25255D%25252FDIV%25255B1%25255D%25252FDIV%25255B1%25255D%25252FDIV%25255B1%25255D%25252FH2%25255B1%25255D%25252FA%25255B1%25255D%2526linkType%253D%2526checksum%253D136%26wd%3D%25E5%25AE%2589%25E5%25B1%2585%25E5%25AE%25A2%26issp%3D1%26f%3D8%26ie%3Dutf-8%26rqlang%3Dcn%26tn%3Dbaiduhome_pg%26inputT%3D1948; twe=2; _ga=GA1.2.1121978770.1577351517; _gid=GA1.2.1689083145.1577351517; 58tj_uuid=1a1fba45-8877-45ea-98cb-8ed1eb5bffb8; als=0; wmda_uuid=8bb205615a2b23ba31938f49295fef7b; wmda_new_uuid=1; wmda_visited_projects=%3B6289197098934; ajk_member_captcha=500f9ee01a071e38a7b3fd6332ac6cae; ctid=11; browse_comm_ids=877628%7C4115%7C370146%7C13777; propertys=w5h0tq-q344vy_vz15az-q344nl_w29qfz-q344nj_v8kbnz-q344nh_w7wajh-q344nd_; wmda_session_id_6289197098934=1577407731013-f471a746-b0c3-41a0; __xsptplusUT_8=1; init_refer=https%253A%252F%252Fshanghai.anjuke.com%252Fsale%252Fpudong%252F; new_uv=2; new_session=0; _gat=1; __xsptplus8=8.5.1577407762.1577407774.3%232%7Csp0.baidu.com%7C%7C%7C%25E5%25AE%2589%25E5%25B1%2585%25E5%25AE%25A2%7C%23%23bBUchTNnvsOLkwoyCQuiVATchJvDjGPd%23; xzfzqtoken=H5UfDE%2B%2BfwNa919azCOIOlx7lp9HQ9xgg02w21ucvwExmcy8EWXnjJOX2oRhtPLHin35brBb%2F%2FeSODvMgkQULA%3D%3D",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "sec-fetch-user": "?1",
            "user-agent": random.choice(self.ua),
            "upgrade-insecure-requests": "1",
        }
        return headers

    def font_desecret(self, url, string_list):
        bin_data = base64.decodebytes(url.encode())
        font = TTFont(BytesIO(bin_data))
        font.saveXML("text.xml")
        font = TTFont(BytesIO(bin_data))
        c = font['cmap'].tables[0].ttFont.tables['cmap'].tables[0].cmap
        res_ls = []
        for string in string_list:
            res_str = ''
            for char in string:
                try:
                    decode_num = ord(char)
                    num = c[decode_num]
                    num = int(num[-2:]) - 1
                except:
                    num = "."
                res_str += str(num)
            res_ls.append(res_str)
        return res_ls

    def start_requests(self):
        yield scrapy.Request(url=self.start_urls[0], callback=self.parse,
                             headers=self.get_headers(), dont_filter=True)

    def parse(self, response):
        tree = etree.HTML(response.text)
        href_list = tree.xpath(
            "//div[@class='div-border items-list']/div[@class='items'][1]/span[@class='elems-l']/div[@class='sub-items sub-level1']/a[position()>1][position()<17]/@href")
        area_list = tree.xpath(
            "//div[@class='div-border items-list']/div[@class='items'][1]/span[@class='elems-l']/div[@class='sub-items sub-level1']/a[position()>1][position()<17]/text()")
        for index, href in enumerate(href_list):
            item = {}
            item['area'] = area_list[index]
            item['city'] = '上海'
            item['resource'] = '安居客'
            yield scrapy.Request(url=href, headers=self.get_headers(), callback=self.handle_plate, dont_filter=True,
                                 meta={'item': item})
            # break

    def handle_plate(self, response):
        tree = etree.HTML(response.text)
        href_list = tree.xpath(
            "//div[@class='div-border items-list']/div[@class='items'][1]/span[@class='elems-l']/div[@class='sub-items sub-level1']/div[@class='sub-items sub-level2']/a[position()>1]/@href")
        area_list = tree.xpath(
            "//div[@class='div-border items-list']/div[@class='items'][1]/span[@class='elems-l']/div[@class='sub-items sub-level1']/div[@class='sub-items sub-level2']/a[position()>1]/text()")
        for index, href in enumerate(href_list):
            item = response.meta['item']
            item['plate'] = area_list[index]
            yield scrapy.Request(url=href, headers=self.get_headers(), callback=self.handle_nextpage, dont_filter=True,
                                 meta={'item': item})

    def handle_nextpage(self, response):
        item = response.meta["item"]
        tree = etree.HTML(response.text)
        href_list = tree.xpath(
            "//div[@class='maincontent']/div[@id='list-content']/div[@class='zu-itemmod']/div[@class='zu-info']/h3/a/@href")
        project_list = tree.xpath(
            "//div[@class='maincontent']/div[@id='list-content']/div[@class='zu-itemmod']/div[@class='zu-info']/h3/a/b[@class='strongbox']/text()")
        for index, href in enumerate(href_list):
            item1 = response.meta["item"]
            item1['house_url'] = href
            item1["house_desc"] = project_list[index]
            yield scrapy.Request(url=href, headers=self.get_headers(), callback=self.handle_detail,
                                 meta={'item': item1})
        next_url = tree.xpath("//a[@class='aNxt']/@href")
        if next_url:
            yield scrapy.Request(url=next_url[0], headers=self.get_headers(), callback=self.handle_nextpage,
                                 meta={"item": item})

    def handle_detail(self, response):
        url = re.search("base64,(.*?)'\)", response.text).group(1)
        item = response.meta['item']
        tree = etree.HTML(response.text)
        string_list = []
        door_model = tree.xpath(
            "//div[@class='lbox']/ul[@class='house-info-zufang cf']/li[@class='house-info-item l-width'][1]/span[@class='info']/b[@class='strongbox']/text()")
        door_model = "".join(door_model)
        string_list.append(door_model)
        build_size = tree.xpath("//span[@class='info-tag no-line']/em/b[@class='strongbox']/text()")[0]
        string_list.append(build_size)
        per_price = tree.xpath("//span[@class='light info-tag']/em/b[@class='strongbox']/text()")[0]
        string_list.append(per_price)
        res_list = self.font_desecret(url=url, string_list=string_list)
        try:
            item["door_model"] = str(res_list[0][0]) + "室" + str(res_list[0][1]) + "厅" + str(res_list[0][2]) + "卫"
        except Exception as e:
            item["door_model"] = None
        # 面积
        item['build_size'] = res_list[1]
        # 单价
        item["per_price"] = res_list[2]
        try:
            # 公司
            item['Pub_company'] = tree.xpath("//div[@class='broker-border']/div[@class='broker-line']/a/@title")[0]
        except:
            item['Pub_company'] = None
        # 经纪人
        try:
            item['agent'] = tree.xpath("//h2[@class='broker-name']/text()")[0].strip()
        except:
            item['agent'] = None
        # 小区
        item['community'] = tree.xpath(
            "//div[@class='lbox']/ul[@class='house-info-zufang cf']/li[@class='house-info-item l-width'][3]/a[@class='link'][1]/text()")[
            0].strip()
        # 朝向
        item['direction'] = tree.xpath(
            "//div[@class='lbox']/ul[@class='house-info-zufang cf']/li[@class='house-info-item'][2]/span[@class='info']/text()")[
            0].strip()
        floor_desc = tree.xpath(
            "//div[@class='lbox']/ul[@class='house-info-zufang cf']/li[@class='house-info-item l-width'][2]/span[@class='info']/text()")[
            0].strip()
        # 总楼层
        item['total_floor'] = re.findall('共(\d+)层', floor_desc)[0]
        try:
            # 当前层
            item['floor'] = re.findall('(\w+)[\(\（]', floor_desc)[0]
        except:
            item['floor'] = 0
        # 装修
        item['decoration'] = tree.xpath("//li[@class='house-info-item'][3]/span[@class='info']/text()")[0].strip()
        item['insert_time'] = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        item['rental_status'] = 1
        item['resource_status'] = '可租'
        self.count += 1
        print(self.count)
        yield item
