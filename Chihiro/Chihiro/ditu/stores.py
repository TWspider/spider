import sys

sys.path.append("../..")
from swift.sql import sql_ditu

import jsonpath
import requests
import datetime
from jsonpath import jsonpath
from scrapy.selector import Selector


def baidu(ls):
    headers = {
        # 'Connection': 'keep-alive',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36',
        'Accept': '*/*',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Dest': 'empty',
        'Referer': 'https://map.baidu.com/search/%E9%93%BE%E5%AE%B6/@13519255.82,3633244.67,12z?querytype=con&from=webmap&c=289&wd=%E9%93%BE%E5%AE%B6&pn=22&nn=210&db=0&sug=0&addr=0&da_src=shareurl&on_gel=1&src=7&gr=3&l=12&device_ratio=2',
        'Accept-Language': 'zh-CN,zh;q=0.9',
    }
    s = requests.session()
    s.keep_alive = False
    for i in ls:
        wd = i.get("wd")
        Company = i.get("Company")
        for i in range(0, 100):
            print(i)
            if i > 1:
                pn = i + 1
                nn = i * 10
            else:
                pn = i
                nn = pn * 10
            params = (
                ('pn', '{pn}'.format(pn=str(pn))),
                ('nn', '{nn}'.format(nn=str(nn))),
                ('newmap', '1'),
                ('reqflag', 'pcmap'),
                ('biz', '1'),
                ('from', ['webmap', 'webmap']),
                ('da_par', 'direct'),
                ('pcevaname', 'pc4.1'),
                ('qt', 's'),
                ('da_src', 'searchBox.button'),
                ('wd', wd),
                ('c', '289'),
                ('src', '0'),
                ('wd2', ''),
                ('sug', '0'),
                ('l', '14'),
                ('b', '(13502322,3618789;13520242,3643173)'),
                ('biz_forward', '/{"scaler":2,"styles":"pl"/}'),
                ('sug_forward', ''),
                ('auth',
                 'Ib5ULR6eP0WAbzMT1FKWHRb93D1USd @ euxHTxHTNLxTt1qo6DF == C1GgvPUDZYOYIZuVt1cv3uVtGccZcuVtPWv3Guxt58Jv7uUvhgMZSguxzBEHLNRTVtcEWe1GD8zv7u @ ZPuEthyHxhjzgjyBKWBKWQOYWxk1dK84yDFICquTTGdFrZZWuV'),
                ('device_ratio', '2'),
                ('tn', 'B_NORMAL_MAP'),
                ('u_loc', '13520357,3635775'),
                ('ie', 'utf-8'),
                ('t', '1591597646869'),
            )
            response = s.get('https://map.baidu.com/', headers=headers, params=params)
            res_json = response.json()
            # test1 = jsonpath(res_json, '$.addrs')
            # print(test1)
            res_ls = jsonpath(res_json, '$.content[*]')
            if res_ls:
                for res in res_ls:
                    item = {}
                    flag = jsonpath(res, '$.di_tag')[0]
                    if '房产中介' in flag:
                        StoreName = jsonpath(res, '$.name')[0]
                        StoreAddr = jsonpath(res, '$.addr')[0]
                        Longitude = jsonpath(res, '$.x')[0]
                        if Longitude:
                            Longitude = list(str(Longitude))
                            Longitude.insert(-2, '.')
                            Longitude = ''.join(Longitude)
                        Latitude = jsonpath(res, '$.y')[0]
                        if Latitude:
                            Latitude = list(str(Latitude))
                            Latitude.insert(-2, '.')
                            Latitude = ''.join(Latitude)
                        Status = jsonpath(res, '$.status')[0]
                        TelPhone = jsonpath(res, '$.ext.detail_info.phone')[0]
                        InsertTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        item["Company"] = Company
                        item["StoreName"] = StoreName
                        item["StoreAddr"] = StoreAddr
                        item["Longitude"] = Longitude
                        item["Latitude"] = Latitude
                        item["Status"] = Status
                        item["TelPhone"] = TelPhone
                        item['InsertTime'] = InsertTime
                        sql_ditu.handle_lo_la(item)
            else:
                print("最大页数")
                break


def iecity(url, Company):
    base_url = 'http://m.iecity.com/shanghai/life/'
    headers = {
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Linux; Android 5.0; SM-G900P Build/LRX21T) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Mobile Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        # 'Referer': 'http://m.iecity.com/shanghai/life/Cate-------------------c1b4bcd2-4.html',
        'Accept-Language': 'zh-CN,zh;q=0.9',
    }

    response = requests.get(url,
                            headers=headers, verify=False)
    response.encoding = 'gb18030'
    res = Selector(response=response)
    res_ls = res.xpath("//ul[@class='LifeList']/a/li")
    print(url)
    for res in res_ls:
        item = {}
        StoreName = res.xpath("./h5/text()").extract_first()
        StoreAddr = res.xpath("./div/div[1]/span/text()").extract_first()
        flag_type = res.xpath("./div/div[2]/text()").extract_first()
        if flag_type:
            if '中介' in flag_type:
                item["Company"] = Company
                item["StoreName"] = StoreName
                item["StoreAddr"] = StoreAddr
                get_x_y(item=item, wd=StoreAddr)
    next_page = res.xpath("//a[@rel='next']/@href").extract_first()
    if next_page:
        next_url = base_url + next_page
        iecity(url=next_url, Company=Company)


def get_x_y(item, wd):
    headers = {
        'Connection': 'keep-alive',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.61 Safari/537.36',
        'Accept': '*/*',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Dest': 'empty',
        # 'Referer': 'https://map.baidu.com/search/%E5%B9%BF%E4%B8%AD%E8%B7%AF598%E5%8F%B7/@13522382.115,3647548.76,19z?querytype=s&da_src=shareurl&wd=%E5%B9%BF%E4%B8%AD%E8%B7%AF598%E5%8F%B7&c=289&src=0&pn=0&sug=0&l=13&b=(13505897.46,3621783.94;13552713.46,3646167.94)&from=webmap&biz_forward=%7B%22scaler%22:2,%22styles%22:%22pl%22%7D&device_ratio=2',
        'Accept-Language': 'zh-CN,zh;q=0.9',
    }

    params = (
        ('newmap', '1'),
        ('reqflag', 'pcmap'),
        ('biz', '1'),
        ('from', ['webmap', 'webmap']),
        ('da_par', 'after_baidu'),
        ('pcevaname', 'pc4.1'),
        ('qt', 's'),
        ('da_src', 'searchBox.button'),
        ('wd', wd),
        ('c', '289'),
        ('src', '0'),
        ('wd2', ''),
        ('pn', '0'),
        ('sug', '0'),
        ('l', '19'),
        ('b', '(13522223.115,3647358.26;13522541.115,3647739.26)'),
        ('biz_forward', '/{"scaler":2,"styles":"pl"/}'),
        ('sug_forward', ''),
        ('auth',
         'VD2wW05WBgvRBE6cFQJNgQLdO@b6CNL9uxHTxRHERVVtBnlQADZZz1GgvPUDZYOYIZuVt1cv3uVtGccZcuVtPWv3GuztQZ3wWvUvhgMZSguxzBEHLNRTVtcEWe1GD8zv7u@ZPuVteuxtf0wd0vyIUySIFOUOuuyWWJ0IcvY1SGpuxEtEjjg2J'),
        ('device_ratio', '2'),
        ('tn', 'B_NORMAL_MAP'),
        ('nn', '0'),
        ('u_loc', '13520351,3635774'),
        ('ie', 'utf-8'),
        ('t', '1591854801846'),
    )

    response = requests.get('https://map.baidu.com/', headers=headers, params=params)
    res_json = response.json()
    res_ls = jsonpath(res_json, '$.addrs[*]')
    if res_ls:
        res = res_ls[0]
        Longitude = jsonpath(res, '$.x')[0]
        if Longitude:
            Longitude = list(str(Longitude))
            Longitude.insert(-2, '.')
            Longitude = ''.join(Longitude)
        Latitude = jsonpath(res, '$.y')[0]
        if Latitude:
            Latitude = list(str(Latitude))
            Latitude.insert(-2, '.')
            Latitude = ''.join(Latitude)
        item["Longitude"] = Longitude
        item["Latitude"] = Latitude
    InsertTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    item['InsertTime'] = InsertTime
    sql_ditu.handle_lo_la(item)


if __name__ == '__main__':
    ls_baidu = [
        {
            "wd": "链家"
            , 'Company': "链家"
        },
        {
            "wd": "我爱我家"
            , 'Company': "我爱我家"
        },
        {
            "wd": "中原地产"
            , 'Company': "中原地产"
        },
    ]

    baidu(ls_baidu)

    ls_iecity = [
        {
            "Company": "链家",
            "url": "http://m.iecity.com/shanghai/life/Cate-------------------c1b4bcd2-1.html",
        },
        {
            "Company": "中原地产",
            "url": "http://m.iecity.com/shanghai/life/Cate-------------------d6d0d4adb5d8b2fa-1.html",
        },
        {
            "Company": "我爱我家",
            "url": "http://m.iecity.com/shanghai/life/Cate-------------------ced2b0aeced2bcd2-1.html",
        },
    ]

    for ie in ls_iecity:
        Company = ie.get("Company")
        url = ie.get("url")
        iecity(Company=Company, url=url)
