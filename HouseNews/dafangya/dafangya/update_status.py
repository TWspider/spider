import datetime
import json

import jsonpath
import pymssql
import requests

base_url = "https://www.dafangya.com/api/v2/search/list?level=12&ot=0&pnr=-1%7C-1&bnr=-1%7C-1&tnr=-1%7C-1&fr=-1%7C-1&bar=-1%7C-1&pdr=-1&pr=-1%7C-1&ar=-1%7C-1&dt=-1&hf=&hut=&ll=&sort=houseFrom%2Casc&sort=publishDate%2Cdesc&sort=auto&size=1000&page={}&q=1&ele=&latL=31.043706&lonL=121.376391&latR=31.408334&lonR=121.564963"
headers = {
    'Accept': 'application/json; version=2.0; charset=utf-8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9',
    'Host': 'www.dafangya.com',
    'Referer': 'https://www.dafangya.com/initSearch.html?businessType=0',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/77.0.3865.90 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest',
    'yys': '0',
}

web_url_set = set()
# 获取网页中的在售的url
for i in range(0, 70):
    url = base_url.format(str(i))
    response = requests.get(url=url, headers=headers)
    try:
        if response.status_code == 200:
            obj = json.loads(response.text)
            house_url_list = jsonpath.jsonpath(obj, '$..shortLink')
            for i in house_url_list:
                web_url_set.add(i)
    except:
        pass
    print(len(web_url_set))
# 连接数据库
conn = pymssql.connect("pymssql", host='10.10.202.12', database='TWSpider',
                       user='bigdata', password='pUb6Qfv7BFxl', charset="utf8", )
cur = conn.cursor()
cur.execute("select  HouseUrl FROM ThirdHouseResource where Resource='大房鸭' and HouseStatus='可售'")
sql_url_set = set([url_tuple[0] for url_tuple in cur.fetchall()])
# 对比数据库不存在于web中的记录
for url in sql_url_set:
    if url not in web_url_set:
        update_sql = "UPDATE ThirdHouseResource SET HouseStatus='{}',UpdateTime='{}' WHERE HouseUrl='{}' collate Chinese_PRC_CS_AI_WS".format(
            '已售',
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            url)
        print(update_sql)
        cur.execute(update_sql)
        conn.commit()

conn.close()
cur.close()
