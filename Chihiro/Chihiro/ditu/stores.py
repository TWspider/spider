import requests
import pymssql
from sqlalchemy import create_engine
from jsonpath import jsonpath


def store(cursor):
    cookies = {
        'routeiconclicked': '1',
        'PSTM': '1590659988',
        'BIDUPSID': '6FBF449654651402107372BC91E5F0C3',
        'BDORZ': 'B490B5EBF6F3CD402E515D22BCDA1598',
        'delPer': '0',
        'PSINO': '5',
        'ZD_ENTRY': 'baidu',
        'BDRCVFR[feWj1Vr5u3D]': 'I67x6TjHwwYf0',
        'BCLID': '11128140246756703374',
        'BDSFRCVID': 'UB4OJeC62614zAoucSvLMoIZZh479JRTH6ao3-oTVgsNA6WZooAtEG0PDU8g0KA-S2EqogKK0eOTHktF_2uxOjjg8UtVJeC6EG0Ptf8g0M5',
        'H_BDCLCKID_SF': 'tRAOoC8atDvHjjrP-trf5DCShUFsLxDJB2Q-5KL-JDDMJljv5tb65xAIhPrzXtkJJb5ZbfbdJJjohT6v3hjJKUCFjRJz2-RCbgTxoUJgQCnJhhvqqq-KQJ_ebPRiJPQ9QgbWLpQ7tt5W8ncFbT7l5hKpbt-q0x-jLTnhVn0MBCK0HPonHjL-DTb-3j',
        'BDUSS': '0I0Y2k3WXZObVI0VzNwMWV1Y3JYSkc0ZXhVazgzSjl2cVVwMVBmbmR5OVNjdjFlSUFBQUFBJCQAAAAAAAAAAAEAAACnVEtHx6fdobqjAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAFLl1V5S5dVeT',
        'H_PS_PSSID': '31357_1444_21127_31069_31254_31463_31714_30823_26350_22157',
        'BAIDUID': 'B959C81FDC267E68142013D5443CDA52:FG=1',
        'MCITY': '-%3A',
        'validate': '16615',
        'M_LG_UID': '1196119207',
        'M_LG_SALT': 'a3d3bd2bb4ecf1ca44becca79e67b84a',
    }

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
    sql_insert = '''
        Insert into ThirdStoreResource(Company,StoreName,StoreAddr,Longitude,Latitude,Status,TelPhone) values(%s,%s,%s,%s,%s,%s,%s)
'''
    # 0、0
    # 1、10
    # 3、20
    # 4、30
    total= 29
    wd = "我爱我家"
    Company = "我爱我家"
    for i in range(0, total):
        print(i)
        if i > 1:
            pn = i + 1
            nn = i * 10
        else:
            pn = i
            nn = pn * 10
        params = (
            ('newmap', '1'),
            ('reqflag', 'pcmap'),
            ('biz', '1'),
            ('from', ['webmap', 'webmap']),
            ('da_par', 'after_baidu'),
            ('pcevaname', 'pc4.1'),
            ('qt', 'con'),
            ('c', '289'),
            ('wd', wd),
            ('wd2', ''),
            ('pn', '{pn}'.format(pn=str(pn))),
            ('nn', '{nn}'.format(nn=str(nn))),
            ('db', '0'),
            ('sug', '0'),
            ('addr', '0'),
            ('', ''),
            ('da_src', 'pcmappg.poi.page'),
            ('on_gel', '1'),
            ('src', '7'),
            ('gr', '3'),
            ('l', '12'),
            ('auth',
             'X9@TW5S9@6eF4G5D=aawRfgPOgGw6z43uxHTxVRLBLVtzljPyBYYxy1uVt1GgvPUDZYOYIZuVt1cv3uVtGccZcuVtPWv3GuzVtPYIuVtUvhgMZSguxzBEHLNRTVtcEWe1GD8zv7u@ZPuLBt0xAXwnpElp1GP@Ga8HVMPDimNNz8ycvY1SGpuxHtFkk0H38'),
            ('device_ratio', '2'),
            ('tn', 'B_NORMAL_MAP'),
            ('u_loc', '13519792,3635685'),
            ('ie', 'utf-8'),
            ('b', '(13501271.82,3608860.67;13537239.82,3657628.67)'),
            ('t', '1591086363113'),
        )
        response = s.get('https://map.baidu.com/', headers=headers, params=params, cookies=cookies)
        res_json = response.json()
        res_ls = jsonpath(res_json, '$.content[*]')
        for res in res_ls:
            item = {}
            flag = jsonpath(res, '$.di_tag')[0]
            if '房产中介' in flag:
                StoreName = jsonpath(res, '$.name')[0]
                StoreAddr = jsonpath(res, '$.addr')[0]
                Longitude = jsonpath(res, '$.x')[0]
                # if Longitude:
                #     Longitude = Longitude
                Latitude = jsonpath(res, '$.y')[0]
                # if Latitude:
                #     Latitude = Latitude * 0.01
                Status = jsonpath(res, '$.status')[0]
                TelPhone = jsonpath(res, '$.ext.detail_info.phone')[0]
                item["Company"] = Company
                item["StoreName"] = StoreName
                item["StoreAddr"] = StoreAddr
                item["Longitude"] = Longitude
                item["Latitude"] = Latitude
                item["Status"] = Status
                item["TelPhone"] = TelPhone
                print(item)
                cursor.execute(sql_insert, (
                    Company, StoreName, StoreAddr, Longitude, Latitude, Status, TelPhone
                ))


if __name__ == '__main__':
    host = '10.10.202.13'
    database = 'TWSpider'
    user = 'bigdata_user'
    password = 'ulyhx3rxqhtw'
    engine_word = create_engine(
        'mssql+pymssql://{}:{}@{}/{}'.format(user, password, host, database))
    connect = pymssql.connect(host=host, database=database,
                              user=user, password=password, charset="utf8")
    with connect.cursor() as cur:
        store(cursor=cur)
        connect.commit()
