# coding=gbk
from selenium import webdriver
import time
import re
from selenium.webdriver.chrome.options import Options
from scrapy.selector import Selector
from selenium.webdriver.common.by import By
import requests


def req_chrome():
    chrome_options = Options()
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--headless')

    # chrome_options.add_argument('blink-settings=imagesEnabled=false')
    # chrome_options.add_argument('--disable-gpu')

    driver = webdriver.Chrome(chrome_options=chrome_options)
    driver.get("https://weixin.sogou.com/")
    # driver.find_element_by_xpath("//input[@id='query']").send_keys("太平洋房屋")
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


def sql_pandas():
    import pandas as pd
    from sqlalchemy.types import NVARCHAR, INT
    from sqlalchemy import create_engine
    host = '10.10.202.13'
    database = 'TWSpider'
    user = 'bigdata_user'
    password = 'ulyhx3rxqhtw'
    engine_word = create_engine(
        'mssql+pymssql://{}:{}@{}/{}'.format(user, password, host, database))
    ls = [

    ]
    ls_type = len(ls) * [1]
    df = pd.DataFrame({"type": ls_type, "content": ls})
    # df.to_sql("News_Params", if_exists="append", index=False, con=engine_word)
    print(df)


def req():
    url = "https://sh.5i5j.com/xiaoqu/"
    req = requests.session()
    headers = {
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        # 'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Cookie': "smidV2=20191008141508bf80bc8aac216b0c6563b998e96f665600d9e27936cc54430; PHPSESSID=2m897hkmnvg19b84uuru7n30s4; domain=sh; xiaoqu_BROWSES=466206; yfx_c_g_u_id_10000001=_ck20051416434515338352286500476; yfx_f_l_v_t_10000001=f_t_1589445825532__r_t_1589445825532__v_t_1589445825532__r_c_0; __TD_deviceId=79CN1BOAF2IBI6B7; _ga=GA1.2.1534363482.1589445826; _gid=GA1.2.346956861.1589445826; gr_user_id=68ce68d9-61d2-4a91-a443-c170c77f65d4; 8fcfcf2bd7c58141_gr_session_id=eb1eff65-bd90-4dc0-8d62-15a4562c7880; Hm_lvt_94ed3d23572054a86ed341d64b267ec6=1589445826; 8fcfcf2bd7c58141_gr_session_id_eb1eff65-bd90-4dc0-8d62-15a4562c7880=true; grwng_uid=515aed24-b908-4ad9-b4aa-532356190486; _Jo0OQK=6D4201B4F950030F5FBB44B959AAE4E8055F495A714D81A7FA4B3BB8E726BFB9FFE4C3C869A4F418060C2CC03D4E4791BB06401EF0E90F2764088C5CF251BD29B69A3A6B6373DCEA275A28E4E02FA79F6B8A28E4E02FA79F6B83EF3BA90F9FD50EFC8DA892D59239CA2GJ1Z1Mg==; a6dc0a476ad768fdfba0fe22cba335f1_pc=TBYRRFZKXlNdAUFbEANUDgQJUQYDBwUNQ08VXF1RDgtRXVUSChIBBQkHCAQFAwcABRIcEkVDVUJ5VBIKEgkHBwYJCQcSHBJYVVFUWV1XEgoSEhwSRF9bVV4SChJVSXoAVWhxWX9ZentmAWFZfHN6WFJ3U1l_WXp5ZUp5AX5ZeUN5XUAAUWN5BnladVlWYR5VSXpAUwN9WX9ZegZRd3ZFagN-WFJZeUN5XUAAUWN5BnladVl8c3pAaWhhWX9adQF_dFsAfnRpSn5KYUN5XXYBanN5BnleRFN5XmZAanZHWX9cR1l_ZFMDflpbBX4BR1l8dkdZUncJXlFnBUVpZwFcaHN5BmhzelN5WUhTeV0FQGkCREVpZwFcaHN5BmhzeUh-ZFsDf3RhAX1KU0d-ZkdZfHZHWWpnAVhRZ0hTeVpARVRnSEN8dkdZagJmRWp3Zkloc3kGUl5mQ1JzSFN5XnpcagJcRlJcR1l_XQUBUndHQ2hzelpRaGIFaHN5BmhzeQVoc3lDaHN6X2pndltRZwFeaHN5BlJeZkNSc0hTeV1cRVRdXABRZwVeaQIJW2pmR1l_XQUBUndHQ2hzeklqZ1RbaWhiXGhzeQZoc3lIfmRXBX50YQJ9SlMAaHN5Q2hzekpUd3YAVGh-U3laX0h8dkdZU3d2SlMBVEZTXWJTeVpAU3ldfl19Z2UCfmd9An1deQB-dHVIfnd6XX1dZllpZGlIfQJhBH5aWwVqd3kEaHN5Q2hzekppZ0gAaHN5BmhzeUh9Wn0AfmRqU3leAFl8c3pKVGd5WX9ZekpUZ3pBamd-AHleAB5iX1EHXGp4AUN7A0VlUkdJUUJ0CUVVVmZ9b31pCENoQmF9Q3sdRwV1AgFHEhwSQ1FcRBIKEgECAwQFBhJN; user_info=TBYRRFZKXlNdAUFbEANUDgQJUQYDBwUNQ08VXF1RDgtRXVUSChIBBQkHCAQFAwcABRIcEkVDVUJ5VBIKEgkHBwYJCQcSHBJYVVFUWV1XEgoSEhwSRF9bVV4SChJVSXoAVWhxWX9ZentmAWFZfHN6WFJ3U1l_WXp5ZUp5AX5ZeUN5XUAAUWN5BnladVlWYR5VSXpAUwN9WX9ZegZRd3ZFagN-WFJZeUN5XUAAUWN5BnladVl8c3pAaWhhWX9adQF_dFsAfnRpSn5KYUN5XXYBanN5BnleRFN5XmZAanZHWX9cR1l_ZFMDflpbBX4BR1l8dkdZUncJXlFnBUVpZwFcaHN5BmhzelN5WUhTeV0FQGkCREVpZwFcaHN5BmhzeUh-ZFsDf3RhAX1KU0d-ZkdZfHZHWWpnAVhRZ0hTeVpARVRnSEN8dkdZagJmRWp3Zkloc3kGUl5mQ1JzSFN5XnpcagJcRlJcR1l_XQUBUndHQ2hzelpRaGIFaHN5BmhzeQVoc3lDaHN6X2pndltRZwFeaHN5BlJeZkNSc0hTeV1cRVRdXABRZwVeaQIJW2pmR1l_XQUBUndHQ2hzeklqZ1RbaWhiXGhzeQZoc3lIfmRXBX50YQJ9SlMAaHN5Q2hzekpUd3YAVGh-U3laX0h8dkdZU3d2SlMBVEZTXWJTeVpAU3ldfl19Z2UCfmd9An1deQB-dHVIfnd6XX1dZllpZGlIfQJhBH5aWwVqd3kEaHN5Q2hzekppZ0gAaHN5BmhzeUh9Wn0AfmRqU3leAFl8c3pKVGd5WX9ZekpUZ3pBamd-AHleAB5iX1EHXGp4AUN7A0VlUkdJUUJ0CUVVVmZ9b31pCENoQmF9Q3sdRwV1AgFHEhwSQ1FcRBIKEgECAwQFBhJN; wiwj_token_ticket=116_233_20_219_15894432009776997; wiwj_token_116_233_20_219_15894432009776997=%7B%22uid%22%3A%229776997%22%7D; tdCookieUid=9776997; 8fcfcf2bd7c58141_gr_last_sent_sid_with_cs1=eb1eff65-bd90-4dc0-8d62-15a4562c7880; 8fcfcf2bd7c58141_gr_last_sent_cs1=9776997; yfx_s_u_id_10000001=9776997; yfx_s_u_name_10000001=15978453705; _gat=1; 8fcfcf2bd7c58141_gr_cs1=9776997; Hm_lpvt_94ed3d23572054a86ed341d64b267ec6=1589446374; C3VK=e7d741",
        'Host': 'sh.5i5j.com',
        # 'Referer': 'https://sh.5i5j.com/ershoufang/',
        'Sec-Fetch-Mode': 'navigate',
        'Sec-Fetch-Site': 'same-origin',
        'Sec-Fetch-User': '?1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_2) AppleWebKit/537.75.14 (KHTML, like Gecko) Version/7.0.3 Safari/537.75.14",
    }
    res = req.get(url=url, headers=headers)
    cookies = req.cookies
    res_extract = res.content.decode("utf8", 'ignore')

    # res.encoding = "UTF8"
    print(cookies)
    print(res_extract)


def sql():
    from sqlalchemy.types import NVARCHAR, INT, DATETIME
    import pandas as pd
    import pyodbc
    import pymssql
    from sqlalchemy import create_engine
    dtype = {
        "id_third": INT,
        "id_tw": INT,
    }
    host = '10.10.202.13'
    user = 'bigdata_user'
    password = 'ulyhx3rxqhtw'
    database = 'TWSpider'
    with pymssql.connect(host=host, database=database,
                         user=user, password=password, charset="utf8") as conn:
        with conn.cursor() as cursor:
            cursor.execute("select count(*) as n from house_rank")
            res = cursor.fetchone()[0]
            if res:
                print(res)
    # df = pd.DataFrame(columns=['one', 'two', 'three'])
    # print(df)
    # df.to_sql("test", con=engine_res, if_exists="replace", index=False,
    #           dtype=dtype)


def item():
    s = '''
    
    中层
                                        / 33层（
    '''
    s1 = '  中楼层 (共6层) '
    res = re.search("(.*)\s+\(", s1).group(1).strip()
    print(res)


if __name__ == "__main__":
    req()

'''
yum install -y chkconfig python bind-utils psmisc libxslt zlib sqlite cyrus-sasl-plain cyrus-sasl-qssapi fuse fuse-libs redhat-lsb
'''
