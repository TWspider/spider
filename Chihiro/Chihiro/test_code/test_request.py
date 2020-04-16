# coding=gbk
import requests
from scrapy.selector import Selector

headers_baidu = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "zh-CN,zh;q=0.9",
    # 'content-type': 'charset=utf8',
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
headers_sogou = \
    {
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

url = 'http://www.baidu.com/link?url=FZHFc1aICLPrM1PJsLW8G58OUq_-iVPxA__7M0qqrsoQdQa_oiVTivcNaRGTJtYybx88YgFXaUPMZ-Mpg6Ux-afktAIiI1b-3LRfxM4mQ4G'


def t1():
    res = requests.get(url=url, headers=headers_baidu)
    res_extract = res.content.decode("utf-8")
    res_handle = Selector(text=res_extract)
    title = res_handle.xpath("//title/text()").extract_first()
    print(title)


# t1()

import pymssql
import datetime


def t2():
    connect = pymssql.connect(host='10.10.202.13', database='TWSpider',
                              user='bigdata_user', password='ulyhx3rxqhtw', charset="utf8")  #

    inserttime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print(inserttime)
    with connect.cursor() as cur:
        cur.execute("select title,source from News where DATEDIFF(d,[inserttime],GETDATE())=0".format(inserttime))
        res = cur.fetchall()
    if ("58ͬ�ǡ����ӿ��߽� ̫ƽ���� ݼӢ�ز� ���������˵İ�������", '��΢��') in res:
        print(res)


# res1 = res_handle.xpath('//meta[@property="og:title"]/@content').extract_first()
# print(res1)


# 1��������
white_list = [
    "̫ƽ����", '̫ƽ�����', '̫ƽ�����', '̫ƽ������', '̫ƽ������', '̫ƽ�����', '������̫ƽ��', '̫ƽ���װ',
    '̫ƽ��֤ȯ', '�׿�һ�а�����ƫ��',
]

related_list = [
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
monitorword_list = [
    "�巣", "����", "��Ƿ����", "�Ӱ��", "Ͷ��", "������",
    "ɧ��", "�Բ��", "������", "̫ĳ��", "̫ĳ����",
    "���н�", "�����н�", '��',
    "��̸", "��·", "�ٿͻ�", "����", "����",
    "ƭ��", '����', "����", '����', '��ƭ', '����',
    '����', '��ƭ', 'ƭ����'
]
content = '''����������ֹ�ע����Ŷ���������������������׼���û�������̫ƽ�󷿲��ľӼ����������ֶ���Ȫ·��һ�׷��ӵ����ھ�������������ԣ��Ҫ�����ϵķ������������õó��׸�Ȼ�����н�ȴ�����¼ҷָ����������߸�˵һ�� ��ôһ����˫���Ͳ�ǩ�˺�ͬ��������¼�һ����һ�ȶԲŷ����Լ������ĸ������ǶԷ�����������·һ�������¼ң����ϼҲ����¼����������еķ��ӻ�û����������֮������ó��׸����н�ȴ�������������ϼҲ����Ż���������޶��������׸������ޡ���·������ƭ�ϼң��¼ҷ����Ѿ������ϼҷ��������ڼ����۷����ֽ��׸�Ҫ��ﵽ�߳ɡ��н���߷�����˵���¼����������г��⣬���ķ����Ѿ������ˣ����ó���������ͬ��������������·�������Ʊ���¼������ӣ���������Ǯ������Ԥ�����еķ�����250�򣬵������������������ۣ��ܿ��ܾ��������¼ҵķ��ӡ��н������ظ�˵��ֻҪ�����Ƕ����������ⲻ�����⣬����ͷ˵�涨ʱ����������ȥ�������۵�2���㡣ǩ��ͬʱ������������������Щ���Ծ�����ϸ�����£����ָ���û����ν�ڹ涨ʱ����û��������������⳥����ʱ�������������ģ���绰���н��ܲ�Ͷ�ߣ��ܲ�˵��������������顣���ǣ���������û��ǩ���û����ӵĶ���ί������Э�飬�º�Ҳ�˽⵽������ͷ�ķ��Ӹ�����������250����·�ģ�����ʱ�� �������¼ҽӴ���������ͬ������ֱ��Ǣ̸���������н鹤����Աȴ�������ӡ���һ��Ҫ���£��Է�����һ��170��ͷ�ĵ绰˵�Ƿ������ĵ绰����ʱһ�����Ϻ�������Ӫ�̵ĵ绰��������Ҳ�е㻳�ɡ����Է�˵�Լ��Ƿ�������ΪҲû��������û���룬������̸�ˡ��������ƣ��н���ֻ��������������Լ��ģ��ӵ绰�϶�Ҳ�����Լ����н������ָ���˽ӵĵ绰����Ӧ�����ڶ���ʱ�䡣�������ͷ������ڼ䶼ǩ�˺�ͬ������������10��Ԫ�Ķ���Ȼ��˫��������֪�������Ǽ�ͬѼ�����������ƽ�����֡�˭֪�н����²����˾ܾ��黹�ϼ�ѹ��������ķ���֤��Ҫ����������֧��2�����Ӷ��ԭ�������ǡ����ϡ������������Ķ������������������6��������4��Ȼ���Ϻ���ү�塱�������������׺�����ԥ�ؾ;ܾ�����������ۼ��������Ҫ�����н���Աʹ�����޳ܵ�һ�е������Ͼ���˫�������˶�����ǩ�������ͷ�����ֻ��ǰ�����������Լ���������ǩ���������ٴ���Э�̵Ŀ����µ��н�ʱ��Ȼ�õ������µĴ������ŵĿ������������в���н鱾��Ӧ���Ƿ������¼��𵽸��õ�������ͨ��������ȴ�����˼�ǿ��ǿ��������ϧ�������յ��������ġ���������������������н��ŵ꣬�곤�������н���Ա�����ڡ���󣬼��߽����ⷴӳ���˸÷����н��ܲ�����ظ����˱�ʾ����˾��Ҫһ�����쳹����£������ʵ�����˴���Υ����Ϊ���Ὺ�������ˣ��������������ദ��������Ϣ���÷����н��Ѿ�������֧����Ӷ���˻�����������������Ҳ�û��˶��𡣶��ϼҷ�����Ҳ�õ������ķ���֤�����༭����·�����¿����ܵķ�˿����Ӵ~~~�����𣬹�ע����������Knews��΢�Ź��ںţ������͵����º�������Է�����Ĺ۵㣬���ǽ���ȡ10λ���˷�˿������Ϻ��仨̴����Ͱװ��25g*8�飩������Ȩ����������ϵ����������Knews���Ҹ��www.kankanews.comδ����Ȩ ����ת�����˽�������ţ���鿴�����Ƶ���뱬����߹��£�ֻ�����ؿ�������app����������ߵ��ʱ������ʶ���ά��������Ķ�ԭ�ġ�'''
monitorword = [k for i in related_list if i in content for j in white_list if j not in content
               for k in monitorword_list if k in content]

len_white_list = len(white_list) - 1
res = []
for i in related_list:
    if i in content:
        # ���
        flag = 0
        for index, j in enumerate(white_list):
            if j in content:
                flag += 1
            elif flag == 0 and len_white_list == index:
                print("���ڰ�������")
                for k in monitorword_list:
                    if k in content:
                        res = []

'''
{'title': '��ħ��حү��ģ�ӣ�����̫ƽ�󷿲��޳��н��������� ����˻�С�ﹺ������', 'content': '����������ֹ�ע����Ŷ���������������������׼���û�������̫ƽ�󷿲��ľӼ����������ֶ���Ȫ·��һ�׷��ӵ����ھ�������������ԣ��Ҫ�����ϵķ������������õó��׸�Ȼ�����н�ȴ�����¼ҷָ����������߸�˵һ�� ��ôһ����˫���Ͳ�ǩ�˺�ͬ��������¼�һ����һ�ȶԲŷ����Լ������ĸ������ǶԷ�����������·һ�������¼ң����ϼҲ����¼����������еķ��ӻ�û����������֮������ó��׸����н�ȴ�������������ϼҲ����Ż���������޶��������׸������ޡ���·������ƭ�ϼң��¼ҷ����Ѿ������ϼҷ��������ڼ����۷����ֽ��׸�Ҫ��ﵽ�߳ɡ��н���߷�����˵���¼����������г��⣬���ķ����Ѿ������ˣ����ó���������ͬ��������������·�������Ʊ���¼������ӣ���������Ǯ������Ԥ�����еķ�����250�򣬵������������������ۣ��ܿ��ܾ��������¼ҵķ��ӡ��н������ظ�˵��ֻҪ�����Ƕ����������ⲻ�����⣬����ͷ˵�涨ʱ����������ȥ�������۵�2���㡣ǩ��ͬʱ������������������Щ���Ծ�����ϸ�����£����ָ���û����ν�ڹ涨ʱ����û��������������⳥����ʱ�������������ģ���绰���н��ܲ�Ͷ�ߣ��ܲ�˵��������������顣���ǣ���������û��ǩ���û����ӵĶ���ί������Э�飬�º�Ҳ�˽⵽������ͷ�ķ��Ӹ�����������250����·�ģ�����ʱ�� �������¼ҽӴ���������ͬ������ֱ��Ǣ̸���������н鹤����Աȴ�������ӡ���һ��Ҫ���£��Է�����һ��170��ͷ�ĵ绰˵�Ƿ������ĵ绰����ʱһ�����Ϻ�������Ӫ�̵ĵ绰��������Ҳ�е㻳�ɡ����Է�˵�Լ��Ƿ�������ΪҲû��������û���룬������̸�ˡ��������ƣ��н���ֻ��������������Լ��ģ��ӵ绰�϶�Ҳ�����Լ����н������ָ���˽ӵĵ绰����Ӧ�����ڶ���ʱ�䡣�������ͷ������ڼ䶼ǩ�˺�ͬ������������10��Ԫ�Ķ���Ȼ��˫��������֪�������Ǽ�ͬѼ�����������ƽ�����֡�˭֪�н����²����˾ܾ��黹�ϼ�ѹ��������ķ���֤��Ҫ����������֧��2�����Ӷ��ԭ�������ǡ����ϡ������������Ķ������������������6��������4��Ȼ���Ϻ���ү�塱�������������׺�����ԥ�ؾ;ܾ�����������ۼ��������Ҫ�����н���Աʹ�����޳ܵ�һ�е������Ͼ���˫�������˶�����ǩ�������ͷ�����ֻ��ǰ�����������Լ���������ǩ���������ٴ���Э�̵Ŀ����µ��н�ʱ��Ȼ�õ������µĴ������ŵĿ������������в���н鱾��Ӧ���Ƿ������¼��𵽸��õ�������ͨ��������ȴ�����˼�ǿ��ǿ��������ϧ�������յ��������ġ���������������������н��ŵ꣬�곤�������н���Ա�����ڡ���󣬼��߽����ⷴӳ���˸÷����н��ܲ�����ظ����˱�ʾ����˾��Ҫһ�����쳹����£������ʵ�����˴���Υ����Ϊ���Ὺ�������ˣ��������������ദ��������Ϣ���÷����н��Ѿ�������֧����Ӷ���˻�����������������Ҳ�û��˶��𡣶��ϼҷ�����Ҳ�õ������ķ���֤�����༭����·�����¿����ܵķ�˿����Ӵ~~~�����𣬹�ע����������Knews��΢�Ź��ںţ������͵����º�������Է�����Ĺ۵㣬���ǽ���ȡ10λ���˷�˿������Ϻ��仨̴����Ͱװ��25g*8�飩������Ȩ����������ϵ����������Knews���Ҹ��www.kankanews.comδ����Ȩ ����ת�����˽�������ţ���鿴�����Ƶ���뱬����߹��£�ֻ�����ؿ�������app����������ߵ��ʱ������ʶ���ά��������Ķ�ԭ�ġ�', 'newurl': 'http://mp.weixin.qq.com/s?src=3&timestamp=1585821078&ver=1&signature=4XcVEm-LOVWGo1FxesHYdax4XwvuqSALrl0ngtrhG3-ChF3Fg8ucTLG7KOie6AkD3gR5pehTqkjy2QsJttWtUB3mcDM3YpPQHOikpKPT8hUhKP7ZkKHI8oZPtLc785jh--s8FNbj19zLw3ZCf703O2Jol2zWiXXZ0XQB93St78Y=', 'searchword': '̫ƽ�󷿲�', 'located': 17}

'''
import hashlib

title = "̫ƽ������һ�Ҽ��˵꿪ҵ��"
sw = "̫ƽ����"
searchword_title = title + sw
s = hashlib.md5(searchword_title.encode(encoding='UTF-8')).hexdigest()
print(s)
import pandas as pd
from sqlalchemy.types import NVARCHAR, INT
from sqlalchemy import create_engine

host = '10.10.202.13'
database = 'TWSpider'
user = 'bigdata_user'
password = 'ulyhx3rxqhtw'
engine_word = create_engine('mssql+pymssql://{}:{}@{}/{}'.format(user, password, host, database))
a = "ɧ��,��·,".rstrip(",").split(",")
result = pd.value_counts(a)
print(result.index, type(result.index))
dict_word = {'monitorword': result.index, 'wordcount': result.values, "source": "", 'searchword': '̫ƽ����'}
dict_word = pd.DataFrame(dict_word)
print(dict_word)
dtype = {
    "monitorword": NVARCHAR,
    "wordcount": INT,

}
# dict_word.to_sql("wordcloud", con=engine_word, if_exists="replace", index=False, dtype=dtype)
a = {}

ls = [["�ѹ�", "̫ƽ����", "����,��·"], ["�ѹ�", "̫ƽ����", "����"], ["�ѹ�", "̫ƽ�󷿲�", "�ӵ�"], ["�ٶ�", "̫ƽ�󷿲�", "�ӵ�"]]
# b = {"�ѹ���ҳ": {"̫ƽ����": ["����", '��·', '����']}, }
# res1 = pd.DataFrame([{"source": "", 'searchword': '̫ƽ����', "monitorword": ["����", '��·', '����']}])
# print(res1)
for i in ls:
    source = i[0]
    searchword = i[1]
    monitorword1 = i[2]
    if source in a.keys():
        searchword_dict = a.get(source)
        if searchword in searchword_dict.keys():
            # �������
            monitorword_str = searchword_dict.get(searchword)
            monitorword_str += "," + monitorword1
            searchword_dict.update({searchword: monitorword_str})
        else:
            searchword_dict.update({searchword: monitorword1})
    else:
        a.update({source: {searchword: monitorword1}})

print(a)
ls = [{"source": i[0], "searchword": j[0], "monitorword": pd.value_counts(j[1].split(",")).index,
       "wordcount": pd.value_counts(j[1].split(",")).values}
      for i in a.items() for j in
      i[1].items()]

print(ls)
res1 = pd.DataFrame()
for k in ls:
    dict_word1 = pd.DataFrame(k)
    res1 = res1.append(dict_word1, ignore_index=True)
print(res1)

'''
JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.242.b07-1.el6_10.x86_64
PATH=$PATH:$JAVA_HOME/bin  
CLASSPATH=.:$JAVA_HOME/lib/dt.jar:$JAVA_HOME/lib/tools.jar  
export JAVA_HOME  CLASSPATH  PATH 


'''