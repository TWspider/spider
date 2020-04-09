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
    if ("58同城、安居客走进 太平洋房屋 菁英地产 房产经纪人的安家生活", '搜微信') in res:
        print(res)


# res1 = res_handle.xpath('//meta[@property="og:title"]/@content').extract_first()
# print(res1)


# 1、白名单
white_list = [
    "太平洋保险", '太平洋金融', '太平洋理财', '太平洋人寿', '太平洋汽车', '太平洋霸主', '爱茉莉太平洋', '太平洋包装',
    '太平洋证券', '抛开一切傲慢与偏见',
]

related_list = [
    "太平洋",
    "tpy",
    "TPY",
    "太屋",
    "菁英地产",
]
# 2、负面词
'''
加班
'''
monitorword_list = [
    "体罚", "罚款", "拖欠工资", "加班多", "投诉", "不靠谱",
    "骚扰", "吃差价", "恶作剧", "太某屋", "太某洋房屋",
    "黑中介", "无良中介", '坑',
    "假谈", "套路", "假客户", "二卡", "报复",
    "骗子", '恶劣', "忽悠", '翻脸', '欺骗', '辱骂',
    '处罚', '被骗', '骗我们'
]
content = '''戳上面的蓝字关注我们哦！本文外地来沪的余先生准备置换房屋在太平洋房产的居间下欲购买浦东南泉路的一套房子但限于经济条件并不宽裕需要把手上的房子卖掉才能拿得出首付然而，中介却把上下家分隔开交流两边各说一词 这么一忽悠双方就草签了合同结果，上下家一碰面一比对才发现自己听到的根本不是对方表达的内容套路一：忽悠下家，称上家不急下家余先生手中的房子还没有卖，卖了之后才能拿出首付。中介却告诉余先生，上家不急着换房，最大限度三个月首付款期限。套路二：欺骗上家，下家房子已经卖了上家冯先生由于急着售房拿现金，首付要求达到七成。中介告诉冯先生说，下家余先生很有诚意，他的房子已经卖掉了，还拿出了卖房合同给冯先生看。套路三：打包票帮下家卖房子，卖不掉赔钱余先生预期手中的房子卖250万，但担忧如果卖不出这个价，很可能就无力买下家的房子。中介拍着胸脯说，只要给他们独家售卖，这不成问题，还口头说规定时间内卖不出去，返房价的2个点。签合同时候，余先生发现条款有些不对劲。仔细看了下，发现根本没有所谓在规定时间内没卖出有两个点的赔偿，这时候他就起了疑心，打电话给中介总部投诉，总部说不可能有这个事情。于是，余先生并没有签订置换房子的独家委托售卖协议，事后也了解到，他手头的房子根本就卖不出250万。套路四：拖延时间 阻挠上下家接触余先生想同冯先生直接洽谈，但房产中介工作人员却重重阻挠。在一再要求下，对方给了一个170开头的电话说是冯先生的电话。当时一打，是上海虚拟运营商的电话，余先生也有点怀疑。但对方说自己是房东，因为也没见过，就没多想，后来就谈了。冯先生称，中介的手机根本不可能是自己的，接电话肯定也不是自己。中介可能是指定人接的电话，答应能延期多少时间。余先生和冯先生期间都签了合同余先生还付了10万元的定金然而双方见面后才知根本就是鸡同鸭讲于是提出和平“分手”谁知中介这下不干了拒绝归还上家压在他们这的房产证还要余先生必须支付2个点的佣金原来他们是“看上”了余先生付的定金还提出冯先生可以拿6万，他们拿4万然而上海“爷叔”冯先生不吃这套毫不犹豫地就拒绝了这个提议眼见这笔生意要泡汤中介人员使出了无耻的一招当天晚上就替双方进行了恶意网签余先生和冯先生只能前往交易中心自己撤下了网签当冯先生再次以协商的口吻致电中介时竟然得到了以下的答复这嚣张的口气赤裸裸的威胁啊中介本来应该是服务上下家起到更好的桥梁沟通作用现在却逼迫人家强买强卖甚至不惜威逼利诱到底哪来的“底气”记者随后来到该中介门店，店长及涉事中介人员都不在。随后，记者将问题反映给了该房产中介总部，相关负责人表示，公司需要一到两天彻查此事，如果查实经纪人存在违规行为，会开除涉事人，并进行其它严肃处理。最新消息：该房产中介已经将部分支付的佣金退还给余先生，余先生也拿回了定金。而上家冯先生也拿到了他的房产证。（编辑：大路）往下看本周的粉丝福利哟~~~现在起，关注“看看新闻Knews”微信公众号，在推送的文章后进行留言发表你的观点，我们将抽取10位幸运粉丝，获得上海蜂花檀香皂桶装（25g*8块）。【版权申明】本文系看看新闻网Knews独家稿件www.kankanews.com未经授权 请勿转载想了解更多新闻？想查看相关视频？想爆料身边故事？只需下载看看新闻app！看看新闻叩击时代长按识别二维码或点击“阅读原文”'''
monitorword = [k for i in related_list if i in content for j in white_list if j not in content
               for k in monitorword_list if k in content]

len_white_list = len(white_list) - 1
res = []
for i in related_list:
    if i in content:
        # 相关
        flag = 0
        for index, j in enumerate(white_list):
            if j in content:
                flag += 1
            elif flag == 0 and len_white_list == index:
                print("不在白名单内")
                for k in monitorword_list:
                    if k in content:
                        res = []

'''
{'title': '看魔都丨爷叔模子！不惧太平洋房产无耻中介威逼利诱 坚决退还小伙购房定金', 'content': '戳上面的蓝字关注我们哦！本文外地来沪的余先生准备置换房屋在太平洋房产的居间下欲购买浦东南泉路的一套房子但限于经济条件并不宽裕需要把手上的房子卖掉才能拿得出首付然而，中介却把上下家分隔开交流两边各说一词 这么一忽悠双方就草签了合同结果，上下家一碰面一比对才发现自己听到的根本不是对方表达的内容套路一：忽悠下家，称上家不急下家余先生手中的房子还没有卖，卖了之后才能拿出首付。中介却告诉余先生，上家不急着换房，最大限度三个月首付款期限。套路二：欺骗上家，下家房子已经卖了上家冯先生由于急着售房拿现金，首付要求达到七成。中介告诉冯先生说，下家余先生很有诚意，他的房子已经卖掉了，还拿出了卖房合同给冯先生看。套路三：打包票帮下家卖房子，卖不掉赔钱余先生预期手中的房子卖250万，但担忧如果卖不出这个价，很可能就无力买下家的房子。中介拍着胸脯说，只要给他们独家售卖，这不成问题，还口头说规定时间内卖不出去，返房价的2个点。签合同时候，余先生发现条款有些不对劲。仔细看了下，发现根本没有所谓在规定时间内没卖出有两个点的赔偿，这时候他就起了疑心，打电话给中介总部投诉，总部说不可能有这个事情。于是，余先生并没有签订置换房子的独家委托售卖协议，事后也了解到，他手头的房子根本就卖不出250万。套路四：拖延时间 阻挠上下家接触余先生想同冯先生直接洽谈，但房产中介工作人员却重重阻挠。在一再要求下，对方给了一个170开头的电话说是冯先生的电话。当时一打，是上海虚拟运营商的电话，余先生也有点怀疑。但对方说自己是房东，因为也没见过，就没多想，后来就谈了。冯先生称，中介的手机根本不可能是自己的，接电话肯定也不是自己。中介可能是指定人接的电话，答应能延期多少时间。余先生和冯先生期间都签了合同余先生还付了10万元的定金然而双方见面后才知根本就是鸡同鸭讲于是提出和平“分手”谁知中介这下不干了拒绝归还上家压在他们这的房产证还要余先生必须支付2个点的佣金原来他们是“看上”了余先生付的定金还提出冯先生可以拿6万，他们拿4万然而上海“爷叔”冯先生不吃这套毫不犹豫地就拒绝了这个提议眼见这笔生意要泡汤中介人员使出了无耻的一招当天晚上就替双方进行了恶意网签余先生和冯先生只能前往交易中心自己撤下了网签当冯先生再次以协商的口吻致电中介时竟然得到了以下的答复这嚣张的口气赤裸裸的威胁啊中介本来应该是服务上下家起到更好的桥梁沟通作用现在却逼迫人家强买强卖甚至不惜威逼利诱到底哪来的“底气”记者随后来到该中介门店，店长及涉事中介人员都不在。随后，记者将问题反映给了该房产中介总部，相关负责人表示，公司需要一到两天彻查此事，如果查实经纪人存在违规行为，会开除涉事人，并进行其它严肃处理。最新消息：该房产中介已经将部分支付的佣金退还给余先生，余先生也拿回了定金。而上家冯先生也拿到了他的房产证。（编辑：大路）往下看本周的粉丝福利哟~~~现在起，关注“看看新闻Knews”微信公众号，在推送的文章后进行留言发表你的观点，我们将抽取10位幸运粉丝，获得上海蜂花檀香皂桶装（25g*8块）。【版权申明】本文系看看新闻网Knews独家稿件www.kankanews.com未经授权 请勿转载想了解更多新闻？想查看相关视频？想爆料身边故事？只需下载看看新闻app！看看新闻叩击时代长按识别二维码或点击“阅读原文”', 'newurl': 'http://mp.weixin.qq.com/s?src=3&timestamp=1585821078&ver=1&signature=4XcVEm-LOVWGo1FxesHYdax4XwvuqSALrl0ngtrhG3-ChF3Fg8ucTLG7KOie6AkD3gR5pehTqkjy2QsJttWtUB3mcDM3YpPQHOikpKPT8hUhKP7ZkKHI8oZPtLc785jh--s8FNbj19zLw3ZCf703O2Jol2zWiXXZ0XQB93St78Y=', 'searchword': '太平洋房产', 'located': 17}

'''
import hashlib

title = "太平洋房屋又一家加盟店开业了"
sw = "太平洋房屋"
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
a = "骚扰,套路,".rstrip(",").split(",")
result = pd.value_counts(a)
print(result.index, type(result.index))
dict_word = {'monitorword': result.index, 'wordcount': result.values, "source": "", 'searchword': '太平洋房屋'}
dict_word = pd.DataFrame(dict_word)
print(dict_word)
dtype = {
    "monitorword": NVARCHAR,
    "wordcount": INT,

}
# dict_word.to_sql("wordcloud", con=engine_word, if_exists="replace", index=False, dtype=dtype)
a = {}

ls = [["搜狗", "太平洋房屋", "处罚,套路"], ["搜狗", "太平洋房屋", "处罚"], ["搜狗", "太平洋房产", "坑爹"], ["百度", "太平洋房产", "坑爹"]]
# b = {"搜狗网页": {"太平洋房屋": ["处罚", '套路', '处罚']}, }
# res1 = pd.DataFrame([{"source": "", 'searchword': '太平洋房屋', "monitorword": ["处罚", '套路', '处罚']}])
# print(res1)
for i in ls:
    source = i[0]
    searchword = i[1]
    monitorword1 = i[2]
    if source in a.keys():
        searchword_dict = a.get(source)
        if searchword in searchword_dict.keys():
            # 加入监测词
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