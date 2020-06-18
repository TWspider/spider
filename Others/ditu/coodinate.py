import requests
import json
from jsonpath import jsonpath
import re
import pandas as pd


def res_handle(response, jqury, distinct, df_ls):
    res_text = response.text
    print(res_text)
    res = re.search("{}\((.*)\)".format(jqury), res_text)
    res = res.group(1)
    res = json.loads(res)
    # print(res)
    ls1 = jsonpath(res, '$.data.list')[0].values()
    # print(ls1)
    for i in ls1:
        item = {}
        item["distinct"] = distinct
        item["name"] = i.get("name")
        item["longitude"] = i.get("longitude")
        item["latitude"] = i.get("latitude")
        item["border"] = i.get("border")
        df_ls.append(item)
    return df_ls


def coordinates():
    headers = {
        'Connection': 'keep-alive',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.138 Safari/537.36',
        'Accept': '*/*',
        'Sec-Fetch-Site': 'same-site',
        'Sec-Fetch-Mode': 'no-cors',
        'Sec-Fetch-Dest': 'script',
        'Referer': 'https://sh.lianjia.com/ditu/',
        'Accept-Language': 'zh-CN,zh;q=0.9',
    }
    params_ls = [
        (
            "上海",
            (
                ('callback', 'jQuery1111016382452405418801_1590735627531'),
                ('city_id', '310000'),
                ('group_type', 'district'),
                ('max_lat', '31.48089'),
                ('min_lat', '30.837148'),
                ('max_lng', '121.873207'),
                ('min_lng', '121.054528'),
                ('sug_id', ''),
                ('sug_type', ''),
                ('filters', '{}'),
                ('request_ts', '1590735632201'),
                ('source', 'ljpc'),
                ('authorization', '37e28cb1848693b5198beb566621ceb4'),
                ('_', '1590735627533'),
            )
        ),
        (
            "上海周边",
            (
                ('callback', 'jQuery1111036627138554275795_1590736098861'),
                ('city_id', '310000'),
                ('group_type', 'bizcircle'),
                ('max_lat', '31.241481'),
                ('min_lat', '31.161048'),
                ('max_lng', '121.024686'),
                ('min_lng', '120.888144'),
                ('sug_id', ''),
                ('sug_type', ''),
                ('filters', '{}'),
                ('request_ts', '1590736198221'),
                ('source', 'ljpc'),
                ('authorization', '21d9e8e0c57ada584235be81bacb20a1'),
                ('_', '1590736098873'),
            )
        ),
        (
            "青浦",
            (
                ('callback', 'jQuery1111036627138554275795_1590736098861'),
                ('city_id', '310000'),
                ('group_type', 'bizcircle'),
                ('max_lat', '31.237101'),
                ('min_lat', '31.156665'),
                ('max_lng', '121.252257'),
                ('min_lng', '121.181542'),
                ('sug_id', ''),
                ('sug_type', ''),
                ('filters', '{}'),
                ('request_ts', '1590736248891'),
                ('source', 'ljpc'),
                ('authorization', '0473e8dfd60da9c31ae8e76cb08b9432'),
                ('_', '1590736098883'),
            )
        ),
        (
            "嘉定",
            (
                ('callback', 'jQuery1111036627138554275795_1590736098861'),
                ('city_id', '310000'),
                ('group_type', 'bizcircle'),
                ('max_lat', '31.358296'),
                ('min_lat', '31.277964'),
                ('max_lng', '121.355625'),
                ('min_lng', '121.214483'),
                ('sug_id', ''),
                ('sug_type', ''),
                ('filters', '{}'),
                ('request_ts', '1590736346213'),
                ('source', 'ljpc'),
                ('authorization', 'a1610b429d082adba9bd57fa6dba4e3c'),
                ('_', '1590736098892'),
            )
        ),
        (
            "松江",
            (
                ('callback', 'jQuery1111036627138554275795_1590736098861'),
                ('city_id', '310000'),
                ('group_type', 'bizcircle'),
                ('max_lat', '31.085976'),
                ('min_lat', '31.00541'),
                ('max_lng', '121.335725'),
                ('min_lng', '121.194584'),
                ('sug_id', ''),
                ('sug_type', ''),
                ('filters', '{}'),
                ('request_ts', '1590736395869'),
                ('source', 'ljpc'),
                ('authorization', '3ad55ff3f20643dd2a318bae6f221c2f'),
                ('_', '1590736098896'),
            )

        ),
        (
            "金山",
            (
                ('callback', 'jQuery1111036627138554275795_1590736098861'),
                ('city_id', '310000'),
                ('group_type', 'bizcircle'),
                ('max_lat', '30.791968'),
                ('min_lat', '30.711152'),
                ('max_lng', '121.396959'),
                ('min_lng', '121.255818'),
                ('sug_id', ''),
                ('sug_type', ''),
                ('filters', '{}'),
                ('request_ts', '1590736445625'),
                ('source', 'ljpc'),
                ('authorization', '19d644c9b32d549f9774b26604309ded'),
                ('_', '1590736098902'),
            )
        ),
        (
            "宝山",
            (
                ('callback', 'jQuery1111036627138554275795_1590736098861'),
                ('city_id', '310000'),
                ('group_type', 'bizcircle'),
                ('max_lat', '31.409612'),
                ('min_lat', '31.329325'),
                ('max_lng', '121.48546'),
                ('min_lng', '121.372201'),
                ('sug_id', ''),
                ('sug_type', ''),
                ('filters', '{}'),
                ('request_ts', '1590736531249'),
                ('source', 'ljpc'),
                ('authorization', '9eff653ea46d4337b4242c6e5b51e79c'),
                ('_', '1590736098911'),
            )
        ),
        (
            "奉贤",
            (
                ('callback', 'jQuery1111036627138554275795_1590736098861'),
                ('city_id', '310000'),
                ('group_type', 'bizcircle'),
                ('max_lat', '30.932226'),
                ('min_lat', '30.851529'),
                ('max_lng', '121.62509'),
                ('min_lng', '121.511832'),
                ('sug_id', ''),
                ('sug_type', ''),
                ('filters', '{}'),
                ('request_ts', '1590736608248'),
                ('source', 'ljpc'),
                ('authorization', '732869a299e639ffdcdca0326c521fc8'),
                ('_', '1590736098916'),
            )
        ),
        (
            "崇明",
            (
                ('callback', 'jQuery1111036627138554275795_1590736098861'),
                ('city_id', '310000'),
                ('group_type', 'bizcircle'),
                ('max_lat', '31.654093'),
                ('min_lat', '31.574018'),
                ('max_lng', '121.633438'),
                ('min_lng', '121.52018'),
                ('sug_id', ''),
                ('sug_type', ''),
                ('filters', '{}'),
                ('request_ts', '1590736651534'),
                ('source', 'ljpc'),
                ('authorization', '4a25d684c7b1ecc398b9a5925f2fa46a'),
                ('_', '1590736098923'),
            )
        ),
        (
            "普陀",
            (
                ('callback', 'jQuery1111036627138554275795_1590736098861'),
                ('city_id', '310000'),
                ('group_type', 'bizcircle'),
                ('max_lat', '31.296939'),
                ('min_lat', '31.216555'),
                ('max_lng', '121.464132'),
                ('min_lng', '121.350874'),
                ('sug_id', ''),
                ('sug_type', ''),
                ('filters', '{}'),
                ('request_ts', '1590736693178'),
                ('source', 'ljpc'),
                ('authorization', 'ac2fc5d590727b1034955cd1bce26ede'),
                ('_', '1590736098929'),
            )
        ),
        (
            "长宁",
            (
                ('callback', 'jQuery1111036627138554275795_1590736098861'),
                ('city_id', '310000'),
                ('group_type', 'bizcircle'),
                ('max_lat', '31.251712'),
                ('min_lat', '31.171288'),
                ('max_lng', '121.444552'),
                ('min_lng', '121.331294'),
                ('sug_id', ''),
                ('sug_type', ''),
                ('filters', '{}'),
                ('request_ts', '1590736721995'),
                ('source', 'ljpc'),
                ('authorization', 'b9080126bb7905b990a2b21a156f2d3a'),
                ('_', '1590736098931'),
            )
        ),
        (
            "闵行",
            (
                ('callback', 'jQuery1111036627138554275795_1590736098861'),
                ('city_id', '310000'),
                ('group_type', 'bizcircle'),
                ('max_lat', '31.13144'),
                ('min_lat', '31.050914'),
                ('max_lng', '121.4648'),
                ('min_lng', '121.351542'),
                ('sug_id', ''),
                ('sug_type', ''),
                ('filters', '{}'),
                ('request_ts', '1590736753631'),
                ('source', 'ljpc'),
                ('authorization', '0b030617d2fde70c3518914e94e0256d'),
                ('_', '1590736098933'),
            )
        ),
        (
            "虹口",
            (
                ('callback', 'jQuery1111036627138554275795_1590736098861'),
                ('city_id', '310000'),
                ('group_type', 'bizcircle'),
                ('max_lat', '31.323519'),
                ('min_lat', '31.243157'),
                ('max_lng', '121.546141'),
                ('min_lng', '121.432883'),
                ('sug_id', ''),
                ('sug_type', ''),
                ('filters', '{}'),
                ('request_ts', '1590736796125'),
                ('source', 'ljpc'),
                ('authorization', '75d128469c9e8e69b7530544761c8335'),
                ('_', '1590736098937'),
            )
        ),
        (
            "静安",
            (
                ('callback', 'jQuery1111036627138554275795_1590736098861'),
                ('city_id', '310000'),
                ('group_type', 'bizcircle'),
                ('max_lat', '31.273652'),
                ('min_lat', '31.193248'),
                ('max_lng', '121.511298'),
                ('min_lng', '121.39804'),
                ('sug_id', ''),
                ('sug_type', ''),
                ('filters', '{}'),
                ('request_ts', '1590736846062'),
                ('source', 'ljpc'),
                ('authorization', 'f4bf4615442248a6d224470244b4d8af'),
                ('_', '1590736098939'),
            )
        ),
        (
            "徐汇",
            (
                ('callback', 'jQuery1111036627138554275795_1590736098861'),
                ('city_id', '310000'),
                ('group_type', 'bizcircle'),
                ('max_lat', '31.215318'),
                ('min_lat', '31.134863'),
                ('max_lng', '121.501743'),
                ('min_lng', '121.388484'),
                ('sug_id', ''),
                ('sug_type', ''),
                ('filters', '{}'),
                ('request_ts', '1590736892902'),
                ('source', 'ljpc'),
                ('authorization', '3d53cf3ffe9f51a15323af04a680b007'),
                ('_', '1590736098944'),
            )
        ),
        (
            "黄浦",
            (
                ('callback', 'jQuery1111036627138554275795_1590736098861'),
                ('city_id', '310000'),
                ('group_type', 'bizcircle'),
                ('max_lat', '31.260852'),
                ('min_lat', '31.180437'),
                ('max_lng', '121.545216'),
                ('min_lng', '121.431958'),
                ('sug_id', ''),
                ('sug_type', ''),
                ('filters', '{}'),
                ('request_ts', '1590736929355'),
                ('source', 'ljpc'),
                ('authorization', 'ba5e8a7b29f626ee2132e80dc76390e9'),
                ('_', '1590736098948'),
            )
        ),
        (
            "杨浦",
            (
                ('callback', 'jQuery1111036627138554275795_1590736098861'),
                ('city_id', '310000'),
                ('group_type', 'bizcircle'),
                ('max_lat', '31.33506'),
                ('min_lat', '31.254708'),
                ('max_lng', '121.592549'),
                ('min_lng', '121.479291'),
                ('sug_id', ''),
                ('sug_type', ''),
                ('filters', '{}'),
                ('request_ts', '1590736969706'),
                ('source', 'ljpc'),
                ('authorization', '320ce0ec3f39a63a719f5e67f06e6531'),
                ('_', '1590736098950'),
            )
        ),
        (
            "浦东",
            (
                ('callback', 'jQuery1111036627138554275795_1590736098861'),
                ('city_id', '310000'),
                ('group_type', 'bizcircle'),
                ('max_lat', '31.248206'),
                ('min_lat', '31.16778'),
                ('max_lng', '121.663161'),
                ('min_lng', '121.549902'),
                ('sug_id', ''),
                ('sug_type', ''),
                ('filters', '{}'),
                ('request_ts', '1590737000353'),
                ('source', 'ljpc'),
                ('authorization', '956ceef5585f41a491a96946b016e536'),
                ('_', '1590736098952'),
            )
        ),
    ]
    df_ls = []
    for params in params_ls:
        distinct = params[0]
        params = params[1]
        jqury = params[0][1]
        response = requests.get('https://ajax.lianjia.com/map/search/ershoufang/', headers=headers, params=params,
                                )

        res_handle(response=response, jqury=jqury, distinct=distinct, df_ls=df_ls)
    df = pd.DataFrame(df_ls, columns=['distinct', "name", 'longitude', 'latitude', 'border'])
    df.to_csv("./coordinates.csv", index=False)


coordinates()
