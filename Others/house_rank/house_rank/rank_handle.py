from sqlalchemy.types import NVARCHAR, INT, DATETIME
from setting import HOST, USER, PASSWORD, DATABASE
from sqlalchemy import create_engine
import pandas as pd

engine_tw_house = create_engine('mssql+pymssql://{}:{}@{}/{}'.format(USER, PASSWORD, HOST, 'TWEstate'))
# engine_res = create_engine(
#     'mssql+pyodbc://{0}:{1}@{2}/{3}?driver=SQL Server Native Client 11.0'.format(USER, PASSWOD, HOST,
#                                                                                  DATABASE),
#     fast_executemany=True)
# 打标记、找交集、剩下的被定级
address = ["road", 'alley']
community_road = ["PropertyCommunity", 'road']
community = ["PropertyCommunity"]
total_floor = ['TotalFloor']
floor = ['Floor']
room_type = ['room', 'hall', 'toilet']
room_area = ['BuildingSquare']
id_third = ["ThirdId"]
id_tw = ["RoomId"]
id_third_resource = ["ThirdId", 'Resource']
id_third_status = ["ThirdId", 'Status']
id_tw_estate = ["RoomId", 'EstateId']


def get_house_tw(fetch_num_tw, sql_select_tw):
    house_list_tw = pd.read_sql(
        sql_select_tw,
        engine_tw_house, chunksize=fetch_num_tw)
    return house_list_tw


def rank_handle_inner(res_match_is, res_match_is_sql, rank_tw, rank_third, rank, field_list, house_third,
                      house_tw):
    '''
    库内房源拆分路段的时候,roomid不变
    :param res_match_is:
    :param res_match_is_sql:
    :param rank_tw:
    :param rank_third:
    :param rank:
    :param field_list:
    :return:
    '''
    rank_tw.loc[:, "flag"] = 1
    labels_drop = ["flag"] + field_list
    # 对比res_match_is对应表和等级对应的第三方房源
    res_match = pd.merge(res_match_is, rank_third,
                         sort=False,
                         how='left')
    # 除去当前位置为空的，到数据库中
    if rank == 1:
        rank_tw.loc[:, "TotalFloor"] = rank_tw["TotalFloor"].astype("str").apply(
            lambda x: x.replace(".0", ""))
    elif rank == 3:
        rank_tw.loc[:, "room"] = rank_tw["room"].astype("str").apply(
            lambda x: x.replace(".0", ""))
        rank_tw.loc[:, "hall"] = rank_tw["hall"].astype("str").apply(
            lambda x: x.replace(".0", ""))
        rank_tw.loc[:, "toilet"] = rank_tw["toilet"].astype("str").apply(
            lambda x: x.replace(".0", ""))
    elif rank == 4:
        rank_tw.loc[:, "BuildingSquare"] = rank_tw["BuildingSquare"].astype("str")
    res_match = pd.merge(res_match, rank_tw,
                         sort=False,
                         how='left')
    res_match = res_match.drop_duplicates()
    res_match_not = res_match[res_match['flag'].isnull()]
    match_not_index = res_match_not.index
    res_match_is = res_match.drop(labels=match_not_index)
    if not res_match_not.empty:
        res_match_not.loc[:, 'StarLevel'] = rank
        res_match_not.drop(labels=labels_drop, axis=1, inplace=True)
        # if rank <= 3:
        res_match_not = res_match_not.drop_duplicates(subset=['ThirdId'])
        res_match_not = pd.merge(
            left=res_match_not, right=house_third.loc[:, id_third_resource], sort=False,
            how='left'
        )
        res_match_not = pd.merge(
            left=res_match_not, right=house_third.loc[:, id_third_status], sort=False,
            how='left'
        )
        res_match_not = pd.merge(
            left=res_match_not, right=house_tw.loc[:, id_tw_estate], sort=False,
            how='left'
        )
        res_match_is_sql = res_match_is_sql.append(res_match_not)
        # res_match_is.to_csv("test2.csv", encoding='gb18030')
        # print("写入csv")
    res_match_is.drop(labels=labels_drop, axis=1, inplace=True)
    return {"res_match_is": res_match_is, 'res_match_is_sql': res_match_is_sql}


def rank_handle(house_third, house_tw, res_match_is_sql, handle_not_address_tw,
                handle_not_community_tw,
                handle_not_address_third,
                handle_not_community_third, handle_community_to_address_third, handle_community_to_address_tw):
    '''
    只保留匹配到的
    只确定当前两条数据的契合度有多深，以第三方房源为主
    '''
    # TW字段
    fields_tw_0_1 = house_tw.loc[:, address + id_tw]
    fields_tw_0_1_null = fields_tw_0_1[fields_tw_0_1['road'].isnull() | fields_tw_0_1['alley'].isnull()]
    index_null = fields_tw_0_1_null.index
    fields_tw_0_1 = fields_tw_0_1.drop(labels=index_null)

    fields_tw_0_2 = house_tw.loc[:, community_road + id_tw]
    fields_tw_0_2_null = fields_tw_0_2[fields_tw_0_2['PropertyCommunity'].isnull() | fields_tw_0_2['road'].isnull()]
    index_null = fields_tw_0_2_null.index
    fields_tw_0_2 = fields_tw_0_2.drop(labels=index_null)

    fields_tw_0_3 = house_tw.loc[:, community + id_tw]
    fields_tw_0_3_null = fields_tw_0_3[fields_tw_0_3['PropertyCommunity'].isnull()]
    index_null = fields_tw_0_3_null.index
    fields_tw_0_3 = fields_tw_0_3.drop(labels=index_null)

    fields_tw_1 = house_tw.loc[:, total_floor + id_tw]
    fields_tw_2 = house_tw.loc[:, floor + id_tw]
    fields_tw_3 = house_tw.loc[:, room_type + id_tw]
    fields_tw_4 = house_tw.loc[:, room_area + id_tw]
    # 第三方字段
    fields_third_0_1 = house_third.loc[:, address + id_third]
    fields_third_0_1_null = fields_third_0_1[
        (fields_third_0_1['road'].isnull() | fields_third_0_1['alley'].isnull())]
    index_null = fields_third_0_1_null.index
    fields_third_0_1 = fields_third_0_1.drop(labels=index_null)

    fields_third_0_2 = house_third.loc[:, community_road + id_third]
    fields_third_0_2_null = fields_third_0_2[
        fields_third_0_2['PropertyCommunity'].isnull() | fields_third_0_2['road'].isnull()]
    index_null = fields_third_0_2_null.index
    fields_third_0_2 = fields_third_0_2.drop(labels=index_null)

    fields_third_0_3 = house_third.loc[:, community + id_third]
    fields_third_0_3_null = fields_third_0_3[fields_third_0_3['PropertyCommunity'].isnull()]
    index_null = fields_third_0_3_null.index
    fields_third_0_3 = fields_third_0_3.drop(labels=index_null)

    fields_third_1 = house_third.loc[:, total_floor + id_third]
    fields_third_2 = house_third.loc[:, floor + id_third]
    fields_third_3 = house_third.loc[:, room_type + id_third]
    fields_third_4 = house_third.loc[:, room_area + id_third]
    # 获取本次可匹配到的数据
    res_match_merge = pd.DataFrame()
    # 匹配地址
    res_match = pd.merge(fields_third_0_1, fields_tw_0_1,
                         sort=False,
                         how='left')
    res_match_not_index = res_match[res_match.loc[:, 'RoomId'].isnull()].index
    res_match_is = res_match.drop(labels=res_match_not_index)
    if not res_match_is.empty:
        res_match_is.drop(labels=address, axis=1, inplace=True)
        res_match_merge = res_match_merge.append(res_match_is)
    # 匹配小区、路
    res_match = pd.merge(fields_third_0_2, fields_tw_0_2,
                         sort=False,
                         how='left')
    res_match_not_index = res_match[res_match.loc[:, 'RoomId'].isnull()].index
    res_match_is = res_match.drop(labels=res_match_not_index)
    if not res_match_is.empty:
        res_match_is.drop(labels=community_road, axis=1, inplace=True)
        res_match_merge = res_match_merge.append(res_match_is)
    # 匹配小区
    res_match = pd.merge(fields_third_0_3, fields_tw_0_3,
                         sort=False,
                         how='left')
    res_match_not_index = res_match[res_match.loc[:, 'RoomId'].isnull()].index
    res_match_is = res_match.drop(labels=res_match_not_index)
    if not res_match_is.empty:
        res_match_is.drop(labels=community, axis=1, inplace=True)
        res_match_merge = res_match_merge.append(res_match_is)
    # 处理交叉情况
    clear_cross_handle(handle_not_address_tw=handle_not_address_tw,
                       handle_not_community_tw=handle_not_community_tw,
                       handle_not_address_third=handle_not_address_third,
                       handle_not_community_third=handle_not_community_third,
                       handle_address_third=fields_third_0_1,
                       handle_address_tw=fields_tw_0_1,
                       handle_community_to_address_third=handle_community_to_address_third,
                       handle_community_to_address_tw=handle_community_to_address_tw
                       , res_match_merge=res_match_merge)
    # 去重
    res_match_merge = res_match_merge.drop_duplicates()
    # res_match_merge.to_csv("test2.csv", encoding="gb18030")
    # 1星
    if not res_match_merge.empty:
        res_match = rank_handle_inner(res_match_is=res_match_merge, res_match_is_sql=res_match_is_sql,
                                      rank_tw=fields_tw_1, rank_third=fields_third_1, rank=1,
                                      field_list=total_floor, house_third=house_third, house_tw=house_tw)
        # 有下一步判断的必要
        res_match_is = res_match.get("res_match_is")
        # 准备写入的数据库df
        res_match_is_sql = res_match.get("res_match_is_sql")
    else:
        return res_match_is_sql
    # 2星
    if not res_match_is.empty:
        res_match = rank_handle_inner(res_match_is=res_match_is, res_match_is_sql=res_match_is_sql,
                                      rank_tw=fields_tw_2, rank_third=fields_third_2, rank=2,
                                      field_list=floor, house_third=house_third, house_tw=house_tw)
        res_match_is = res_match.get("res_match_is")
        res_match_is_sql = res_match.get("res_match_is_sql")
    else:
        return res_match_is_sql

    # 3星
    if not res_match_is.empty:
        res_match = rank_handle_inner(res_match_is=res_match_is, res_match_is_sql=res_match_is_sql,
                                      rank_tw=fields_tw_3, rank_third=fields_third_3, rank=3,
                                      field_list=room_type, house_third=house_third, house_tw=house_tw)
        res_match_is = res_match.get("res_match_is")
        res_match_is_sql = res_match.get("res_match_is_sql")
    else:
        return res_match_is_sql

    # 4星
    if not res_match_is.empty:
        res_match = rank_handle_inner(res_match_is=res_match_is, res_match_is_sql=res_match_is_sql,
                                      rank_tw=fields_tw_4, rank_third=fields_third_4, rank=4,
                                      field_list=room_area, house_third=house_third, house_tw=house_tw)
        res_match_is = res_match.get("res_match_is")
        res_match_is_sql = res_match.get("res_match_is_sql")
        if not res_match_is.empty:
            res_match_is.loc[:, 'StarLevel'] = 5
            res_match_is = pd.merge(
                left=res_match_is, right=house_third.loc[:, id_third_resource], sort=False,
                how='left'
            )
            res_match_is = pd.merge(
                left=res_match_is, right=house_third.loc[:, id_third_status], sort=False,
                how='left'
            )
            res_match_is = pd.merge(
                left=res_match_is, right=house_tw.loc[:, id_tw_estate], sort=False,
                how='left'
            )
            res_match_is_sql = res_match_is_sql.append(res_match_is)
        return res_match_is_sql
    else:
        return res_match_is_sql


def clear_cross_handle(handle_not_address_tw,
                       handle_not_community_tw,
                       handle_not_address_third,
                       handle_not_community_third, handle_address_tw, handle_address_third,
                       handle_community_to_address_third,
                       handle_community_to_address_tw, res_match_merge):
    # 小区匹配：库内地址|第三方小区
    # 排除第三方为空的情况
    handle_not_community_third = handle_not_community_third[
        handle_not_community_third['PropertyCommunity'].isnull()]
    # 匹配指定字段
    res_match = pd.merge(handle_not_community_third, handle_not_address_tw,
                         left_on=["PropertyCommunity"], right_on=["PropertyAddress"],
                         how='left')
    res_match_not_index = res_match[res_match['RoomId'].isnull()].index
    # 找出匹配到的
    res_match_is = res_match.drop(labels=res_match_not_index)
    # 如果不为空
    if not res_match_is.empty:
        # 删除指定字段
        res_match_is.drop(labels=["PropertyCommunity", "PropertyAddress"], axis=1, inplace=True)
        # 加入res_match_merge
        res_match_merge = res_match_merge.append(res_match_is)

    # 小区匹配：第三方地址|库内小区
    # 排除第三方为空的情况
    handle_not_address_third = handle_not_address_third[
        handle_not_address_third['PropertyAddress'].isnull()]
    # 匹配指定字段
    res_match = pd.merge(handle_not_address_third, handle_not_community_tw,
                         left_on=["PropertyAddress"], right_on=["PropertyCommunity"],
                         how='left')
    res_match_not_index = res_match[res_match['RoomId'].isnull()].index
    # 找出匹配到的
    res_match_is = res_match.drop(labels=res_match_not_index)
    # 如果不为空
    if not res_match_is.empty:
        # 删除指定字段
        res_match_is.drop(labels=["PropertyCommunity", "PropertyAddress"], axis=1, inplace=True)
        # 加入res_match_merge
        res_match_merge = res_match_merge.append(res_match_is)

    # 地址匹配：第三方地址|库内小区转地址
    # 排除第三方为空的情况
    handle_community_to_address_tw = handle_community_to_address_tw[
        handle_community_to_address_tw['road'].isnull() | handle_community_to_address_tw['alley'].isnull()][
        ["RoomId", 'road', 'alley']]
    index_null = handle_community_to_address_tw.index
    handle_community_to_address_tw = handle_community_to_address_tw.drop(labels=index_null)
    # 匹配指定字段
    res_match = pd.merge(handle_address_third, handle_community_to_address_tw,
                         how='left')
    res_match_not_index = res_match[res_match['RoomId'].isnull()].index
    # 找出匹配到的
    res_match_is = res_match.drop(labels=res_match_not_index)
    # 如果不为空
    if not res_match_is.empty:
        # 删除指定字段
        res_match_is.drop(labels=["road", "alley"], axis=1, inplace=True)
        # 加入res_match_merge
        res_match_merge = res_match_merge.append(res_match_is)

    # 地址匹配：第三方小区转地址|库内地址
    # 排除第三方为空的情况
    handle_community_to_address_third = handle_community_to_address_third[
        handle_community_to_address_third['road'].isnull() | handle_community_to_address_third['alley'].isnull()][
        ["ThirdId", 'road', 'alley']]
    index_null = handle_community_to_address_third.index
    handle_community_to_address_third = handle_community_to_address_third.drop(labels=index_null)
    # 匹配指定字段
    res_match = pd.merge(handle_community_to_address_third, handle_address_tw,
                         how='left')
    res_match_not_index = res_match[res_match['RoomId'].isnull()].index
    # 找出匹配到的
    res_match_is = res_match.drop(labels=res_match_not_index)
    # 如果不为空
    if not res_match_is.empty:
        # 删除指定字段
        res_match_is.drop(labels=["road", "alley"], axis=1, inplace=True)
        # 加入res_match_merge
        res_match_merge = res_match_merge.append(res_match_is, sort=False)
    return res_match_merge
