import pandas as pd
import time
import datetime
from sqlalchemy import create_engine
from setting import HOST, USER, PASSWORD, DATABASE
from sqlalchemy.types import NVARCHAR, INT, DATETIME
from clear_data import *
from rank_handle import *

engine_third_house = create_engine('mssql+pymssql://{}:{}@{}/{}'.format(USER, PASSWORD, HOST, DATABASE))
engine_res = create_engine(
    'mssql+pyodbc://{0}:{1}@{2}/{3}?driver=SQL Server Native Client 11.0'.format(USER, PASSWORD, HOST,
                                                                                 DATABASE), fast_executemany=True)
dtype = {
    "ThirdId": INT,
    "RoomId": INT,
    "EstateId": INT,
    "Resource": NVARCHAR(100),
    "StarLevel": INT,
    "InsertTime": DATETIME,
    "UpdateTime": DATETIME,
    "Status": NVARCHAR(100)
}


def match_rank(house_rank, sql_select_third, sql_select_tw, fetch_num_tw, fetch_num_third, flag):
    # 新增匹配
    house_third_list = pd.read_sql(
        sql_select_third,
        engine_third_house, chunksize=fetch_num_third)
    res_match_is_sql = pd.DataFrame(
        columns=["ThirdId", "RoomId", 'EstateId', 'Resource', "StarLevel", "InsertTime", 'UpdateTime',
                 'Status'])
    res_match_is_sql.to_sql(house_rank, con=engine_res, if_exists="append", index=False,
                            dtype=dtype)
    for house_third in house_third_list:

        time_start_inner = time.time()
        if not house_third.empty:
            # # 处理第三方房源
            # # 处理地址和小区交叉情况
            handle_not_address_third = house_third[["ThirdId", 'PropertyAddress']]
            handle_not_community_third = house_third[["ThirdId", 'PropertyCommunity']]
            handle_community_to_address_third = handle_community_to_address_third_handle(house_third)
            # 处理对应情况
            house_third = handle_address_third(house_third)
            house_third = handle_community_third(house_third)
            house_third = handle_total_floor_third(house_third)
            house_third = handle_floor_third(house_third)
            house_third = handle_room_type_third(house_third)
            house_third = handle_room_area_third(house_third, flag)
            house_third = house_third.drop(labels=["HouseType", 'HouseDesc', 'PriceUnit'], axis=1)
            # # 读取、清理库内房源
            house_list_tw = get_house_tw(fetch_num_tw=fetch_num_tw, sql_select_tw=sql_select_tw)
            # # 获取每次匹配到的集合
            id_third_match_is = set()
            # # 循环清洗、并设定星级
            for house_tw in house_list_tw:

                res_match_is_sql = pd.DataFrame(
                    columns=["ThirdId", "RoomId", 'EstateId', 'Resource', "StarLevel", "InsertTime", 'UpdateTime',
                             'Status'])
                # 处理地址和小区交叉情况
                handle_not_address_tw = house_tw[["RoomId", 'PropertyAddress']]
                handle_not_community_tw = house_tw[["RoomId", 'PropertyCommunity']]
                handle_community_to_address_tw = handle_address_tw(house_tw)
                # 处理对应情况
                res_tw = handle_address_tw(house_tw)
                res_tw = handle_floor_tw(res_tw)
                res_match_is_sql = rank_handle(house_third=house_third, house_tw=res_tw,
                                               res_match_is_sql=res_match_is_sql,
                                               handle_not_address_tw=handle_not_address_tw,
                                               handle_not_community_tw=handle_not_community_tw,
                                               handle_not_address_third=handle_not_address_third,
                                               handle_not_community_third=handle_not_community_third,
                                               handle_community_to_address_third=handle_community_to_address_third,
                                               handle_community_to_address_tw=handle_community_to_address_tw
                                               )
                res_match_is_sql = res_match_is_sql.drop_duplicates()
                if not res_match_is_sql.empty:
                    input_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    res_match_is_sql.loc[:, 'InsertTime'] = input_time
                    res_match_is_sql.to_sql(house_rank, con=engine_res, if_exists="append",
                                            index=False,
                                            dtype=dtype)
                    id_third_match_is |= set(res_match_is_sql['ThirdId'].to_list())
                    print("插入匹配数据")
                else:
                    print("未匹配到数据")
            # 计算差值,获取为空的id插入数据库
            id_third_all = set(house_third.loc[:, "ThirdId"])
            id_third_match_not = list(id_third_all - id_third_match_is)
            input_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            if id_third_match_not:
                # 合并来源
                house_third = house_third[["ThirdId", 'Resource', 'Status']]
                res_match_not_sql = house_third.loc[house_third['ThirdId'].isin(id_third_match_not)]
                res_match_not_sql.loc[:, 'StarLevel'] = 0
                res_match_not_sql.loc[:, 'InsertTime'] = input_time
                res_match_not_sql.loc[:, 'RoomId'] = None
                res_match_not_sql.loc[:, 'EstateId'] = None
                res_match_not_sql.to_sql(house_rank, con=engine_res, if_exists="append",
                                         index=False,
                                         dtype=dtype)
                print("插入未匹配数据")
        else:
            print("无新增数据")
        time_end_inner = time.time()
        print('单次匹配时间', (time_end_inner - time_start_inner))



def match_rank_t(house_rank, sql_select_third, sql_select_tw, fetch_num_tw, fetch_num_third, flag):
    # 新增匹配
    house_third_list = pd.read_sql(
        sql_select_third,
        engine_third_house, chunksize=fetch_num_third)
    res_match_is_sql = pd.DataFrame(
        columns=["ThirdId", "RoomId", 'EstateId', 'Resource', "StarLevel", "InsertTime", 'UpdateTime',
                 'Status'])
    res_match_is_sql.to_sql(house_rank, con=engine_res, if_exists="append", index=False,
                            dtype=dtype)
    for house_third in house_third_list:
        house_list_tw = get_house_tw(fetch_num_tw=fetch_num_tw, sql_select_tw=sql_select_tw)
        if not house_third.empty:
            for index,house_tw in enumerate(house_list_tw):
                print(index)

