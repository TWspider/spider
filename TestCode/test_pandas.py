import pandas as pd
from sqlalchemy import create_engine

import difflib
import jieba
from sqlalchemy.types import VARCHAR, INT
import time
import re
import numpy as np


# import pymssql
# import pyodbc


class TestPanda:
    def __init__(self):
        pass
        # self.engine_tw_house = create_engine('mssql+pymssql://tw_user:123456@10.55.5.7/TWEstate')
        # self.engine_third_house = create_engine('mssql+pymssql://tw_user:123456@10.55.5.7/TWSpider')

    def sql_select(self):
        '''
        用pandas读取数据库数据
        :return:
        '''
        res = pd.read_sql(
            "select RoomId as id_third,HouseUrl,PropertyAddress,PropertyCommunity,TotalFloor,Floor,HouseType,BuildingSquare,HouseDesc,PriceUnit from ThirdHouseResource",
            self.engine_tw_house)
        return res

    def sql_insert(self, df):
        '''
        传入dataframe插入temp表
        :param df:
        :return:
        '''
        type_res = {
            'id_third': VARCHAR(length=255),
            'PropertyAddress': VARCHAR(length=255),
            'PropertyCommunity': VARCHAR(length=255),
            'TotalFloor': VARCHAR(length=255),
            'Floor': VARCHAR(length=255),
            'HouseType': VARCHAR(length=255),
            'BuildingSquare': VARCHAR(length=255),
            'HouseDesc': VARCHAR(length=255),
            'PriceUnit': VARCHAR(length=255),
        }
        df.to_sql('temp', con=self.engine_third_house, if_exists="append", index=False, dtype=type_res)

    def panda_method(self, df1, df2):
        '''
        :return:
        '''
        # 取反
        res = df1.loc[~df1["b"].isin(list(['6'])), "sd"]
        # 列操作
        df1['ID'] = df1.loc[:, 'ID'].apply(lambda x: x.replace(".0", ""))
        # 合并
        pd.merge(df1, df2,
                 sort=False,
                 how='left')
        # 转为dataframe
        df1 = df1["b"].to_frame()
        # 删除行、列
        df1.drop(labels=["sda"], axis=1)
        # 去重
        df1.drop_duplicates()
        # 附加
        df1 = df1.append(df2)
        # 重命名
        res = df1.rename(columns={'key': 'test'}, inplace=True)
        # array转列表
        res = df1.to_list()
        # dataframe转列表
        res = df1.values
        # 重新排序
        df1 = df1.reindex
        return res

    def handle_split_line(self, ls_col):
        new_col = []
        new_index = []
        for index, col in enumerate(ls_col):
            if col != None:
                col = str(col).split(",")
                new_col += col
                len_col = len(col) * [index]
                new_index += len_col
        return new_col,new_index

    def leave_word(self, house_third):
        self.handle_split_line(house_third['alley'])
        # print(pd.DataFrame({"alley": ["e1", "e2", "e2", "e3", ]})["alley"])
        third_house = house_third.drop('alley', axis=1).join(
            pd.DataFrame({"alley": ["e1", "e2", "e2", "e3", ]}, index=[0, 0, 1, 2])["alley"]
        )
        print(third_house)
        # third_house = third_house.drop('road', axis=1).join(
        #     third_house['road'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).rename('road')
        # )

        # new_road = []
        # new_alley = []
        # for road in ls_road:
        #     if road == None:
        #         new_road.append(road)
        #     else:
        #         new_road += road
        #
        # house_third = house_third.drop('road', axis=1).join(new_road)
        # ls_alley = house_third["alley"].str.split(',', expand=False)
        # for alley in ls_alley:
        #     if alley == None:
        #         new_alley.append(alley)
        #     else:
        #         new_alley += alley

    def test(self):
        left = pd.DataFrame({'road': ['xx路,yy路,dd', "1", "2", None], 'alley': ["e1,e2", "e2", "e3", None]})
        df1 = pd.DataFrame({'B': [1, 2, 3, 4], 'E': ["e1", "e2", "e3", "e4"]})
        # print(df1['B'], type(df1['B']))
        df2 = pd.DataFrame({'C': [1, 2, 3, 4], 'F': [("f" + str(i)) for i in range(1, 5)]})
        self.leave_word(left)


if __name__ == '__main__':
    tp = TestPanda()
    time_start = time.time()
    tp.test()
    time_end = time.time()
    # 运行时间
    print(time_end - time_start)
    '''
    192.168.160.128    h1
    192.168.160.129    h2
    192.168.160.130    h3
        
    '''
