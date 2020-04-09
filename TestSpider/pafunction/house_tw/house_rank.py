import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import difflib
import jieba
import time
import re
from sqlalchemy.types import VARCHAR, INT
import pymssql


class ClearDate(object):
    '''
    包括分级处理、以及常用的处理函数111
    '''

    def __init__(self):
        jieba.load_userdict('community.csv')
        self.shcomm = set(pd.read_csv('community.csv', header=None)[0].tolist())
        jieba.load_userdict('./road.csv')
        self.shroad = set(pd.read_csv('road.csv', header=None)[0].tolist())

    def extract_other(self, index, PropertyCommunity_list, third_house):
        # 已经填补完desc，将小区为空的记录取出来
        try:
            word_list = jieba.cut(third_house.loc[index, 'HouseDesc'], cut_all=False, HMM=False)
            PropertyCommunity_list.append(list(word_list)[0])
        except Exception as e:
            PropertyCommunity_list.append(None)

    def add_comm(self, word_list, shcomm):
        flag = 0
        word_set = set()
        for word in word_list:
            if word in shcomm:
                flag = 1
                word_set.add(word)
        word_str = ','.join(word_set)
        return flag, word_str

    def leave_word(self, house_third):
        third_house = house_third.drop('alley', axis=1).join(
            house_third['alley'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).rename('alley'))

        third_house = third_house.drop('road', axis=1).join(
            third_house['road'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).rename('road'))
        third_house.loc[:, 'road_'] = third_house['road'] + third_house['alley']
        third_house.drop_duplicates(inplace=True)
        third_house.reset_index(drop=True, inplace=True)
        third_house.fillna('', inplace=True)
        return third_house

    def other_discomm(self, third_house):
        # 提取出所有小区名,作为停用词(小区+desc中提取的小区)
        stop_word_comm = third_house['PropertyCommunity'].unique().tolist()
        # 从desc中提取小区
        ext_comm = []
        cond = third_house['PropertyCommunity'].isnull()
        for desc in third_house[cond]['HouseDesc'].unique():
            pattern = re.compile('.*?·(.*?)\s')
            res = pattern.findall(desc)
            try:
                ext_comm.append(res[0])
            except:
                ext_comm.append(desc)
        # 将desc中提取的小区和直接取出的小区求交集
        finally_comm = set()
        for comm in ext_comm + stop_word_comm:
            patt = re.compile('(.*?)[\(\（]')
            try:
                #         print(comm)
                res = patt.findall(comm)
                finally_comm.add(res[0])
            except:
                finally_comm.add(comm)
        lj_total_comm = pd.DataFrame(data=finally_comm, columns=['comm'])
        lj_total_comm.to_csv('community.csv', index=None)
        # 将停用词文件读取出来，设置特定词语不被分开
        stop_word_set = set(pd.read_csv('./community.csv')['comm'].to_list())
        # 加载自己的停用词典
        jieba.load_userdict('community.csv')
        # 调整词典，使特定的词语不被分开
        for word in stop_word_set:
            if word != None:
                jieba.suggest_freq(str(word), True)

    def add_road(self, word_list):
        flag = 0
        word_set = set()
        for word in word_list:
            if word in self.shroad:
                flag = 1
                word_set.add(word)
        word_str = ','.join(word_set)
        return flag, word_str

    def which_reg(self, addr, alley_list):
        try:
            reg1 = re.compile('(\d+)-(\d+)号双')
            reg2 = re.compile('(\d+)-(\d+)双号')
            reg3 = re.compile('(\d+)-(\d+)\(双\)号')
            reg4 = re.compile('(\d+)-(\d+)号\(双\)')
            reg_double = reg1.findall(addr) or reg2.findall(addr) or reg3.findall(addr) or reg4.findall(addr)

            reg5 = re.compile('(\d+)-(\d+)\(单号\)')
            reg6 = re.compile('(\d+)-(\d+)单号')
            reg7 = re.compile('(\d+)-(\d+)\(单\)号')
            reg8 = re.compile('(\d+)-(\d+)号\(单\)')
            reg9 = re.compile('(\d+)-(\d+)\(单\)')
            reg_single = reg5.findall(addr) or reg6.findall(addr) or reg7.findall(addr) or reg8.findall(
                addr) or reg9.findall(addr)

            reg10 = re.compile('路(\d+)-(\d+)号')
            reg11 = re.compile('街(\d+)-(\d+)号')
            reg_continuous = reg10.findall(addr) or reg11.findall(addr)

            reg12 = re.compile('路(\d+)弄')
            reg13 = re.compile('路(\d+)号')
            reg14 = re.compile('街(\d+)弄')
            reg15 = re.compile('街(\d+)号')
            reg16 = re.compile('路(\d+)弄\d+号')
            reg17 = re.compile('道(\d+)号')
            reg18 = re.compile('村(\d+)号')
            reg19 = re.compile('村(\d+)弄')
            reg29 = re.compile('道(\d+)弄')
            reg_base = reg12.findall(addr) or reg13.findall(addr) or reg14.findall(addr) or reg15.findall(
                addr) or reg16.findall(addr) or reg17.findall(addr) or reg18.findall(addr) or reg19.findall(
                addr) or reg29.findall(addr)

            reg20 = re.compile('(\d+)-(\d+)-(\d+)-(\d+)-(\d+)-(\d+)-(\d+)-(\d+)-(\d+)-(\d+)-(\d+)')
            reg21 = re.compile('(\d+)-(\d+)-(\d+)-(\d+)-(\d+)-(\d+)-(\d+)-(\d+)-(\d+)-(\d+)')
            reg22 = re.compile('(\d+)-(\d+)-(\d+)-(\d+)-(\d+)-(\d+)-(\d+)-(\d+)-(\d+)')
            reg23 = re.compile('(\d+)-(\d+)-(\d+)-(\d+)-(\d+)-(\d+)-(\d+)-(\d+)')
            reg24 = re.compile('(\d+)-(\d+)-(\d+)-(\d+)-(\d+)-(\d+)-(\d+)')
            reg25 = re.compile('(\d+)-(\d+)-(\d+)-(\d+)-(\d+)-(\d+)')
            reg26 = re.compile('(\d+)-(\d+)-(\d+)-(\d+)-(\d+)')
            reg27 = re.compile('(\d+)-(\d+)-(\d+)-(\d+)')
            reg28 = re.compile('(\d+)-(\d+)-(\d+)')
            reg_random = reg20.findall(addr) or reg21.findall(addr) or reg23.findall(addr) or reg24.findall(
                addr) or reg25.findall(addr) or reg26.findall(addr) or reg27.findall(addr) or reg28.findall(addr)

            reg30 = re.compile('\d+')
            reg_other = reg30.findall(addr)

            if len(reg_double) > 0 and int(reg_double[0][0]) < int(reg_double[0][1]):
                res = [str(i) for i in range(int(reg_double[0][0]), int(reg_double[0][1]) + 1, 2)]
                alley = ','.join(res)
                alley_list.append(alley)
            #                     print(reg_double,addr,'=========',alley)

            elif len(reg_single) > 0 and int(reg_single[0][0]) < int(reg_single[0][1]):
                res = [str(i) for i in range(int(reg_single[0][0]), int(reg_single[0][1]) + 1, 2)]
                alley = ','.join(res)
                alley_list.append(alley)
            #                     print(reg_single,addr,'$$$$$$$',alley)

            elif len(reg_continuous) > 0 and int(reg_continuous[0][0]) < int(reg_continuous[0][1]):
                res = [str(i) for i in range(int(reg_continuous[0][0]), int(reg_continuous[0][1]) + 1)]
                alley = ','.join(res)
                alley_list.append(alley)
            #                     print(reg_continuous,addr,'*******',alley)
            elif len(reg_random) > 0:
                res = [str(i) for i in reg_random[0]]
                alley = ','.join(res)
                alley_list.append(alley)
            #             print(reg_random,addr,'@@@@@@@@@',alley)
            elif len(reg_base) > 0:
                res = [str(i) for i in reg_base]
                alley = ','.join(res)
                alley_list.append(alley)
            #             print(reg_base,addr,'!!!!!!!!!!!!',alley)
            elif len(reg_other) > 0:
                res = [str(i) for i in reg_other]
                alley = ','.join(res)
                alley_list.append(alley)
            #             print(reg_base,addr,'###########',alley)
            else:
                alley_list.append(None)
        except:
            alley_list.append(None)

    def handle_address_tw(self, res_tw):
        # 处理数据库中的地址
        road_list = []
        for addr in res_tw['PropertyAddress']:
            try:
                word_list = list(jieba.cut(addr, cut_all=False, HMM=False))
                flag, word_str = self.add_road(word_list)
                if flag == 1:
                    road_list.append(word_str)
                else:
                    # 非路号，而是小区
                    #         print(addr)
                    try:
                        sp_comm = addr.split(' ')
                        road_list.append(None)
                    #                 road_list.append(sp_comm[1])
                    except:
                        #                 print(addr)
                        road_list.append(None)
            except:
                road_list.append(None)
        res_tw.loc[:, 'road'] = pd.Series(data=road_list)
        alley_list = []
        for addr in res_tw['PropertyAddress']:
            self.which_reg(addr, alley_list)
        res_tw.loc[:, 'alley'] = pd.Series(data=alley_list)
        # 将路拆开
        res_tw = res_tw.drop('road', axis=1).join(
            res_tw.loc[:, 'road'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).rename("road"))
        # 将号拆开
        res_tw = res_tw.drop('alley', axis=1).join(
            res_tw.loc[:, 'alley'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).rename("alley"))
        res_tw.drop_duplicates(inplace=True)
        res_tw.reset_index(drop=True, inplace=True)
        return res_tw

    def handle_community_to_address_tw(self, res_tw):
        # 处理数据库中的地址
        road_list = []
        for addr in res_tw['PropertyCommunity']:
            try:
                word_list = list(jieba.cut(addr, cut_all=False, HMM=False))
                flag, word_str = self.add_road(word_list)
                if flag == 1:
                    road_list.append(word_str)
                else:
                    # 非路号，而是小区
                    #         print(addr)
                    try:
                        sp_comm = addr.split(' ')
                        road_list.append(None)
                    #                 road_list.append(sp_comm[1])
                    except:
                        #                 print(addr)
                        road_list.append(None)
            except:
                road_list.append(None)
        res_tw.loc[:, 'road'] = pd.Series(data=road_list)
        alley_list = []
        for addr in res_tw['PropertyCommunity']:
            self.which_reg(addr, alley_list)
        res_tw.loc[:, 'alley'] = pd.Series(data=alley_list)
        # 将路拆开
        res_tw = res_tw.drop('road', axis=1).join(
            res_tw.loc[:, 'road'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).rename("road"))
        # 将号拆开
        res_tw = res_tw.drop('alley', axis=1).join(
            res_tw.loc[:, 'alley'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).rename("alley"))
        res_tw.drop_duplicates(inplace=True)
        res_tw.reset_index(drop=True, inplace=True)
        return res_tw

    def handle_floor_tw(self, res_tw):
        floor_list_new = []
        floor_list = res_tw['Floor']
        for floor in floor_list:
            if floor == '高楼层':
                floor_list_new.append('高层')
            elif floor == '低楼层':
                floor_list_new.append('低层')
            elif floor == '中楼层':
                floor_list_new.append('中层')
            elif floor == 'None':
                floor_list_new.append('')
            else:
                floor_list_new.append(floor)
        res_tw.loc[:, 'Floor'] = pd.Series(data=floor_list_new)
        return res_tw

    def handle_address_third(self, third_house):
        # 从房源描述中提取小区名
        self.other_discomm(third_house)
        # 对路进行提取
        for road in self.shroad:
            if road != None:
                jieba.suggest_freq(str(road), True)
        road_list = []
        for addr in third_house['PropertyAddress']:
            try:
                word_list = list(jieba.cut(addr, cut_all=False, HMM=False))
                flag, word_str = self.add_road(word_list)
                if flag == 1:
                    road_list.append(word_str)
                else:
                    # 非路号，而是小区
                    #         print(addr)
                    try:
                        road_list.append(None)
                    except:
                        road_list.append(None)
            except:
                road_list.append(None)
        third_house.loc[:, 'road'] = pd.Series(data=road_list)
        # 对号进行提取
        alley_list = []
        reg = re.compile('\d+')
        for addr in third_house['PropertyAddress']:
            try:
                res = reg.findall(addr)
                if len(res) > 0:
                    alley = ','.join(res)
                    alley_list.append(alley)
                else:
                    alley_list.append(None)
            except:
                alley_list.append(None)
        third_house.loc[:, 'alley'] = pd.Series(data=alley_list)
        # 拆分路和号，将一条记录拆分成多条
        third_house = self.leave_word(third_house)
        # 排除无效地址的情况
        drop_list = ['距离地铁', '近', '中环', '外环', '内环', '附近']
        for word in drop_list:
            cond = third_house['PropertyAddress'].str.contains(word)
            index = third_house[cond].index
            third_house.loc[index, 'road'] = pd.Series(data=['' for i in range(len(index))])
            third_house.loc[index, 'road_'] = pd.Series(data=['' for i in range(len(index))])
            third_house.loc[index, 'alley'] = pd.Series(data=['' for i in range(len(index))])

        # 对路进行提取
        road_list = []
        for addr in third_house['PropertyAddress']:
            try:
                word_list = list(jieba.cut(addr, cut_all=False, HMM=False))
                flag, word_str = self.add_road(word_list)
                if flag == 1:
                    road_list.append(word_str)
                else:
                    # 非路号，而是小区
                    #         print(addr)
                    try:
                        road_list.append(None)
                    except:
                        road_list.append(None)
            except:
                road_list.append(None)
        third_house.loc[:, 'road'] = pd.Series(data=road_list)
        # 对号进行提取
        alley_list = []
        reg = re.compile('\d+')
        for addr in third_house['PropertyAddress']:
            try:
                res = reg.findall(addr)
                if len(res) > 0:
                    alley = ','.join(res)
                    alley_list.append(alley)
                else:
                    alley_list.append(None)
            except:
                alley_list.append(None)
        third_house.loc[:, 'alley'] = pd.Series(data=alley_list)
        # 拆分路和号
        self.leave_word(third_house)
        return third_house

    def handle_community_to_address_third(self, third_house):
        # 从房源描述中提取小区名
        self.other_discomm(third_house)
        # 对路进行提取
        for road in self.shroad:
            if road != None:
                jieba.suggest_freq(str(road), True)
        road_list = []
        for addr in third_house['PropertyCommunity']:
            try:
                word_list = list(jieba.cut(addr, cut_all=False, HMM=False))
                flag, word_str = self.add_road(word_list)
                if flag == 1:
                    road_list.append(word_str)
                else:
                    # 非路号，而是小区
                    #         print(addr)
                    try:
                        road_list.append(None)
                    except:
                        road_list.append(None)
            except:
                road_list.append(None)
        third_house.loc[:, 'road'] = pd.Series(data=road_list)
        # 对号进行提取
        alley_list = []
        reg = re.compile('\d+')
        for addr in third_house['PropertyCommunity']:
            try:
                res = reg.findall(addr)
                if len(res) > 0:
                    alley = ','.join(res)
                    alley_list.append(alley)
                else:
                    alley_list.append(None)
            except:
                alley_list.append(None)
        third_house.loc[:, 'alley'] = pd.Series(data=alley_list)
        # 拆分路和号，将一条记录拆分成多条
        third_house = self.leave_word(third_house)
        # 排除无效地址的情况
        drop_list = ['距离地铁', '近', '中环', '外环', '内环', '附近']
        for word in drop_list:
            cond = third_house['PropertyCommunity'].str.contains(word)
            index = third_house[cond].index
            third_house.loc[index, 'road'] = pd.Series(data=['' for i in range(len(index))])
            third_house.loc[index, 'road_'] = pd.Series(data=['' for i in range(len(index))])
            third_house.loc[index, 'alley'] = pd.Series(data=['' for i in range(len(index))])
        # 对路进行提取
        road_list = []
        for addr in third_house['PropertyCommunity']:
            try:
                word_list = list(jieba.cut(addr, cut_all=False, HMM=False))
                flag, word_str = self.add_road(word_list)
                if flag == 1:
                    road_list.append(word_str)
                else:
                    # 非路号，而是小区
                    #         print(addr)
                    try:
                        road_list.append(None)
                    except:
                        road_list.append(None)
            except:
                road_list.append(None)
        third_house.loc[:, 'road'] = pd.Series(data=road_list)
        # 对号进行提取
        alley_list = []
        reg = re.compile('\d+')
        for addr in third_house['PropertyCommunity']:
            try:
                res = reg.findall(addr)
                if len(res) > 0:
                    alley = ','.join(res)
                    alley_list.append(alley)
                else:
                    alley_list.append(None)
            except:
                alley_list.append(None)
        third_house.loc[:, 'alley'] = pd.Series(data=alley_list)
        # 拆分路和号
        self.leave_word(third_house)
        return third_house

    def handle_community_third(self, third_house):
        for index, comm in enumerate(third_house['PropertyCommunity']):
            if comm == '' and third_house.loc[index, 'HouseDesc'] == '':
                third_house.loc[index, 'HouseDesc'] = third_house.loc[index, 'PriceUnit']
        cond = third_house['PropertyCommunity'].isnull()
        PropertyCommunity_list = []
        for index in third_house[cond].index:
            desc = third_house.loc[index, 'HouseDesc']
            pattern = re.compile('(.*?)·(.*?)\s')
            res = pattern.findall(desc)
            try:
                # 链家整租，直接提取
                if res[0][0] == '整租':
                    PropertyCommunity_list.append(res[0][1])
                else:
                    self.extract_other(index, PropertyCommunity_list, third_house)
            except:
                self.extract_other(index, PropertyCommunity_list, third_house)
        temp = third_house[cond]
        temp.loc[:, 'PropertyCommunity'] = pd.Series(data=PropertyCommunity_list).values
        temp_array = np.concatenate((third_house[third_house['PropertyCommunity'].notnull()].values, temp.values))
        third_house = pd.DataFrame(data=temp_array, columns=third_house.columns)
        # 处理小区字段是地址的情况
        # 调整词典，使特定的词语不被分开
        for comm in self.shcomm:
            if comm != None:
                jieba.suggest_freq(str(comm), True)
        comm_list = []
        for comm in third_house['PropertyCommunity']:
            word_list = list(jieba.cut(comm, cut_all=False, HMM=False))
            flag, word_str = self.add_comm(word_list, self.shcomm)
            if flag == 1:
                comm_list.append(word_str)
            else:
                # 非小区，而是路号
                #         print(addr)
                try:
                    comm_list.append(comm)
                except:
                    comm_list.append(comm)
        third_house.loc[:, 'PropertyCommunity'] = pd.Series(data=comm_list)
        return third_house

    def handle_total_floor_third(self, third_house):
        TotalFloor_list = []
        reg = re.compile('\d+')
        for totalfloor in third_house['TotalFloor']:
            try:
                res = reg.findall(totalfloor)
                if len(res) > 0:
                    TotalFloor_list.append(res[0])
                else:
                    TotalFloor_list.append(None)
            except:
                # None
                TotalFloor_list.append(None)
        third_house.loc[:, 'TotalFloor'] = pd.Series(data=TotalFloor_list)
        return third_house

    def handle_floor_third(self, third_house):
        floor_list_new = []
        floor_list = third_house['Floor']
        for floor in floor_list:
            if floor == '高楼层':
                floor_list_new.append('高层')
            elif floor == '低楼层':
                floor_list_new.append('低层')
            elif floor == '中楼层':
                floor_list_new.append('中层')
            elif floor == 'None':
                floor_list_new.append('')
            else:
                floor_list_new.append(floor)
        third_house.loc[:, 'Floor'] = pd.Series(data=floor_list_new)
        return third_house

    def handle_room_type_third(self, third_house):
        HouseType = third_house['HouseType']
        room_list = []
        hall_list = []
        toilet_list = []
        for index, type_ in enumerate(HouseType):
            try:
                room = re.findall('(\d+)室', type_)
                if room == []:
                    room_list.insert(index, 0)
                else:
                    room_list.insert(index, room[0])

                hall = re.findall('(\d+)厅', type_)
                if hall == []:
                    hall_list.insert(index, 0)
                else:
                    hall_list.insert(index, hall[0])

                toilet = re.findall('(\d+)卫', type_)
                if toilet == []:
                    toilet_list.insert(index, 0)
                else:
                    toilet_list.insert(index, toilet[0])
            except:
                # None
                room_list.insert(index, None)
                hall_list.insert(index, None)
                toilet_list.insert(index, None)
        if len(room_list) == len(hall_list) == len(toilet_list) == third_house['HouseType'].shape[0]:
            third_house.loc[:, 'room'] = pd.Series(data=room_list)
            third_house.loc[:, 'hall'] = pd.Series(data=hall_list)
            third_house.loc[:, 'toilet'] = pd.Series(data=toilet_list)
        return third_house

    def handle_room_area_third(self, third_house):
        BuildingSquare_list = third_house['BuildingSquare'].tolist()
        BuildingSquare = []
        for square in BuildingSquare_list:
            try:
                res = re.split(" |平米|㎡", square)
                BuildingSquare.append(res[0])
            except:
                BuildingSquare.append(None)
        # 把面积替换掉
        if len(BuildingSquare) == third_house['BuildingSquare'].shape[0]:
            third_house.loc[:, 'BuildingSquare'] = pd.Series(data=BuildingSquare)
        else:
            print('上面出错,长度不够')
        return third_house


# 通过roomid判断是否匹配


class RankHanle(object):
    def __init__(self):
        self.engine_third_house = create_engine('mssql+pymssql://bigdata:pUb6Qfv7BFxl@10.10.202.12/TWSpider')
        self.engine_tw_house = create_engine('mssql+pymssql://bigdata:pUb6Qfv7BFxl@10.10.202.12/TWEstate')
        self.engine_res = create_engine(
            'mssql+pyodbc://{0}:{1}@{2}?driver=SQL Server Native Client 11.0'.format("bigdata", "pUb6Qfv7BFxl",
                                                                                     "10.10.202.12/TWSpider"),
            fast_executemany=True)
        # 打标记、找交集、剩下的被定级

        self.house_list_tw = pd.read_sql(
            '''
            select e.EstateAddress as PropertyAddress,e.EstateAreaName as PropertyCommunity,r.TotalLayer as TotalFloor,r.LayerHighLowTypeName as Floor,r.RoomNum as room,r.HallNum as hall,r.ToiletNum as toilet,r.PropertySquare as BuildingSquare,r.RoomId as id_tw from Estate as e inner join room as r on e.EstateId=r.EstateId GROUP BY e.EstateAddress,e.EstateAreaName,r.TotalLayer,r.LayerHighLowTypeName,r.RoomNum,r.HallNum,r.ToiletNum,r.PropertySquare,r.RoomId
            ''',
            self.engine_tw_house, chunksize=100000)
        self.address = ["road", 'alley']
        self.community_road = ["PropertyCommunity", 'road']
        self.community = ["PropertyCommunity"]
        self.total_floor = ['TotalFloor']
        self.floor = ['Floor']
        self.room_type = ['room', 'hall', 'toilet']
        self.room_area = ['BuildingSquare']
        self.id_third = ["id_third"]
        self.id_tw = ["id_tw"]
        self.dtype = {
            "id_third": INT,
            "id_tw": INT,
            "rank": INT,
        }

    def rank_handle_inner(self, res_match_is, res_match_is_sql, rank_tw, rank_third, rank, field_list):
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
        # 除去当前位置为空的，到数据库中pass
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
            res_match_not.loc[:, 'rank'] = rank
            res_match_not.drop(labels=labels_drop, axis=1, inplace=True)
            res_match_is_sql = res_match_is_sql.append(res_match_not)
            # res_match_is.to_csv("test2.csv", encoding='gb18030')
            # print("写入csv")
        res_match_is.drop(labels=labels_drop, axis=1, inplace=True)
        return {"res_match_is": res_match_is, 'res_match_is_sql': res_match_is_sql}

    def rank_handle(self, house_third, house_tw, res_match_is_sql, handle_not_address_tw,
                    handle_not_community_tw,
                    handle_not_address_third,
                    handle_not_community_third, handle_community_to_address_third, handle_community_to_address_tw):
        '''
        只保留匹配到的
        只确定当前两条数据的契合度有多深，以第三方房源为主
        '''
        # TW字段
        fields_tw_0_1 = house_tw[self.address + self.id_tw]
        fields_tw_0_1_null = fields_tw_0_1[fields_tw_0_1['road'].isnull() | fields_tw_0_1['alley'].isnull()]
        index_null = fields_tw_0_1_null.index
        fields_tw_0_1 = fields_tw_0_1.drop(labels=index_null)

        fields_tw_0_2 = house_tw[self.community_road + self.id_tw]
        fields_tw_0_2_null = fields_tw_0_2[fields_tw_0_2['PropertyCommunity'].isnull() | fields_tw_0_2['road'].isnull()]
        index_null = fields_tw_0_2_null.index
        fields_tw_0_2 = fields_tw_0_2.drop(labels=index_null)

        fields_tw_0_3 = house_tw[self.community + self.id_tw]
        fields_tw_0_3_null = fields_tw_0_3[fields_tw_0_3['PropertyCommunity'].isnull()]
        index_null = fields_tw_0_3_null.index
        fields_tw_0_3 = fields_tw_0_3.drop(labels=index_null)

        fields_tw_1 = house_tw[self.total_floor + self.id_tw]
        fields_tw_2 = house_tw[self.floor + self.id_tw]
        fields_tw_3 = house_tw[self.room_type + self.id_tw]
        fields_tw_4 = house_tw[self.room_area + self.id_tw]
        # 第三方字段
        fields_third_0_1 = house_third[self.address + self.id_third]
        fields_third_0_1_null = fields_third_0_1[
            (fields_third_0_1['road'].isnull() | fields_third_0_1['alley'].isnull())]
        index_null = fields_third_0_1_null.index
        fields_third_0_1 = fields_third_0_1.drop(labels=index_null)

        fields_third_0_2 = house_third[self.community_road + self.id_third]
        fields_third_0_2_null = fields_third_0_2[
            fields_third_0_2['PropertyCommunity'].isnull() | fields_third_0_2['road'].isnull()]
        index_null = fields_third_0_2_null.index
        fields_third_0_2 = fields_third_0_2.drop(labels=index_null)

        fields_third_0_3 = house_third[self.community + self.id_third]
        fields_third_0_3_null = fields_third_0_3[fields_third_0_3['PropertyCommunity'].isnull()]
        index_null = fields_third_0_3_null.index
        fields_third_0_3 = fields_third_0_3.drop(labels=index_null)

        fields_third_1 = house_third[self.total_floor + self.id_third]
        fields_third_2 = house_third[self.floor + self.id_third]
        fields_third_3 = house_third[self.room_type + self.id_third]
        fields_third_4 = house_third[self.room_area + self.id_third]
        # 获取本次可匹配到的数据
        res_match_merge = pd.DataFrame()
        # 匹配地址
        res_match = pd.merge(fields_third_0_1, fields_tw_0_1,
                             sort=False,
                             how='left')
        res_match_not_index = res_match[res_match['id_tw'].isnull()].index
        res_match_is = res_match.drop(labels=res_match_not_index)
        if not res_match_is.empty:
            res_match_is.drop(labels=self.address, axis=1, inplace=True)
            res_match_merge = res_match_merge.append(res_match_is)
        # 匹配小区、路
        res_match = pd.merge(fields_third_0_2, fields_tw_0_2,
                             sort=False,
                             how='left')
        res_match_not_index = res_match[res_match['id_tw'].isnull()].index
        res_match_is = res_match.drop(labels=res_match_not_index)
        if not res_match_is.empty:
            res_match_is.drop(labels=self.community_road, axis=1, inplace=True)
            res_match_merge = res_match_merge.append(res_match_is)
        # 匹配小区
        res_match = pd.merge(fields_third_0_3, fields_tw_0_3,
                             sort=False,
                             how='left')
        res_match_not_index = res_match[res_match['id_tw'].isnull()].index
        res_match_is = res_match.drop(labels=res_match_not_index)
        if not res_match_is.empty:
            res_match_is.drop(labels=self.community, axis=1, inplace=True)
            res_match_merge = res_match_merge.append(res_match_is)
        # 处理交叉情况
        self.clear_cross_handle(handle_not_address_tw=handle_not_address_tw,
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
            res_match = self.rank_handle_inner(res_match_is=res_match_merge, res_match_is_sql=res_match_is_sql,
                                               rank_tw=fields_tw_1, rank_third=fields_third_1, rank=1,
                                               field_list=self.total_floor)
            # 有下一步判断的必要
            res_match_is = res_match.get("res_match_is")
            # 准备写入的数据库df
            res_match_is_sql = res_match.get("res_match_is_sql")
        else:
            return res_match_is_sql
        # 2星
        if not res_match_is.empty:
            res_match = self.rank_handle_inner(res_match_is=res_match_is, res_match_is_sql=res_match_is_sql,
                                               rank_tw=fields_tw_2, rank_third=fields_third_2, rank=2,
                                               field_list=self.floor)
            res_match_is = res_match.get("res_match_is")
            res_match_is_sql = res_match.get("res_match_is_sql")
        else:
            return res_match_is_sql
        # 3星
        if not res_match_is.empty:
            res_match = self.rank_handle_inner(res_match_is=res_match_is, res_match_is_sql=res_match_is_sql,
                                               rank_tw=fields_tw_3, rank_third=fields_third_3, rank=3,
                                               field_list=self.room_type)
            res_match_is = res_match.get("res_match_is")
            res_match_is_sql = res_match.get("res_match_is_sql")
        else:
            return res_match_is_sql
        # 4星
        if not res_match_is.empty:
            res_match = self.rank_handle_inner(res_match_is=res_match_is, res_match_is_sql=res_match_is_sql,
                                               rank_tw=fields_tw_4, rank_third=fields_third_4, rank=4,
                                               field_list=self.room_area)
            res_match_is = res_match.get("res_match_is")
            res_match_is_sql = res_match.get("res_match_is_sql")
            if not res_match_is.empty:
                res_match_is.loc[:, 'rank'] = 5
                res_match_is_sql = res_match_is_sql.append(res_match_is)
            return res_match_is_sql
        else:
            return res_match_is_sql

    def clear_cross_handle(self, handle_not_address_tw,
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
                             sort=False, left_on=["PropertyCommunity"], right_on=["PropertyAddress"],
                             how='left')
        res_match_not_index = res_match[res_match['id_tw'].isnull()].index
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
                             sort=False, left_on=["PropertyAddress"], right_on=["PropertyCommunity"],
                             how='left')
        res_match_not_index = res_match[res_match['id_tw'].isnull()].index
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
            ["id_tw", 'road', 'alley']]
        index_null = handle_community_to_address_tw.index
        handle_community_to_address_tw = handle_community_to_address_tw.drop(labels=index_null)
        # 匹配指定字段
        res_match = pd.merge(handle_address_third, handle_community_to_address_tw,
                             sort=False,
                             how='left')
        res_match_not_index = res_match[res_match['id_tw'].isnull()].index
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
            ["id_third", 'road', 'alley']]
        index_null = handle_community_to_address_third.index
        handle_community_to_address_third = handle_community_to_address_third.drop(labels=index_null)
        # 匹配指定字段
        res_match = pd.merge(handle_community_to_address_third, handle_address_tw,
                             sort=False,
                             how='left')
        res_match_not_index = res_match[res_match['id_tw'].isnull()].index
        # 找出匹配到的
        res_match_is = res_match.drop(labels=res_match_not_index)
        # 如果不为空
        if not res_match_is.empty:
            # 删除指定字段
            res_match_is.drop(labels=["road", "alley"], axis=1, inplace=True)
            # 加入res_match_merge
            res_match_merge = res_match_merge.append(res_match_is)
        return res_match_merge


class MatchRank:
    '''
    flag_all：判断是否跑增量
    '''

    def __init__(self, flag_all=False):
        self.cd = ClearDate()
        self.rh = RankHanle()
        self.engine_third_house = create_engine('mssql+pymssql://bigdata:pUb6Qfv7BFxl@10.10.202.12/TWSpider')
        if flag_all:
            self.house_list_third = pd.read_sql(
                "select RoomId as id_third,PropertyAddress,PropertyCommunity,TotalFloor,Floor,HouseType,BuildingSquare,HouseDesc,PriceUnit from ThirdHouseResource",
                self.engine_third_house)
        else:
            sql_str_day = "select DISTINCT RoomId as id_third,PropertyAddress,PropertyCommunity,TotalFloor,Floor,HouseType,BuildingSquare,HouseDesc,PriceUnit from ThirdHouseResource WHERE RoomId not in (select id_third from match_rank GROUP BY id_third)"
            # sql_str_three = "select RoomId as id_third,PropertyAddress,PropertyCommunity,TotalFloor,Floor,HouseType,BuildingSquare,HouseDesc,PriceUnit from ThirdHouseResource where Resource='大房鸭' and datediff(day,InsertTime,getdate())<= 2 and datediff(day,InsertTime,getdate())>= 0"
            self.house_list_third = pd.read_sql(
                sql_str_day,
                self.engine_third_house)

    def match_rank(self, house_third):
        if not house_third.empty:
            # 处理第三方房源
            # 处理地址和小区交叉情况
            handle_not_address_third = house_third[["id_third", 'PropertyAddress']]
            handle_not_community_third = house_third[["id_third", 'PropertyCommunity']]
            handle_community_to_address_third = self.cd.handle_community_to_address_third(house_third)
            # 处理对应情况
            house_third = self.cd.handle_address_third(house_third)
            house_third = self.cd.handle_community_third(house_third)
            house_third = self.cd.handle_total_floor_third(house_third)
            house_third = self.cd.handle_floor_third(house_third)
            house_third = self.cd.handle_room_type_third(house_third)
            house_third = self.cd.handle_room_area_third(house_third)
            house_third = house_third.drop(labels=["HouseType", 'HouseDesc', 'PriceUnit'], axis=1)
            # 读取、清理库内房源
            house_list_tw = self.rh.house_list_tw
            # 获取每次匹配到的集合
            id_third_match_is = set()
            # 循环清洗、并设定星级
            for house_tw in house_list_tw:
                res_match_is_sql = pd.DataFrame(columns=["id_third", "id_tw", "rank"])
                # 处理地址和小区交叉情况
                handle_not_address_tw = house_tw[["id_tw", 'PropertyAddress']]
                handle_not_community_tw = house_tw[["id_tw", 'PropertyCommunity']]
                handle_community_to_address_tw = self.cd.handle_address_tw(house_tw)
                # 处理对应情况
                res_tw = self.cd.handle_address_tw(house_tw)
                res_tw = self.cd.handle_floor_tw(res_tw)
                res_match_is_sql = self.rh.rank_handle(house_third=house_third, house_tw=res_tw,
                                                       res_match_is_sql=res_match_is_sql,
                                                       handle_not_address_tw=handle_not_address_tw,
                                                       handle_not_community_tw=handle_not_community_tw,
                                                       handle_not_address_third=handle_not_address_third,
                                                       handle_not_community_third=handle_not_community_third,
                                                       handle_community_to_address_third=handle_community_to_address_third,
                                                       handle_community_to_address_tw=handle_community_to_address_tw
                                                       )
                res_match_is_sql = res_match_is_sql.drop_duplicates()
                res_match_is_sql.to_sql('house_rank', con=self.rh.engine_res, if_exists="append", index=False,
                                        dtype=self.rh.dtype)
                id_third_match_is |= set(res_match_is_sql['id_third'].to_list())
                print("插入数据库")
            # 计算差值,获取为空的id插入数据库
            id_third_all = set(house_third.loc[:, "id_third"])
            id_third_match_not = list(id_third_all - id_third_match_is)
            if id_third_match_not:
                res_match_not_sql = pd.DataFrame({"id_third": id_third_match_not})
                res_match_not_sql.loc[:, 'rank'] = 0
                res_match_not_sql.loc[:, 'id_tw'] = None
                res_match_not_sql.to_sql('house_rank', con=self.rh.engine_res, if_exists="append", index=False,
                                         dtype=self.rh.dtype)
        else:
            print("无新增数据")


if __name__ == '__main__':
    time_start = time.time()
    # 判断总量还是增量flag_all
    mr = MatchRank(flag_all=False)
    house_third = mr.house_list_third
    # print(house_third)
    # 处理第三方房源
    mr.match_rank(house_third)
    time_end = time.time()
    print('totally cost', (time_end - time_start))
else:
    mr = MatchRank(flag_all=False)
    house_third = mr.house_list_third
    # rh = RankHanle()
    # house_third = rh.house_list_third
