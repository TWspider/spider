import pandas as pd
import numpy as np
import difflib
import jieba
from sqlalchemy import create_engine
import time
import re
from sqlalchemy.types import NVARCHAR, INT


class ClearDate(object):
    '''
    包括分级处理、以及常用的处理函数111
    '''

    def __init__(self):
        jieba.load_userdict('1.csv')
        self.shcomm = set(pd.read_csv('1.csv', header=None)[0].tolist())
        jieba.load_userdict('./sh.csv')
        self.shroad = set(pd.read_csv('sh.csv', header=None)[0].tolist())

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
        third_house['road_'] = third_house['road'] + third_house['alley']
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
        lj_total_comm.to_csv('1.csv', index=None)
        # 将停用词文件读取出来，设置特定词语不被分开
        stop_word_set = set(pd.read_csv('./1.csv')['comm'].to_list())
        # 加载自己的停用词典
        jieba.load_userdict('1.csv')
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
        for addr in res_tw['EstateAddress']:
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
        res_tw['road'] = pd.Series(data=road_list)
        alley_list = []
        for addr in res_tw['EstateAddress']:
            self.which_reg(addr, alley_list)
        res_tw['alley'] = pd.Series(data=alley_list)
        # 将路拆开
        res_tw = res_tw.drop('road', axis=1).join(
            res_tw['road'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).rename('road_tw'))
        # 将号拆开
        res_tw = res_tw.drop('alley', axis=1).join(
            res_tw['alley'].str.split(',', expand=True).stack().reset_index(level=1, drop=True).rename('alley_tw'))
        res_tw.drop_duplicates(inplace=True)
        # res_tw['road_'] = res_tw['road_tw'] + res_tw['alley_tw']
        res_tw.reset_index(drop=True, inplace=True)
        return res_tw

    def handle_floor_tw(self, res_tw):
        floor_list_new = []
        floor_list = res_tw['LayerHighLowTypeName']
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
        res_tw['LayerHighLowTypeName'] = pd.Series(data=floor_list_new)
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
        third_house['road'] = pd.Series(data=road_list)
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
        third_house['alley'] = pd.Series(data=alley_list)
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
        third_house['PropertyCommunity'] = pd.Series(data=comm_list)
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
        third_house['road'] = pd.Series(data=road_list)
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
        third_house['alley'] = pd.Series(data=alley_list)
        # 拆分路和号
        self.leave_word(third_house)

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
                    TotalFloor_list.append(totalfloor)
            except:
                # None
                TotalFloor_list.append(None)

        third_house['TotalFloor'] = pd.Series(data=TotalFloor_list)
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
        third_house['Floor'] = pd.Series(data=floor_list_new)
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
            third_house['room'] = pd.Series(data=room_list)
            third_house['hall'] = pd.Series(data=hall_list)
            third_house['toilet'] = pd.Series(data=toilet_list)
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
            third_house['BuildingSquare'] = pd.Series(data=BuildingSquare)
        else:
            print('上面出错,长度不够')
        return third_house


# 通过roomid判断是否匹配


class RankHanle(object):
    def __init__(self):
        self.engine_third_house = create_engine('mssql+pymssql://tw_user:123456@10.55.5.215/TWSpider')
        self.engine_tw_house = create_engine('mssql+pymssql://tw_user:123456@10.55.5.7/TWEstate')
        # 打标记、找交集、剩下的被定级
        self.house_list_third = pd.read_sql(
            "select top 1000 RoomId as id_third,HouseUrl,PropertyAddress,PropertyCommunity,TotalFloor,Floor,HouseType,BuildingSquare,HouseDesc,PriceUnit from ThirdHouseResource",
            self.engine_third_house, chunksize=200)

        self.house_list_tw = pd.read_sql(
            '''
            select EstateAddress,EstateAreaName,EstateId into #e2
        from Estate GROUP BY EstateId,EstateAddress,EstateAreaName;
        select EstateId,TotalLayer,LayerHighLowTypeName,RoomNum,HallNum,ToiletNum,PropertySquare,RoomId into #r2 
        from room;
        select e.EstateAddress,e.EstateAreaName,r.TotalLayer,r.LayerHighLowTypeName,r.RoomNum,r.HallNum,r.ToiletNum,r.PropertySquare,r.RoomId
        from #e2 e,#r2 r
        WHERE e.EstateId=r.EstateId
            ''',
            self.engine_tw_house)
        self.address = ["road", 'alley', "RoomId"]
        self.community_road = ["PropertyCommunity", 'road', 'RoomId']
        self.community = ["PropertyCommunity", 'RoomId']
        self.dtype = {
            "id_third": NVARCHAR(length=50),
            "HouseUrl": NVARCHAR(length=255),
            "PropertyAddress": NVARCHAR(length=50),
            "PropertyCommunity": NVARCHAR(length=50),
            "Floor": NVARCHAR(length=50),
            "BuildingSquare": NVARCHAR(length=50),
            "road": NVARCHAR(length=50),
            "alley": NVARCHAR(length=50),
            "room": NVARCHAR(length=50),
            "hall": NVARCHAR(length=50),
            "toilet": NVARCHAR(length=50),
            "TotalFloor": NVARCHAR(length=50),
            "RoomId": NVARCHAR(length=50),
            "rank": INT,
        }

    def rename_tw(self, res_tw):
        '''
        重命名库内字段名
        :param res_tw:
        :return:
        '''
        # 地址
        res_tw.rename(columns={'road_tw': 'road'}, inplace=True)
        res_tw.rename(columns={'alley_tw': 'alley'}, inplace=True)
        # 小区
        res_tw.rename(columns={'EstateAreaName': 'PropertyCommunity'}, inplace=True)
        # 总楼层
        res_tw.rename(columns={'TotalLayer': 'TotalFloor'}, inplace=True)
        # 所在层
        res_tw.rename(columns={'LayerHighLowTypeName': 'Floor'}, inplace=True)
        # 房型
        res_tw.rename(columns={'RoomNum': 'room'}, inplace=True)
        res_tw.rename(columns={'HallNum': 'hall'}, inplace=True)
        res_tw.rename(columns={'ToiletNum': 'toilet'}, inplace=True)
        # 面积
        res_tw.rename(columns={'PropertySquare': 'BuildingSquare'}, inplace=True)
        return res_tw

    def rank_handle_start(self, house_third, house_tw):
        '''
        # 找到未匹配的情况：(删除未匹配的余下的下一步)、(设为未上架、把找到的插入数据库)
        :param house_third:
        :param house_tw:
        :param field_list:
        :return:
        '''
        route_address = house_tw[self.address]
        route_community_road = house_tw[self.community_road]
        route_community = house_tw[self.community]
        # 地址匹配
        res_match_address = pd.merge(house_third, route_address,
                                     sort=False,
                                     how='left')
        # 找出匹配结果为空值的（不会重复）
        res_match_next = res_match_address[res_match_address['RoomId'].isnull()]
        # 余下的进入下一级
        res_match_remain_address = res_match_address.drop(labels=res_match_next.index)
        if not res_match_remain_address.empty:
            self.rank_handle_1_5(rank=0, house_third=res_match_remain_address, house_tw=house_tw)
        # 接着匹配
        res_match_next = res_match_next.drop(labels=["RoomId"], axis=1)
        # 小区、路匹配
        if not res_match_next.empty:
            res_match_community_road = pd.merge(res_match_next, route_community_road,
                                                sort=False,
                                                how='left')
            # 找出匹配结果为空值的（不会重复）
            res_match_next = res_match_community_road[res_match_community_road['RoomId'].isnull()]
            # 余下的进入下一级
            res_match_remain_community_road = res_match_community_road.drop(labels=res_match_next.index)
            if not res_match_remain_community_road.empty:
                self.rank_handle_1_5(rank=0, house_third=res_match_remain_community_road,
                                     house_tw=house_tw)
            # 接着匹配
            res_match_next = res_match_next.drop(labels=["RoomId"], axis=1)

        # 小区匹配
        if not res_match_next.empty:
            res_match_community = pd.merge(res_match_next, route_community,
                                           sort=False,
                                           how='left')
            res_match_sql = res_match_community[res_match_community['RoomId'].isnull()]
            # 保存到数据库内
            res_match_sql["rank"] = 0
            res_match_sql["RoomId"] = None
            # print(res_match_sql)
            res_match_sql.to_sql('house_new', con=self.engine_third_house, if_exists="append", index=False,
                                 dtype=self.dtype)
            # 返回余下的给下个等级处理
            res_match_remain_community = res_match_community.drop(labels=res_match_sql.index)
            if not res_match_remain_community.empty:
                self.rank_handle_1_5(rank=0, house_third=res_match_remain_community, house_tw=house_tw)

    def rank_handle_1_5(self, rank, house_third, house_tw):
        rank += 1
        if rank == 1:
            # 存在roomid
            extract_fields = house_tw[['TotalFloor', 'RoomId']]
        elif rank == 2:
            extract_fields = house_tw[['Floor', 'RoomId']]
        elif rank == 3:
            extract_fields = house_tw[['room', 'hall', 'toilet', 'RoomId']]
        else:
            extract_fields = house_tw[['BuildingSquare', 'RoomId']]
            extract_fields["rank"] = 0
            res_match = pd.merge(house_third, extract_fields,
                                 sort=False,
                                 how='left')
            # 找出匹配结果为空值的（不会重复）
            res_match_sql = res_match[res_match['rank'].isnull()]
            res_match_sql["rank"] = rank
            res_match_sql.to_sql('house_new', con=self.engine_third_house, if_exists="append", index=False,
                                 dtype=self.dtype)
            # 余下的设为5星
            res_match_remain = res_match.drop(labels=res_match_sql.index)
            if not res_match_remain.empty:
                res_match_remain['rank'] = 5
                # 插入数据库
                res_match_remain.to_sql('house_new', con=self.engine_third_house, if_exists="append", index=False,
                                        dtype=self.dtype)
            return
        # 为了找出哪些匹配不到,而做的标记为0
        extract_fields["rank"] = 0
        res_match = pd.merge(house_third, extract_fields,
                             sort=False,
                             how='left')
        # 找出匹配结果为空值的（不会重复）
        res_match_sql = res_match[res_match['rank'].isnull()]
        res_match_sql["rank"] = rank
        # 插入数据库
        res_match_sql.to_sql('house_new', con=self.engine_third_house, if_exists="append", index=False,
                             dtype=self.dtype)
        # 余下的进入下一级
        res_match_remain = res_match.drop(labels=res_match_sql.index)
        if not res_match_remain.empty:
            res_match_remain = res_match_remain.drop(labels=["rank"], axis=1)
            self.rank_handle_1_5(rank=rank, house_third=res_match_remain, house_tw=house_tw)


if __name__ == '__main__':
    time_start = time.time()
    rh = RankHanle()
    cd = ClearDate()
    # 读取第三方房源
    house_list_third = rh.house_list_third
    # 读取、清理库内房源
    house_list_tw = rh.house_list_tw
    res_tw = cd.handle_address_tw(house_list_tw)
    res_tw = cd.handle_floor_tw(res_tw)
    res_tw = res_tw.astype("str")
    res_tw = rh.rename_tw(res_tw)
    # 循环清洗、并设定星级
    for house_third in house_list_third:
        # 清洗字段
        cd.handle_address_third(house_third)
        house_third = cd.handle_community_third(house_third)
        house_third = cd.handle_total_floor_third(house_third)
        house_third = cd.handle_floor_third(house_third)
        house_third = cd.handle_room_type_third(house_third)
        house_third = cd.handle_room_area_third(house_third)
        house_third = house_third.astype("str")
        house_third = house_third.drop(labels=["HouseType", 'HouseDesc', 'PriceUnit'], axis=1)
        # 设定星级
        res = rh.rank_handle_start(house_third=house_third, house_tw=res_tw)
    time_end = time.time()
    print('totally cost', time_end - time_start)
