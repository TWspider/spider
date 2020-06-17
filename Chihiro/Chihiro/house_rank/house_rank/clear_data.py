import pandas as pd
import numpy as np
import jieba
import os
import re

current_path = os.path.abspath("")
community = current_path + r"\community.csv"
road_handle = current_path + r"\road.csv"
jieba.load_userdict(community)
comm = set(pd.read_csv(community, header=None)[0].tolist())
jieba.load_userdict(road_handle)
road = set(pd.read_csv(road_handle, header=None)[0].tolist())


def extract_other(index, PropertyCommunity_list, third_house):
    # 已经填补完desc，将小区为空的记录取出来
    try:
        word_list = jieba.cut(third_house.loc[index, 'HouseDesc'], cut_all=False, HMM=False)
        PropertyCommunity_list.append(list(word_list)[0])
    except Exception as e:
        PropertyCommunity_list.append(None)


def add_comm(word_list, comm):
    flag = 0
    word_set = set()
    for word in word_list:
        if word in comm:
            flag = 1
            word_set.add(word)
    word_str = ','.join(word_set)
    return flag, word_str


def handle_split_line(ls_col):
    new_col = []
    new_index = []
    for index, col in enumerate(ls_col):
        if col != None:
            col = str(col).split(",")
            new_col += col
            len_col = len(col) * [index]
            new_index += len_col
    return new_col, new_index


def leave_word(house_third):
    new_col, new_index = handle_split_line(house_third["alley"])

    third_house = house_third.drop('alley', axis=1).join(
        pd.DataFrame({"alley": new_col}, index=new_index)["alley"]
    )
    new_col, new_index = handle_split_line(house_third["road"])

    third_house = third_house.drop('road', axis=1).join(
        pd.DataFrame({"road": new_col}, index=new_index)["road"]
    )

    third_house.loc[:, 'road_'] = third_house['road'] + third_house['alley']
    third_house.drop_duplicates(inplace=True)
    third_house.reset_index(drop=True, inplace=True)
    third_house.fillna('', inplace=True)
    return third_house


def other_discomm(third_house):
    # 提取出所有小区名,作为停用词(小区+desc中提取的小区)
    stop_word_comm = third_house['PropertyCommunity'].unique().tolist()
    # 从desc中提取小区
    ext_comm = []
    cond = third_house['PropertyCommunity'].isnull()
    for desc in third_house[cond]['HouseDesc'].unique():
        pattern = re.compile('.*?·(.*?)\s')
        try:
            res = pattern.findall(desc)
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
    lj_total_comm.to_csv(community, index=None)
    # 将停用词文件读取出来，设置特定词语不被分开
    stop_word_set = set(pd.read_csv(community)['comm'].to_list())
    # 加载自己的停用词典
    jieba.load_userdict(community)
    # 调整词典，使特定的词语不被分开
    for word in stop_word_set:
        if word != None:
            jieba.suggest_freq(str(word), True)


def add_road(word_list):
    flag = 0
    word_set = set()
    for word in word_list:
        if word in road:
            flag = 1
            word_set.add(word)
    word_str = ','.join(word_set)
    return flag, word_str


def which_reg(addr, alley_list):
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


def handle_address_tw(res_tw):
    # 处理数据库中的地址
    road_list = []
    for addr in res_tw['PropertyAddress']:
        try:
            word_list = list(jieba.cut(addr, cut_all=False, HMM=False))
            flag, word_str = add_road(word_list)
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
        which_reg(addr, alley_list)
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


def handle_community_to_address_tw(res_tw):
    # 处理数据库中的地址
    road_list = []
    for addr in res_tw['PropertyCommunity']:
        try:
            word_list = list(jieba.cut(addr, cut_all=False, HMM=False))
            flag, word_str = add_road(word_list)
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
        which_reg(addr, alley_list)
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


def handle_floor_tw(res_tw):
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


def handle_address_third(third_house):
    # 从房源描述中提取小区名
    other_discomm(third_house)
    # 对路进行提取
    for ro in road:
        if ro != None:
            jieba.suggest_freq(str(ro), True)
    road_list = []
    for addr in third_house['PropertyAddress']:
        try:
            word_list = list(jieba.cut(addr, cut_all=False, HMM=False))
            flag, word_str = add_road(word_list)
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
    third_house = leave_word(third_house)
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
            flag, word_str = add_road(word_list)
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
    leave_word(third_house)
    return third_house


def handle_community_to_address_third_handle(third_house):
    # 从房源描述中提取小区名
    other_discomm(third_house)
    # 对路进行提取
    for ro in road:
        if ro != None:
            jieba.suggest_freq(str(ro), True)
    road_list = []
    for addr in third_house['PropertyCommunity']:
        try:
            word_list = list(jieba.cut(addr, cut_all=False, HMM=False))
            flag, word_str = add_road(word_list)
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
    third_house = leave_word(third_house)
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
            flag, word_str = add_road(word_list)
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
    leave_word(third_house)
    return third_house


def handle_community_third(third_house):
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
                extract_other(index, PropertyCommunity_list, third_house)
        except:
            extract_other(index, PropertyCommunity_list, third_house)
    temp = third_house[cond]
    temp.loc[:, 'PropertyCommunity'] = pd.Series(data=PropertyCommunity_list).values
    temp_array = np.concatenate((third_house[third_house['PropertyCommunity'].notnull()].values, temp.values))
    third_house = pd.DataFrame(data=temp_array, columns=third_house.columns)
    # 处理小区字段是地址的情况
    # 调整词典，使特定的词语不被分开
    for comm in comm:
        if comm != None:
            jieba.suggest_freq(str(comm), True)
    comm_list = []
    for comm in third_house['PropertyCommunity']:
        word_list = list(jieba.cut(comm, cut_all=False, HMM=False))
        flag, word_str = add_comm(word_list, comm)
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


def handle_total_floor_third(third_house):
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


def handle_floor_third(third_house):
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


def handle_room_type_third(third_house):
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


def handle_room_area_third(third_house, flag):
    BuildingSquare_list = third_house['BuildingSquare'].tolist()
    BuildingSquare = []
    for square in BuildingSquare_list:
        try:
            res = re.split(" |平米|㎡", square)
            if flag:
                BuildingSquare.append(res[0])
            else:
                BuildingSquare.append(str(int(float(res[0]))))
        except:
            BuildingSquare.append(None)
    # 把面积替换掉
    if len(BuildingSquare) == third_house['BuildingSquare'].shape[0]:
        third_house.loc[:, 'BuildingSquare'] = pd.Series(data=BuildingSquare)
    else:
        print('上面出错,长度不够')
    return third_house
