import pandas as pd
from sqlalchemy import create_engine
import string

house_list_third = pd.read_excel(
    r"C:\Users\1\Documents\Taiwu\Bigdata\101.xls")
house_list_third_column = house_list_third.columns.to_list()

engine_tw_house = create_engine('mssql+pymssql://bigdata:pUb6Qfv7BFxl@10.10.202.12/TWEstate')
house_list_tw = pd.read_sql(
    "select EstateId,EstateName,EstateAreaName,EstateAddress,EstateOtherAddress from Estate GROUP BY EstateName,EstateAreaName,EstateAddress,EstateOtherAddress,EstateId",
    engine_tw_house)

house_list_tw.loc[:, "EstateName"] = house_list_tw["EstateName"].astype("str").apply(
    lambda x: x.rstrip(string.digits))

# 获取本次可匹配到的数据
res_match_merge = pd.DataFrame()


# 匹配小区名称

def match(house_list_tw, house_list_third, left_on, right_on):
    global res_match_merge
    res_match = pd.merge(house_list_tw, house_list_third,
                         sort=False,
                         how='left', left_on=left_on, right_on=right_on)
    res_match_not_index = res_match[res_match['小区id'].isnull()].index
    plus_index = res_match_not_index.to_list()
    res_match_is = res_match.drop(labels=res_match_not_index)
    if not res_match_is.empty:
        house_list_tw = res_match.loc[plus_index]
        if not house_list_tw.empty:
            house_list_tw.drop(labels=house_list_third_column, axis=1, inplace=True)
            res_match_merge = res_match_merge.append(res_match_is)
            return house_list_tw



def match_final(house_list_tw, house_list_third, left_on, right_on):
    global res_match_merge
    res_match = pd.merge(house_list_tw, house_list_third,
                         sort=False,
                         how='left', left_on=left_on, right_on=right_on)

    res_match.drop(labels=["小区id_x"], axis=1, inplace=True)
    res_match_merge = res_match_merge.append(res_match)



if __name__ == '__main__':
    def match_group(x):
        for y in house_list_third['地址']:
            if y in x:
                return y
        return '未匹到'
    house_list_tw = match(house_list_tw, house_list_third, left_on=["EstateAreaName"], right_on=["小区名称"])
    if not house_list_tw.empty:
        house_list_tw = match(house_list_tw, house_list_third, left_on=["EstateName"], right_on=["小区名称"])
        if not house_list_tw.empty:
            house_list_tw = match(house_list_tw, house_list_third, left_on=["EstateAddress"], right_on=["地址"])
            if not house_list_tw.empty:
                # 匹配包含关系
                pass
                house_list_tw["小区id"] = house_list_tw["EstateOtherAddress"].apply(match_group)
                res_match_not_index = house_list_tw[house_list_tw["小区id"] == "未匹到"].index.to_list()
                # 最后一批匹配到的
                res_match_is = house_list_tw.drop(labels=res_match_not_index)
                # 合并
                match_final(res_match_is, house_list_third, left_on=["小区id"], right_on=["地址"])
                # 最终未匹配到的
                res_match_not = house_list_tw.loc[res_match_not_index]
                res_match_merge = res_match_merge.append(res_match_not)



    print(res_match_merge)
    # 写入excel
    res_match_merge = res_match_merge.drop_duplicates().reset_index(drop=True)
    res_match_merge.to_excel("match_result1.xls")


