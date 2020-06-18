import time
from status_update import update
from match_rank import *

if __name__ == '__main__':
    flag_list = [True, False]
    fetch_num_tw = 50000
    fetch_num_third = 200000
    for flag in flag_list:
        time_start = time.time()
        if flag:
            sql_select_tw = '''
            select e.EstateId,e.EstateAddress as PropertyAddress,e.EstateAreaName as PropertyCommunity,r.TotalLayer as TotalFloor,r.LayerHighLowTypeName as Floor,r.RoomNum as room,r.HallNum as hall,r.ToiletNum as toilet,r.PropertySquare as BuildingSquare,r.RoomId from Estate as e inner join room as r on e.EstateId=r.EstateId GROUP BY e.EstateId,e.EstateAddress,e.EstateAreaName,r.TotalLayer,r.LayerHighLowTypeName,r.RoomNum,r.HallNum,r.ToiletNum,r.PropertySquare,r.RoomId
            '''
            house_rank = 'ThirdHouseRank'
            sql_update_third = "SELECT DISTINCT t.HouseStatus,t.roomid from ThirdHouseResource as t inner join ThirdHouseRank as r on t.HouseStatus!=r.Status and t.RoomId=r.thirdid where t.Resource !='安居客'"
            sql_select_third = "select t.RoomId as ThirdId,t.PropertyCommunity,t.PropertyAddress,t.TotalFloor,t.Floor,t.HouseType,t.BuildingSquare,t.HouseDesc,t.PriceUnit,t.Resource,t.HouseStatus as Status from ThirdHouseResource as t LEFT JOIN ThirdHouseRank as r on t.RoomId=r.ThirdId where r.ThirdId is null and t.Resource !='安居客'"

        else:
            sql_select_tw = '''
            select e.EstateId,e.EstateAddress as PropertyAddress,e.EstateAreaName as PropertyCommunity,r.TotalLayer as TotalFloor,r.LayerHighLowTypeName as Floor,r.RoomNum as room,r.HallNum as hall,r.ToiletNum as toilet,CONVERT(int,r.PropertySquare) as BuildingSquare,r.RoomId from Estate as e inner join room as r on e.EstateId=r.EstateId GROUP BY e.EstateId,e.EstateAddress,e.EstateAreaName,r.TotalLayer,r.LayerHighLowTypeName,r.RoomNum,r.HallNum,r.ToiletNum,r.PropertySquare,r.RoomId
            '''
            house_rank = 'ThirdHouseRankAnjuke'
            sql_update_third = "SELECT DISTINCT t.HouseStatus,t.roomid from ThirdHouseResource as t inner join ThirdHouseRankAnjuke as r on t.HouseStatus!=r.Status and t.RoomId=r.thirdid where t.Resource ='安居客'"
            sql_select_third = "select t.RoomId as ThirdId,t.PropertyCommunity,t.PropertyAddress,t.TotalFloor,t.Floor,t.HouseType,t.BuildingSquare,t.HouseDesc,t.PriceUnit,t.Resource,t.HouseStatus as Status from ThirdHouseResource as t LEFT JOIN ThirdHouseRank as r on t.RoomId=r.ThirdId where r.ThirdId is null and t.Resource ='安居客'"
        print("开始更新状态")
        update(sql_update_third=sql_update_third, house_rank=house_rank)
        time_middle = time.time()
        print('更新时间', (time_middle - time_start))
        print("开始匹配：{}".format(sql_select_third))
        match_rank(house_rank, sql_select_third, sql_select_tw, fetch_num_tw, fetch_num_third, flag)
        time_end = time.time()
        print('总匹配时间', (time_end - time_start))
