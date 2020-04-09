import datetime

import jsonpath
import pymssql
import redis


# 创建sqlserver连接
def get_sqlserver_conn():
    conn = pymssql.connect("pymssql", host='10.10.202.12', database='TWSpider',
                           user='bigdata', password='pUb6Qfv7BFxl', charset="utf8",
                           )
    cur = conn.cursor()
    return conn, cur


# 创建redis连接
def get_redis_conn():
    redis_conn = redis.Redis(host='127.0.0.1', port=6379)
    return redis_conn


def save_redis(cur, redis_conn):
    cur.execute("select RoomId,HouseUrl,HouseStatus from ThirdHouseResource where Resource='大房鸭'")
    data_list = cur.fetchall()
    for data in data_list:
        redis_conn.hmset(data[1], {'id': data[0], 'house_status': data[2]})


# 获取首页数据
def get_home_data(obj):
    city_list = jsonpath.jsonpath(obj, '$..provinceName')
    area_list = jsonpath.jsonpath(obj, '$..districtName')
    road_list = jsonpath.jsonpath(obj, '$..plateName')
    house_url_list = jsonpath.jsonpath(obj, '$..shortLink')
    build_time_list = jsonpath.jsonpath(obj, '$..buildYear')
    release_time_list = jsonpath.jsonpath(obj, '$..publishDate')  # TODO 时间戳 准换为时间
    addr_list = jsonpath.jsonpath(obj, '$..address')
    floor_list = jsonpath.jsonpath(obj, '$..floor')
    total_price_list = jsonpath.jsonpath(obj, '$..price')
    community_list = jsonpath.jsonpath(obj, '$..neighborhoodName')
    build_size_list = jsonpath.jsonpath(obj, '$..area')
    elevator_list = jsonpath.jsonpath(obj, '$..elevator')
    offlineDate_list = jsonpath.jsonpath(obj, '$..offlineDate')
    data = {'city': city_list, 'area': area_list,
            'road': road_list, 'house_url': house_url_list, 'build_time': build_time_list,
            'release_time': release_time_list, 'addr': addr_list,
            'floor': floor_list, 'total_price': total_price_list,
            'build_size': build_size_list, 'community': community_list,
            'elevator': elevator_list, 'resource_status': offlineDate_list,
            }
    return data


# 获取次页数据
def get_plate_data(tree, item):
    try:
        door_model = tree.xpath('//span[contains(@class,"margin-left24")]/text()')[0].strip().split('（')[0]
        house_use = tree.xpath('//span[@class="condition condition1"]/text()')[0].strip()
        direction = tree.xpath('//span[@class="condition condition2"]/text()')[0].strip()
        decoration = tree.xpath('//span[@class="condition condition4"]/text()')[0].strip()
        per_price = tree.xpath('//span[@class="font-normal font-white font-size14 margin-left10"]/text()')[
            0].strip().split(
            '￥')[-1]
        total_floor = tree.xpath('//span[@id="house_floor_total_span"]/text()')[0].strip()
        # print('===================', total_floor)
        item['door_model'] = door_model
        item['total_floor'] = total_floor or 0
        item['house_use'] = house_use
        item['direction'] = direction
        item['decoration'] = decoration
        item['per_price'] = per_price
        item['insert_time'] = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        item['resource_status'] = '可售'
        item['resource'] = '大房鸭'
        item['rental_status'] = '0'
        return item
    except Exception as e:
        print('下架，已售')
