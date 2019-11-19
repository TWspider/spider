from Chihiro.test_celery.testapp import app
from celery.utils.log import get_task_logger
import requests
import re
import datetime
from Chihiro.pafunction.handle_item import ItemSqlHandle
from Chihiro.pafunction.start_setting import *
from Chihiro.pafunction.setting import *
from scrapy.selector import Selector

logger = get_task_logger(__name__)


@app.task(
    name="home_page.zhongyuan",
    bind=True,
    rate_limit='100/s',
    autoretry_for=(requests.Timeout, requests.ConnectionError),
    retry_kwargs={
        'max_retries': 5,
        # 'countdown': 1
    },
    acks_late=True,
)
def home_page_zhongyuan(self, start_url):
    res = requests.get(url=start_url, headers=headers_zhongyuan)
    response = Selector(response=res)
    region_xpath_list = response.xpath(
        "//ul[contains(@class,'tap_show')]/li/a")
    for region_xpath in region_xpath_list:
        region = region_xpath.xpath("./text()").extract_first()
        region_url_handle = region_xpath.xpath("./@href").extract_first()
        region_url = BASE_URL + region_url_handle
        app.send_task("plate_page.zhongyuan", args=(region_url, {"region": region}), queue="test_page")

@app.task(
    name="plate_page.zhongyuan",
    bind=True,
    rate_limit='100/s',
    autoretry_for=(requests.Timeout, requests.ConnectionError),
    retry_kwargs={
        'max_retries': 5,
        # 'countdown': 1
    },
    acks_late=True,
)
def plate_page_zhongyuan(self, region_url, params):
    res = requests.get(url=region_url, headers=headers_zhongyuan)
    response = Selector(response=res)
    plate_xpath_list = response.xpath(
        "//div[@class='tagbox_wrapper_main']/div[@class='tagbox_wrapper_cd']/ul[contains(@class,'tap_show')]/li[position()>1]/a")
    for plate_xpath in plate_xpath_list:
        plate = plate_xpath.xpath("./text()").extract_first()
        plate_url_handle = plate_xpath.xpath("./@href").extract_first()
        ershoufang_plate_url = BASE_URL + plate_url_handle
        zufang_plate_url = BASE_URL + plate_url_handle.replace("ershoufang", "zufang")
        params.update({"plate": plate})
        app.send_task("next_page.zhongyuan", args=(ershoufang_plate_url, params), queue="test_page")
        app.send_task("next_page.zhongyuan", args=(zufang_plate_url, params), queue="test_page")


@app.task(
    name="next_page.zhongyuan",
    bind=True,
    rate_limit='100/s',
    autoretry_for=(requests.Timeout, requests.ConnectionError),
    retry_kwargs={
        'max_retries': 5,
        # 'countdown': 1
    },
    acks_late=True,
)
def next_page_zhongyuan(self, plate_url, params):
    res = requests.get(url=plate_url, headers=headers_zhongyuan)
    print(res.text)
    response = Selector(response=res)
    total_num = response.xpath(
        "//div[@class='hs-counts']/h3/span/text()").extract_first()
    if total_num:
        total_num = int(total_num)
        reminder_num = total_num % 20
        page_num = total_num // 20
        if total_num:
            if reminder_num:
                for i in range(1, page_num + 2):
                    housing_list_url = plate_url + "g" + str(i) + "/"
                    app.send_task("housing_list_page.zhongyuan", args=(housing_list_url, params), queue="test_page")
            else:
                for i in range(1, page_num + 1):
                    housing_list_url = plate_url + "g" + str(i) + "/"
                    app.send_task("housing_list_page.zhongyuan", args=(housing_list_url, params), queue="test_page")


@app.task(
    name="housing_list_page.zhongyuan",
    bind=True,
    rate_limit='100/s',
    autoretry_for=(requests.Timeout, requests.ConnectionError),
    retry_kwargs={
        'max_retries': 10,
        # 'countdown': 1
    },
    acks_late=True,
)
def housing_list_page_zhongyuan(self, housing_list_url, params):
    res = requests.get(url=housing_list_url, headers=headers_zhongyuan)
    response = Selector(response=res)
    housing_list = response.xpath(
        "//div[@class='wrap-content']/ul[@id='ShowStyleByTable']/li/div[contains(@class,'wp-ct-box')]")
    region = params.get("region")
    plate = params.get("plate")
    for housing in housing_list:
        # 列表页面字段获取
        item = {}
        item["城市"] = CITY
        item["来源"] = RESOURCE
        item["区域"] = region
        item["板块"] = plate
        housing_description = housing.xpath(
            "./div[@class='ct-box-c']/div[@class='box-c-tt']/a/text()").extract_first()
        housing_url = housing.xpath("./div[@class='ct-box-c']/div[@class='box-c-tt']/a/@href").extract_first()
        housing_url = BASE_URL + housing_url
        item["房源描述"] = housing_description
        item["房源链接"] = housing_url
        # 小区
        community = housing.xpath("./div[@class='ct-box-c']/div[@class='box-c-tp']/a/text()").extract_first()
        # 地址
        address = housing.xpath("./div[@class='ct-box-c']/div[@class='box-c-lc']/p/text()").extract_first()
        housing_info = housing.xpath(
            "./div[@class='ct-box-c']/div[@class='box-c-tp']").xpath(
            "string(.)").extract_first()
        housing_info_handle = housing_info.replace(community, '').replace(' ', '').replace("\n", '')
        housing_info_handle_two = housing_info_handle.split("|")
        # 单价
        unit_price = housing.xpath("./div[@class='ct-box-r']/p/text()").extract_first()
        # 总价
        total_price = housing.xpath("./div[@class='ct-box-r']/h3/span/text()").extract_first()
        item["小区"] = community
        item["地址"] = address
        item["单价"] = unit_price
        item["总价"] = total_price
        if housing_info_handle_two:
            # 房屋户型
            room_type = housing_info_handle_two[0]
            # 建筑面积
            room_area = housing_info_handle_two[1]
            # 房屋朝向
            room_orientation = housing_info_handle_two[2]
            # 所在楼层
            location_floor = housing_info_handle_two[3]
            # 总楼层
            total_floor = housing_info_handle_two[4]
            # 装修情况
            room_decorate = housing_info_handle_two[5]
            # 建成年份
            build_year = housing_info_handle_two[6]
            item["房屋户型"] = room_type
            item["建筑面积"] = room_area
            item["房屋朝向"] = room_orientation
            item["总楼层"] = total_floor
            item["所在楼层"] = location_floor
            item["装修情况"] = room_decorate
            item["建成年份"] = build_year
        input_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        item["插入时间"] = input_time
        # 获取已有的url
        SET_LIST.add(housing_url)
        if "ershoufang" in housing_list_url:
            item["房源状态"] = "可售"
            item["租售状态"] = 0
            flag_filter = (housing_url, "可售")
            if flag_filter in HOUSING_SQL_LIST:
                pass
            else:
                if housing_url in HOUSING_URL_LIST:
                    # 已售变为可售
                    item["flag_status"] = "可售"
                    item["更新时间"] = input_time
                app.send_task("item.zhongyuan", args=(item,), queue="test_item")
        else:
            item["房源状态"] = "可租"
            item["租售状态"] = 1
            flag_filter = (housing_url, "可租")
            if flag_filter in HOUSING_SQL_LIST:
                pass
            else:
                if housing_url in HOUSING_URL_LIST:
                    # 已售变为可售
                    item["flag_status"] = "可租"
                    item["更新时间"] = input_time
                app.send_task("item.zhongyuan", args=(item,), queue="test_item")


@app.task(
    name="home_page.i5j",
    bind=True,
    rate_limit='30/s',
    autoretry_for=(requests.Timeout, requests.ConnectionError),
    retry_kwargs={
        'max_retries': 10,
        # 'countdown': 1
    },
    acks_late=True,
)
def home_page_i5j(self, start_url):
    res = requests.get(url=start_url, headers=headers_i5j)
    response = Selector(response=res)
    region_xpath_list = response.xpath(
        "//ul[contains(@class,'new_di_tab')]/a[position()>1]")
    for region_xpath in region_xpath_list:
        region = region_xpath.xpath("./li/text()").extract_first()
        region_url_handle = region_xpath.xpath("./@href").extract_first()
        region_url = BASE_URL + region_url_handle
        app.send_task("plate_page.i5j", args=(region_url, {"region": region, "start_url": start_url}),
                      queue='test_page')


@app.task(
    name="plate_page.i5j",
    bind=True,
    rate_limit='100/s',
    autoretry_for=(requests.Timeout, requests.ConnectionError),
    retry_kwargs={
        'max_retries': 10,
        # 'countdown': 1
    },
    acks_late=True,
)
def plate_page_i5j(self, region_url, params):
    res = requests.get(url=region_url, headers=headers_i5j)
    response = Selector(response=res)
    plate_xpath_list = response.xpath(
        "//div[contains(@class,'block')]/dl[contains(@class,'quyuCon')]/dd[@class='block']/a")
    for plate_xpath in plate_xpath_list:
        plate = plate_xpath.xpath("./text()").extract_first()
        plate_url_handle = plate_xpath.xpath("./@href").extract_first()
        plate_url = BASE_URL + plate_url_handle
        params.update({"plate": plate})
        app.send_task("next_page.i5j", args=(plate_url, params), queue="test_page")


@app.task(
    name="next_page.i5j",
    bind=True,
    rate_limit='100/s',
    autoretry_for=(requests.Timeout, requests.ConnectionError),
    retry_kwargs={
        'max_retries': 5,
        # 'countdown': 1
    },
    acks_late=True,
)
def next_page_i5j(self, plate_url, params):
    res = requests.get(url=plate_url, headers=headers_i5j)
    response = Selector(response=res)
    total_num = response.xpath(
        "//div[contains(@class,'total-box')]/span/text()").extract_first()
    if total_num:
        total_num = int(total_num)
        reminder_num = total_num % 20
        page_num = total_num // 20
        if total_num:
            if reminder_num:
                for i in range(1, page_num + 2):
                    housing_list_url = plate_url + "n" + str(i) + "/"
                    app.send_task("housing_list_page.i5j", args=(housing_list_url, params), queue="test_page")
            else:
                for i in range(1, page_num + 1):
                    housing_list_url = plate_url + "n" + str(i) + "/"
                    app.send_task("housing_list_page.i5j", args=(housing_list_url, params), queue="test_page")


@app.task(
    name="housing_list_page.i5j",
    bind=True,
    rate_limit='100/s',
    autoretry_for=(requests.Timeout, requests.ConnectionError),
    retry_kwargs={
        'max_retries': 10,
        # 'countdown': 1
    },
    acks_late=True,
)
def housing_list_page_i5j(self, housing_list_url, params):
    res = requests.get(url=housing_list_url, headers=headers_i5j)
    print(res.text)
    response = Selector(response=res)
    housing_list = response.xpath("//div[@class='list-con-box'][1]/ul[@class='pList']/li/div[@class='listCon']")
    region = params.get("region")
    plate = params.get("plate")
    for housing in housing_list:
        item = {}
        item["区域"] = region
        item["板块"] = plate
        housing_url = housing.xpath("./h3/a/@href").extract_first()
        housing_url = BASE_URL + housing_url
        housing_description = housing.xpath("./h3/a/text()").extract_first()
        housing_info = housing.xpath("./div[@class='listX']/p[1]/text()").extract_first().replace(" ", '')
        housing_info = housing_info.split("·")
        # 二手房为总价；租房为单价
        info_red = housing.xpath(
            "./div[@class='listX']/div[@class='jia']/p[@class='redC']/strong/text()").extract_first()
        info_grey = housing.xpath("./div[@class='listX']/div[@class='jia']/p[2]/text()").extract_first()
        flag_exception = None
        try:
            room_type = housing_info[0]
        except Exception as e:
            room_type = flag_exception
        try:
            room_area = housing_info[1]
        except Exception as e:
            room_area = flag_exception
        try:
            room_orientation = housing_info[2]
        except Exception as e:
            room_orientation = flag_exception
        try:
            location_floor = re.search("(.*?)/", housing_info[3]).group(1)
        except Exception as e:
            location_floor = flag_exception
        try:
            total_floor = re.search(".*?/(.*)", housing_info[3]).group(1)
        except Exception as e:
            total_floor = flag_exception
        try:
            room_decorate = housing_info[4]
        except Exception as e:
            room_decorate = flag_exception
        try:
            build_year = housing_info[5]
        except Exception as e:
            build_year = flag_exception
        item["房源描述"] = housing_description
        item["房源链接"] = housing_url
        item["城市"] = CITY
        item["来源"] = RESOURCE
        # 房屋户型
        item["房屋户型"] = room_type
        # 建筑面积
        item["建筑面积"] = room_area
        # 房屋朝向
        item["房屋朝向"] = room_orientation
        # 总楼层
        item["总楼层"] = total_floor
        # 所在楼层
        item["所在楼层"] = location_floor
        # 装修情况
        item["装修情况"] = room_decorate
        # 建成年份
        item["建成年份"] = build_year
        # 小区
        community = housing.xpath("./div[@class='listX']/p[2]/a[1]/text()").extract_first()
        # 地址
        address = housing.xpath("./div[@class='listX']/p[2]/a[2]/text()").extract_first()
        if address:
            address = address.replace(" ", '')
        item["小区"] = community
        item["地址"] = address
        input_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        item["插入时间"] = input_time
        # 获取已有的url
        SET_LIST.add(housing_url)
        if "ershoufang" in housing_list_url:
            item["租售状态"] = 0
            item["总价"] = info_red
            item["单价"] = info_grey.replace("单价", "")
            item["房源状态"] = "可售"
            flag_filter = (housing_url, "可售")
            if flag_filter in HOUSING_SQL_LIST:
                pass
            else:
                if housing_url in HOUSING_URL_LIST:
                    # 已售变为可售
                    item["flag_status"] = "可售"
                    item["更新时间"] = input_time
                    yield item
                app.send_task("item.i5j", args=(item,), queue="test_item")
        else:
            item["租售状态"] = 1
            item["总价"] = None
            item["单价"] = info_red
            item["房源状态"] = "可租"
            if info_grey:
                rental_way = info_grey.replace("出租方式：", "")
                item["租赁方式"] = rental_way
            flag_filter = (housing_url, "可租")
            if flag_filter in HOUSING_SQL_LIST:
                pass
            else:
                if housing_url in HOUSING_URL_LIST:
                    item["flag_status"] = "可租"
                    item["更新时间"] = input_time
                app.send_task("item.i5j", args=(item,), queue="test_item")

# 指定item
@app.task(
    name="item_zhongyuan",
    bind=True,
    rate_limit='100/s',
    autoretry_for=(requests.Timeout, requests.ConnectionError),
    retry_kwargs={
        'max_retries': 10,
        # 'countdown': 1
    },
    acks_late=True,
)
def item(self, item):
    # 只有已售变为可售；已租变为可租会有flag_status；还有一类新的
    '''

    :param self:
    :param item:
    :return:
    '''
    # house
    item_business = ItemSqlHandle(table_name=TABLE_NAME, item=item)
    item_business.do_select()
