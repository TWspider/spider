# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.loader.processors import TakeFirst


class ChihiroItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    LastTradingTime = scrapy.Field(output_processor=TakeFirst())
    TradingOwnerShip = scrapy.Field(output_processor=TakeFirst())
    PropertyYears = scrapy.Field(output_processor=TakeFirst())
    PropertyBelong = scrapy.Field(output_processor=TakeFirst())
    AreaName = scrapy.Field(output_processor=TakeFirst())
    PriceUnit = scrapy.Field(output_processor=TakeFirst())
    PropertyAddress = scrapy.Field(output_processor=TakeFirst())
    PropertyCity = scrapy.Field(output_processor=TakeFirst())
    PropertyWithinSquare = scrapy.Field(output_processor=TakeFirst())
    PropertyCommunity = scrapy.Field(output_processor=TakeFirst())
    BuildingType = scrapy.Field(output_processor=TakeFirst())
    BuildingStructure = scrapy.Field(output_processor=TakeFirst())
    BuildingSquare = scrapy.Field(output_processor=TakeFirst())
    TotalPrice = scrapy.Field(output_processor=TakeFirst())
    TotalFloor = scrapy.Field(output_processor=TakeFirst())
    HouseStructure = scrapy.Field(output_processor=TakeFirst())
    HouseYears = scrapy.Field(output_processor=TakeFirst())
    HouseType = scrapy.Field(output_processor=TakeFirst())
    HouseDirection = scrapy.Field(output_processor=TakeFirst())
    HouseUse = scrapy.Field(output_processor=TakeFirst())
    HouseCertificate = scrapy.Field(output_processor=TakeFirst())
    Floor = scrapy.Field(output_processor=TakeFirst())
    MortgageInfo = scrapy.Field(output_processor=TakeFirst())
    UpShelfDate = scrapy.Field(output_processor=TakeFirst())
    Resource = scrapy.Field(output_processor=TakeFirst())
    PlateName = scrapy.Field(output_processor=TakeFirst())
    LadderProtition = scrapy.Field(output_processor=TakeFirst())
    RentalStatus = scrapy.Field(output_processor=TakeFirst())
    FixTypeName = scrapy.Field(output_processor=TakeFirst())
    TimeToLive = scrapy.Field(output_processor=TakeFirst())
    TimeToRelease = scrapy.Field(output_processor=TakeFirst())
    HasGas = scrapy.Field(output_processor=TakeFirst())
    WaterType = scrapy.Field(output_processor=TakeFirst())
    ElectriciType = scrapy.Field(output_processor=TakeFirst())
    HasElevator = scrapy.Field(output_processor=TakeFirst())
    WatchHouse = scrapy.Field(output_processor=TakeFirst())
    LeaseTime = scrapy.Field(output_processor=TakeFirst())
    LeaseType = scrapy.Field(output_processor=TakeFirst())
    HasParkingPlace = scrapy.Field(output_processor=TakeFirst())
    HasHot = scrapy.Field(output_processor=TakeFirst())
    InsertTime = scrapy.Field(output_processor=TakeFirst())
    UpdateTime = scrapy.Field(output_processor=TakeFirst())
    HouseStatus = scrapy.Field(output_processor=TakeFirst())
    HouseUrl = scrapy.Field(output_processor=TakeFirst())
    HouseDesc = scrapy.Field(output_processor=TakeFirst())
    BuildedTime = scrapy.Field(output_processor=TakeFirst())
    PubCompany = scrapy.Field(output_processor=TakeFirst())
    Agent = scrapy.Field(output_processor=TakeFirst())

# 特殊字段：27
        # LastTradingTime   上次成交    pass
        # TradingOwnerShip  交易所属权    pass
        # PropertyYears     产权年限    pass
        # PropertyBelong    产权所属    pass
        # PropertyWithinSquare  套内面积    pass
        # BuildingType      建筑类型    pass
        # BuildingStructure 建筑结构    pass
        # HouseStructure    户型结构    pass
        # HouseYears        房屋年限    pass
        # HouseUse          用途    pass
        # HouseCertificate  房本备件    pass
        # MortgageInfo      抵押信息    pass
        # UpShelfDate       挂牌时间    pass
        # LadderProtition   电梯比例    pass
        # FixTypeName       装修    pass
        # TimeToLive        入住    pass
        # TimeToRelease     发布    pass
        # HasGas            燃气    pass
        # WaterType         用水    pass
        # ElectriciType     用电    pass
        # HasElevator       电梯    pass
        # WatchHouse        看房    pass
        # LeaseTime         租期    pass
        # LeaseType         租赁方式    pass
        # HasParkingPlace   车位    pass
        # HasHot            采暖    pass
        # BuildedTime       建成时间    pass

# 公共字段：16
        # AreaName          区域    pass
        # PlateName         板块    pass

        # PropertyCommunity     小区    pass
        # PropertyAddress   地址    pass

        # TotalFloor        总楼层    pass
        # Floor             所在楼层    pass

        # HouseUrl          链接    pass
        # HouseDesc         描述    pass

        # HouseType         户型    pass
        # BuildingSquare    面积    pass
        # TotalPrice        总价    pass
        # PriceUnit         单价    pass

        # HouseDirection    朝向    pass
        # FixTypeName       装修    pass
        # PubCompany        发布公司    pass
        # Agent             经纪人    pass

# 默认字段
# PropertyCity
# Resource
# InsertTime
# UpdateTime
# HouseStatus
# RentalStatus


