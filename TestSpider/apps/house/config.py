from TW.spider.Test_spider.setting import *

# 城市
CITY = "上海"
# 来源网站
RESOURCE = '中原'
# item计数
TOTAL_ITEM = 0
# 默认请求头
DEFAULT_REQUEST_HEADERS = {
    "Accept-Encoding": "gzip, deflate, br",
}

# request

# 基本url
BASE_URL = "https://sh.centanet.com"
# 请求的生产者队列
PRODUCTER_QUEUE_REQUEST = "test_page"
# 请求的生产者下级队列
PRODUCTER_QUEUE_REQUEST_NEXT = "test_item"
# 消费速率
RATE_LIMIT_REQUEST = "20/s"
# 最大重试次数
MAX_RETRIES_REQUEST = 3
# 冷却时间
COUNTDOWN_REQUEST = 3

# item


# item的生产者队列
PRODUCTER_QUEUE_ITEM = "test_page"
# item的生产者下级队列
PRODUCTER_QUEUE_ITEM_NEXT = "test_sql"
# 消费速率
RATE_LIMIT_ITEM = "100/s"
# 最大重试次数
MAX_RETRIES_ITEM = 1
# 冷却时间
COUNTDOWN_ITEM = 1

# sql

# 消费速率
RATE_LIMIT_SQL = "80/s"
# 最大重试次数
MAX_RETRIES_SQL = 1
# 冷却时间
COUNTDOWN_SQL = 3


# Item配置

