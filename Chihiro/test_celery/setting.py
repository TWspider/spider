from .handle_item import ItemSqlHandle

# 起始参数
CITY = "上海"
RESOURCE = '中原'

SET_LIST = set()
BASE_URL = "https://sh.centanet.com"
TABLE_NAME = 'house'
IT = ItemSqlHandle(table_name=TABLE_NAME)
select_sql = "select HouseUrl,HouseStatus from %s where Resource='%s'" % (TABLE_NAME, RESOURCE)
IT.cursor.execute(select_sql)
HOUSING_SQL_LIST = IT.cursor.fetchall()
HOUSING_URL_LIST = set([x[0] for x in HOUSING_SQL_LIST])

