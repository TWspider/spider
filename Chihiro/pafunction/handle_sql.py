import pymssql
from DBUtils.PooledDB import PooledDB


class HouseHandle(object):
    __pool = None

    def __init__(self, table_name, item=None):
        self.item = item
        self.conn = HouseHandle.getsqlconn()
        self.cursor = self.conn.cursor()
        if item:
            self.fields = list(item.keys())
            self.items_sql = ",".join(['item.get("{}")'.format(x) for x in self.fields])
            self.fields_sql = ",".join(self.fields)
            self.values_sql = ','.join(['%s'] * len(self.fields))
            self.insert_sql = 'insert {}({}) values({})'.format(table_name, self.fields_sql, self.values_sql)
            self.update_sql = "UPDATE {} SET HouseStatus=%s, UpdateTime=%s where HouseUrl = %s".format(table_name)
            # print(self.items_sql)
            # print(self.insert_sql)

    # 数据库连接池连接
    @staticmethod
    def getsqlconn():
        if HouseHandle.__pool is None:
            __pool = PooledDB(pymssql, 20, host='10.55.5.215', user='tw_user', password='123456', database='TWSpider')
        return __pool.connection()

    def do_select(self):
        flag_status = self.item.get("flag_status")
        if flag_status == "可售":
            self.do_update()
        else:
            self.do_insert()

    def do_insert(self):
        self.cursor.execute(self.insert_sql, (
            self.item.get("上次交易"), self.item.get("交易权属"), self.item.get("产权年限"), self.item.get("产权所属"),
            self.item.get("区域"), self.item.get("单价"),
            self.item.get("地址"), self.item.get("城市"), self.item.get("套内面积"), self.item.get("小区"), self.item.get("建筑类型"),
            self.item.get("建筑结构"),
            self.item.get("建筑面积"), self.item.get("总价"), self.item.get("总楼层"), self.item.get("户型结构"),
            self.item.get("房屋年限"),
            self.item.get("房屋户型"), self.item.get("房屋朝向"), self.item.get("房屋用途"), self.item.get("房本备件"),
            self.item.get("所在楼层"),
            self.item.get("抵押信息"), self.item.get("挂牌时间"), self.item.get("来源"), self.item.get("板块"),
            self.item.get("梯户比例"),
            self.item.get("租售状态"), self.item.get("装修情况"), self.item.get("配备电梯"), self.item.get("入住"),
            self.item.get("发布"),
            self.item.get("燃气"), self.item.get("用水"), self.item.get("用电"), self.item.get("电梯"), self.item.get("看房"),
            self.item.get("租期"), self.item.get("租赁方式"), self.item.get("车位"), self.item.get("采暖"), self.item.get("插入时间"),
            self.item.get("更新时间"), self.item.get("房源状态"), self.item.get("房源链接"), self.item.get("房源描述"),
            self.item.get("建成年份")
        ))
        self.conn.commit()

    def do_update(self):
        self.cursor.execute(self.update_sql, (self.item.get("房源状态"), self.item.get("更新时间"), self.item.get("房源链接")))
        self.conn.commit()

    def close(self):
        self.conn.close()
        self.cursor.close()

if __name__ == "__main__":
    item_house = HouseHandle("temp")
