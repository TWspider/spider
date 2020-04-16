import os
from pybloom_live import BloomFilter

# coon = pymysql.connect(host='127.0.0.1', user='root', passwd='qwer', db='haining')
# cur = coon.cursor()
# cur.execute("SELECT room_id from haining_room")
# room_urls = cur.fetchall()

ls = ["1049be49dc584707"]
os.chdir(r'E:\Myproject\Scan\chizhou\chizhou\spiders')

is_exist = os.path.exists('chizhou.blm')
# 判断是否存在bloom文件
# 判断存在就读取
if is_exist:
    bf = BloomFilter.fromfile(open('chizhou.blm', 'rb'))
    # 没有该文件则创建bf对象 最后的时候保存文件
else:
    bf = BloomFilter(1000000, 0.0000001)

i = 1
for room_url in ls:
    if room_url in bf:
        print('pass')
        pass
    else:
        # 加入布隆列表
        bf.add(room_url)
        print('添加了 %s 个' % i)
        i += 1
# 创建，写入布隆文件(单次写入)
bf.tofile(open('chizhou.blm', 'wb'))

print("测试git")

# cur.close()
# coon.close()
