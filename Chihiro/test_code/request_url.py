import pandas as pd
import difflib
import jieba
from sqlalchemy import create_engine
import time
import re

# 一星
time_start = time.time()

# engine_tw_house = create_engine('mssql+pymssql://tw_user:123456@10.55.5.7/TWEstate')
# tw_house_rank_1_community = pd.read_sql(
#     "select EstateAreaName from Estate GROUP BY EstateAreaName",
#     engine_tw_house)
#
# extract_tw_house = pd.read_sql(
#     "select top 10000000 * from room",
#     engine_tw_house, chunksize=50000)
#
#
# 不写on默认匹配相同字段；写on则只匹配on中的、剩下同名区分为_x、_y；同名字段全匹配
df1 = pd.DataFrame({'key': ["a", 'b', ''], 'data1': [2, 3, 3]})
df2 = pd.DataFrame({'key': ['a', 'a', ], 'data2': [2, 3, ], 'test2': [1, 2]})
# res = df1.rename(columns={'key': 'test'}, inplace=True)
# df1.loc[df1.query('key==""').index, 'key'] = 'ffff'
# print(df1[['key', 'data']], type(df1[['key', 'data']]))
res = pd.merge(df1, df2, how='left')
res.loc["t"] = 1
print(res)

# 同时设置两列、则可同时and判断两列的
# res = df1.append(df2).reindex
# print(res)

# 小区名+路名

time_end = time.time()
print('totally cost', time_end - time_start)

# 读取数据库所有需要的字段

# df_sql.to_sql('%s_new' % table, con=engine_sql, if_exists="append", index=False,
#               dtype=dtype_dict)

# 分割处理
# df2.loc[(df2["key1"] == "a"), 'rank'] = "asd"
# # 删掉的部分
# res = df2[df2['rank'] == 'asd']
# print(res)
# # 余下的
# res1 = df2.drop(labels=res.index)
# print(res1)

# 打印结果
# res_match_next.to_csv("res_match_next.csv", encoding='gb18030')
# print("success")