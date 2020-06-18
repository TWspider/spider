from setting import HOST, USER, PASSWORD, DATABASE
import pymssql
import traceback
import datetime


def update(sql_update_third, house_rank):
    try:
        with pymssql.connect(host=HOST, database=DATABASE,
                             user=USER, password=PASSWORD, charset="utf8") as conn:
            with conn.cursor() as cursor:
                # 查出需要更新的字段
                cursor.execute(sql_update_third)
                res = cursor.fetchall()
                update_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                cursor.executemany(
                    "UPDATE {} SET Status=%s, UpdateTime='{}' where ThirdId=%s".format(house_rank,
                                                                                       update_time),
                    res
                )
                conn.commit()
    except:
        traceback.print_exc()
