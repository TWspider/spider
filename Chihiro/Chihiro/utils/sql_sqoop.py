import pandas as pd
from sqlalchemy.types import NVARCHAR, INT
from sqlalchemy import create_engine


def t3():
    '''
    导入数据到表内
    :return:
    '''

    host = '10.10.202.13'
    database = 'TWSpider'
    user = 'bigdata_user'
    password = 'ulyhx3rxqhtw'
    engine_word = create_engine(
        'mssql+pymssql://{}:{}@{}/{}'.format(user, password, host, database))
    # 要导入的数据
    ls = [
        "骗.*太平洋",
        "黑.*太平洋"
    ]
    ls_type = len(ls) * [1]
    df = pd.DataFrame({"type": ls_type, "content": ls})
    df.to_sql("News_Params", if_exists="append", index=False, con=engine_word)
    print(df)

if __name__ == '__main__':
    pass