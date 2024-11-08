import pymysql
import sqlalchemy
from sqlalchemy import create_engine
import pandas as pd


from urllib.parse import quote_plus as urlquote

#首先声明此程序仅供学习交流使用，不可商用不可实盘否则后果自负！
#首先声明此程序仅供学习交流使用，不可商用不可实盘否则后果自负！
#首先声明此程序仅供学习交流使用，不可商用不可实盘否则后果自负！

userName = 'thisone'
password = "123456"     #策略3
dbHost = '127.0.0.1'
dbPort = 3306
dbName = 'stock'
DB_CONNECT = f'mysql+pymysql://{userName}:{urlquote(password)}@{dbHost}:{dbPort}/{dbName}?charset=utf8'


def query_data1(sql_cmd):
    try:
        engine = create_engine(DB_CONNECT)
        # sql 命令
        df = pd.read_sql(sql=sql_cmd, con=engine)
    except Exception as ee:
        print("sql查询出错或为空",ee)
        dff = pd.DataFrame()
        return dff
    finally:
        engine.dispose()  #查询后关闭mysql连接
    return df

def replace_todb(df,table_name):
    try:
        engine = create_engine(DB_CONNECT)
        df.to_sql(table_name,con=engine, schema='stock', index=False, index_label=False, if_exists='replace')
    except Exception as ee:
        print("replace_todb fialed", ee)
    finally:
        engine.dispose()  #查询后关闭mysql连接
    return

def append_todb(df,table_name):
    try:
        engine = create_engine(DB_CONNECT)
        df.to_sql(table_name,con=engine, schema='stock', index=False, index_label=False, if_exists='append')
    except Exception as ee:
       print("append_todb fialed",ee)
    finally:
        engine.dispose()  #查询后关闭mysql连接
    return


