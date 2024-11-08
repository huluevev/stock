import pandas as pd
import tushare as ts
import datetime
import time

import pymysql
#from sqlalchemy import create_engine
from sqlalchemy import create_engine,text
from urllib.parse import quote_plus as urlquote

import tkinter as tk  # 使用Tkinter前需要先导入
# import matplotlib
# 创建画布需要的库

# 导入绘图需要的模块
# from matplotlib.figure import Figure
from tkinter import scrolledtext  # 导入滚动文本框的模块

import threading
#import multiprocessing

from concurrent.futures import ProcessPoolExecutor as Pool


aa = pd.DataFrame({"ts_code": ['600300.SH', '000000', '000001'], "B": [3, 2, 2]})

#首先声明此程序仅供学习交流使用，不可商用不可实盘否则后果自负！
#首先声明此程序仅供学习交流使用，不可商用不可实盘否则后果自负！
#首先声明此程序仅供学习交流使用，不可商用不可实盘否则后果自负！



#这个文件的作用是 连接tushare（百度一下一种数据源）将必要的日线数据存入数据库中 
#每天的数据一张表比如： mysql（我记得是5.7版本）数据库的t20231204表 
# t20231204表中有些字段是tushare中获得 还有一部分“大单数据”是通过计算Leve2盘后数据得来的，他们共同组成了t20231204表。
#Leve2盘后数据是csv格式的 可以从某宝获得。路径--程序运行后显示在程序界面上。
#程序运行后，一次可以 起:20231204  止20231204（只算当天） 起:20231201  止20231204 ;也可以起:20231101  止20231204(虽然程序使用了多进程但这样跨度大很慢且容易失败)注意，日期只能是交易日
#每天运行前，需要提前一天准备好数据库数据 如 起:20231204  止20231204
#先填写好起、止日期、csv文件路径（比如20231204当日的所有csv就放在20231204这个文件夹中），之后依次点击
#之后依次点击：'+g'(作用是从tshare获得交易日期等基础数据) “+j” (作用是从tshare获得日线数据)，再点击“+K”(计算本地leve2数据csv文件，并入库).

#我有2个数据库  其实一个就可以 请更具实际情况配置以下代码
userName = 'this'
password = '123456'
dbHost = '127.0.0.1'
dbPort = 3306
dbName = 'stock'
DB_CONNECT = f"mysql+pymysql://{userName}:{urlquote(password)}@{dbHost}:{dbPort}/{dbName}?charset=utf8"

userName1 = 'kaifa'
password1 = '654321'
dbHost1 = '10.10.10.24'
dbPort1 = 3306
dbName1 = 'stock'
DB_CONNECT1 = f"mysql+pymysql://{userName1}:{urlquote(password1)}@{dbHost1}:{dbPort1}/{dbName1}?charset=utf8"

#需要根据你的tushare账号token更改
ts_token = 'cccccccccccaaaaaaaaaaaaaaaaaaabbbbbbbbbbbbbb6'

len_n = 0
lastday = 0

df_daily_basic = pd.DataFrame()
df_cal_g = pd.DataFrame()

df_x = pd.DataFrame()
cal_dates= pd.DataFrame()

token = ts_token
ts.set_token(token)
pro = ts.pro_api()


# 获取从起始日期到截止日期中间的的所有日期，前后都是封闭区间
def get_date_list(begin_date, end_date):
    date_list = []
    while begin_date <= end_date:
        # date_str = str(begin_date)
        date_list.append(begin_date)
        begin_date += datetime.timedelta(days=1)
    return date_list


'''
def get_date_list_stock(end_date):        
    date_list=[]
    m=80
    while m:
        m = m-1
        date_list.append(end_date)
        end_date -= datetime.timedelta(days=1)
    return date_list
'''


def get_date_list_stock(end_date):
    begin_date = str2date('20190301', date_format="%Y%m%d")
    #    begin_date1 = var_begin_date.get()
    #    begin_date = str2date(begin_date1,date_format="%Y%m%d")
    date_list = []
    while begin_date <= end_date:
        # date_str = str(begin_date)
        date_list.append(end_date)
        end_date -= datetime.timedelta(days=1)
    return date_list


def date2str(date, date_formate="%Y%m%d"):
    str = date.strftime(date_formate)
    return str


def str2date(str, date_format="%Y%m%d"):
    date = datetime.datetime.strptime(str, date_format)
    return date


def date2path(begin_date, end_date, rootdir, stocks):

    open_date_str_list = []
    path_str_list = []
    date_list = []

    date_list = date2sqlTable(date2str(begin_date),date2str(end_date))

    for date_str_t in date_list:
        date_str = date_str_t[1:]
        for stock_code_t in stocks.ts_code:
            stock_code=stock_code_t[0:6]
            path_str_list.append(
                rootdir + "/" + date_str[0:6] + "/" + date_str[0:4] + "-" + date_str[4:6] + "-" + date_str[
                                                                                                    6:8] + "/" + stock_code + ".csv") 
    return path_str_list


def date2path_stock(end_date, rootdir, stock):
    #    global open_date_str_list
    open_date_str_list = []
    path_str_list = []
    date_list = []

    date_list = get_date_list_stock(str2date(end_date))

    ##    print('date_list_stock',date_list)
    #    str_date=str(date) #2019-04-12
    #    str_date1=date.strftime('%Y%m%d')  #20190412
    for date in date_list:
        #        print('date2str(date)',date2str(date))
        #        print('iat01',cal_dates[cal_dates['cal_date']==date2str(date)])
        if cal_dates[cal_dates['cal_date'] == date2str(date)].iat[0, 2] == 1:
            #            print('okokokok',date)
            open_date_str_list.append(str(date.strftime('%Y%m%d')))  # 20190412,20190413....
        else:
            continue
    for date_str in open_date_str_list[0:-1]:
        path_str_list.append(
            rootdir + "\\" + date_str[0:6] + "\\" + date_str[0:4] + "-" + date_str[4:6] + "-" + date_str[
                                                                                                6:8] + "\\" + stock + ".csv")  # +stock_code[0:6]
    #                print('path_str_list',path_str_list)

    #    print('open_date_str_list',open_date_str_list)
    ##    print('path_str_list111111_stock:',path_str_list)
    return path_str_list


def csv2excel(path_str_list, bigDanAmount):
    n = 6
    # fen_n = int(len(path_str_list) / n)
    #
    # m = 0
    # p = Pool()
    # res_l = []
    # for i in range(n - 1):
    #     res = p.apply_async(csv_2_10XThread, args=(path_str_list[(fen_n * m): (fen_n * (m + 1))], bigDanAmount))
    #     res_l.append(res)
    #     m = m + 1
    #
    # res1 = p.apply_async(csv_2_10XThread, args=(path_str_list[(fen_n * m): len(path_str_list)], bigDanAmount))
    # res_l.append(res1)
    #
    # p.close()
    # p.join()  # 等待进程池中所有进程执行完毕
    #
    # frames = []  # 拼接所有df列不变行扩展
    # for res in res_l:
    #     frames.append(res.get())  # 拿到所有结果
    #
    # data = pd.concat(frames, axis=0, ignore_index=False, )
    # data = data.reset_index(drop=True)
    #
    # ##    data['bizhi'] = data['大单买小单卖']/data['大单卖小单买']
    # ##    data = data[['日期','ts_code','大单买小单卖1','大单卖小单买1','大单买小单卖','大单卖小单买','bizhi']]
    # #    data.dropna(axis=0,how='any')
    return


def save_2_excel(df, stock_code, output_dir):
    #    output_dir = 'D:\\python\\'+stock_code+'-74.xlsx'  #下载数据的存放路径

    return


################################################

def save2excel(df2, output_dir, date_list):

    return


def _excelAddSheet(self, dataframe, excelWriter, sheet_name):

    return

#计算全部 --交易日历入库###
def jisuan():
    global cal_dates, df_x, pro, df_cal_g,df_daily_basic,lastday,var_begin_date,var_end_date

    begin_date = var_begin_date.get()
    end_date = var_end_date.get()




    #交易日历入库#################################
    dt = datetime.datetime.now()  # 创建一个datetime类对象
    today = dt.strftime('%Y%m%d')
    cal_dates_1 = pro.trade_cal(exchange='', start_date='20200101', end_date=today)  # 从20160101以来的交易日历

    temp_df = cal_dates_1.loc[cal_dates_1.is_open == 1].cal_date
    lastday = temp_df.iloc[-1]
    engine = create_engine(DB_CONNECT)
    engine1 = create_engine(DB_CONNECT1)
    ok_flag = 0
    try:
        temp_df.to_sql(name="cal_date" , con=engine, if_exists='replace', index=False, index_label=False)
        print( "cal_date to_sql ok +++++++:")
        ok_flag = 1
        time.sleep(5)
    except Exception as e:
        print( "cal_date to_sql Fule +++++++:")
        print(e)
        pass

    try:
        temp_df.to_sql(name="cal_date" , con=engine1, if_exists='replace', index=False, index_label=False)
        print( "cal_date to_sql ok222 +++++++:")
        ok_flag = 1
        time.sleep(5)
    except Exception as e:
        print( "cal_date to_sql Fule222 +++++++:")
        print(e)
        pass


    if ok_flag ==1:
        sqlTables = date2sqlTable(begin_date, end_date)

        #日线数据
        frames = pd.DataFrame()

        m = 0
        frist = 0
        # 循环获取单个股票的日线行情
        for date in sqlTables:

            day_df = pro.daily(trade_date=date[1:])  #日线数据
            adj_df = pro.adj_factor(ts_code='', trade_date=date[1:])  #日线复权数据
            adj_df1 = adj_df[['ts_code', 'adj_factor', ]]
            adj_df1.rename(columns={'ts_code': 'ts_code1',}, inplace=True)
            day_df_res = pd.merge(day_df, adj_df1, how='inner', left_on='ts_code', right_on='ts_code1', sort=False)
            day_df_res.drop('ts_code1', axis='columns')  # same
            #print('day_df_res',day_df_res.head(3))
            if frist == 0:
                frames = day_df_res
                frist=1
            else:
                # frames = frames.append(day_df_res)
                frames = pd.concat([frames, day_df_res], ignore_index=True)

            time.sleep(1.2)

        print('sqlTables',sqlTables)
        print('frames',frames.tail(3))
        # df_cal_g = pd.concat(frames, axis=0, ignore_index=False, )  # 得到begin_date, end_date之间的日线数据
        df_cal_g = frames.reset_index(drop=True)
        #print("df_cal_g11111:", df_cal_g.head(3))



        df_z = pd.DataFrame()
        for trade_day in sqlTables:
            df_tmp = pro.daily_basic(ts_code='', trade_date=trade_day[1:],
                             fields='ts_code,trade_date,turnover_rate_f,volume_ratio,pe,pb,float_share,free_share')
            #turnover_rate_f 换手率（自由流通股）;volume_ratio量比,pe市盈率（总市值/净利润， 亏损的PE为空）
            # pb 市净率（总市值/净资产)float_share 流通股本 （万股） # free_share 自由流通股本 （万）
            df_tmp.fillna(0, inplace=True)  #用0替换nan 因为此df中有的值为nan
            # df_daily_basic = df_daily_basic.append(df_tmp)
            df_z = pd.concat([df_z,df_tmp],ignore_index=True)
            time.sleep(1.2)

        df_daily_basic=df_z

        #####加入上证指数获取
        get_dapan_ma()
        return df_daily_basic
    else:
        df_xf = pd.DataFrame({"shibai": ['shibai']})
        textPad.delete(1.0, tk.END)  # 使用 delete
        textPad.insert(tk.END, str(df_xf))
        textPad.place(x=0, y=60, anchor='nw')
        print('获取交易日历出错')
        return df_daily_basic

#入库
def get_10X_stock():
    global cal_dates, df_x, pro,var_begin_date,var_end_date
    df_x = pd.DataFrame({"ts_code": ['600300.SH']})

    if df_x.empty == False:
        df_x.drop(df_x.index, inplace=True)  # 清空dataframe
    token = ts_token
    ts.set_token(token)
    pro = ts.pro_api()

#    stocks = pro.stock_basic(is_hs='S',exchange='', list_status='L', fields='ts_code')
    stocks = pro.stock_basic( exchange='', list_status='L', fields='ts_code')
    #    print(type(stocks))
    #    print(stocks)
    begin_date = var_begin_date.get()
    end_date = var_end_date.get()
    # cal_dates = ts.trade_cal() #返回交易所日历，类型为DataFrame, calendarDate  isOpen
    cal_dates = pd.DataFrame({"cal_date": ['20160101']})
    if cal_dates.empty == False:
        cal_dates.drop(cal_dates.index, inplace=True)  # 清空dataframe

    cal_dates = pro.trade_cal(exchange='', start_date='20140101', end_date=end_date)
    ##    print('cal_dates22222:::::',cal_dates)
    dftemp = pd.DataFrame()
    # rootdir = 'D:\python\DATA'
    rootdir = var_path.get()
    bigDanAmount = 190000
    code = '888888'

    #    stock_code =
    output_dir = var_path.get() + '\\' + 'data.xlsx'  # 下载数据的存放路径
    #    dftemp.to_excel(output_dir,sheet_name='sheet1')

    #    print('begin_date::::::',begin_date)

    yyyy_b = int(begin_date[0:4])
    mm_b = int(begin_date[4:6])
    dd_b = int(begin_date[6:8])
    yyyy_e = int(end_date[0:4])
    mm_e = int(end_date[4:6])
    dd_e = int(end_date[6:8])

    #    print(yyyy_b)
    #    print(mm_b)
    #    print(dd_b)
    #    time.sleep(2)
    #    print(yyyy_e)
    #    print(mm_e)
    #    print(dd_e)
    start = time.process_time()
    print("Thread start_time", start)

    aa.ts_code = ['600300.SH', '600010.SH', '600020.SH']
   
    path_str_list = date2path(datetime.date(yyyy_b, mm_b, dd_b), datetime.date(yyyy_e, mm_e, dd_e), rootdir,
                                        stocks) 



    csv_2_10Xstock(path_str_list, bigDanAmount,df_daily_basic)
##    df2 =csv_2_10Xstock_test(path_str_list, bigDanAmount)
    time.sleep(7)
    ma_jisuan_start()
    #################################
    end = time.process_time()
    elapsed = end - start
    print("Time end:", end)

 #   textPad.delete(1.0, tk.END)  # 使用 delete
 #   textPad.insert(tk.END, str(df_x))
 #   textPad.place(x=0, y=60, anchor='nw')
    #    tk.update()
    return

def ma_jisuan_start():
    begin_date = var_begin_date.get()
    end_date = var_end_date.get()
    ma_df = ma_jisuan(begin_date, end_date)

    sqlTables = date2sqlTable(begin_date, end_date)
    ###################################
    # 这里一定要写成mysql+pymysql，不要写成mysql+mysqldb
    engine = create_engine(DB_CONNECT)
    engine1 = create_engine(DB_CONNECT1)
    for table in sqlTables:
        sql = "select * from " + table
        sql_df = query_data1(sql)
        if sql_df.shape[1] < 38: #如果列数小于38列那么
            ma_df_sqlday = ma_df.loc[ma_df.ma_trade_date == table[1:]]
            ma_df_sqlday2 = ma_df_sqlday[['ma5','ma10','ma20','ma55']]
            ma_df_sqlday3 = round(ma_df_sqlday2, 3)  #保留3位小数
            save_df = pd.merge( sql_df,ma_df_sqlday3, how='outer', left_on='ts_code',
                       right_index=True, sort=False)
         #   save_df2 = save_df[['','','','','','','','','','','','','','','','','','']]

            try:
                save_df.to_sql(name=table, con=engine, if_exists='replace', index=False, index_label=False)
                print(table + "ma to_sql ok +++++++:")
            except Exception as e:
                print(table + "ma to_sql jump +++++++:")
                pass
            try:
                save_df.to_sql(name=table, con=engine1, if_exists='replace', index=False, index_label=False)
                print(table + "ma to_sql ok222 +++++++:")
            except Exception as e:
                print(table + "ma to_sql jump222 +++++++:")
                pass

            continue
    return


    return

def ma_jisuan(begin_date,end_date):
    ma_all_df = pd.DataFrame(
        { "ts_code": ['1'],"trade_date": ['1'],"close": ['1']})
    if ma_all_df.empty == False:
        ma_all_df.drop(ma_all_df.index, inplace=True)  # 清空dataframe

    ma_df = pd.DataFrame(
        {"ts_code": ['1'], "trade_date": ['1'], "close": ['1']})
    if ma_df.empty == False:
        ma_df.drop(ma_df.index, inplace=True)  # 清空dataframe

    sqlTables_forward = date2sqlTable('20190301', begin_date)  # 数据只有从20190301起
    ma_sqltables_forward = sqlTables_forward[-55:]
    ma_sqltables_back = date2sqlTable(begin_date, end_date)
    ma_sqltables = ma_sqltables_forward + ma_sqltables_back


    for table in ma_sqltables:  # 回测前一天到回测前8天
        day_sql = "select ts_code,trade_date,close from " + table
        day_temp_df = query_data1(day_sql)
        # ma_all_df = ma_all_df.append(day_temp_df)
        ma_all_df = pd.concat([ma_all_df, day_temp_df], ignore_index=True)

    ma_sqltables_back_reverse = ma_sqltables_back.copy()
    ma_sqltables_back_reverse.reverse()  # 列表顺序倒置

    n = 0
    for table_b in ma_sqltables_back_reverse:  #从后向前遍历

        jisuan_day = table_b[1:]   #去除't'得日期
        ma_5_day = ma_sqltables[-5 - n][1:]
        ma_10_day = ma_sqltables[-10 - n][1:]
        ma_20_day = ma_sqltables[-20 - n][1:]
        ma_55_day = ma_sqltables[-55 - n][1:]

        ma_5_df =  ma_all_df.loc[(ma_all_df.trade_date >= ma_5_day) & (ma_all_df.trade_date <= jisuan_day)]
        ma_5_df_t= ma_5_df.groupby(by='ts_code').agg({'close': 'mean'}).rename(columns={'close': 'ma5'})

        ma_10_df = ma_all_df.loc[(ma_all_df.trade_date >= ma_10_day) & (ma_all_df.trade_date <= jisuan_day)]
        ma_10_df_t = ma_10_df.groupby(by='ts_code').agg({'close': 'mean'}).rename(columns={'close': 'ma10'})

        ma_20_df = ma_all_df.loc[(ma_all_df.trade_date >= ma_20_day) & (ma_all_df.trade_date <= jisuan_day)]
        ma_20_df_t = ma_20_df.groupby(by='ts_code').agg({'close': 'mean'}).rename(columns={'close': 'ma20'})

        ma_55_df = ma_all_df.loc[(ma_all_df.trade_date >= ma_55_day) & (ma_all_df.trade_date <= jisuan_day)]
        ma_55_df_t = ma_55_df.groupby(by='ts_code').agg({'close': 'mean'}).rename(columns={'close': 'ma55'})

        ma_df_2 = pd.merge(ma_5_df_t, ma_10_df_t, how='inner', left_index=True, right_index=True, sort=False)
        ma_df_3 = pd.merge(ma_df_2, ma_20_df_t, how='inner', left_index=True, right_index=True, sort=False)
        ma_df_4 = pd.merge(ma_df_3, ma_55_df_t, how='inner', left_index=True, right_index=True, sort=False)
        ma_df_4['ma_trade_date'] = jisuan_day

        n=n+1
        # ma_df = ma_df.append(ma_df_4)
        ma_df = pd.concat([ma_df, ma_df_4], ignore_index=True)

    return ma_df


def date2sqlTable(start_date,end_date): #start_date,end_date为str型： 20190606
    path_str_list = []
    sqlTable_list=[]
    sql = "select cal_date from cal_date where cal_date >= '" + start_date + "' and cal_date <= '" + end_date + "'"
    df_day = query_data1(sql)
    open_date_str_list = df_day['cal_date'].tolist()
    for date_str in open_date_str_list:
        sqlTable_list.append(
            "t" + date_str)  
    return sqlTable_list




#10线程
def csv_2_10Xstock(path_str_list, bigDanAmount,df_daily_basic):
    n = 10  # 线程数  4或7好
    fen_n = int(len(path_str_list) / n) #每个线程变量的list长度
    item_list =[]
    tempList = []
    for i in range(10):
        if i < (10-1):
            item_temp = path_str_list[fen_n*i: fen_n*(i+1)]
            item_list.append(item_temp)
        else:
            item_temp = path_str_list[fen_n * i: len(path_str_list)]
            item_list.append(item_temp)
        # item_list.append(bigDanAmount)

    print('len11:',len(item_list))
    print(item_list[0])
    # data = csv_2_10XThread(item_list[0])
    #
    with Pool(max_workers=10) as outer_pool:
        for pool_res in outer_pool.map(csv_2_10XThread, item_list):
            # print('7777 ok')
            tempList.append(pool_res)

    reslist = []
    for x in tempList:
        if x is None:
            pass
        else:
            reslist.append(x)
            # data_t = pd.concat(x, axis=0, ignore_index=False, )
            # data_res = data_res.append(data_t)
    if (len(reslist) >= 1) :
        data = pd.concat(reslist, axis=0, ignore_index=False, )


    data = data.reset_index(drop=True)
#    print('data:',data.head(3))
#    print('df_daily_basic:',df_daily_basic.head(3))
    new_df = pd.merge(data,df_daily_basic, how = 'left',left_on = ['ts_code','trade_date'],right_on = ['ts_code','trade_date'])
##不能要    new_df = new_df.dropna(axis=0, how='any')

    #拼接并入库
    print('new_df:',new_df.head(3))
    save2sql_and_excel(new_df)

    return



def csv_2_10XThread(list_c):
    path_str_list = list_c
    bigDanAmount = 190000

    stock_list = []
    excel_date_list = []
    bigbuysmall_1 = []
    bigsellsmall_1 = []

    bigbuysmall_50 = []
    bigsellsmall_50 = []
    bigbuysmall_100 = []
    bigsellsmall_100 = []
    bigbuysmall_150 = []
    bigsellsmall_150 = []
    bigbuysmall_200 = []
    bigsellsmall_200 = []

    zb_19 = []
    zb_50 = []
    zb_100 = []
    zb_200 = []
    zs_19 = []
    zs_50 = []
    zs_100 = []
    zs_200 = []
    buy_l = []
    sell_l = []
    b50_l = []
    s50_l = []
    b100_l =[]
    s100_l=[]
    dadan_cnt100_l = []
    dadan_cnt17_l = []
    dadan_cnt34_l = []
    temp_df = pd.DataFrame(
        {"trade_date": ['1'], "ts_code": ['1'], "大单买小单卖1": ['1'], "大单卖小单买1": ['1']})
#    if temp_df.empty == False:
 #       temp_df.drop(temp_df.index, inplace=True)  # 清空dataframe

    for path in path_str_list:

        stock_code = path[-10:-4]
        print('path11',path ,'tsc',stock_code)
        try:
            #            stock_code = path[-13:-4]
            #            print('stock_code22222',stock_code)
            #            print('path[0:-7]+".csv"',(path[0:-7]+".csv"))
            ddf = pd.read_csv(path)  # 读取训练数据

            csvdf = ddf  # 读取训练数据
            # print('11111 ok')

            df_b = csvdf.loc[csvdf.Type == 'B']
            df_s = csvdf.loc[csvdf.Type == 'S']
            df_b['buy'] = df_b['Price'] * df_b['Volume']
            buy = round(df_b['buy'].sum()/10000,1)
            buy_l.append(buy)
            df_s['sell'] = df_s['Price'] * df_s['Volume']
            sell = round(df_s['sell'].sum()/10000,1)
            sell_l.append(sell)
            # cha = buy - sell

            df50_b = csvdf.loc[((csvdf.BuyOrderVolume * csvdf.BuyOrderPrice) > 500000) & (csvdf.Type == 'B')]
            df50_b['b50'] = df50_b['Price'] * df50_b['Volume']
            b50 = round(df50_b['b50'].sum() / 10000, 1)
            b50_l.append(b50)
            df50_s = csvdf.loc[((csvdf.SaleOrderVolume * csvdf.SaleOrderPrice) > 500000) & (csvdf.Type == 'S')]
            df50_s['s50'] = df50_s['Price'] * df50_s['Volume']
            s50 = round(df50_s['s50'].sum() / 10000, 1)
            s50_l.append(s50)


            df100_b = csvdf.loc[((csvdf.BuyOrderVolume * csvdf.BuyOrderPrice)>1000000) & (csvdf.Type == 'B')]
            df100_b['b100'] = df100_b['Price'] * df100_b['Volume']
            b100 = round(df100_b['b100'].sum()/10000,1)
            b100_l.append(b100)
            df100_s = csvdf.loc[((csvdf.SaleOrderVolume * csvdf.SaleOrderPrice) > 1000000) & (csvdf.Type == 'S')]
            df100_s['s100'] = df100_s['Price'] * df100_s['Volume']
            s100 = round(df100_s['s100'].sum() / 10000,1)
            s100_l.append(s100)
            # cha100 = b100- s100

            # print('22222 ok')
            #        excel_date_list.append(open_date_str_list[n]) #excel中的日期列
            #        print('excel_date_list',excel_date_list)


            #极大单计数 就是一手买入890000股以上的单子个数
            # dadan_max = ddf.BuyOrderVolume.max()

            # dadan_cnt_df100 = ddf.loc[ddf.BuyOrderVolume == 1000000]
            # if dadan_cnt_df100.empty == False:
            #     dadan_cnt100_r = pd.DataFrame(dadan_cnt_df100.groupby('BuyOrderID'))
            #     dadan_cnt100 = dadan_cnt100_r.shape[0]   #行数
            # else:
            #     dadan_cnt100 = 0.0
            # #   588200<单数<1000000
            # dadan_cnt_df17 = ddf.loc[(ddf.BuyOrderVolume >= (dadan_max/1.7)) & (ddf.BuyOrderVolume < dadan_max)]
            # if dadan_cnt_df17.empty == False:
            #     dadan_cnt17_r = pd.DataFrame(dadan_cnt_df17.groupby('BuyOrderID'))
            #     dadan_cnt17 = dadan_cnt17_r.shape[0]
            # else:
            #     dadan_cnt17 = 0.0
            # #   294000<单数<588200
            # dadan_cnt_df34 = ddf.loc[(ddf.BuyOrderVolume >= (dadan_max / 3.4)) & (ddf.BuyOrderVolume < dadan_max/1.7)]
            # if dadan_cnt_df34.empty == False:
            #     dadan_cnt34_r = pd.DataFrame(dadan_cnt_df34.groupby('BuyOrderID'))
            #     dadan_cnt34 = dadan_cnt34_r.shape[0]
            # else:
            #     dadan_cnt34 = 0.0
            #
            # dadan_cnt100_l.append(dadan_cnt100)
            # dadan_cnt17_l.append(dadan_cnt17)
            # dadan_cnt34_l.append(dadan_cnt34)

            # 19万==========================================       #第一张图y方向正轴图
            bigsmall_1 = ddf.loc[(((ddf.BuyOrderVolume * ddf.BuyOrderPrice) >= bigDanAmount) & (
                        (ddf.SaleOrderVolume * ddf.SaleOrderPrice) < bigDanAmount)) |
                             (((ddf.BuyOrderVolume * ddf.BuyOrderPrice) >= bigDanAmount) & ((ddf.SaleOrderVolume * ddf.SaleOrderPrice) >= bigDanAmount) &
                              (ddf.BuyOrderVolume >=ddf.SaleOrderVolume))]
            if bigsmall_1.empty == False:
                bigsmall_1['sum_s'] = (bigsmall_1['Volume'] * bigsmall_1['Price'])
                bigsmall_s_1 = bigsmall_1['sum_s'].sum() / 10000
                bigsmall_s_1 = round(bigsmall_s_1)
                bigbuysmall_1.append(bigsmall_s_1)  #第一张图y方向正轴图
            else:
                bigbuysmall_1.append(0.0)  # 第一张图y方向正轴图
            # 卖单收小单资金      Sql = "select sum(Volume * Price)/10000 From [" & f & "] a where BuyOrderVolume * BuyOrderPrice<=" & bigdeal & _
            # " and SaleOrderVolume * SaleOrderPrice > " & bigdeal
            bigsmall_sell_1 = ddf.loc[(((ddf.BuyOrderVolume * ddf.BuyOrderPrice) <= bigDanAmount) & (
                        (ddf.SaleOrderVolume * ddf.SaleOrderPrice) > bigDanAmount)) |
                             (((ddf.BuyOrderVolume * ddf.BuyOrderPrice) >= bigDanAmount) & ((
                                         ddf.SaleOrderVolume * ddf.SaleOrderPrice) >= bigDanAmount) &
                             (ddf.BuyOrderVolume <= ddf.SaleOrderVolume))]
            #        if bigsmall_sell.isnull==True:
            if bigsmall_sell_1.empty == False:
                bigsmall_sell_1['sum_s'] = bigsmall_sell_1['Volume'] * bigsmall_sell_1['Price']
                bigsmall_sell_1 = bigsmall_sell_1['sum_s'].sum()/ 10000
                bigsmall_sell_1 = round(bigsmall_sell_1)
                bigsellsmall_1.append(bigsmall_sell_1)  # 第一张图y方向正轴图
            else:
                bigsellsmall_1.append(0.0)  # 第一张图y方向正轴图
            # ==============================================================
            # 50万
            bigsmall_50 = ddf.loc[(((ddf.BuyOrderVolume * ddf.BuyOrderPrice) >= 500000) & (
                    (ddf.SaleOrderVolume * ddf.SaleOrderPrice) < 500000))|
                             (((ddf.BuyOrderVolume * ddf.BuyOrderPrice) >= 500000) & ((ddf.SaleOrderVolume * ddf.SaleOrderPrice) >= 500000) &
                              (ddf.BuyOrderVolume >=ddf.SaleOrderVolume))]
            if bigsmall_50.empty == False:
                bigsmall_50['sum_s'] = bigsmall_50['Volume'] * bigsmall_50['Price']
                bigsmall_s_50 = bigsmall_50['sum_s'].sum() / 10000
                bigsmall_s_50 = round(bigsmall_s_50)
                bigbuysmall_50.append(bigsmall_s_50)  # 第一张图y方向正轴图
            else:
                bigbuysmall_50.append(0.0)  # 第一张图y方向正轴图
            # 卖单收小单资金      Sql = "select sum(Volume * Price)/10000 From [" & f & "] a where BuyOrderVolume * BuyOrderPrice<=" & bigdeal & _
            # " and SaleOrderVolume * SaleOrderPrice > " & bigdeal
            bigsmall_sell_50 = ddf.loc[((ddf.BuyOrderVolume * ddf.BuyOrderPrice) <= 500000) & (
                    (ddf.SaleOrderVolume * ddf.SaleOrderPrice) > 500000)|
                             (((ddf.BuyOrderVolume * ddf.BuyOrderPrice) >= 500000) & ((
                                         ddf.SaleOrderVolume * ddf.SaleOrderPrice) >= 500000) &
                             (ddf.BuyOrderVolume <= ddf.SaleOrderVolume))]
            #        if bigsmall_sell.isnull==True:
            if bigsmall_sell_50.empty == False:
                bigsmall_sell_50['sum_s'] = bigsmall_sell_50['Volume'] * bigsmall_sell_50['Price']
                bigsmall_sell_50 = bigsmall_sell_50['sum_s'].sum() / 10000
                bigsmall_sell_50 = round(bigsmall_sell_50)
                bigsellsmall_50.append(bigsmall_sell_50)  # 第一张图y方向正轴图
            else:
                bigsellsmall_50.append(0.0)  # 第一张图y方向正轴图
            # 负轴均价相关的图========================================================第一张图y方向负轴图

            # 100万
            bigsmall_100 = ddf.loc[(((ddf.BuyOrderVolume * ddf.BuyOrderPrice) >= 1000000) & (
                    (ddf.SaleOrderVolume * ddf.SaleOrderPrice) < 1000000))|
                             (((ddf.BuyOrderVolume * ddf.BuyOrderPrice) >= 1000000) & ((ddf.SaleOrderVolume * ddf.SaleOrderPrice) >= 1000000) &
                              (ddf.BuyOrderVolume >=ddf.SaleOrderVolume))]
            if bigsmall_100.empty == False:
                bigsmall_100['sum_s'] = bigsmall_100['Volume'] * bigsmall_100['Price']
                bigsmall_s_100 = bigsmall_100['sum_s'].sum() / 10000
                bigsmall_s_100 = round(bigsmall_s_100)
                bigbuysmall_100.append(bigsmall_s_100)  # 第一张图y方向正轴图
            else:
                bigbuysmall_100.append(0.0)  # 第一张图y方向正轴图
            # 卖单收小单资金      Sql = "select sum(Volume * Price)/10000 From [" & f & "] a where BuyOrderVolume * BuyOrderPrice<=" & bigdeal & _
            # " and SaleOrderVolume * SaleOrderPrice > " & bigdeal
            bigsmall_sell_100 = ddf.loc[(((ddf.BuyOrderVolume * ddf.BuyOrderPrice) <= 1000000) & (
                    (ddf.SaleOrderVolume * ddf.SaleOrderPrice) > 1000000))|
                             (((ddf.BuyOrderVolume * ddf.BuyOrderPrice) >= 1000000) & ((
                                         ddf.SaleOrderVolume * ddf.SaleOrderPrice) >= 1000000) &
                             (ddf.BuyOrderVolume <= ddf.SaleOrderVolume))]
            #        if bigsmall_sell.isnull==True:
            if bigsmall_sell_100.empty == False:
                bigsmall_sell_100['sum_s'] = bigsmall_sell_100['Volume'] * bigsmall_sell_100['Price']
                bigsmall_sell_100 = bigsmall_sell_100['sum_s'].sum() / 10000
                bigsmall_sell_100 = round(bigsmall_sell_100)
                bigsellsmall_100.append(bigsmall_sell_100)  # 第一张图y方向正轴图
            else:
                bigsellsmall_100.append(0.0)  # 第一张图y方向正轴图
            # ==============================================================

            # 150万
            bigsmall_150 = ddf.loc[(((ddf.BuyOrderVolume * ddf.BuyOrderPrice) >= 1500000) & (
                    (ddf.SaleOrderVolume * ddf.SaleOrderPrice) < 1500000))|
                             (((ddf.BuyOrderVolume * ddf.BuyOrderPrice) >= 1500000) & ((ddf.SaleOrderVolume * ddf.SaleOrderPrice) >= 1500000) &
                              (ddf.BuyOrderVolume >=ddf.SaleOrderVolume))]
            if bigsmall_150.empty == False:
                bigsmall_150['sum_s'] = bigsmall_150['Volume'] * bigsmall_150['Price']
                bigsmall_s_150 = bigsmall_150['sum_s'].sum() / 10000
                bigsmall_s_150 = round(bigsmall_s_150)
                bigbuysmall_150.append(bigsmall_s_150)  # 第一张图y方向正轴图
            else:
                bigbuysmall_150.append(0.0)  # 第一张图y方向正轴图
            # 卖单收小单资金      Sql = "select sum(Volume * Price)/10000 From [" & f & "] a where BuyOrderVolume * BuyOrderPrice<=" & bigdeal & _
            # " and SaleOrderVolume * SaleOrderPrice > " & bigdeal
            bigsmall_sell_150 = ddf.loc[(((ddf.BuyOrderVolume * ddf.BuyOrderPrice) <= 1500000) & (
                    (ddf.SaleOrderVolume * ddf.SaleOrderPrice) > 1500000))|
                             (((ddf.BuyOrderVolume * ddf.BuyOrderPrice) >= 1500000) & ((
                                         ddf.SaleOrderVolume * ddf.SaleOrderPrice) >= 1500000) &
                             (ddf.BuyOrderVolume <= ddf.SaleOrderVolume))]
            #        if bigsmall_sell.isnull==True:
            if bigsmall_sell_150.empty == False:
                bigsmall_sell_150['sum_s'] = bigsmall_sell_150['Volume'] * bigsmall_sell_150['Price']
                bigsmall_sell_150 = bigsmall_sell_150['sum_s'].sum() / 10000
                bigsmall_sell_150 = round(bigsmall_sell_150)
                bigsellsmall_150.append(bigsmall_sell_150)  # 第一张图y方向正轴图
            else:
                bigsellsmall_150.append(0.0)  # 第一张图y方向正轴图
            # ==============================================================

            #200万
            bigsmall_200 = ddf.loc[(((ddf.BuyOrderVolume * ddf.BuyOrderPrice) >= 2000000) & (
                    (ddf.SaleOrderVolume * ddf.SaleOrderPrice) < 2000000))|
                             (((ddf.BuyOrderVolume * ddf.BuyOrderPrice) >= 2000000) & ((ddf.SaleOrderVolume * ddf.SaleOrderPrice) >= 2000000) &
                              (ddf.BuyOrderVolume >=ddf.SaleOrderVolume))]
            if bigsmall_200.empty == False:
                bigsmall_200['sum_s'] = bigsmall_200['Volume'] * bigsmall_200['Price']
                bigsmall_s_200 = bigsmall_200['sum_s'].sum() / 10000
                bigsmall_s_200 = round(bigsmall_s_200)
                bigbuysmall_200.append(bigsmall_s_200)  # 第一张图y方向正轴图
            else:
                bigbuysmall_200.append(0.0)  # 第一张图y方向正轴图
            # 卖单收小单资金      Sql = "select sum(Volume * Price)/10000 From [" & f & "] a where BuyOrderVolume * BuyOrderPrice<=" & bigdeal & _
            # " and SaleOrderVolume * SaleOrderPrice > " & bigdeal
            bigsmall_sell_200 = ddf.loc[(((ddf.BuyOrderVolume * ddf.BuyOrderPrice) <= 2000000) & (
                    (ddf.SaleOrderVolume * ddf.SaleOrderPrice) > 2000000))|
                             (((ddf.BuyOrderVolume * ddf.BuyOrderPrice) >= 2000000) & ((
                                         ddf.SaleOrderVolume * ddf.SaleOrderPrice) >= 2000000) &
                             (ddf.BuyOrderVolume <= ddf.SaleOrderVolume))]
            #        if bigsmall_sell.isnull==True:
            if bigsmall_sell_200.empty == False:
                bigsmall_sell_200['sum_s'] = bigsmall_sell_200['Volume'] * bigsmall_sell_200['Price']
                bigsmall_sell_200 = bigsmall_sell_200['sum_s'].sum() / 10000
                bigsmall_sell_200 = round(bigsmall_sell_200)
                bigsellsmall_200.append(bigsmall_sell_200)  # 第一张图y方向正轴图
            else:
                bigsellsmall_200.append(0.0)  # 第一张图y方向正轴图
            # ==============================================================

            #####早盘
            ddf1 = ddf.loc[ddf.Time <= '09:30:00']
            # 19万==========================================       #第一张图y方向正轴图
            z_1 = ddf1.loc[((ddf1.BuyOrderVolume * ddf1.BuyOrderPrice) >= bigDanAmount) & (
                    (ddf1.SaleOrderVolume * ddf1.SaleOrderPrice) < bigDanAmount)|
                             (((ddf.BuyOrderVolume * ddf.BuyOrderPrice) >= bigDanAmount) & ((ddf.SaleOrderVolume * ddf.SaleOrderPrice) >= bigDanAmount) &
                              (ddf.BuyOrderVolume >=ddf.SaleOrderVolume))]
            if z_1.empty == False:
                z_1['sum_s'] = (z_1['Volume'] * z_1['Price'])
                z_s_1 = z_1['sum_s'].sum() / 10000
                z_s_1 = round(z_s_1)
                zb_19.append(z_s_1)  # 第一张图y方向正轴图
            else:
                zb_19.append(0.0)  # 第一张图y方向正轴图
            # 卖单收小单资金      Sql = "select sum(Volume * Price)/10000 From [" & f & "] a where BuyOrderVolume * BuyOrderPrice<=" & bigdeal & _
            # " and SaleOrderVolume * SaleOrderPrice > " & bigdeal
            z_sell_1 = ddf1.loc[(((ddf1.BuyOrderVolume * ddf1.BuyOrderPrice) <= bigDanAmount) & (
                    (ddf1.SaleOrderVolume * ddf1.SaleOrderPrice) > bigDanAmount))|
                             (((ddf.BuyOrderVolume * ddf.BuyOrderPrice) >= bigDanAmount) & ((
                                         ddf.SaleOrderVolume * ddf.SaleOrderPrice) >= bigDanAmount) &
                             (ddf.BuyOrderVolume <= ddf.SaleOrderVolume))]
            #        if z_sell.isnull==True:
            if z_sell_1.empty == False:
                z_sell_1['sum_s'] = z_sell_1['Volume'] * z_sell_1['Price']
                z_sell_1 = z_sell_1['sum_s'].sum() / 10000
                z_sell_1 = round(z_sell_1)
                zs_19.append(z_sell_1)  # 第一张图y方向正轴图
            else:
                zs_19.append(0.0)  # 第一张图y方向正轴图
            # ==============================================================
            # 50万
            z_50 = ddf1.loc[(((ddf1.BuyOrderVolume * ddf1.BuyOrderPrice) >= 500000) & (
                    (ddf1.SaleOrderVolume * ddf1.SaleOrderPrice) < 500000))|
                             (((ddf.BuyOrderVolume * ddf.BuyOrderPrice) >= 500000) & ((ddf.SaleOrderVolume * ddf.SaleOrderPrice) >= 500000) &
                              (ddf.BuyOrderVolume >=ddf.SaleOrderVolume))]
            if z_50.empty == False:
                z_50['sum_s'] = z_50['Volume'] * z_50['Price']
                z_s_50 = z_50['sum_s'].sum() / 10000
                z_s_50 = round(z_s_50)
                zb_50.append(z_s_50)  # 第一张图y方向正轴图
            else:
                zb_50.append(0.0)  # 第一张图y方向正轴图
            # 卖单收小单资金      Sql = "select sum(Volume * Price)/10000 From [" & f & "] a where BuyOrderVolume * BuyOrderPrice<=" & bigdeal & _
            # " and SaleOrderVolume * SaleOrderPrice > " & bigdeal
            z_sell_50 = ddf1.loc[(((ddf1.BuyOrderVolume * ddf1.BuyOrderPrice) <= 500000) & (
                    (ddf1.SaleOrderVolume * ddf1.SaleOrderPrice) > 500000))|
                             (((ddf.BuyOrderVolume * ddf.BuyOrderPrice) >= 500000) & ((
                                         ddf.SaleOrderVolume * ddf.SaleOrderPrice) >= 500000) &
                             (ddf.BuyOrderVolume <= ddf.SaleOrderVolume))]
            #        if z_sell.isnull==True:
            if z_sell_50.empty == False:
                z_sell_50['sum_s'] = z_sell_50['Volume'] * z_sell_50['Price']
                z_sell_50 = z_sell_50['sum_s'].sum() / 10000
                z_sell_50 = round(z_sell_50)
                zs_50.append(z_sell_50)  # 第一张图y方向正轴图
            else:
                zs_50.append(0.0)  # 第一张图y方向正轴图
            # 负轴均价相关的图========================================================第一张图y方向负轴图

            # 100万
            z_100 = ddf1.loc[(((ddf1.BuyOrderVolume * ddf1.BuyOrderPrice) >= 1000000) & (
                    (ddf1.SaleOrderVolume * ddf1.SaleOrderPrice) < 1000000))|
                             (((ddf.BuyOrderVolume * ddf.BuyOrderPrice) >= 1000000) & ((ddf.SaleOrderVolume * ddf.SaleOrderPrice) >= 1000000) &
                              (ddf.BuyOrderVolume >=ddf.SaleOrderVolume))]
            if z_100.empty == False:
                z_100['sum_s'] = z_100['Volume'] * z_100['Price']
                z_s_100 = z_100['sum_s'].sum() / 10000
                z_s_100 = round(z_s_100)
                zb_100.append(z_s_100)  # 第一张图y方向正轴图
            else:
                zb_100.append(0.0)  # 第一张图y方向正轴图
            # 卖单收小单资金      Sql = "select sum(Volume * Price)/10000 From [" & f & "] a where BuyOrderVolume * BuyOrderPrice<=" & bigdeal & _
            # " and SaleOrderVolume * SaleOrderPrice > " & bigdeal
            z_sell_100 = ddf1.loc[(((ddf1.BuyOrderVolume * ddf1.BuyOrderPrice) <= 1000000) & (
                    (ddf1.SaleOrderVolume * ddf1.SaleOrderPrice) > 1000000))|
                             (((ddf.BuyOrderVolume * ddf.BuyOrderPrice) >= 1000000) & ((
                                         ddf.SaleOrderVolume * ddf.SaleOrderPrice) >= 1000000) &
                             (ddf.BuyOrderVolume <= ddf.SaleOrderVolume))]
            #        if z_sell.isnull==True:
            if z_sell_100.empty == False:
                z_sell_100['sum_s'] = z_sell_100['Volume'] * z_sell_100['Price']
                z_sell_100 = z_sell_100['sum_s'].sum() / 10000
                z_sell_100 = round(z_sell_100)
                zs_100.append(z_sell_100)  # 第一张图y方向正轴图
            else:
                zs_100.append(0.0)  # 第一张图y方向正轴图
            # ==============================================================


            # 200万
            z_200 = ddf1.loc[(((ddf1.BuyOrderVolume * ddf1.BuyOrderPrice) >= 2000000) & (
                    (ddf1.SaleOrderVolume * ddf1.SaleOrderPrice) < 2000000))|
                             (((ddf.BuyOrderVolume * ddf.BuyOrderPrice) >= 2000000) & ((ddf.SaleOrderVolume * ddf.SaleOrderPrice) >= 2000000) &
                              (ddf.BuyOrderVolume >=ddf.SaleOrderVolume))]
            if z_200.empty == False:
                z_200['sum_s'] = z_200['Volume'] * z_200['Price']
                z_s_200 = z_200['sum_s'].sum() / 10000
                z_s_200 = round(z_s_200)
                zb_200.append(z_s_200)  # 第一张图y方向正轴图
            else:
                zb_200.append(0.0)  # 第一张图y方向正轴图
            # 卖单收小单资金      Sql = "select sum(Volume * Price)/10000 From [" & f & "] a where BuyOrderVolume * BuyOrderPrice<=" & bigdeal & _
            # " and SaleOrderVolume * SaleOrderPrice > " & bigdeal
            z_sell_200 = ddf1.loc[((ddf1.BuyOrderVolume * ddf1.BuyOrderPrice) <= 2000000) & (
                    (ddf1.SaleOrderVolume * ddf1.SaleOrderPrice) > 2000000)|
                             (((ddf.BuyOrderVolume * ddf.BuyOrderPrice) >= 2000000) & ((
                                         ddf.SaleOrderVolume * ddf.SaleOrderPrice) >= 2000000) &
                             (ddf.BuyOrderVolume <= ddf.SaleOrderVolume))]
            #        if z_sell.isnull==True:
            if z_sell_200.empty == False:
                z_sell_200['sum_s'] = z_sell_200['Volume'] * z_sell_200['Price']
                z_sell_200 = z_sell_200['sum_s'].sum() / 10000
                z_sell_200 = round(z_sell_200)
                zs_200.append(z_sell_200)  # 第一张图y方向正轴图
            else:
                zs_200.append(0.0)  # 第一张图y方向正轴图
            # ==============================================================
            
            if stock_code[0] == '6':
                ts_code=stock_code+'.SH'
            if stock_code[0] == '3':
                ts_code=stock_code+'.SZ'
            if stock_code[0] == '0':
                ts_code=stock_code+'.SZ'
            if stock_code[0] == '4':
                ts_code=stock_code+'.BJ'
            if stock_code[0] == '8':
                ts_code=stock_code+'.BJ'
            stock_list.append(ts_code)
            excel_date_list.append(path[-21:-17] + path[-16:-14] + path[-13:-11])

            print('path:',path[-21:-17] + path[-16:-14] + path[-13:-11])
        except Exception as e:
            print(e)
            print('eeeeeepath',path)
            excel_date_list.append(path[-21:-17] + path[-16:-14] + path[-13:-11])
            stock_list.append(ts_code)
            bigbuysmall_1.append(0.0)
            bigsellsmall_1.append(0.0)
            bigbuysmall_50.append(0.0)
            bigsellsmall_50.append(0.0)
            bigbuysmall_100.append(0.0)
            bigsellsmall_100.append(0.0)
            bigbuysmall_150.append(0.0)
            bigsellsmall_150.append(0.0)
            bigbuysmall_200.append(0.0)
            bigsellsmall_200.append(0.0)
            zb_19.append(0.0)
            zb_50.append(0.0)
            zb_100.append(0.0)
            zb_200.append(0.0)
            zs_19.append(0.0)
            zs_50.append(0.0)
            zs_100.append(0.0)
            zs_200.append(0.0)
            buy_l.append(0.0)
            sell_l.append(0.0)
            b50_l.append(0.0)
            s50_l.append(0.0)
            b100_l.append(0.0)
            s100_l.append(0.0)
            # dadan_cnt100_l.append(0.0)
            # dadan_cnt17_l.append(0.0)
            # dadan_cnt34_l.append(0.0)
            pass
            continue
    # print('44444 ok')
    c = {'trade_date': excel_date_list,
         'ts_code': stock_list,
         '大单买小单卖1': bigbuysmall_1,  # 第一张图y方向正轴图
         '大单卖小单买1': bigsellsmall_1, # 第一张图y方向正轴图
         'bbuys50':bigbuysmall_50,
         'bsells50':bigsellsmall_50,
         'bbuys100': bigbuysmall_100,
         'bsells100': bigsellsmall_100,
         'bbuys150': bigbuysmall_150,
         'bsells150': bigsellsmall_150,
         'bbuys200': bigbuysmall_200,
         'bsells200': bigsellsmall_200,

         'zb19': zb_19,
         'zs19': zs_19,
         'zb50': zb_50,
         'zs50': zs_50,
         'zb100': zb_100,
         'zs100': zs_100,
         'zb200': zb_200,
         'zs200': zs_200,
         'buy':buy_l,
         'sell':sell_l,
         'b50':b50_l,
         's50':s50_l,
         'b100':b100_l,
         's100':s100_l,
         # 'dadan_cnt100':dadan_cnt100_l,
         # 'dadan_cnt17':dadan_cnt17_l,
         # 'dadan_cnt34':dadan_cnt34_l
         }

    try:
        # print('55555 ok')
        data_n = pd.DataFrame(c)
        # print('66666 ok')
    except Exception as e:
        print (e)

        print('trade_date', len(excel_date_list))
        print('stock_list', len(stock_list))
        print('bigbuysmall_1', len(bigbuysmall_1))
        print('bigsellsmall_1', len(bigsellsmall_1))
        print('bigbuysmall_50', len(bigbuysmall_50))
        print('bigsellsmall_50', len(bigsellsmall_50))
        print('bigbuysmall_100', len(bigbuysmall_100))
        print('bigsellsmall_100', len(bigsellsmall_100))
        print('bigbuysmall_150', len(bigbuysmall_150))
        print('bigsellsmall_150', len(bigsellsmall_150))
        print('bigbuysmall_200', len(bigbuysmall_200))
        print('bigsellsmall_200', len(bigsellsmall_200))
        # print('bigsellsmall_200', len(bigsellsmall_200))
        print('zb_19', len(zb_19))
        print('zb_50', len(zb_50))
        print('zb_100', len(zb_100))
        print('zb_200', len(zb_200))
        print('zs_19', len(zs_19))
        print('zs_50', len(zs_50))
        print('zs_100', len(zs_100))
        print('zs_200', len(zs_200))
    #    q.put(data_n)
    return data_n



def get_csv_stock():
    global df_x,df_cal_g
    rootdir = var_path.get() + '\\' + 'data.xlsx'

    sheet = pd.read_excel(rootdir, sheet_name=0)
    sheet = sheet[['ts_code', 'bizhi', 'pct_chg']]
    if df_x.empty == False:
        df_x.drop(df_x.index, inplace=True)  # 清空dataframe
    df_x = sheet
    textPad.delete(1.0, tk.END)  # 使用 delete
    textPad.insert(tk.END, str(df_x))
    textPad.place(x=0, y=60, anchor='nw')


def save2sql_and_excel(data):
    global df_cal_g
    date_list = []  # list
    output_dir1 = var_path.get() + '\\' + 'data_big.xlsx'

    ##################日线与大单数据拼接 列扩展行数不变
    print("get dfdata, start PinJie_data!")
    data.rename(columns={"日期": "trade_date", "大单买小单卖1": "bbuys1", "大单卖小单买1": "bsells1", "大单买小单卖": "bbuys2",
                         "大单卖小单买": "bsells2", }, inplace=True)
    print("data:",data.head(3))
    print("df_cal_g:",df_cal_g.tail(3))
    data2 = pd.merge(data, df_cal_g, how='right')
    #    print('data2:::__::',data2)
    print("End PinJie_data!")
    data_date = data2.drop_duplicates(subset=['trade_date'], keep='first',
                                      inplace=False)  # 去重，去除日期列数值相同的重复行，keep保留第一行，inplace=true在原data上删除 false生成一个副本
    #    print('data_date',data_date)
    date_list = data_date['trade_date'].to_list()
    print('date_list:::::++++',date_list)

    ###################################
    # 这里一定要写成mysql+pymysql，不要写成mysql+mysqldb
    engine = create_engine(DB_CONNECT)
    engine1 = create_engine(DB_CONNECT1)
    #    df = pd.read_excel('a.xls')
    #    df = pd.DataFrame()
    for day in date_list:
        data_temp = data2[data2['trade_date'] == day]
        #        print("data_temp:_____",data_temp)

        try:
            data_temp.to_sql(name="t" + day, con=engine, if_exists='replace', index=False, index_label=False)
            print(day + "to_sql ok +++++++:")
        except Exception as e:
            print(day + "to_sql jump +++++++:")
            pass
        try:
            data_temp.to_sql(name="t" + day, con=engine1, if_exists='replace', index=False, index_label=False)
            print(day + "to_sql ok222 +++++++:")
        except Exception as e:
            print(day + "to_sql jump222 +++++++:")
            pass

        continue
    return
##########################

def temp_test():
    token = ts_token
    ts.set_token(token)
    pro = ts.pro_api()

    engine = create_engine(DB_CONNECT)
    engine1 = create_engine(DB_CONNECT1)
    begin_date = var_begin_date.get()
    end_date = var_end_date.get()

    sqlTables = date2sqlTable(begin_date, end_date)

    #test
    adj_df_tset = pro.adj_factor(ts_code='', trade_date='20220610')  # 日线复权数据
    adj_df_tset_1= adj_df_tset.loc[adj_df_tset.ts_code  == '000002.SZ']
    # test


    #日线数据
    day_data_new = pd.DataFrame()

    m = 0
    # 循环获取单个股票的日线行情
    frist_time = 1
    for date in sqlTables:
        sql = "select * from " + date
        day_df = query_data1(sql)
        adj_df = pro.adj_factor(ts_code='', trade_date = date[1:] )  #日线复权数据
        adj_df1 = adj_df[['ts_code', 'adj_factor', ]]
        adj_df1 = adj_df1.rename(columns={'ts_code': 'ts_code1',})

        day_df_res = pd.merge(day_df, adj_df1, how='inner', left_on='ts_code', right_on='ts_code1', sort=False)
        day_df_res.drop('ts_code1', axis='columns')  # same
        if frist_time ==1:
            day_data_new = day_df_res.copy()
        else:
            day_data_new.append(day_df_res)
        time.sleep(0.8)

        try:
         day_data_new.to_sql(name=date, con=engine, if_exists='replace', index=False, index_label=False)
         print("day_data_new to_sql ok +++++++:",date)
        except Exception as e:
         print("day_data_new to_sql jump +++++++:",date)

        try:
         day_data_new.to_sql(name=date, con=engine1, if_exists='replace', index=False, index_label=False)
         print("day_data_new to_sql ok222 +++++++:",date)
        except Exception as e:
         print("day_data_new to_sql jump222 +++++++:",date)

    print("day_data_new  ++++++ end end+++++++:", )
    return


def temp_test2():

    bankuai_df_new = pd.DataFrame(
        {"tscode": ['1'], "hangye": ['1'], "zhuti": ['1'], })
    if bankuai_df_new.empty == False:
        bankuai_df_new.drop(bankuai_df_new.index, inplace=True)  # 清空dataframe

    rootdir =  'D:\\python\\data\\板块.xlsx'
    bankuai_df = pd.read_excel(rootdir, sheet_name=0)
    # bankuai_df['tscode'] = bankuai_df['tscode_t'][0:6]
    # print('bankuai_df',bankuai_df.head(20))
    # time.sleep(20)
    bankuai_df_new = pd.DataFrame()
    for index,row in bankuai_df.iterrows():
        zhuti_row = row['所属主题']
        tscode = row['tscode_t'][0:6]
        if tscode[0] == '6':
            ts_code = tscode+'.SH'
        elif tscode[0] == '8':
            ts_code = tscode + '.BJ'
        else:
            ts_code = tscode + '.SZ'
        hangye = row['行业']
        begin = 0
        n = 0
        zhuti = ''
        for char in zhuti_row:
            if char == ',':
                zhuti = zhuti_row[begin:n]
                begin = n+1
                new_row = {'ts_code': [ts_code],
                             'hangye': [hangye],
                             'zhuti': [zhuti],
                           }
                data_n = pd.DataFrame(new_row)
                # print('data_n',data_n)
                bankuai_df_new = bankuai_df_new.append(data_n)
            n = n+1

    print((bankuai_df_new.head(100)))
    try:
        engine = create_engine(DB_CONNECT)
        bankuai_df_new.to_sql(name="bankuai", con=engine, if_exists='replace', index=False, index_label=False)
        print("bankuai_df_new to_sql ok +++++++:")
    except Exception as e:
        print("bankuai_df_new to_sql jump +++++++:")
        pass
    try:
        engine1 = create_engine(DB_CONNECT1)
        bankuai_df_new.to_sql(name="bankuai", con=engine1, if_exists='replace', index=False, index_label=False)
        print("bankuai_df_new to_sql ok222 +++++++:")
    except Exception as e:
        print("bankuai_df_new to_sql jump222 +++++++:")
        pass
    return

def temp_test3():
    sql = "select tscode,hangye,zhuti  from bankuai  "  ###+ " where ts_code =" + "'" + ts_code + "'"
    data_df = query_data1(sql)
    # data_df.groupby('zhuti').apply(bankuai_gro)
    return

#################################
def temp_test4():
    token = ts_token
    ts.set_token(token)
    pro = ts.pro_api()

    engine = create_engine(DB_CONNECT)

    begin_date = var_begin_date.get()
    end_date = var_end_date.get()

    sqlTables = date2sqlTable(begin_date, end_date)




    #日线数据
    day_data_new = pd.DataFrame()

    m = 0
    # 循环获取单个股票的日线行情
    frist_time = 1
    for date in sqlTables:
        sql = "select * from " + date
        day_df = query_data1(sql)



        adj_df = pro.adj_factor(ts_code='', trade_date = date[1:] )  #日线复权数据
        adj_df1 = adj_df[['ts_code', 'adj_factor', ]]
        adj_df1 = adj_df1.rename(columns={'ts_code': 'ts_code1',})

        day_df_res = pd.merge(day_df, adj_df1, how='inner', left_on='ts_code', right_on='ts_code1', sort=False)
        day_df_res.drop('ts_code1', axis='columns')  # same
        if frist_time ==1:
            day_data_new = day_df_res.copy()
        else:
            day_data_new.append(day_df_res)
        time.sleep(0.8)

        try:
         day_data_new.to_sql(name=date, con=engine, if_exists='replace', index=False, index_label=False)
         print("day_data_new to_sql ok +++++++:",date)
        except Exception as e:
         print("day_data_new to_sql jump +++++++:",date)

    print("day_data_new  ++++++ end end+++++++:", )
    return

#############################

#取月线
def get_month_stock():
    token = ts_token
    ts.set_token(token)
    pro = ts.pro_api()
    sql = "select cal_date from cal_date c where c.cal_date>20220201"    ###+ " where ts_code =" + "'" + ts_code + "'"
    date_df = query_data1(sql)
    date_list = date_df.cal_date.tolist()
    month_date_list=[]
    old_date = '20220201'
    engine = create_engine(DB_CONNECT)

    for date in date_list:
#        if date[0:4] != old_date[0:4]:  #前4位不相等 从2015开始

        if date[0:6] != old_date[0:6] : #前6位不相等
            date_index = date_list.index(date)
            month_date_list.append(date_list[date_index - 1])
            old_date = date
    #print(month_date_list)

    c = {"month_cal": month_date_list}  # 将列表month_date_list转换成字典
    month_df = DataFrame(c)  # 将字典转换成为数据框
    try:
        month_df.to_sql(name="month_cal", con=engine, if_exists='replace', index=False, index_label=False)
        print("month_cal to_sql ok +++++++:")
    except Exception as e:
        print("month_cal to_sql jump +++++++:")
        pass

    if 1:
        dt = datetime.datetime.now()  # 创建一个datetime类对象
        today = dt.strftime('%Y%m%d')
        now_adj = pro.query('adj_factor', trade_date=lastday)
        now_adj1 = now_adj[['ts_code','adj_factor']]
        for m_date in month_date_list:
            mon_df = pro.monthly(trade_date=m_date, fields='ts_code,trade_date,open,high,low,close,vol,amount,pct_chg,pre_close,change')
            mon_fq_df = pro.query('adj_factor', trade_date=m_date)
            mon_z_df = pd.merge(mon_df, mon_fq_df, how='inner', left_on='ts_code',
                                     right_on='ts_code', sort=False)
            mon_z_df1 = pd.merge(mon_z_df, now_adj1, how='inner', left_on='ts_code',
                                right_on='ts_code', sort=False)
            mon_z_df1['adj_ok'] = mon_z_df1['adj_factor_x']/mon_z_df1['adj_factor_y']
            mon_z_df2 = mon_z_df1[['ts_code','trade_date_x','close','open','high','low','pre_close','change','pct_chg','vol','amount','adj_ok']]
            mon_z_df2.rename(columns={"trade_date_x": "trade_date"}, inplace=True)
            try:
                mon_z_df2.to_sql(name="m" + m_date[0:6], con=engine, if_exists='replace', index=False, index_label=False)
                #       data_temp.to_sql(name = "t"+day,con = engine,if_exists = 'fail',index = False,index_label = False)
                print(m_date[0:6] + "to_sql ok +++++++:")
            except Exception as e:

                print(m_date[0:6] + "to_sql jump +++++++:")
                pass
            continue
    return

def get_basic():
    token = ts_token
    ts.set_token(token)
    pro = ts.pro_api()
    data = pro.query('stock_basic', exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,market,list_status,list_date')
    engine = create_engine(DB_CONNECT)
    engine1 = create_engine(DB_CONNECT1)

    try:
     data.to_sql(name="stock_basic", con=engine, if_exists='replace', index=False, index_label=False)
     print("stock_basic to_sql ok +++++++:")
    except Exception as e:
     print("stock_basic to_sql jump +++++++:")

    try:
     data.to_sql(name="stock_basic", con=engine1, if_exists='replace', index=False, index_label=False)
     print("stock_basic to_sql ok222 +++++++:")
    except Exception as e:
     print("stock_basic to_sql jump222 +++++++:")

  #  tscode = var_stock.get()
  #  data = pro.adj_factor(ts_code= tscode, trade_date='')
  #  data.to_excel('C:\\Users\\txm\\Desktop\\adj_factor.xlsx')
    # begin_date = var_begin_date.get()
    # end_date = var_end_date.get()
    # tables = date2sqlTable(begin_date,end_date)
    # for table in tables :
    #     insert_or_update_data('alter table ' + table + ' add column adj_factor double not null default 1')
    #     table_df = query_data1('select ts_code from ' + table )
    #     trade_date = table[1:]
    #     factor_df = pro.adj_factor(ts_code='', trade_date=trade_date)
    #     table_merge = pd.merge( table_df,factor_df,on='ts_code',left_index=True )
    #     for row in table_merge.iterrows() :
    #         sql_tmp = 'update ' + table + ' set adj_factor=%f ' % (float( row[1].adj_factor ))   + ' where ts_code=\'' + row[1].ts_code +'\''
    #         insert_or_update_data( sql_tmp )
        # factor_df.to_excel('C:\\Users\\txm\\Desktop\\' + table + '.xlsx')
    return
def test() :
    # token = ts_token
    # ts.set_token(token)
    # pro = ts.pro_api()
    tscode = var_stock.get()
    begin_date = var_begin_date.get()
    # end_date = var_end_date.get()
    # # print(tscode,begin_date,end_date)
    # df_adj = pro.adj_factor(ts_code=tscode, start_date=begin_date, end_date=end_date)
    # # print(df_adj)
    # if df_adj.empty == False:
    #     adj_factor_buyday = df_adj.iloc[0, 2]
    #     adj_factor_sellday = df_adj.iloc[-1, 2]
    #     print(adj_factor_buyday,adj_factor_sellday)

    rootdir = var_path.get()
    # stock_path_list = date2path_stock_new(begin_date,begin_date, rootdir, tscode)
    # # print('~~~',stock_path_list)
    # real_stock_path = stock_path_list[0]
    # try:
    #     # csv_df = pd.read_csv(real_stock_path)
    # except:
    #     print('卖出任务执行时读实时交易文件失败')
    #     return

    # csv_df['chazhi19'] = 0
    # csv_df['chazhi100'] = 0
    # csv_df['sum14'] = 0
    # for stock in csv_df.iterrows():
    #     csv_df_tmp = csv_df.loc[:stock[0]]
    #     dadan_seri = get_real_data(csv_df_tmp)
    #     if dadan_seri.empty == False:
    #         csv_df.loc[stock[0],'chazhi19'] = dadan_seri['chazhi19'] / 10000
    #         csv_df.loc[stock[0],'chazhi100'] = dadan_seri['chazhi100'] / 10000
    #         csv_df.loc[stock[0],'sum14'] += dadan_seri['chazhi19'] / 10000
    # csv_df.to_excel('C:\\Users\\txm\\Desktop\\sellp\\' + tscode +'(' + begin_date + ')' + '.xlsx')
    return


def get_chicang():
    begin_date = var_begin_date.get()
    end_date = var_end_date.get()
    ts_code = var_stock.get()
    sqlTables = date2sqlTable(begin_date,end_date)
    # print(sqlTables)
    datasN_all= pd.DataFrame()

    for tableName in sqlTables:
        sql = "select trade_date,ts_code,bbuys100,bsells100,bbuys150,bsells150,bbuys200,bsells200,open,close from " + tableName + " where ts_code = " + ts_code
        # sql = "select trade_date,ts_code,bbuys1,bsells1,close from " + tableName + " where ts_code = " + ts_code
        datasN = query_data1(sql)
        if len(datasN) > 0 :
            datasN.fillna(0,inplace=True)
            # datasN.loc[0,'chazhi19'] = datasN.loc[0,'bbuys1']- datasN.loc[0,'bsells1']
            # datasN.loc[0,'chazhi50'] = datasN.loc[0,'bbuys50']- datasN.loc[0,'bsells50']
            datasN.loc[0,'chazhi100'] = datasN.loc[0,'bbuys100']- datasN.loc[0,'bsells100']
            datasN.loc[0,'chazhi200'] = datasN.loc[0,'bbuys200']- datasN.loc[0,'bsells200']
            # datasN.loc[0,'sum19'] = datasN.loc[0,'chazhi19']
            datasN.loc[0,'sum100'] = datasN.loc[0,'chazhi100']
            datasN.loc[0,'sum200'] = datasN.loc[0,'chazhi200']
            datasN.loc[0,'mean100'] = 0
            datasN.loc[0, 'min_price'] = 0
            datasN.loc[0, 'price_rate'] = 0
            datasN.loc[0, 'price_rt0'] = 0
            datasN.loc[0, 'price_rt'] = 0
            datasN.loc[0, 'buy_sell'] = 0
            datasN.loc[0, 'rate_100_b'] = 0
            datasN.loc[0, 'rate_200_b'] = 0
            datasN.loc[0, 'rate_100_s'] = 0
            datasN.loc[0, 'rate_200_s'] = 0
            # if (datasN.loc[0,'chazhi200'] < -400 ):
                # datasN.loc[0, 'can_sell'] = datasN.loc[0,'chazhi200']
            datasN_all = datasN_all.append(datasN,ignore_index=True)
            ct = datasN_all.shape[0]
            # 前5天的200万大单累计
            # if ct< 5 :
            #     datas_5 = datasN_all
            # else :
            #     row_beg_0 = ct - 5
            #     datas_5 =  datasN_all.iloc[row_beg_0:ct]
            # datasN_all.loc[ct - 1,'sum200'] = datas_5['chazhi200'].sum()

            # 大单前10天累计值
            if ct< 10 :
                datas_10 = datasN_all
            else :
                row_beg = ct - 10
                datas_10 =  datasN_all.iloc[row_beg:ct]
            # 19大单前14天累计值
            if ct< 20 :
                datas_40 = datasN_all
            else :
                row_beg = ct - 20
                datas_40 =  datasN_all.iloc[row_beg:ct]
            # 100大单前80天绝对平均值
            if ct < 5 :
                datas_abs_100_5 = datasN_all
            else :
                datas_abs_100_5 = datasN_all.iloc[ ct-5 :]            # 100大单前5天绝对平均值
            if ct < 30 :
                datas_abs_100 = datasN_all
            else :
                datas_abs_100 = datasN_all.iloc[ ct-30 :]
                # row_beg = ct - 14
                # datas_40 = datasN_all.iloc[row_beg:ct]
            abs_100 = datas_abs_100['chazhi100'].abs()
            abs_100_5 = datas_abs_100_5['chazhi100'].abs()
            datasN_all.loc[ct - 1,'min_price'] = datas_10['close'].min()
            datasN_all.loc[ct - 1,'sum100'] = datas_40['chazhi100'].sum()
            datasN_all.loc[ct - 1,'sum200'] = datas_40['chazhi200'].sum()
            datasN_all.loc[ct - 1,'mean100'] = round( abs_100.mean(),2)
            if ct > 1 :
                d = datasN_all.loc[ct - 1,'trade_date']
                last_open = datasN_all.loc[ct - 2,'open']
                open = datasN_all.loc[ct - 1,'open']
                last_close = datasN_all.loc[ct - 2,'close']
                close = datasN_all.loc[ct - 1,'close']
                min_price = datasN_all.loc[ct - 1,'min_price']
                price_rate = 0
                if ( min_price !=0 ) :
                    price_rate =  round((close - min_price) / min_price, 3)
                price_rt0 = 0
                if (open != 0):
                    price_rt0 = (close - open) / open
                price_rt = 0
                if (last_close != 0):
                    price_rt = abs((close - last_close) / last_close)
                sum_100 = datasN_all.loc[ct - 1,'sum100']
                sum_200 = datasN_all.loc[ct - 1,'sum200']
                last_sum_100 = datasN_all.loc[ct - 2,'sum100']
                last_sum_200 = datasN_all.loc[ct - 2,'sum200']
                chazhi_100 = datasN_all.loc[ct - 1,'chazhi100']
                chazhi_200 = datasN_all.loc[ct - 1,'chazhi200']
                mean_100 = datasN_all.loc[ct - 1,'mean100']
                rate_sum100_buy = 0
                rate_sum100_sell = 0
                rate_sum200_buy = 0
                rate_sum200_sell = 0
                if   last_sum_100 != 0 :
                    rate_sum100_buy =   (  sum_100 - last_sum_100)  / last_sum_100
                    rate_sum100_sell =  (last_sum_100 -  sum_100)  / last_sum_100
                if   last_sum_200 != 0 :
                    rate_sum200_buy =  (  sum_200 - last_sum_200) /  last_sum_200
                    rate_sum200_sell =  (last_sum_200 - sum_200)  /  last_sum_200
                # 买点临界点
                can_buy = (mean_100 > 1000) and ( close >= open )   and (
                        ((chazhi_100 > 3000) and (chazhi_200 > -1) and (sum_100 > 0) and (sum_200 > 0) and (
                                rate_sum100_buy > 0) and (rate_sum100_buy < 15)) and (rate_sum200_buy > 0) and ( rate_sum200_buy < 15 )
                        or ((chazhi_200 >2000) and (chazhi_100 > 0)  and (sum_200 > 0) and (
                        rate_sum200_buy > 0) and (rate_sum200_buy < 15) and (rate_sum100_buy > 0) and (
                                    rate_sum100_buy < 15)))
                # 卖点临界点
                can_sell = (mean_100 > 1000) and (
                        ((chazhi_100 < -1000)     and (rate_sum100_sell > 0))
                        or ((chazhi_200 < -800)  and (rate_sum200_sell > 0)))
                datasN_all.loc[ct - 1, 'price_rate'] = price_rate
                datasN_all.loc[ct - 1, 'price_rt0'] = price_rt0
                datasN_all.loc[ct - 1, 'price_rt'] = price_rt
                datasN_all.loc[ct - 1, 'rate_100_b'] = round ( rate_sum100_buy , 5)
                datasN_all.loc[ct - 1, 'rate_200_b'] = round ( rate_sum200_buy , 5)
                datasN_all.loc[ct - 1, 'rate_100_s'] = round ( rate_sum100_sell , 5)
                datasN_all.loc[ct - 1, 'rate_200_s'] = round ( rate_sum200_sell , 5)
                if  ( can_buy ) :
                    datasN_all.loc[ct - 1, 'buy_sell'] = 666
                if  ( can_sell ) :
                    datasN_all.loc[ct - 1, 'buy_sell'] = 888

            # if (ct > 2) and  ( datasN_all.loc[ct - 1,'chazhi200'] < -300 ) :
            #     last_sum = datasN_all.loc[ct - 2,'sum19']
            #     sell_rate =  ( datasN_all.loc[ct - 1,'chazhi19'] - last_sum ) / datasN_all.loc[ct - 1,'chazhi200']
            #     datasN_all.loc[ct - 1, 'sell_rate'] = round( sell_rate,2)
                # print(last_sum, datasN_all.loc[ct - 1,'chazhi19'],datasN_all.loc[ct - 1,'chazhi100'],sell_rate)

    datasN_all.to_excel('C:\\dadan\\test' +ts_code + '.xlsx')
    return datasN_all


#def query_data1(sql_cmd):
#    try:
#        engine = create_engine(DB_CONNECT)
#        # sql 命令
#        df = pd.read_sql(sql=sql_cmd, con=engine)
#        return df
#    finally:
#        pass
#    return


# 计算大盘的均线值等  --by txm 2022-03-08   # txm  ma3 ma10
def get_dapan_ma() :        #start_date前12日   end_date昨天 入库时是当日
    begin_date = var_begin_date.get()
    end_date = var_end_date.get()
    begin_date = '20200101'
    token = ts_token
    ts.set_token(token)
    pro = ts.pro_api()

    # 这里一定要写成mysql+pymysql，不要写成mysql+mysqldb
    engine = create_engine(DB_CONNECT)
    engine1 = create_engine(DB_CONNECT1)

    sql = "select * from shangzheng_zhishu where trade_date <= " + end_date
    shzheng_old_df = query_data1(sql)
    if shzheng_old_df.empty == False:
        shzheng_old_df = shzheng_old_df.sort_values('trade_date', ascending=True)  #最下面的是最大的日期
        shzheng_begin_day = shzheng_old_df.iloc[-1,1]
    else:
        shzheng_begin_day = '20200101'
    # print('shzheng_begin_day:',shzheng_begin_day)

    try:
        df_dapan_t = pro.index_daily(ts_code='000001.SH', start_date = shzheng_begin_day,end_date = end_date)

        if df_dapan_t.empty == False:
            # df_dapan_t =  df_dapan_t.sort_values('trade_date')#, ascending=False)  #True
            df_dapan_t = df_dapan_t.sort_values('trade_date', ascending=True)   #最下面的是最大的日期
            # print('df_dapan_t:', df_dapan_t)
            df_dapan = df_dapan_t[1:] #shzheng_begin_day这天的数据不要  否则数据重复一天


    except:
        print('上证指数获取失败')

    ###################################


    # shzheng_new_df = shzheng_old_df.append(df_dapan, ignore_index=True)
    shzheng_new_df = pd.concat([shzheng_old_df, df_dapan], ignore_index=True)
    shzheng_new_df['shzheng_ma_3'] = shzheng_new_df.rolling(3, min_periods=1)['close'].mean()
    shzheng_new_df['shzheng_ma_5'] = shzheng_new_df.rolling(5, min_periods=1)['close'].mean()
    shzheng_new_df['shzheng_ma_10'] = shzheng_new_df.rolling(10, min_periods=1)['close'].mean()  ##???

    # shzheng_new_df = df_dapan
    try:
        shzheng_new_df.to_sql(name='shangzheng_zhishu', con=engine, if_exists='replace', index=False, index_label=False)
        print("shangzheng ma to_sql ok +++++++:")
    except Exception as e:
        print("shangzheng ma to_sql jump +++++++:")
        pass
    try:
        shzheng_new_df.to_sql(name='shangzheng_zhishu', con=engine1, if_exists='replace', index=False, index_label=False)
        print("shangzheng ma to_sql ok222 +++++++:")
    except Exception as e:
        print("shangzheng ma to_sql jump222 +++++++:")
        pass

    return


def query_data1(sql_cmd):
    try:
        engine = create_engine(DB_CONNECT)
        # sql 命令
        # df = pd.read_sql(sql=sql_cmd, con=engine)
        df = pd.read_sql(text(sql_cmd), con=engine.connect())

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

    try:
        engine1 = create_engine(DB_CONNECT1)
        df.to_sql(table_name,con=engine1, schema='stock', index=False, index_label=False, if_exists='replace')
    except Exception as ee:
        print("replace_todb fialed", ee)
    finally:
        engine1.dispose()  #查询后关闭mysql连接
    return




if __name__ == '__main__':


    pd.set_option('display.max_columns', None)
    # 显示所有列
    pd.set_option('display.max_rows', None)

    window = tk.Tk()

    # 第2步，给窗口的可视化起名字
    window.title('111')

    # 第3步，设定窗口的大小(长 * 宽)
    window.geometry('1000x600')  # 这里的乘是小x

    #    list_test=[1,2,3,4,5,6,7,8,9,10,11]
    #    print(list_test[1:3])
    #    print(list_test[4:8])
    #    print(list_test[9:10])

    var_begin_date = tk.StringVar()
    var_begin_date.set('202311')
    entry_begin_date = tk.Entry(window, textvariable=var_begin_date, font=('Arial', 14))
    entry_begin_date.place(x=0, y=0)

    var_end_date = tk.StringVar()
    var_end_date.set('202311')  # 使用注意： 在10倍功能上var_end_date.set('20190821')data最新数据是0821那么要写0822要往后写一天
    entry_end_date = tk.Entry(window, textvariable=var_end_date, font=('Arial', 14))
    entry_end_date.place(x=100, y=0)
    # 第5步，在窗口界面设置放置Button按键
    # b = tk.Button(window, text='计算全部', font=('Arial', 12), width=10, height=1, command=jisuan)
    btn_jisuan = tk.Button(window, text='+j', command=jisuan)
    btn_jisuan.pack()
    btn_jisuan.place(x=450, y=0)
    #   df3 = btn_jisuan.invoke()
 #   btn_up_one = tk.Button(window, text='上一个', command=up_one)
 #   btn_up_one.place(x=400, y=0)
 #   btn_down_one = tk.Button(window, text='下一个', command=down_one)
 #   btn_down_one.place(x=500, y=0)




    btn_10_n = tk.Button(window, text='+k', command=get_10X_stock)
    btn_10_n.pack()
    btn_10_n.place(x=600, y=0)

    btn_read_csv = tk.Button(window, text='temp功能', command=temp_test2)
    btn_read_csv.place(x=680, y=0)

    btn_read_csv = tk.Button(window, text='取月线(先计算全部)', command=get_month_stock)
    btn_read_csv.place(x=780, y=0)

    var_path = tk.StringVar()
    var_path.set('/run/media/kaifa1/data1g')
    entry_path = tk.Entry(window, textvariable=var_path, font=('Arial', 14))
    entry_path.place(x=10, y=30)

    var_stock = tk.StringVar()
    var_stock.set('300550')
    entry_stock = tk.Entry(window, textvariable=var_stock, width=50, font=('Arial', 14))
    entry_stock.place(x=300, y=30)

    btn_chicang = tk.Button(window, text='看大单', command=get_chicang)
    btn_chicang.place(x=400, y=30)

    btn_basic = tk.Button(window, text='+g', command=get_basic)
    btn_basic.place(x=300, y=0)

    btn_basic = tk.Button(window, text='卖点', command=test)
    btn_basic.place(x=600, y=30)

    # 第6步，主窗口循环显示
    # 第4步，在图形界面上创建 500 * 300 大小的画布并放置各种元素


    canvas = tk.Canvas(window, bg='white', height=700, width=1600)
    canvas.place(x=300, y=60)
    # =====================================


    canvas2 = tk.Canvas(window, bg='white', height=400, width=1600)
    canvas2.place(x=300, y=600)


 #   textPad = scrolledtext.ScrolledText(window, width=38, height=65)  # 列表宽度 高度
 #   textPad.insert(tk.END, str(s))
 #   textPad.place(x=0, y=60, anchor='nw')

    window.mainloop()

#    df = pd.DataFrame(df2.rand(n,2),columns=['a','b'])
#    df2.plot.bar(xticks = '日期',figsize=(20,10))
# 使用注意：在10倍功能上  var_end_date.set('20190821')data最新数据是0821那么要写0822要往后写一天
