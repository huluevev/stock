import pandas as pd
import datetime
import warnings

warnings.filterwarnings('ignore')


from pandas.core.frame import DataFrame
import tkinter as tk  # 使用Tkinter前需要先导入
from multiprocessing import Pool
import db


#首先声明此程序仅供学习交流使用，不可商用不可实盘否则后果自负！
#首先声明此程序仅供学习交流使用，不可商用不可实盘否则后果自负！
#首先声明此程序仅供学习交流使用，不可商用不可实盘否则后果自负！

# df = dd.read_csv(...)
# df.x.sum().compute()  # This uses the single-machine scheduler by default
aa = pd.DataFrame({"ts_code": ['600300.SH', '000000', '000001'], "B": [3, 2, 2]})
#['TranID','Time','Price','Volume']
bigbuysmall_detail_h=bigsmall_sell_detail_h=pd.DataFrame({"TranID": ['1'], "date_time": ['1'],"Price":['1'],"Volume":['1']})

len_n = 0
cal_dates = pd.DataFrame({"cal_date": ['20160101']})
#bigbuysmall_detail_lsit = []
#bigsmall_sell_detail_list = []
#g.bigbuysmall_detail_lsit = []
#g.bigsmall_sell_detail_list = []

# 获取从起始日期到截止日期中间的的所有日期，前后都是封闭区间
def get_date_list(begin_date, end_date):
    date_list = []
    while begin_date <= end_date:
        # date_str = str(begin_date)
        date_list.append(begin_date)
        begin_date += datetime.timedelta(days=1)
    return date_list

def get_date_list_stock(start_date,end_date):
#    begin_date = str2date('20190301', date_format="%Y%m%d")
    begin_date = str2date(start_date, date_format="%Y%m%d")
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
    #    global open_date_str_list
    open_date_str_list = []
    path_str_list = []
    date_list = []

    date_list = get_date_list(begin_date, end_date)

    print('date_list', date_list)
    #    str_date=str(date) #2019-04-12
    #    str_date1=date.strftime('%Y%m%d')  #20190412
    for date in date_list:
        #        print('iat01',cal_dates[cal_dates['cal_date']==date2str(date)].iat[0,2])
        if cal_dates[cal_dates['cal_date'] == date2str(date)].iat[0, 2] == 1:
            #            print('okokokok',date)
            open_date_str_list.append(str(date.strftime('%Y%m%d')))  # 20190412,20190413....
        else:
            continue
    for date_str in open_date_str_list[0:-1]:
        for stock_code in stocks.ts_code:
            path_str_list.append(
                rootdir + "\\" + date_str[0:6] + "\\" + date_str[0:4] + "-" + date_str[4:6] + "-" + date_str[
                                                                                                    6:8] + "\\" + stock_code + ".csv")  # +stock_code[0:6]

    return path_str_list, open_date_str_list[-1]


def date2path_stock_bak(start_date,end_date, rootdir, stock,cal_date):  #老的备份
#    global cal_date
    open_date_str_list = []
    path_str_list = []
    date_list = []

    date_list = get_date_list_stock(start_date,str2date(end_date))
    print('cal_date::::::',cal_date)
    print('date_list::::::',date_list)
    ##    print('date_list_stock',date_list)
    #    str_date=str(date) #2019-04-12
    #    str_date1=date.strftime('%Y%m%d')  #20190412
    for date in date_list:
        #        print('date2str(date)',date2str(date))
        #        print('iat01',cal_dates[cal_dates['cal_date']==date2str(date)])
        a=date2str(date)
        b=cal_date.iat[0, 1]
        if cal_date[cal_date['cal_date'] == date2str(date)].iat[0, 2] == 1:
            #            print('okokokok',date)
            open_date_str_list.append(str(date.strftime('%Y%m%d')))  # 20190412,20190413....
        else:
            continue
    for date_str in open_date_str_list:
        path_str_list.append(
            rootdir + "\\" + date_str[0:6] + "\\" + date_str[0:4] + "-" + date_str[4:6] + "-" + date_str[6:8] + "\\" + stock + ".csv")  # +stock_code[0:6]

    return path_str_list

def date2path_stock(start_date,end_date, rootdir, stock):  #新的用这个
#    global cal_date
    open_date_str_list = []
    path_str_list = []

    sql = "select cal_date from cal_date where cal_date >= '" + start_date +"' and cal_date <= '"+ end_date +"'"
    df_day = db.query_data1(sql)
    open_date_str_list = df_day['cal_date'].tolist()
    #########
    for date_str in open_date_str_list:
        path_str_list.append(
            rootdir + "\\" + date_str[0:6] + "\\" + date_str[0:4] + "-" + date_str[4:6] + "-" + date_str[6:8] + "\\" + stock + ".csv")  # +stock_code[0:6]
    #                print('path_str_list',path_str_list)

    #    print('open_date_str_list',open_date_str_list)
    ##    print('path_str_list111111_stock:',path_str_list)
    return path_str_list

def csv2excel(path_str_list, bigDanAmount): #44444444444444
    n = 7 #  线程数  4或7好
    fen_n = int(len(path_str_list) / n)
    m = 0
#    data_n,b_data,s_data,t_data_n = csv_2_10XThread(path_str_list, bigDanAmount)
    p = Pool()
    res_l = []
    flag8 = 0
    buy_data_l = []
    sell_data_l = []
#    for i in range(n+1):
#        if(i<n):
#            res = p.apply_async(csv_2_10XThread, args=(path_str_list[(fen_n * m): (fen_n * (m + 1))], bigDanAmount)) #33333333333
#            m = m + 1
#        else:
#            x = fen_n * m
#            y=len(path_str_list)
#            res = p.apply_async(csv_2_10XThread, args=(path_str_list[(fen_n * m): len(path_str_list)], bigDanAmount))
#        res_l.append(res)

    res1 = p.apply_async(csv_2_10XThread, args=(path_str_list[0: (fen_n)], bigDanAmount)) #33333333333
    res_l.append(res1)
    res2 = p.apply_async(csv_2_10XThread,
                        args=(path_str_list[(fen_n): (fen_n * 2)], bigDanAmount))  # 33333333333
    res_l.append(res2)
    res3 = p.apply_async(csv_2_10XThread,
                        args=(path_str_list[(fen_n * 2): (fen_n * 3)], bigDanAmount))  # 33333333333
    res_l.append(res3)
    res4 = p.apply_async(csv_2_10XThread,
                        args=(path_str_list[(fen_n * 3): (fen_n * 4)], bigDanAmount))  # 33333333333
    res_l.append(res4)
    res5 = p.apply_async(csv_2_10XThread,
                        args=(path_str_list[(fen_n * 4): (fen_n *5)], bigDanAmount))  # 33333333333
    res_l.append(res5)
    res6 = p.apply_async(csv_2_10XThread,
                        args=(path_str_list[(fen_n * 5): (fen_n *6)], bigDanAmount))  # 33333333333
    res_l.append(res6)
    res7 = p.apply_async(csv_2_10XThread,
                        args=(path_str_list[(fen_n *6): (fen_n * 7)], bigDanAmount))  # 33333333333
    res_l.append(res7)
#    lee= path_str_list[(fen_n *7): (len(path_str_list)-1)]
    if (fen_n *7)<(len(path_str_list)):
        flag8=1
        res8 = p.apply_async(csv_2_10XThread, args=(path_str_list[(fen_n *7): (len(path_str_list))], bigDanAmount))
        res_l.append(res8)



    p.close()
    p.join()  # 等待进程池中所有进程执行完毕

    if res1.successful()==False:
        print(':::::::::::::线程1数据出错！',path_str_list[0: (fen_n)])
    if res2.successful()==False:
        print(':::::::::::::线程2数据出错！',path_str_list[(fen_n * 1): (fen_n * 2)])
    if res3.successful()==False:
        print(':::::::::::::线程3数据出错！',path_str_list[(fen_n * 2): (fen_n * 3)])
    if res4.successful()==False:
        print(':::::::::::::线程4数据出错！',path_str_list[(fen_n * 3): (fen_n * 4)])
    if res5.successful()==False:
        print(':::::::::::::线程5数据出错！',path_str_list[(fen_n * 4): (fen_n * 5)])
    if res6.successful()==False:
        print(':::::::::::::线程6数据出错！',path_str_list[(fen_n * 5): (fen_n * 6)])
    if res7.successful()==False:
        print(':::::::::::::线程7数据出错！',path_str_list[(fen_n * 6): (fen_n * 7)])
    if ((fen_n *7)<(len(path_str_list)))&flag8:
        if (res8.successful() == False):
            print(':::::::::::::线程8数据出错！',path_str_list[(fen_n *7): (len(path_str_list))])
            flag8=0
    tempList = []
    for rest in res_l:
        tempList.append(rest.get())  # 拿到所有结果

#    print('tempList0::::::::',tempList[0][0])
#    print('tempList1::::::::', tempList[1][0])
#    print('tempList2::::::::', tempList[2][0])
    data_n = []
    buy_data = []
    sell_data = []
    t_data_n = []
    if flag8 ==0:
        n=n-1

    for i in range(n+1):
        data_n.append(tempList[i][0])
        buy_data.append(tempList[i][1])
        sell_data.append(tempList[i][2])
        t_data_n.append(tempList[i][3])  #new++

    data_nf = pd.concat(data_n, axis=0, ignore_index=False, )
    data_nf = data_nf.reset_index(drop=True)
    buy_dataf = pd.concat(buy_data, axis=0, ignore_index=False, )
    buy_dataf = buy_dataf.reset_index(drop=True)
    sell_dataf = pd.concat(sell_data, axis=0, ignore_index=False, )
    sell_dataf = sell_dataf.reset_index(drop=True)

    t_data_nf = pd.concat(t_data_n, axis=0, ignore_index=False, )
    t_data_nf = t_data_nf.reset_index(drop=True)

    ##    data['bizhi'] = data['大单买小单卖']/data['大单卖小单买']
    ##    data = data[['日期','ts_code','大单买小单卖1','大单卖小单买1','大单买小单卖','大单卖小单买','bizhi']]
    #    data.dropna(axis=0,how='any')
#@    print("data::::::", data)
#    print("b_data::::::", b_data)
 #   print("s_data::::::", s_data)
    return data_nf,buy_dataf,sell_dataf,t_data_nf


def save_2_excel(df, stock_code):
    # output_dir = 'D:\\python\\'+stock_code+'.xlsx'  #下载数据的存放路径
    # df.to_excel(output_dir, sheet_name='888888')
    # book = load_workbook(output_dir)
    # writer = pd.ExcelWriter(output_dir, engine='openpyxl')
    # writer.book = book
    # writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
    # df.to_excel(writer, stock_code, index=1, startrow=0, startcol=0)
    # writer.save()
    return


################################################




def _excelAddSheet(self, dataframe, excelWriter, sheet_name):
    # book = load_workbook(excelWriter.path)
    # excelWriter.book = book
    # #       dataframe.to_excel(excel_writer=excelWriter,sheet_name=sheet_name)
    # dataframe.to_excel(excel_writer=excelWriter, sheet_name=sheet_name)
    # excelWriter.close()
    return



def get_cal_dates(start_date,end_date):#获得交易日历
    token = '9051c46d645de5c62f69611eae5dc21a310c15839a851c352d383d39'
    ts.set_token(token)
    pro = ts.pro_api()

    stocks = pro.stock_basic(exchange='', list_status='L', fields='ts_code')
    #    print(type(stocks))
    #    print(stocks)
 #   begin_date = var_begin_date
#    end_date = var_end_date
    cal_dates = pd.DataFrame({"cal_date": ['20160101']})
    if cal_dates.empty == False:
        cal_dates.drop(cal_dates.index, inplace=True)  # 清空dataframe

#    cal_dates = pro.trade_cal(exchange='', start_date='20160101', end_date=end_date)
    cal_dates = pro.trade_cal(exchange='', start_date=start_date, end_date=end_date)
    return cal_dates

def find_shuju_stock(start_date,end_date,rootdir,ts_code_t,dadan):   ##11111单只股票计算

    if ts_code_t[0] == "6":
        ts_code = ts_code_t + ".SH"
    else:
        ts_code = ts_code_t + ".SZ"

#    cal_dates = get_cal_dates(start_date,end_date)
    stock_path_list = date2path_stock(start_date,end_date, rootdir, ts_code)  #####wr


#$    start_date1 = stock_path_list[0][-24:-20] + stock_path_list[0][-19:-17] + stock_path_list[0][-16:-14]
#$    token = '9051c46d645de5c62f69611eae5dc21a310c15839a851c352d383d39'
#$    ts.set_token(token)
#$    pro = ts.pro_api()
#$    df = pro.daily(ts_code=ts_code, start_date=start_date1, end_date=end_date)
#$    df = df.sort_values(by='trade_date', axis=0, ascending=True, inplace=False, kind='quicksort',
#$                        na_position='last')  # 升序
#$    close_list = df['close'].tolist()
############
    close_list=[]
    sqlTable = date2sqlTable(start_date, end_date)

    if len(sqlTable) :
        for table in sqlTable:
            sql = "select close from " + table + " where ts_code = '" + ts_code+"'"
            t_df = db.query_data1(sql)
            close_t = t_df['close'].tolist()
            close_list.append(close_t[0])
###############
    df_f,b_data,s_data,df_f_t = csv2excel(stock_path_list, dadan)  #190000 文件里对应的是股数 1股是送股配股  ##22222222222
#    print("df_f:::::::::",df_f.head(5))
    df_ff = df_f.sort_values(by='日期', axis=0, ascending=1, inplace=False, kind='quicksort', na_position='last')  # 升序
    buy_data = b_data.sort_values(by='date_time', axis=0, ascending=1, inplace=False, kind='quicksort', na_position='last')  # 升序
    sell_data = s_data.sort_values(by='date_time', axis=0, ascending=1, inplace=False, kind='quicksort', na_position='last')  # 升序
    df_ff_t = df_f_t.sort_values(by='日期', axis=0, ascending=1, inplace=False, kind='quicksort', na_position='last')  # 升序

    df_ff = df_ff.reset_index(drop=True)
    buy_data = buy_data.reset_index(drop=True)
    sell_data = sell_data.reset_index(drop=True)
    df_ff_t = df_ff_t.reset_index(drop=True)

#分别按时间聚合vol
    dataBuy = buy_data.groupby(by='date_time').agg({'Price':max,'Volume': sum})
    dataSell = sell_data.groupby(by='date_time').agg({'Price': max, 'Volume': sum})
#修改列名
    dataBuy.rename(columns={'Price': 'buyPrice', 'Volume': 'buyVol'}, inplace=True)
    dataSell.rename(columns={'Price': 'sellPrice', 'Volume': 'sellVol'}, inplace=True)
#转char为time
    dataBuy.index = pd.to_datetime(dataBuy.index)
    dataSell.index = pd.to_datetime(dataSell.index)

#只留两个字段
#    dataBuy = dataBuy[['buyPrice','buyVol']]
#    dataSell = dataBuy[['sellPrice', 'sellVol']]

    list_t = df_ff['日期'].tolist()
    for i in list_t:
#        i = ''+ymd[0:4]+'-'+ymd[4:6]+'-'+ymd[6:8]
        dates = pd.date_range(start=(i + ' 09:24:59'), end=(i + ' 09:25:00'), freq='S', normalize=False)
#        print('i:::::::::', i)
        df  = pd.date_range(start=(i+' 09:25:00'),end=(i+' 11:30:00'),freq='S',normalize=False)#.DatetimeIndex.strftime('%yyyy%mm%dd %hh:%mm:%ss')
#        print('df:::::::::',df)
        df2 = pd.date_range(start=(i+' 13:00:00'),end=(i+' 15:00:00'),freq='S',normalize=False)#.DatetimeIndex.strftime('%yyyy%mm%dd %hh:%mm:%ss')
        dx = df.append(df2)
        dates = dates.append(dx)

#    buy_sell_data = pd.DataFrame(
#        { "buyPrice": ['0'], "sellPrice": ['0'], "buyVol": ['0'], "sellVol": ['0']},index=dates)
#生成只有index的df
    buy_sell_data = pd.DataFrame({},index=dates)
    #合并buy_sell_data, dataBuy 使用buy_sell_data的index合并（left_index = True），数据使用dataBuy的（how）sort = False不排序可以加速
    buy_sell_data = pd.merge(buy_sell_data, dataBuy, how='outer', right_index = True, left_index = True,  sort = False)
    buy_sell_data = pd.merge(buy_sell_data, dataSell,how='outer', right_index = True, left_index = True,  sort = False)
#所有NaN变0
    buy_sell_data['buyVol'] = buy_sell_data['buyVol'].fillna(0)
    buy_sell_data['sellVol'] = buy_sell_data['sellVol'].fillna(0)
#价格nan的空挡用上一个值填充
    buy_sell_data['buyPrice'] = buy_sell_data['buyPrice'].fillna(method='pad')
    buy_sell_data['sellPrice'] = buy_sell_data['sellPrice'].fillna(method='pad' )

    return df_ff,close_list,buy_sell_data,df_ff_t



def get_csv_stock():
    global df_x
    rootdir = var_path.get() + '\\' + 'data.xlsx'

    sheet = pd.read_excel(rootdir, sheet_name=0)
    sheet = sheet[['ts_code', 'bizhi', 'pct_chg']]
    if df_x.empty == False:
        df_x.drop(df_x.index, inplace=True)  # 清空dataframe
    df_x = sheet



if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    # 显示所有列
    pd.set_option('display.max_rows', None)

    window = tk.Tk()

    # 第2步，给窗口的可视化起名字
    window.title('111多进程')

    # 第3步，设定窗口的大小(长 * 宽)
    window.geometry('1900x1080')  # 这里的乘是小x

    #    list_test=[1,2,3,4,5,6,7,8,9,10,11]
    #    print(list_test[1:3])
    #    print(list_test[4:8])
    #    print(list_test[9:10])

    var_begin_date = tk.StringVar()
    var_begin_date.set('20190801')
    entry_begin_date = tk.Entry(window, textvariable=var_begin_date, font=('Arial', 14))
    entry_begin_date.place(x=0, y=0)

    var_end_date = tk.StringVar()
    var_end_date.set('20191009')
    entry_end_date = tk.Entry(window, textvariable=var_end_date, font=('Arial', 14))
    entry_end_date.place(x=100, y=0)
    # 第5步，在窗口界面设置放置Button按键
    # b = tk.Button(window, text='计算全部', font=('Arial', 12), width=10, height=1, command=jisuan)


    var_n = tk.StringVar()
    var_n.set('0')
    entry_n = tk.Entry(window, textvariable=var_n, font=('Arial', 14))
    entry_n.place(x=600, y=0)


    var_ts_code = tk.StringVar()
    var_ts_code.set('0')
    entry_ts_code = tk.Entry(window, textvariable=var_ts_code, font=('Arial', 14))
    entry_ts_code.place(x=800, y=0)

    var_ts_code_in = tk.StringVar()
    var_ts_code_in.set('002687')
    entry_ts_code_in = tk.Entry(window, textvariable=var_ts_code_in, font=('Arial', 14))
    entry_ts_code_in.place(x=900, y=0)

    btn_go_n = tk.Button(window, text='单只stock', command=find_shuju_stock)
    btn_go_n.place(x=1000, y=0)


    btn_read_csv = tk.Button(window, text='读csv', command=get_csv_stock)
    btn_read_csv.place(x=1180, y=0)

    var_path = tk.StringVar()
    var_path.set('d:\python\DATA')
    entry_path = tk.Entry(window, textvariable=var_path, font=('Arial', 14))
    entry_path.place(x=10, y=30)


#    df = pd.DataFrame(df2.rand(n,2),columns=['a','b'])
#    df2.plot.bar(xticks = '日期',figsize=(20,10))


#新函数 以这个为准   零时注释掉
def csv_2_10XThread_tt(path_str_list, bigDanAmount):
    #    if q.empty():	# 如果队列空了，就退出循环
    #        return
    csv_list = []
    t = 0
    m = 0
    close = []
    price_age = []
    bigbuycount = []
    bigbuyamout = []
    bigsellcount = []
    bigsellamout = []
    smallbuycount = []
    smallbuyamout = []
    smallsellcount = []
    smallsellamout = []
    bigbuysmall = []
    bigsellsmall = []
    stock_list = []
    excel_date_list = []
    bigbuysmall_1 = []
    bigsellsmall_1 =[]
    bigbuysmall_detail_list = []
    bigsmall_sell_detail_list = []

    bigbuysmall_detail_list.clear() #清空list
    bigsmall_sell_detail_list.clear()

    if bigbuysmall_detail_h.empty == False:
        bigbuysmall_detail_h.drop(bigbuysmall_detail_h.index, inplace=True)  # 清空dataframe
    if bigsmall_sell_detail_h.empty == False:
        bigsmall_sell_detail_h.drop(bigsmall_sell_detail_h.index, inplace=True)  # 清空dataframe

    for path in path_str_list:

        #        t=t+1
        #        if t>10: break
        try:
            #            stock_code = path[-13:-4]
            #            print('stock_code22222',stock_code)
            #            print('path[0:-7]+".csv"',(path[0:-7]+".csv"))
            ddf = pd.read_csv(path[0:-7] + ".csv")  # 读取训练数据
            stock_code = path[-13:-4]
            #print('csv_data::::::::::::',csv_data)
        ##            print('stock_code22222',stock_code)
            day = path[-24:-20] +'-'+ path[-19:-17] +'-'+  path[-16:-14]  ###wr
            excel_date_list.append(day)
        except Exception as e:
            ##            print('eeeeee',e)
            pass
            continue
#        day = path[-24:-20] + path[-19:-17] + path[-16:-14]
#        excel_date_list.append(day)
 #       excel_date_list.append(path[-24:-20] + path[-19:-17] + path[-16:-14])
        #        excel_date_list.append(open_date_str_list[n]) #excel中的日期列
        #        print('excel_date_list',excel_date_list)
        #ddf =csv_data
        #        ddf = ddf.loc[:,['TranID','Time','Price','Volume','Type']]
        # df['col3'] = df.apply(lambda x: x['col1'] + 2 * x['col2'], axis=1)
        #            csv_dd['Amount'] = csv_dd.apply(lambda x: x['Price'] * x['Volume'], axis=1)
#大单买小单：
        bigsmall_1 = ddf.loc[((ddf.BuyOrderVolume * ddf.BuyOrderPrice) >= bigDanAmount) & (
                    (ddf.SaleOrderVolume * ddf.SaleOrderPrice) < bigDanAmount)]
#        print('bigsmall_1:::::::::::::',bigsmall_1)
        if bigsmall_1.empty == False:
            bigsmall_1['sum_s'] = (bigsmall_1.Volume * bigsmall_1.Price)
            bigbuysmall_detail = bigsmall_1[['TranID','Time','Price','Volume']]
            bigbuysmall_detail["date_time"] = [day+' % s' %s for s in bigbuysmall_detail["Time"]] #新建一列date_time 为日期加时间
            bigbuysmall_detail = bigbuysmall_detail[['date_time','Price','Volume']]
            bigbuysmall_detail_list.append(bigbuysmall_detail)#大买单详情之和

            bigsmall_s_1 = bigsmall_1['sum_s'].sum() / 10000
            bigsmall_s_1 = round(bigsmall_s_1)
            bigbuysmall_1.append(bigsmall_s_1)
        else:
            bigbuysmall_1.append(0.0)
            bigbuysmall_detail_list.append(0.0)
        # 卖单收小单资金      Sql = "select sum(Volume * Price)/10000 From [" & f & "] a where BuyOrderVolume * BuyOrderPrice<=" & bigdeal & _
        # " and SaleOrderVolume * SaleOrderPrice > " & bigdeal
        bigsmall_sell_1 = ddf.loc[((ddf.BuyOrderVolume * ddf.BuyOrderPrice) <= bigDanAmount) & (
                    (ddf.SaleOrderVolume * ddf.SaleOrderPrice) > bigDanAmount)]
        #        if bigsmall_sell.isnull==True:
        if bigsmall_sell_1.empty == False:
            bigsmall_sell_1['sum_s'] = (bigsmall_sell_1.Volume * bigsmall_sell_1.Price)
            bigsmall_sell_detail = bigsmall_sell_1[['TranID','Time','Price','Volume']]
            bigsmall_sell_detail["date_time"] = [day+' % s' %s for s in bigsmall_sell_detail["Time"]]  #新建一列date_time 为日期加时间
            bigsmall_sell_detail = bigsmall_sell_detail[['date_time', 'Price', 'Volume']]
            bigsmall_sell_detail_list.append(bigsmall_sell_detail)#大卖单详情之和



            bigsmall_sell_1 = bigsmall_sell_1['sum_s'].sum() / 10000
            bigsmall_sell_1 = round(bigsmall_sell_1)
            bigsellsmall_1.append(bigsmall_sell_1)
        else:
            bigsellsmall_1.append(0.0)
            bigsmall_sell_detail_list.append(0.0)
        stock_list.append(stock_code)

#    print('bigbuysmall_detail_list:::::::',bigbuysmall_detail_list)

    b_data = pd.concat(bigbuysmall_detail_list, axis=0, ignore_index=False, )  # 大买单详情之和
    s_data = pd.concat(bigsmall_sell_detail_list, axis=0, ignore_index=False, )  # 大卖单详情之和
    b_data = b_data.dropna(axis=0, how='any', inplace=False)###3
    s_data = s_data.dropna(axis=0, how='any', inplace=False)###3
#    buy_data = b_data.reset_index(drop=True)
 #   sell_data =  s_data.reset_index(drop=True)
#    print("buy_data11111::::::", buy_data)
#    print("sell_data11111::::::", sell_data)

    c = {'日期': excel_date_list,
         'ts_code': stock_list,
         '大单买小单卖1': bigbuysmall_1,
         '大单卖小单买1': bigsellsmall_1
         }
    data_n = DataFrame(c)
    data_n = data_n.dropna(axis=0, how='any',inplace=False)###3
    #    q.put(data_n)
    return (data_n,b_data,s_data)


def date2sqlTable_bak(start_date,end_date): #start_date,end_date为str型： 20190606
    token = '9051c46d645de5c62f69611eae5dc21a310c15839a851c352d383d39'
    ts.set_token(token)
    pro = ts.pro_api()

#    stocks = pro.stock_basic(exchange='', list_status='L', fields='ts_code')

    cal_dates = pd.DataFrame({"cal_date": ['20160101']})
    if cal_dates.empty == False:
        cal_dates.drop(cal_dates.index, inplace=True)  # 清空dataframe

#    cal_dates = pro.trade_cal(exchange='', start_date='20160101', end_date=end_date)
    cal_dates = pro.trade_cal(exchange='', start_date=start_date, end_date=end_date)

    open_date_str_list = []
    sqlTable_list = []
    date_list = []

    date_list = get_date_list_stock(start_date,str2date(end_date))
#    print('cal_date::::::',cal_date)
#    print('date_list::::::',date_list)
    ##    print('date_list_stock',date_list)
    #    str_date=str(date) #2019-04-12
    #    str_date1=date.strftime('%Y%m%d')  #20190412
    for date in date_list:
        #        print('date2str(date)',date2str(date))
        #        print('iat01',cal_dates[cal_dates['cal_date']==date2str(date)])
        a=date2str(date)
        b=cal_dates.iat[0, 1]
        if cal_dates[cal_dates['cal_date'] == date2str(date)].iat[0, 2] == 1:
            #            print('okokokok',date)
            open_date_str_list.append(str(date.strftime('%Y%m%d')))  # 20190412,20190413....
        else:
            continue
    for date_str in open_date_str_list:
        sqlTable_list.append(
            "t" + date_str)  # +stock_code[0:6]
    #                print('path_str_list',path_str_list)

    #    print('open_date_str_list',open_date_str_list)
#    print('sqlTable_list111111:',sqlTable_list)
    return sqlTable_list

def date2sqlTable(start_date,end_date): #start_date,end_date为str型： 20190606
    path_str_list = []
    sqlTable_list=[]
    sql = "select cal_date from cal_date where cal_date >= '" + start_date + "' and cal_date <= '" + end_date + "'"
    df_day = db.query_data1(sql)
    # print('df_day',df_day)
    open_date_str_list = df_day['cal_date'].tolist()
    for date_str in open_date_str_list:
        sqlTable_list.append(
            "t" + date_str)  # +stock_code[0:6]
    #                print('path_str_list',path_str_list)
    #    print('open_date_str_list',open_date_str_list)
#    print('sqlTable_list111111:',sqlTable_list)
    sqlTable_list.reverse()
    return sqlTable_list



#集合竞价函数 以这个为准
def csv_2_10XThread925(path_str_list, bigDanAmount):
    #    if q.empty():	# 如果队列空了，就退出循环
    #        return
    csv_list = []
    t = 0
    m = 0
    close = []
    price_age = []
    bigbuycount = []
    bigbuyamout = []
    bigsellcount = []
    bigsellamout = []
    smallbuycount = []
    smallbuyamout = []
    smallsellcount = []
    smallsellamout = []
    bigbuysmall = []
    bigsellsmall = []
    stock_list = []
    excel_date_list = []
    bigbuysmall_1 = []
    bigsellsmall_1 =[]
    bigbuysmall_detail_list = []
    bigsmall_sell_detail_list = []

    bigbuysmall_detail_list.clear() #清空list
    bigsmall_sell_detail_list.clear()

    if bigbuysmall_detail_h.empty == False:
        bigbuysmall_detail_h.drop(bigbuysmall_detail_h.index, inplace=True)  # 清空dataframe
    if bigsmall_sell_detail_h.empty == False:
        bigsmall_sell_detail_h.drop(bigsmall_sell_detail_h.index, inplace=True)  # 清空dataframe

    for path in path_str_list:

        #        t=t+1
        #        if t>10: break
        try:
            #            stock_code = path[-13:-4]
            #            print('stock_code22222',stock_code)
            #            print('path[0:-7]+".csv"',(path[0:-7]+".csv"))
            ddf = pd.read_csv(path)  # 读取训练数据
            stock_code = path[-10:-4]
            #print('csv_data::::::::::::',csv_data)
        ##            print('stock_code22222',stock_code)
#            day = path[-24:-20] +'-'+ path[-19:-17] +'-'+  path[-16:-14]  ###wr
#            excel_date_list.append(day)
            pass
        except Exception as e:
            ##            print('eeeeee',e)
            pass
            continue
#        day = path[-24:-20] + path[-19:-17] + path[-16:-14]
#        excel_date_list.append(day)
 #       excel_date_list.append(path[-24:-20] + path[-19:-17] + path[-16:-14])
        #        excel_date_list.append(open_date_str_list[n]) #excel中的日期列
        #        print('excel_date_list',excel_date_list)
        #ddf =csv_data
        #        ddf = ddf.loc[:,['TranID','Time','Price','Volume','Type']]
        # df['col3'] = df.apply(lambda x: x['col1'] + 2 * x['col2'], axis=1)
        #            csv_dd['Amount'] = csv_dd.apply(lambda x: x['Price'] * x['Volume'], axis=1)
#大单买小单：
        bigsmall_1 = ddf.loc[((ddf.BuyOrderVolume * ddf.BuyOrderPrice) >= bigDanAmount) & (
                    (ddf.SaleOrderVolume * ddf.SaleOrderPrice) < bigDanAmount) & (ddf.Time < '9:30:00')]
#        print('bigsmall_1:::::::::::::',bigsmall_1)
        if bigsmall_1.empty == False:
            bigsmall_1['sum_s'] = (bigsmall_1.Volume * bigsmall_1.Price)

            bigsmall_s_1 = bigsmall_1['sum_s'].sum() / 10000
            bigsmall_s_1 = round(bigsmall_s_1)
            bigbuysmall_1.append(bigsmall_s_1)
        else:
            bigbuysmall_1.append(0.0)

        # 卖单收小单资金      Sql = "select sum(Volume * Price)/10000 From [" & f & "] a where BuyOrderVolume * BuyOrderPrice<=" & bigdeal & _
        # " and SaleOrderVolume * SaleOrderPrice > " & bigdeal
        bigsmall_sell_1 = ddf.loc[((ddf.BuyOrderVolume * ddf.BuyOrderPrice) <= bigDanAmount) & (
                    (ddf.SaleOrderVolume * ddf.SaleOrderPrice) > bigDanAmount) & (ddf.Time < '9:30:00')]
        #        if bigsmall_sell.isnull==True:
        if bigsmall_sell_1.empty == False:
            bigsmall_sell_1['sum_s'] = (bigsmall_sell_1.Volume * bigsmall_sell_1.Price)
            bigsmall_sell_1 = bigsmall_sell_1['sum_s'].sum() / 10000
            bigsmall_sell_1 = round(bigsmall_sell_1)
            bigsellsmall_1.append(bigsmall_sell_1)
        else:
            bigsellsmall_1.append(0.0)

        stock_list.append(stock_code)

#    print('bigbuysmall_detail_list:::::::',bigbuysmall_detail_list)


#    buy_data = b_data.reset_index(drop=True)
 #   sell_data =  s_data.reset_index(drop=True)
#    print("buy_data11111::::::", buy_data)
#    print("sell_data11111::::::", sell_data)

    c = {#'日期': excel_date_list,
         'ts_code': stock_list,
         '大单买小单卖1': bigbuysmall_1,
         '大单卖小单买1': bigsellsmall_1
         }
    data_n = DataFrame(c)
    data_n = data_n.dropna(axis=0, how='any',inplace=False)###3
    #    q.put(data_n)
    return (data_n)

def csv2excel925(path_str_list, bigDanAmount): #44444444444444
    n = 5 #5
    fen_n = int(len(path_str_list) / n)

    m = 0
    p = Pool()
    res_l = []
    buy_data_l = []
    sell_data_l = []
    for i in range(n+1):
        if(i<n):
            res = p.apply_async(csv_2_10XThread925, args=(path_str_list[(fen_n * m): (fen_n * (m + 1))], bigDanAmount)) #33333333333
            m = m + 1
        else:
            x = fen_n * m
            y=len(path_str_list)
            res = p.apply_async(csv_2_10XThread925, args=(path_str_list[(fen_n * m): len(path_str_list)], bigDanAmount))
        res_l.append(res)

    p.close()
    p.join()  # 等待进程池中所有进程执行完毕

    tempList = []
    for rest in res_l:
        tempList.append(rest.get())  # 拿到所有结果

#    print('tempList0::::::::',tempList[0][0])
#    print('tempList1::::::::', tempList[1][0])
#    print('tempList2::::::::', tempList[2][0])
    data_nf = pd.concat(tempList, axis=0, ignore_index=False, )
    data_nf = data_nf.reset_index(drop=True)


    return data_nf

def jihejingjia(date,rootdir):
    path_str_list =[]
    token = '9051c46d645de5c62f69611eae5dc21a310c15839a851c352d383d39'
    ts.set_token(token)
    pro = ts.pro_api()
    # 查询当前所有正常上市交易的股票列表
#    ts_code    symbol    name    area    industry    list_date
#    000001.SZ    000001   平安银行  深圳  银行  19910403
    data = pro.stock_basic(exchange='', list_status='L', fields='symbol')
    symbol_list= data['symbol'].tolist()

    for symbol in symbol_list:
        path_str_list.append(
            rootdir + "\\" + date[:-2] + "\\"+ date[0:4] + "-" + date[4:6] + "-" + date[6:8] + "\\" + symbol + ".csv")

    df_f= csv2excel925(path_str_list, 190000)
    df_tf =df_f
    df_tf['比值'] = df_f['大单买小单卖1']/df_f['大单卖小单买1']
    df_tf['权重'] = df_f['大单买小单卖1'] * df_f['比值']
    df = df_tf.sort_values(by='权重', axis=0, ascending=0, inplace=False, kind='quicksort',
                                   na_position='last')  # 降序
    df = df.reset_index(drop=True)
    return df

#测试函数 全部大单
def csv_2_10XThread(path_str_list, bigDanAmount):
    #    if q.empty():	# 如果队列空了，就退出循环
    #        return
    csv_list = []
    t = 0
    m = 0
    close = []
    price_age = []
    bigbuycount = []
    bigbuyamout = []
    bigsellcount = []
    bigsellamout = []
    smallbuycount = []
    smallbuyamout = []
    smallsellcount = []
    smallsellamout = []
    bigbuysmall = []
    bigsellsmall = []
    stock_list = []
    excel_date_list = []
    bigbuysmall_1 = []
    bigsellsmall_1 =[]
    t_bigbuysmall_1 = []
    bigbuysmall_detail_list = []
    bigsmall_sell_detail_list = []
    t_bigbuysmall_detail_list = []
    t_bigsmall_sell_detail_list = []
    t_bigsellsmall_1 =[]

    bigbuysmall_detail_list.clear() #清空list
    bigsmall_sell_detail_list.clear()

    if bigbuysmall_detail_h.empty == False:
        bigbuysmall_detail_h.drop(bigbuysmall_detail_h.index, inplace=True)  # 清空dataframe
    if bigsmall_sell_detail_h.empty == False:
        bigsmall_sell_detail_h.drop(bigsmall_sell_detail_h.index, inplace=True)  # 清空dataframe

    for path in path_str_list:

        #        t=t+1
        #        if t>10: break
        try:
            #            stock_code = path[-13:-4]
            #            print('stock_code22222',stock_code)
            #            print('path[0:-7]+".csv"',(path[0:-7]+".csv"))
            ddf = pd.read_csv(path[0:-7] + ".csv")  # 读取训练数据
            stock_code = path[-13:-4]
            #print('csv_data::::::::::::',csv_data)
        ##            print('stock_code22222',stock_code)
            day = path[-24:-20] +'-'+ path[-19:-17] +'-'+  path[-16:-14]  ###wr
            excel_date_list.append(day)

#        day = path[-24:-20] + path[-19:-17] + path[-16:-14]
#        excel_date_list.append(day)
 #       excel_date_list.append(path[-24:-20] + path[-19:-17] + path[-16:-14])
        #        excel_date_list.append(open_date_str_list[n]) #excel中的日期列
        #        print('excel_date_list',excel_date_list)
        #ddf =csv_data
        #        ddf = ddf.loc[:,['TranID','Time','Price','Volume','Type']]
        # df['col3'] = df.apply(lambda x: x['col1'] + 2 * x['col2'], axis=1)
        #            csv_dd['Amount'] = csv_dd.apply(lambda x: x['Price'] * x['Volume'], axis=1)
# 大单买小单：
            bigsmall_1 = ddf.loc[((ddf.BuyOrderVolume * ddf.BuyOrderPrice) >= bigDanAmount) & (
                    (ddf.SaleOrderVolume * ddf.SaleOrderPrice) < bigDanAmount)]
            #        print('bigsmall_1:::::::::::::',bigsmall_1)
            if bigsmall_1.empty == False:
                bigsmall_1['sum_s'] = (bigsmall_1.Volume * bigsmall_1.Price)
                bigbuysmall_detail = bigsmall_1[['TranID', 'Time', 'Price', 'Volume']]
                bigbuysmall_detail["date_time"] = [day + ' % s' % s for s in
                                                   bigbuysmall_detail["Time"]]  # 新建一列date_time 为日期加时间
                bigbuysmall_detail = bigbuysmall_detail[['date_time', 'Price', 'Volume']]
                bigbuysmall_detail_list.append(bigbuysmall_detail)  # 大买单详情之和

                bigsmall_s_1 = bigsmall_1['sum_s'].sum() / 10000
                bigsmall_s_1 = round(bigsmall_s_1)
                bigbuysmall_1.append(bigsmall_s_1)
            else:
                bigbuysmall_1.append(0.0)
                temp_df = pd.DataFrame(
                    {"date_time": [day + ' 10:00:00',day + ' 10:01:00',day + ' 10:02:00'], "Price": ['0','0','0'], "Volume": ['1','1','1']})
                bigbuysmall_detail_list.append(temp_df)
                #bigbuysmall_detail_list.append(0.0)
            # 卖单收小单资金      Sql = "select sum(Volume * Price)/10000 From [" & f & "] a where BuyOrderVolume * BuyOrderPrice<=" & bigdeal & _
            # " and SaleOrderVolume * SaleOrderPrice > " & bigdeal
            bigsmall_sell_1 = ddf.loc[((ddf.BuyOrderVolume * ddf.BuyOrderPrice) <= bigDanAmount) & (
                    (ddf.SaleOrderVolume * ddf.SaleOrderPrice) > bigDanAmount)]
            #        if bigsmall_sell.isnull==True:
            if bigsmall_sell_1.empty == False:
                bigsmall_sell_1['sum_s'] = (bigsmall_sell_1.Volume * bigsmall_sell_1.Price)
                bigsmall_sell_detail = bigsmall_sell_1[['TranID', 'Time', 'Price', 'Volume']]
                bigsmall_sell_detail["date_time"] = [day + ' % s' % s for s in
                                                     bigsmall_sell_detail["Time"]]  # 新建一列date_time 为日期加时间
                bigsmall_sell_detail = bigsmall_sell_detail[['date_time', 'Price', 'Volume']]
                bigsmall_sell_detail_list.append(bigsmall_sell_detail)  # 大卖单详情之和

                bigsmall_sell_1 = bigsmall_sell_1['sum_s'].sum() / 10000
                bigsmall_sell_1 = round(bigsmall_sell_1)
                bigsellsmall_1.append(bigsmall_sell_1)
            else:
                bigsellsmall_1.append(0.0)
                temp_df = pd.DataFrame(
                    {"date_time": [day + ' 10:00:00',day + ' 10:01:00',day + ' 10:02:00'], "Price": ['0','0','0'], "Volume": ['1','1','1']})
                bigsmall_sell_detail_list.append(temp_df)
                #bigsmall_sell_detail_list.append(0.0)
            stock_list.append(stock_code)

        #    print('bigbuysmall_detail_list:::::::',bigbuysmall_detail_list)
            #####################################################
            # 纯大单：
            t_bigsmall_1 = ddf.loc[((ddf.Price * ddf.Volume) >= bigDanAmount) &
                                   (ddf.Type == 'B')]
            #        print('t_bigsmall_1:::::::::::::',t_bigsmall_1)
            if t_bigsmall_1.empty == False:
                t_bigsmall_1['sum_s'] = (t_bigsmall_1.Volume * t_bigsmall_1.Price)
                t_bigbuysmall_detail = t_bigsmall_1[['TranID', 'Time', 'Price', 'Volume']]
                t_bigbuysmall_detail["date_time"] = [day + ' % s' % s for s in
                                                     t_bigbuysmall_detail["Time"]]  # 新建一列date_time 为日期加时间
                t_bigbuysmall_detail = t_bigbuysmall_detail[['date_time', 'Price', 'Volume']]
                t_bigbuysmall_detail_list.append(t_bigbuysmall_detail)  # 大买单详情之和

                t_bigsmall_s_1 = t_bigsmall_1['sum_s'].sum() / 10000
                t_bigsmall_s_1 = round(t_bigsmall_s_1)
                t_bigbuysmall_1.append(t_bigsmall_s_1)
            else:
                t_bigbuysmall_1.append(0.0)
                temp_df = pd.DataFrame(
                    {"date_time": [day + ' 10:00:00',day + ' 10:01:00',day + ' 10:02:00'], "Price": ['0','0','0'], "Volume": ['1','1','1']})
                t_bigbuysmall_detail_list.append(temp_df)
                #t_bigbuysmall_detail_list.append(0.0)
            # 卖单收小单资金      Sql = "select sum(Volume * Price)/10000 From [" & f & "] a where BuyOrderVolume * BuyOrderPrice<=" & bigdeal & _
            # " and SaleOrderVolume * SaleOrderPrice > " & bigdeal
            t_bigsmall_sell_1 = ddf.loc[((ddf.Price * ddf.Volume) >= bigDanAmount) &
                                        (ddf.Type == 'S')]
            #        if bigsmall_sell.isnull==True:
            if t_bigsmall_sell_1.empty == False:
                t_bigsmall_sell_1['sum_s'] = (t_bigsmall_sell_1.Volume * t_bigsmall_sell_1.Price)
                t_bigsmall_sell_detail = t_bigsmall_sell_1[['TranID', 'Time', 'Price', 'Volume']]
                t_bigsmall_sell_detail["date_time"] = [day + ' % s' % s for s in t_bigsmall_sell_detail["Time"]]  # 新建一列date_time 为日期加时间
                t_bigsmall_sell_detail = t_bigsmall_sell_detail[['date_time', 'Price', 'Volume']]
                t_bigsmall_sell_detail_list.append(t_bigsmall_sell_detail)  # 大卖单详情之和

                t_bigsmall_sell_1 = t_bigsmall_sell_1['sum_s'].sum() / 10000
                t_bigsmall_sell_1 = round(t_bigsmall_sell_1)
                t_bigsellsmall_1.append(t_bigsmall_sell_1)
            else:
                t_bigsellsmall_1.append(0.0)
                temp_df = pd.DataFrame(
                    {"date_time": [day + ' 10:00:00',day + ' 10:01:00',day + ' 10:02:00'], "Price": ['0','0','0'], "Volume": ['1','1','1']})
                t_bigsmall_sell_detail_list.append(temp_df)
                #t_bigsmall_sell_detail_list.append(0.0)
            #        t_stock_list.append(stock_code)

            #    print('t_bigbuysmall_detail_list:::::::',t_bigbuysmall_detail_list)

        except Exception as e:
            pass
        continue

    b_data = pd.concat(bigbuysmall_detail_list, axis=0, ignore_index=False, )  # 大买单详情df之和
    s_data = pd.concat(bigsmall_sell_detail_list, axis=0, ignore_index=False, )  # 大卖单详情之和
    b_data = b_data.dropna(axis=0, how='any', inplace=False)  ###3
    s_data = s_data.dropna(axis=0, how='any', inplace=False)  ###3
    #    buy_data = b_data.reset_index(drop=True)
    #   sell_data =  s_data.reset_index(drop=True)
    #    print("buy_data11111::::::", buy_data)
    #    print("sell_data11111::::::", sell_data)

    c = {'日期': excel_date_list,
         'ts_code': stock_list,
         '大单买小单卖1': bigbuysmall_1,
         '大单卖小单买1': bigsellsmall_1
         }
    data_n = DataFrame(c)
    data_n = data_n.dropna(axis=0, how='any', inplace=False)  ###3
    #    q.put(data_n)


#    t_b_data = pd.concat(t_bigbuysmall_detail_list, axis=0, ignore_index=False, )  # 大买单详情之和
#    t_s_data = pd.concat(t_bigsmall_sell_detail_list, axis=0, ignore_index=False, )  # 大卖单详情之和
#    t_b_data = t_b_data.dropna(axis=0, how='any', inplace=False)###3
#    t_s_data = t_s_data.dropna(axis=0, how='any', inplace=False)###3


    t_c = {'日期': excel_date_list,
         'ts_code': stock_list,
         '纯大单买': t_bigbuysmall_1,
         '纯大单卖': t_bigsellsmall_1
         }
    t_data_n = DataFrame(t_c)
    t_data_n = t_data_n.dropna(axis=0, how='any',inplace=False)###3
    #    q.put(data_n)
    return (data_n,b_data,s_data,t_data_n)

###########################################  end3min
def find_shuju_stock_end3min(start_date,end_date,rootdir,ts_code_t):   ##11111单只股票计算
    #    stock_date_list=[]
    close_list_np = []
    zero_list = []
    men_means1 = []
    women_means1 = []
    men_means2 = []
    women_means2 = []
    chazhi_list = []
    close_list_np2 = []
#!#    ts_code_t = var_ts_code_in.get()
    if ts_code_t[0] == "6":
        ts_code = ts_code_t + ".SH"
    else:
        ts_code = ts_code_t + ".SZ"
    #    print('ts_code',ts_code)

    #    print('date',date)
    #    rootdir = 'D:\python\DATA'
#!#    rootdir = var_path.get()

  #!#  end_date = var_end_date.get()
#    cal_dates = get_cal_dates(start_date,end_date)
    stock_path_list = date2path_stock(start_date,end_date, rootdir, ts_code)  #####wr

#$    start_date1 = stock_path_list[-1][-24:-20] + stock_path_list[-1][-19:-17] + stock_path_list[-1][-16:-14]
#$    token = '9051c46d645de5c62f69611eae5dc21a310c15839a851c352d383d39'
#$    ts.set_token(token)
#$    pro = ts.pro_api()
#$    df = pro.daily(ts_code=ts_code, start_date=start_date1, end_date=end_date)
#$    df = df.sort_values(by='trade_date', axis=0, ascending=True, inplace=False, kind='quicksort',
#$                        na_position='last')  # 升序
#$    close_list = df['close'].tolist()

    ############
    close_list = []
    sqlTable = date2sqlTable(start_date, end_date)

    if len(sqlTable):
        for table in sqlTable:
            sql = "select close from " + table + " where ts_code = '" + ts_code + "'"
            t_df = db.query_data1(sql)
            close_t = t_df['close'].tolist()
            close_list.append(close_t[0])
    ###############
#    df_f,b_data,s_data,df_f_t = csv2excel_end3min(stock_path_list, 190000)  #19w 文件里对应的是股数 1股是送股配股  ##22222222222
    df_f, b_data, s_data = csv2excel_end3min(stock_path_list, 190000)  # 19w 文件里对应的是股数 1股是送股配股  ##22222222222
#    print("df_f:::::::::",df_f.head(5))
    df_ff = df_f.sort_values(by='日期', axis=0, ascending=1, inplace=False, kind='quicksort', na_position='last')  # 升序
    buy_data = b_data.sort_values(by='date_time', axis=0, ascending=1, inplace=False, kind='quicksort', na_position='last')  # 升序
    sell_data = s_data.sort_values(by='date_time', axis=0, ascending=1, inplace=False, kind='quicksort', na_position='last')  # 升序
#    df_ff_t = df_f_t.sort_values(by='日期', axis=0, ascending=1, inplace=False, kind='quicksort', na_position='last')  # 升序

    df_ff = df_ff.reset_index(drop=True)
    buy_data = buy_data.reset_index(drop=True)
    sell_data = sell_data.reset_index(drop=True)
#    df_ff_t = df_ff_t.reset_index(drop=True)

#分别按时间聚合vol
    dataBuy = buy_data.groupby(by='date_time').agg({'Price':max,'Volume': sum})
    dataSell = sell_data.groupby(by='date_time').agg({'Price': max, 'Volume': sum})
#修改列名
    dataBuy.rename(columns={'Price': 'buyPrice', 'Volume': 'buyVol'}, inplace=True)
    dataSell.rename(columns={'Price': 'sellPrice', 'Volume': 'sellVol'}, inplace=True)
#转char为time
    dataBuy.index = pd.to_datetime(dataBuy.index)
    dataSell.index = pd.to_datetime(dataSell.index)

#只留两个字段
#    dataBuy = dataBuy[['buyPrice','buyVol']]
#    dataSell = dataBuy[['sellPrice', 'sellVol']]

    list_t = df_ff['日期'].tolist()
    for i in list_t:
#        i = ''+ymd[0:4]+'-'+ymd[4:6]+'-'+ymd[6:8]
        dates = pd.date_range(start=(i + ' 09:24:59'), end=(i + ' 09:25:00'), freq='S', normalize=False)
#        print('i:::::::::', i)
        df  = pd.date_range(start=(i+' 09:25:00'),end=(i+' 11:30:00'),freq='S',normalize=False)#.DatetimeIndex.strftime('%yyyy%mm%dd %hh:%mm:%ss')
#        print('df:::::::::',df)
        df2 = pd.date_range(start=(i+' 13:00:00'),end=(i+' 15:00:00'),freq='S',normalize=False)#.DatetimeIndex.strftime('%yyyy%mm%dd %hh:%mm:%ss')
        dx = df.append(df2)
        dates = dates.append(dx)

#    buy_sell_data = pd.DataFrame(
#        { "buyPrice": ['0'], "sellPrice": ['0'], "buyVol": ['0'], "sellVol": ['0']},index=dates)
#生成只有index的df
    buy_sell_data = pd.DataFrame({},index=dates)
    #合并buy_sell_data, dataBuy 使用buy_sell_data的index合并（left_index = True），数据使用dataBuy的（how）sort = False不排序可以加速
    buy_sell_data = pd.merge(buy_sell_data, dataBuy, how='outer', right_index = True, left_index = True,  sort = False)
    buy_sell_data = pd.merge(buy_sell_data, dataSell,how='outer', right_index = True, left_index = True,  sort = False)
#所有NaN变0
    buy_sell_data['buyVol'] = buy_sell_data['buyVol'].fillna(0)
    buy_sell_data['sellVol'] = buy_sell_data['sellVol'].fillna(0)
#价格nan的空挡用上一个值填充
    buy_sell_data['buyPrice'] = buy_sell_data['buyPrice'].fillna(method='pad')
    buy_sell_data['sellPrice'] = buy_sell_data['sellPrice'].fillna(method='pad' )

    return df_ff,close_list,buy_sell_data#,df_ff_t

def csv2excel_end3min(path_str_list, bigDanAmount): #44444444444444
    n = 7 #  线程数  4或7好
    fen_n = int(len(path_str_list) / n)
    m = 0
#    data_n,b_data,s_data,t_data_n = csv_2_10XThread(path_str_list, bigDanAmount)
    p = Pool()
    res_l = []
    flag8 = 0
    buy_data_l = []
    sell_data_l = []
#    for i in range(n+1):
#        if(i<n):
#            res = p.apply_async(csv_2_10XThread, args=(path_str_list[(fen_n * m): (fen_n * (m + 1))], bigDanAmount)) #33333333333
#            m = m + 1
#        else:
#            x = fen_n * m
#            y=len(path_str_list)
#            res = p.apply_async(csv_2_10XThread, args=(path_str_list[(fen_n * m): len(path_str_list)], bigDanAmount))
#        res_l.append(res)

    res1 = p.apply_async(csv_2_10XThread_end3min, args=(path_str_list[0: (fen_n)], bigDanAmount)) #33333333333
    res_l.append(res1)
    res2 = p.apply_async(csv_2_10XThread_end3min,
                        args=(path_str_list[(fen_n): (fen_n * 2)], bigDanAmount))  # 33333333333
    res_l.append(res2)
    res3 = p.apply_async(csv_2_10XThread_end3min,
                        args=(path_str_list[(fen_n * 2): (fen_n * 3)], bigDanAmount))  # 33333333333
    res_l.append(res3)
    res4 = p.apply_async(csv_2_10XThread_end3min,
                        args=(path_str_list[(fen_n * 3): (fen_n * 4)], bigDanAmount))  # 33333333333
    res_l.append(res4)
    res5 = p.apply_async(csv_2_10XThread_end3min,
                        args=(path_str_list[(fen_n * 4): (fen_n *5)], bigDanAmount))  # 33333333333
    res_l.append(res5)
    res6 = p.apply_async(csv_2_10XThread_end3min,
                        args=(path_str_list[(fen_n * 5): (fen_n *6)], bigDanAmount))  # 33333333333
    res_l.append(res6)
    res7 = p.apply_async(csv_2_10XThread_end3min,
                        args=(path_str_list[(fen_n *6): (fen_n * 7)], bigDanAmount))  # 33333333333
    res_l.append(res7)
#    lee= path_str_list[(fen_n *7): (len(path_str_list)-1)]
    if (fen_n *7)<(len(path_str_list)):
        flag8=1
        res8 = p.apply_async(csv_2_10XThread_end3min, args=(path_str_list[(fen_n *7): (len(path_str_list))], bigDanAmount))
        res_l.append(res8)



    p.close()
    p.join()  # 等待进程池中所有进程执行完毕

    if res1.successful()==False:
        print(':::::::::::::线程1数据出错！',path_str_list[0: (fen_n)])
    if res2.successful()==False:
        print(':::::::::::::线程2数据出错！',path_str_list[(fen_n * 1): (fen_n * 2)])
    if res3.successful()==False:
        print(':::::::::::::线程3数据出错！',path_str_list[(fen_n * 2): (fen_n * 3)])
    if res4.successful()==False:
        print(':::::::::::::线程4数据出错！',path_str_list[(fen_n * 3): (fen_n * 4)])
    if res5.successful()==False:
        print(':::::::::::::线程5数据出错！',path_str_list[(fen_n * 4): (fen_n * 5)])
    if res6.successful()==False:
        print(':::::::::::::线程6数据出错！',path_str_list[(fen_n * 5): (fen_n * 6)])
    if res7.successful()==False:
        print(':::::::::::::线程7数据出错！',path_str_list[(fen_n * 6): (fen_n * 7)])
    if ((fen_n *7)<(len(path_str_list)))&flag8:
        if (res8.successful() == False):
            print(':::::::::::::线程8数据出错！',path_str_list[(fen_n *7): (len(path_str_list))])
            flag8=0
    tempList = []
    for rest in res_l:
        tempList.append(rest.get())  # 拿到所有结果

#    print('tempList0::::::::',tempList[0][0])
#    print('tempList1::::::::', tempList[1][0])
#    print('tempList2::::::::', tempList[2][0])
    data_n = []
    buy_data = []
    sell_data = []
    t_data_n = []
    if flag8 ==0:
        n=n-1

    for i in range(n+1):
        data_n.append(tempList[i][0])
        buy_data.append(tempList[i][1])
        sell_data.append(tempList[i][2])
       # t_data_n.append(tempList[i][3])  #new++

    data_nf = pd.concat(data_n, axis=0, ignore_index=False, )
    data_nf = data_nf.reset_index(drop=True)
    buy_dataf = pd.concat(buy_data, axis=0, ignore_index=False, )
    buy_dataf = buy_dataf.reset_index(drop=True)
    sell_dataf = pd.concat(sell_data, axis=0, ignore_index=False, )
    sell_dataf = sell_dataf.reset_index(drop=True)

#    t_data_nf = pd.concat(t_data_n, axis=0, ignore_index=False, )
#    t_data_nf = t_data_nf.reset_index(drop=True)

    ##    data['bizhi'] = data['大单买小单卖']/data['大单卖小单买']
    ##    data = data[['日期','ts_code','大单买小单卖1','大单卖小单买1','大单买小单卖','大单卖小单买','bizhi']]
    #    data.dropna(axis=0,how='any')
#@    print("data::::::", data)
#    print("b_data::::::", b_data)
 #   print("s_data::::::", s_data)
#    return data_nf,buy_dataf,sell_dataf,t_data_nf
    return data_nf,buy_dataf,sell_dataf


def csv_2_10XThread_end3min(path_str_list, bigDanAmount):
    #    if q.empty():	# 如果队列空了，就退出循环
    #        return
    csv_list = []
    t = 0
    m = 0

    stock_list = []
    excel_date_list = []
    bigbuysmall_1 = []
    bigsellsmall_1 =[]
    t_bigbuysmall_1 = []
    bigbuysmall_detail_list = []
    bigsmall_sell_detail_list = []
    t_bigbuysmall_detail_list = []
    t_bigsmall_sell_detail_list = []
    t_bigsellsmall_1 =[]

    bigbuysmall_detail_list.clear() #清空list
    bigsmall_sell_detail_list.clear()

    if bigbuysmall_detail_h.empty == False:
        bigbuysmall_detail_h.drop(bigbuysmall_detail_h.index, inplace=True)  # 清空dataframe
    if bigsmall_sell_detail_h.empty == False:
        bigsmall_sell_detail_h.drop(bigsmall_sell_detail_h.index, inplace=True)  # 清空dataframe

    for path in path_str_list:

        #        t=t+1
        #        if t>10: break
        try:
            #            stock_code = path[-13:-4]
            #            print('stock_code22222',stock_code)
            #            print('path[0:-7]+".csv"',(path[0:-7]+".csv"))
            ddf = pd.read_csv(path[0:-7] + ".csv")  # 读取训练数据
            stock_code = path[-13:-4]
            #print('csv_data::::::::::::',csv_data)
        ##            print('stock_code22222',stock_code)
            day = path[-24:-20] +'-'+ path[-19:-17] +'-'+  path[-16:-14]  ###wr
            excel_date_list.append(day)

#        day = path[-24:-20] + path[-19:-17] + path[-16:-14]
#        excel_date_list.append(day)
 #       excel_date_list.append(path[-24:-20] + path[-19:-17] + path[-16:-14])
        #        excel_date_list.append(open_date_str_list[n]) #excel中的日期列
        #        print('excel_date_list',excel_date_list)
        #ddf =csv_data
        #        ddf = ddf.loc[:,['TranID','Time','Price','Volume','Type']]
        # df['col3'] = df.apply(lambda x: x['col1'] + 2 * x['col2'], axis=1)
        #            csv_dd['Amount'] = csv_dd.apply(lambda x: x['Price'] * x['Volume'], axis=1)
# 大单买小单：
#            bigsmall_1 = ddf.loc[((ddf.BuyOrderVolume * ddf.BuyOrderPrice) >= bigDanAmount) & (
#                    (ddf.SaleOrderVolume * ddf.SaleOrderPrice) < bigDanAmount)]
            bigsmall_1 = ddf.loc[(ddf.Time == '15:00:00') & (ddf.Type == 'B')]
            #        print('bigsmall_1:::::::::::::',bigsmall_1)
            if bigsmall_1.empty == False:
                bigsmall_1['sum_s'] = (bigsmall_1.Volume * bigsmall_1.Price)
                bigbuysmall_detail = bigsmall_1[['TranID', 'Time', 'Price', 'Volume']]
                bigbuysmall_detail["date_time"] = [day + ' % s' % s for s in
                                                   bigbuysmall_detail["Time"]]  # 新建一列date_time 为日期加时间
                bigbuysmall_detail = bigbuysmall_detail[['date_time', 'Price', 'Volume']]
                bigbuysmall_detail_list.append(bigbuysmall_detail)  # 大买单详情之和

                bigsmall_s_1 = bigsmall_1['sum_s'].sum() / 10000
                bigsmall_s_1 = round(bigsmall_s_1)
                bigbuysmall_1.append(bigsmall_s_1)
            else:
                temp_df = pd.DataFrame(
                    { "date_time": [day + ' 15:00:00'], "Price": ['0'], "Volume": ['0']})
                bigbuysmall_1.append(0.0)
                #bigbuysmall_detail_list.append(0.0)
                bigbuysmall_detail_list.append(temp_df)
            # 卖单收小单资金      Sql = "select sum(Volume * Price)/10000 From [" & f & "] a where BuyOrderVolume * BuyOrderPrice<=" & bigdeal & _
            # " and SaleOrderVolume * SaleOrderPrice > " & bigdeal
#            bigsmall_sell_1 = ddf.loc[((ddf.BuyOrderVolume * ddf.BuyOrderPrice) <= bigDanAmount) & (
#                    (ddf.SaleOrderVolume * ddf.SaleOrderPrice) > bigDanAmount)]
            bigsmall_sell_1 = ddf.loc[(ddf.Time == '15:00:00') & (ddf.Type == 'S')]
            #        if bigsmall_sell.isnull==True:
            if bigsmall_sell_1.empty == False:
                bigsmall_sell_1['sum_s'] = (bigsmall_sell_1.Volume * bigsmall_sell_1.Price)
                bigsmall_sell_detail = bigsmall_sell_1[['TranID', 'Time', 'Price', 'Volume']]
                bigsmall_sell_detail["date_time"] = [day + ' % s' % s for s in
                                                     bigsmall_sell_detail["Time"]]  # 新建一列date_time 为日期加时间
                bigsmall_sell_detail = bigsmall_sell_detail[['date_time', 'Price', 'Volume']]
                bigsmall_sell_detail_list.append(bigsmall_sell_detail)  # 大卖单详情之和

                bigsmall_sell_1 = bigsmall_sell_1['sum_s'].sum() / 10000
                bigsmall_sell_1 = round(bigsmall_sell_1)
                bigsellsmall_1.append(bigsmall_sell_1)
            else:
                bigsellsmall_1.append(0.0)
                temp_df = pd.DataFrame(
                    {"date_time": [day + ' 15:00:00'], "Price": ['0'], "Volume": ['0']})
                bigsmall_sell_detail_list.append(temp_df)
            stock_list.append(stock_code)

        #    print('bigbuysmall_detail_list:::::::',bigbuysmall_detail_list)


        except Exception as e:
            pass
        continue

    b_data = pd.concat(bigbuysmall_detail_list, axis=0, ignore_index=False, )  # 大买单详情df之和
    s_data = pd.concat(bigsmall_sell_detail_list, axis=0, ignore_index=False, )  # 大卖单详情之和
    b_data = b_data.dropna(axis=0, how='any', inplace=False)  ###3
    s_data = s_data.dropna(axis=0, how='any', inplace=False)  ###3
    #    buy_data = b_data.reset_index(drop=True)
    #   sell_data =  s_data.reset_index(drop=True)
    #    print("buy_data11111::::::", buy_data)
    #    print("sell_data11111::::::", sell_data)

    c = {'日期': excel_date_list,
         'ts_code': stock_list,
         '大单买小单卖1': bigbuysmall_1,
         '大单卖小单买1': bigsellsmall_1
         }
    data_n = DataFrame(c)
    data_n = data_n.dropna(axis=0, how='any', inplace=False)  ###3
    #    q.put(data_n)

    return (data_n,b_data,s_data)
