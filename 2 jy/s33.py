#!/usr/bin/env Python
# coding=utf-8
# import scheduler

#首先声明此程序仅供学习交流使用，不可商用不可实盘否则后果自负！
#首先声明此程序仅供学习交流使用，不可商用不可实盘否则后果自负！
#首先声明此程序仅供学习交流使用，不可商用不可实盘否则后果自负！

import sys
import os
import tkinter as tk
from apscheduler.schedulers.blocking import  BlockingScheduler as sche

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import urllib.request
from urllib.request import Request, urlopen
import data_def
import urllib.parse
import mystock
# import io
#from datetime import datetime
import json
import myhttp
import ntplib
import socket

# import tushare as ts
import pandas as pd
import db
# import time
# import datetime as dt
import datetime
#from multiprocessing import Pool
# from ftplib import FTP
# import time
# import getL2Data
# import ftp
from functools import reduce

# import multiprocessing
from multiprocessing.managers import BaseManager
import threading
import buy_mission_file221010 as buy_mission_file
import sell_mission221010 as sell_mission
# import com

#多进程共享变量
# 锁可以通过global也可以在Process中传无所谓
share_lock = threading.Lock()
manager = BaseManager()
# 一定要在start前注册，不然就注册无效
manager.register('Global_jiaoyi', data_def.Global_jiaoyi)
manager.start()
G_jy = manager.Global_jiaoyi()

scheduler = sche()

from concurrent.futures import ProcessPoolExecutor as Pool
from itertools import repeat
from multiprocessing import current_process
import time

def pid():
    return current_process().pid

jiaoyi_log_df = pd.DataFrame(
    {"ts_code": ['1'], "buyprice": [1], "buytime": ['1'], "sellprice": [1], "selltime": ['1'], "shouyi": [1],
     "selltype": ['1'], "state": ['1'], })  ##state:chiyou\selled\cirisell     次日sell
if jiaoyi_log_df.empty == False:
    jiaoyi_log_df.drop(jiaoyi_log_df.index, inplace=True)  # 清空dataframe

tempList =[]
test_len_600 = 1000

###############################################################


#################################
def jiaoyi_main():
    
    dt = datetime.datetime.now()  # 创建一个datetime类对象
    time_now_1 = dt.strftime('%H:%M:%S')  
   # print('time_now_1',time_now_1)
    if(time_now_1<='09:30:00'):  #TT
        ntp_server1 = 'ntp1.aliyun.com'
        ntp_server2 = 'ntp2.aliyun.com'
        ntp_server3 = 'ntp3.aliyun.com'
        ntp_server4 = 'ntp4.aliyun.com'
        ntp_server5 = 'ntp5.aliyun.com'
        client = ntplib.NTPClient()
        socket.setdefaulttimeout(2)   #3s chaoshi

        try:
            response = client.request(ntp_server5)
        except e:
            try:
                response = client.request(ntp_server4)
            except e:
                try:
                    response = client.request(ntp_server3)
                except e:
                    try:
                        response = client.request(ntp_server2)
                    except e:
                        try:
                            response = client.request(ntp_server1)
                        except e:
                            print('time out')
        unix_timestamp = response.tx_time
        ntp_time = datetime.datetime.fromtimestamp(unix_timestamp)
        formatted_time = ntp_time.strftime('%H%M%S')
        time_now = ntp_time.strftime('%H:%M:%S')  # H应该是24小时制
        if len(formatted_time)>4:
            print('ntp time:',formatted_time)
        else:
            print('ntp time 获取失败')
    else:
        formatted_time='093001'
        time_now = time_now_1
    #T
#    formatted_time='092840'
#    temp_buy_df = pd.DataFrame({"ts_code": '000777', "buyprice": 2.11, "buytime": '20231127', "sellprice": 0, #"selltime": '',"shouyi": 0, "selltype": 0, "state": 'chiyou',}, index=[0])
#    db.append_todb(temp_buy_df, 'jiaoyi_log')
    #T
   # print('time_now_1',time_now_1)

    if(time_now_1>='09:00:00')and(time_now_1<='15:00:00'):
       # print('okokok',time_now_1)
        try:
            print('try',time_now_1)
            myhttp.http_request_fromVc('time', '0', [formatted_time],1)
            time.sleep(0.001)
            print('1号机time...ok')
            myhttp.http_request_fromVc('time', '0', [formatted_time],2)
            time.sleep(0.001)
            print('2号机time...ok')
        except Exception as e:
            print(e)


    global share_lock

    today = dt.strftime('%Y%m%d')
    #today='20221113'
    #time_now = ntp_time.strftime('%H:%M:%S')  # H应该是24小时制
    #T
    #time_now = '09:28:20'
    #time_now = '09:30:01'
    #T
    print('启动交易 ',today, time_now)
    shift1_date = '0'
    forword_day = 30  # 向前30天
    sqlTables_forward = mystock.date2sqlTable('20220701', today)
    if today == sqlTables_forward[-1][1:]:
        shift1_date = sqlTables_forward[-2][1:]
        shift5_date = sqlTables_forward[-6][1:]
    else:
        shift1_date = sqlTables_forward[-1][1:]
        shift5_date = sqlTables_forward[-5][1:]

    # 启动程序时如果已经过了9:00了 一样药顺序执行一遍下列功能
    # 8:30首先计算今天要买的buy_stocks_list

    # sell_lastsum_df空  那么创建他
    sell_lastsum_c = pd.DataFrame(
        {"ts_code": ['000000'], "last_sum100": [0], "last_sum200": [0],'is_awayed':[0],"is_zhaban": [0]})
    G_jy.set_sell_lastsum_df(sell_lastsum_c)

    datas0_t = pd.DataFrame({"ts_code": [0],"trade_date": [0],"chunmai50": [0],"chunmai100": [0],"chunmai200": [0],
                             "abs100": [0],"Price_x": [0],"Price_y": [0],"mean100": [0],"sum50": [0],
                             "sum100": [0],"sum200": [0],"last_sum100": [0],"last_sum200": [0],"last_status": [0],})
    G_jy.set_datas0_2(datas0_t)

    #选股入股池 启动买入前计算 并准备前40天数据为买
    if (time_now < '00:02:00'):
        scheduler.add_job(func=back_jisuan, id='buy_jisuan', args=(today, shift1_date), trigger='cron',
                              day_of_week='0-4', hour=8, minute=00,
                              second=00, replace_existing=True)
        print('add_job  buy_jisuan 08:00:00')
    else:
        back_jisuan(today,shift1_date)
        scheduler.add_job(func=back_jisuan, id='buy_jisuan', args=(today, shift1_date),
                              trigger='cron', day_of_week='0-4', hour=8, minute=00,
                              second=00, replace_existing=True)
        print('back_jisuan  now')

    # 9：15读取持仓情况 剩余资金、 持仓stocks、持仓股数、买入价格、买入时间
    if (time_now < '01:00:00'):
        scheduler.add_job(func=chicangqk, id='chicangqk_for_sell', args=(today,1),  #获取持仓 并准备数据
                              trigger='cron', day_of_week='0-4', hour=1, minute=0,
                              second=0, replace_existing=True)
        print('add_job  chicangqk_for_sell 09:00:00')
    else:
        #T
        chicangqk(today,1)  #获取持仓 并准备数据为卖
        #T
        scheduler.add_job(func=chicangqk, id='chicangqk_for_sell', args=(today,1),  #获取持仓 并准备数据
                              trigger='cron', day_of_week='0-4', hour=1, minute=0,
                              second=0, replace_existing=True)
        print('add_job  chicangqk_for_sell 09:00:00')
    # 9：30计算持仓股票 计算卖出
    if time_now < '09:30:00':

        scheduler.add_job(func=strat_sell_mission, id='strat_sell_buy_mission_AM', args=(today,0,G_jy,shift1_date),   #0--9:00分卖出当日买次日卖出的
                              trigger='cron', day_of_week='0-4', hour=9, minute=30,
                              second=0, replace_existing=True)
        print('add_job  strat_sell_mission 09:30:00')
    else:

        strat_sell_mission(today,0,G_jy,shift1_date)#0--9:00分卖出当日买次日卖出的
        scheduler.add_job(func=strat_sell_mission, id='strat_sell_buy_mission_AM', args=(today,0,G_jy,shift1_date),   #0--9:00分卖出当日买次日卖出的
                              trigger='cron', day_of_week='0-4', hour=9, minute=30,
                              second=0, replace_existing=True)
        print('now  strat_sell_mission 09:30:00')


        # if (time_now > '14:47:00') and (time_now < '14:55:00'):
        #     strat_buy_mission1447(today, shift1_date, G_jy,1447)
        #     print('now strat_buy_mission935 09:35:00<time<10:00:00')

    # # 9:30 先卖出！一定是先卖出 “当天差值满足次日卖出” 以open价、9:30实时价格、启动时的实时格卖出， 和其他条件卖出
    if time_now < '09:30:30':
        scheduler.add_job(func=strat_buy_mission930, id='strat_buy_mission_930', args=(today, shift1_date, G_jy,930,shift5_date),
                              trigger='cron', day_of_week='0-4', hour=9, minute=30,
                              second=30, replace_existing=True)
        print('add_job  strat_buy_mission93030 ')
    elif time_now<'13:00:00':
        strat_buy_mission930(today, shift1_date, G_jy,930,shift5_date)
        scheduler.add_job(func=strat_buy_mission930, id='strat_buy_mission_930', args=(today, shift1_date, G_jy,930,shift5_date),
                              trigger='cron', day_of_week='0-4', hour=9, minute=30,
                              second=30, replace_existing=True)
        print('now strat_buy_mission930 09:30:30')

    if time_now < '09:31:30':
        scheduler.add_job(func=strat_buy_mission930_2, id='strat_buy_mission_930_2', args=(today, shift1_date, G_jy,930,shift5_date),
                              trigger='cron', day_of_week='0-4', hour=9, minute=31,
                              second=30, replace_existing=True)
        print('add_job  strat_buy_mission93030 ')
    elif time_now<'13:00:00':
        strat_buy_mission930_2(today, shift1_date, G_jy,930,shift5_date)
        scheduler.add_job(func=strat_buy_mission930_2, id='strat_buy_mission_930_2', args=(today, shift1_date, G_jy,930,shift5_date),
                              trigger='cron', day_of_week='0-4', hour=9, minute=31,
                              second=30, replace_existing=True)
        print('now strat_buy_mission930 09:30:30')

    # 11:30停止所有任务
    if (time_now < '11:30:00'):
        scheduler.add_job(func=end_all_trade, id='end_all_trade1130', args=(1,today,shift1_date,shift5_date),
                              trigger='cron', day_of_week='0-4', hour=11, minute=30,
                              second=0, replace_existing=True)
        print('add_job  end_all_trade1130 11:30:00')
    else:
        if time_now < '14:55:00':  # 11:30~13:00之间启动程序的话
            end_all_trade(1,today,shift1_date ,shift5_date)
            scheduler.add_job(func=end_all_trade, id='end_all_trade1130', args=(1,today,shift1_date,shift5_date),
                                  trigger='cron', day_of_week='0-4', hour=11, minute=30,
                                  second=0, replace_existing=True)
            print('add_job  end_all_trade1130 11:30:00')


    # 14:55停止所有任务  尾盘集合竞价 14：55后已不能下单了
    if time_now > '14:57:00':
        end_all_trade(0, today,shift1_date,shift5_date)
        scheduler.add_job(func=end_all_trade, id='end_all_trade1500', args=(0,today,shift1_date,shift5_date),
                              trigger='cron', day_of_week='0-4', hour=14, minute=57,
                              second=0, replace_existing=True)
        print('add_job  end_all_trade1500 14:57:00')
    else:
        scheduler.add_job(func=end_all_trade, id='end_all_trade1500', args=(0,today,shift1_date,shift5_date),
                              trigger='cron', day_of_week='0-4', hour=14, minute=57,
                              second=0, replace_existing=True)
        print('add_job  end_all_trade1500 14:57:00')
    # 之后2分钟读取持仓情况 剩余资金、 持仓stocks、持仓股数、买入价格、买入时间
    # 如果没有持仓该股  买入 如果剩余资金买的起   用剩余资金（10000左右）买buy_stocks_list其中买的起的
    # （买入--即时买入
    #  再读取持仓情况 剩余资金、 持仓stocks、持仓股数、买入价格、买入时间
    # 持仓了相应股数就从buy_stocks_list去除一个
    # 再买入  直到buy_stocks_list买完

    # 之后 读取持仓情况 剩余资金、 持仓stocks、持仓股数、买入价格、买入时间
    # 计算一遍sell_init 得到当天要卖掉多少差值 等条件符合就卖出
    # 轮询sell_init_amo  满足条件就卖出
    scheduler.start()
    return 500

##############################################################
#准备交易：在9：15运行chicangqk()后，紧接着运行此函数.
# 作用：1、上传ftp需要跟踪卖的股票；
def zhunbei_sell_data(today):
    #ntp_server = 'ntp1.aliyun.com'
    #client = ntplib.NTPClient()
    #response = client.request(ntp_server)
    #unix_timestamp = response.tx_time
    #ntp_time = datetime.datetime.fromtimestamp(unix_timestamp)
    #formatted_time = ntp_time.strftime('%H%M%S')
    #print(formatted_time)

    ts_code_forsell_list=[]
    chicang_df_t = pd.DataFrame()
    
    # 添加一个G_jy.selljisuan_df_list  把today交易前30日的数据都算出来作为全局变量  卖出时直接调用  只算一次省cpu时间
    chicang_df = G_jy.get_global_chichang_df()
    if chicang_df.empty==False:
        chicang_df_t = chicang_df.loc[chicang_df.可用股份 >= '1']
        ts_code_forsell_list = chicang_df_t.证券代码.tolist()   #持仓list

    #股池中的股票list  和粗选list
    guchi_buy_list = G_jy.get_buy_list()
    guchi_buy_list_1 = []
    guchi_buy_list_2 = []


    if len(guchi_buy_list)>=980:  #mvc设计为800只（不加ts_code_forsell_list的800只）
        len21 = int(len(guchi_buy_list)/2)

        guchi_buy_list_1 = guchi_buy_list[0:len21]
        guchi_buy_list_2 = guchi_buy_list[len21:len(guchi_buy_list)]

        print('上传列表中...1号机')
        myhttp.http_request_fromVc('cang', '0', ts_code_forsell_list,1)
        time.sleep(0.001)
        myhttp.http_request_fromVc('up', '0', guchi_buy_list_1,1)
        print('上传okok...1号机')
        myhttp.http_request_fromVc('up', '0', guchi_buy_list_2, 2)
        print('上传okok...2号机end')

        #TT
        # shift1_date = '20221114'
        # strat_buy_mission930(today, shift1_date, G_jy,930)
        #TT
    else:
        guchi_buy_list_1 = guchi_buy_list
        print('上传列表中...1号机')
        myhttp.http_request_fromVc('cang', '0', ts_code_forsell_list, 1)
        time.sleep(0.001)
        myhttp.http_request_fromVc('up', '0', guchi_buy_list_1, 1)
        print('上传okok...1号机end')



    # 读取粗选300只股在数据库中的数据，前n天的数据---在backjisuan中实现了

    # 开始实时数据的转换
    # com.L2_from_com(up_txt_list, G_jy, )

    G_jy.set_buy_list1(guchi_buy_list_1)
    G_jy.set_buy_list2(guchi_buy_list_2)
    temp_cnt = 0
    G_jy.set_mission930_20_cnt(temp_cnt)

    global share_lock
    share_lock.acquire()
    G_jy.set_jiaoyi_locked(0)
    share_lock.release()

    print('卖出数据准备完毕')
    return


###########################################################
# buy_list = back_jisuan(forward_day,shift1_date)
# shift1_date 前一交易日日期
#启动买入前计算
#  @b1
def back_jisuan(today, shift1_date):
    dt = datetime.datetime.now()  # 创建一个datetime类对象
    time_now_2 = dt.strftime('%H:%M:%S')  
 
    if time_now_2>'15:30:00':
        shift1_date = today
        tomorrow = datetime.datetime.today() + datetime.timedelta(days=1)
        today = tomorrow.strftime('%Y%m%d')  
        print('tomorrow',tomorrow)


    if 1:
        buy_list = []
        print('启动买入前计算 并准备前115天数据' + shift1_date)

        try:
            filename = 'buy_list_'+today+'.txt'
            with open(filename, 'r') as file:
                buy_list = [line.strip() for line in file]

            print('buy_list_'+today+'.txt'+'读取成功')
            print('通过读取今日待买股票', len(buy_list), '只  ', buy_list)

            cuxuan_all_df = pd.read_csv( today +'_cuxuan_all_df_shipan.csv',
                                  dtype={'ts_code':str,'trade_date': str})  # 读取训练数据
            # print('read_csv',cuxuan_all_df.tail(10))
            G_jy.set_cuxuan_all_df(cuxuan_all_df)
        except :# Exception as e:

            # @b3  ######################
            buy_list_t = buy_mission_file.buy_stock_jisuan(shift1_date, today,G_jy)
            #############################


            buy_df = pd.DataFrame()
            buy_df['ts_code'] = buy_list_t
            db.replace_todb(buy_df, 'buy'+shift1_date)

            # buy_list_t= buy_list_t[0:6]


            for ts_code in buy_list_t:
                buy_list.append(ts_code[0:6])

            save_list_to_txt(buy_list,'buy_list_'+today+'.txt')
            ###Y    stock_list = list(set( buy_lst ))  # 去重   （会影响好坏排序）
            print('通过分析',shift1_date, '选出今日待买股票', len(buy_list),'只  ',buy_list)



    global share_lock
    share_lock.acquire()
    G_jy.set_buy_list(buy_list)
    share_lock.release()

    return


######################################################
def save_list_to_txt(lst,filename):
    with open(filename,'w')as file:
        for item in lst:
            file.write(str(item)+'\n')

def read_list_from_txt(filename):
    try:
        with open(filename,'r') as file:
            lst=[line.strip()for line in file]
            return  lst
    except FileNotFoundError:
            return []
######################################################
seconds_perday = 24 * 60 * 60









##############################################ok  vc++
def jisuan_dadan(csv_df,  tscode):
    bigbuysmall_detail_list = []
    bigsmall_sell_detail_list = []

    bigbuysmall_detail_list.clear()  # 清空list
    bigsmall_sell_detail_list.clear()

    bigbuysmall_detail_h = bigsmall_sell_detail_h = pd.DataFrame(
        {"TranID": ['1'], "date_time": ['1'], "Price": ['1'], "Volume": ['1']})

    temp_df50b = pd.DataFrame(
        {"sum_buy50": [0],  "Time": ['0'], "Price": [0], "Volume": [0]})
    temp_df50s = pd.DataFrame(
        { "sum_sell50": [0], "Time": ['0'], "Price": [0], "Volume": [0]})
    temp_dfb = pd.DataFrame(
        {"sum_buy100": [0], "Time": ['0'], "Price": [0], "Volume": [0]})
    temp_dfs = pd.DataFrame(
        {"sum_sell100": [0], "Time": ['0'], "Price": [0], "Volume": [0]})
    temp_df200b = pd.DataFrame(
        {"sum_buy200": [0], "Time": ['0'], "Price": [0], "Volume": [0]})
    temp_df200s = pd.DataFrame(
        {"sum_sell200": [0], "Time": ['0'], "Price": [0], "Volume": [0]})

    if bigbuysmall_detail_h.empty == False:
        bigbuysmall_detail_h.drop(bigbuysmall_detail_h.index, inplace=True)  # 清空dataframe
    if bigsmall_sell_detail_h.empty == False:
        bigsmall_sell_detail_h.drop(bigsmall_sell_detail_h.index, inplace=True)  # 清空dataframe

    bigbuysmall_detail100 = pd.DataFrame()
    bigsmall_sell_detail100 = pd.DataFrame()
    bigbuysmall_detail200 = pd.DataFrame()
    bigsmall_sell_detail200 = pd.DataFrame()
    bigsmall_sell_100 = pd.DataFrame()
    bigsmall_sell_200 = pd.DataFrame()
    bigsmall_200 = pd.DataFrame()
    bigsmall_100 = pd.DataFrame()

    try:
        ddf = csv_df
        if tscode[0] == '6':
            ddf.BuyOrderPrice = ddf.Price
            ddf.SaleOrderPrice = ddf.Price

        # 50万

        bigsmall_50 = ddf.loc[((ddf.BuyOrderVolume * ddf.BuyOrderPrice) >= 500000) & (
                (ddf.SaleOrderVolume * ddf.SaleOrderPrice) < 500000)]
        if bigsmall_50.empty == False:
            bigsmall_50['sum_buy50'] = (bigsmall_50.Volume * bigsmall_50.Price)
            bigbuysmall_detail50 = bigsmall_50[['sum_buy50', 'Time', 'Price', 'Volume']]
            sum_buy50_z = bigbuysmall_detail50['sum_buy50'].sum()
            Time = bigbuysmall_detail50.iloc[[-1],].Time.tolist()[0]
            Price = bigbuysmall_detail50.iloc[[-1],].Price.tolist()[0]
        else:
            bigbuysmall_detail50 = temp_df50b
            sum_buy50_z = 0
            # sum_buy100_z = 0
            # Time = 0
            # Price = 0
        # 卖单收小单资金      Sql = "select sum(Volume * Price)/10000 From [" & f & "] a where BuyOrderVolume * BuyOrderPrice<=" & bigdeal & _
        # " and SaleOrderVolume * SaleOrderPrice > " & bigdeal
        bigsmall_sell_50 = ddf.loc[((ddf.BuyOrderVolume * ddf.BuyOrderPrice) <= 500000) & (
                (ddf.SaleOrderVolume * ddf.SaleOrderPrice) > 500000)]
        #        if bigsmall_sell.isnull==True:
        if bigsmall_sell_50.empty == False:
            bigsmall_sell_50['sum_sell50'] = (bigsmall_sell_50.Volume * bigsmall_sell_50.Price)
            bigsmall_sell_detail50 = bigsmall_sell_50[['sum_sell50', 'Time', 'Price', 'Volume']]
            sum_sell50_z = bigsmall_sell_detail50['sum_sell50'].sum()
            Time = bigsmall_sell_detail50.iloc[[-1],].Time.tolist()[0]
            Price = bigsmall_sell_detail50.iloc[[-1],].Price.tolist()[0]
        else:
            bigsmall_sell_detail50 = temp_df50s
            sum_sell50_z = 0
            # sum_sell100_z = 0
            # Time = 0
            # Price = 0
        # ==============================================================50 end

        # 100万

        bigsmall_100 = ddf.loc[((ddf.BuyOrderVolume * ddf.BuyOrderPrice) >= 1000000) & (
                (ddf.SaleOrderVolume * ddf.SaleOrderPrice) < 1000000)]
        if bigsmall_100.empty == False:
            bigsmall_100['sum_buy100'] = (bigsmall_100.Volume * bigsmall_100.Price)
            bigbuysmall_detail100 = bigsmall_100[['sum_buy100', 'Time', 'Price', 'Volume']]
            sum_buy100_z = bigbuysmall_detail100['sum_buy100'].sum()
            Time = bigbuysmall_detail100.iloc[[-1],].Time.tolist()[0]
            Price = bigbuysmall_detail100.iloc[[-1],].Price.tolist()[0]
        else:
            bigbuysmall_detail100 = temp_dfb
            sum_buy100_z = 0
            Time = 0
            Price = 0
        # 卖单收小单资金      Sql = "select sum(Volume * Price)/10000 From [" & f & "] a where BuyOrderVolume * BuyOrderPrice<=" & bigdeal & _
        # " and SaleOrderVolume * SaleOrderPrice > " & bigdeal
        bigsmall_sell_100 = ddf.loc[((ddf.BuyOrderVolume * ddf.BuyOrderPrice) <= 1000000) & (
                (ddf.SaleOrderVolume * ddf.SaleOrderPrice) > 1000000)]
        #        if bigsmall_sell.isnull==True:
        if bigsmall_sell_100.empty == False:
            bigsmall_sell_100['sum_sell100'] = (bigsmall_sell_100.Volume * bigsmall_sell_100.Price)
            bigsmall_sell_detail100 = bigsmall_sell_100[['sum_sell100', 'Time', 'Price', 'Volume']]
            sum_sell100_z = bigsmall_sell_detail100['sum_sell100'].sum()
            Time = bigsmall_sell_detail100.iloc[[-1],].Time.tolist()[0]
            Price = bigsmall_sell_detail100.iloc[[-1],].Price.tolist()[0]
        else:
            bigsmall_sell_detail100 = temp_dfs
            sum_sell100_z = 0
            Time = 0
            Price = 0
        # ==============================================================

        # 200万

        bigsmall_200 = ddf.loc[((ddf.BuyOrderVolume * ddf.BuyOrderPrice) >= 2000000) & (
                (ddf.SaleOrderVolume * ddf.SaleOrderPrice) < 2000000)]
        if bigsmall_200.empty == False:
            bigsmall_200['sum_buy200'] = (bigsmall_200['Volume'] * bigsmall_200['Price'])
            bigbuysmall_detail200 = bigsmall_200[['sum_buy200', 'Time', 'Price', 'Volume']]
            sum_buy200_z = bigbuysmall_detail200['sum_buy200'].sum()
            # Time = bigsmall_sell_200.iloc[[-1],].Time.tolist()[0]
            # Price = bigsmall_sell_200.iloc[[-1],].Price.tolist()[0]
        else:
            bigbuysmall_detail200 = temp_df200b
            sum_buy200_z = 0
            # Time = 0
            # Price = 0
        # 卖单收小单资金      Sql = "select sum(Volume * Price)/10000 From [" & f & "] a where BuyOrderVolume * BuyOrderPrice<=" & bigdeal & _
        # " and SaleOrderVolume * SaleOrderPrice > " & bigdeal
        bigsmall_sell_200 = ddf.loc[((ddf.BuyOrderVolume * ddf.BuyOrderPrice) <= 2000000) & (
                (ddf.SaleOrderVolume * ddf.SaleOrderPrice) > 2000000)]
        #        if bigsmall_sell.isnull==True:
        if bigsmall_sell_200.empty == False:
            bigsmall_sell_200['sum_sell200'] = (bigsmall_sell_200.Volume * bigsmall_sell_200.Price)
            sum_sell200_z = bigsmall_sell_200['sum_sell200'].sum()
        # Time = bigsmall_sell_200.iloc[[-1],].Time.tolist()[0]
        # Price = bigsmall_sell_200.iloc[[-1],].Price.tolist()[0]
        # bigsmall_sell_detail200 = bigsmall_sell_200[['sum_sell200', 'Time', 'Price', 'Volume']]
        else:
            bigsmall_sell_detail200 = temp_df200s
            sum_sell200_z = 0
        # Time= 0
        # Price = 0
        # ==============================================================


    except Exception as e:
        pass
        print("没插硬盘,或路径不对"+tscode)
        chazhi50 = 0
        chazhi100 = 0
        chazhi200 = 0
        Time=0
        Price_x=0
        Price_y=0

    chazhi50 =  (sum_buy50_z - sum_sell50_z)/10000
    chazhi100 =  (sum_buy100_z - sum_sell100_z)/10000
    chazhi200 = (sum_buy200_z - sum_sell200_z)/10000


    data = {
        "chazhi50":[chazhi50],
        "chazhi100":[chazhi100],
        "chazhi200": [chazhi200],
        "Time":[Time],
        "Price_x":[Price],
        "Price_y": [Price]
    }
    res_df = pd.DataFrame(data)

    if bigsmall_sell_detail50.empty == False:
        bigsmall_sell_detail50.drop(bigsmall_sell_detail50.index, inplace=True)  # 清空dataframe
    if bigbuysmall_detail50.empty == False:
        bigbuysmall_detail50.drop(bigbuysmall_detail50.index, inplace=True)  # 清空dataframe
    if bigsmall_sell_detail200.empty == False:
        bigsmall_sell_detail200.drop(bigsmall_sell_detail200.index, inplace=True)  # 清空dataframe
    if bigbuysmall_detail200.empty == False:
        bigbuysmall_detail200.drop(bigbuysmall_detail200.index, inplace=True)  # 清空dataframe
    if bigsmall_sell_detail100.empty == False:
        bigsmall_sell_detail100.drop(bigsmall_sell_detail100.index, inplace=True)  # 清空dataframe
    if bigbuysmall_detail100.empty == False:
        bigbuysmall_detail100.drop(bigbuysmall_detail100.index, inplace=True)  # 清空dataframe

    if bigsmall_sell_50.empty == False:
        bigsmall_sell_50.drop(bigsmall_sell_50.index, inplace=True)  # 清空dataframe
    if bigsmall_sell_200.empty == False:
        bigsmall_sell_200.drop(bigsmall_sell_200.index, inplace=True)  # 清空dataframe
    if bigsmall_sell_100.empty == False:
        bigsmall_sell_100.drop(bigsmall_sell_100.index, inplace=True)  # 清空dataframe
    if bigsmall_50.empty == False:
        bigsmall_50.drop(bigsmall_50.index, inplace=True)  # 清空dataframe
    if bigsmall_100.empty == False:
        bigsmall_100.drop(bigsmall_100.index, inplace=True)  # 清空dataframe
    if bigsmall_200.empty == False:
        bigsmall_200.drop(bigsmall_200.index, inplace=True)  # 清空dataframe
    return res_df

############################
    # 个股结构
    # trade_date,ts_code,bbuys100,bsells100,bbuys200,bsells200,open,high,low,close,pct_chg,pe
    # chunmai100，chunmai200，abs100，mean100，sum100，sum200，buy_sell,buy_rate,sell_mean_100,sell_rate


##################实盘中这块last_sum100、last_sum200的更新位置和内容都不变  begin
    # 更新全局变量中的 last_sum_100 last_sum_200
    sell_lastsum_df = G_jy.get_sell_lastsum_df()
    if sell_lastsum_df.empty == False:
        sell_lastsum_df_1 = sell_lastsum_df.loc[sell_lastsum_df.ts_code == ts_code[0:6]]
        if sell_lastsum_df_1.empty == False:
            label_l = sell_lastsum_df_1.loc[sell_lastsum_df_1.ts_code == ts_code[0:6]].index
            sell_lastsum_df = sell_lastsum_df.drop(labels=label_l)
            new_last_sum100 = sum_100  # 'sum100'
            new_last_sum200 = sum_200  # 'sum200'
            new_is_awayed = is_awayed  # 'is_awayed'
            new_is_zhaban = is_zhaban

            sell_lastsum_temp = pd.DataFrame(
                {"ts_code": [ts_code[0:6]], "last_sum100": [new_last_sum100], "last_sum200": [new_last_sum200],'is_awayed':[new_is_awayed],'is_zhaban':[new_is_zhaban]})
            sell_lastsum_df = sell_lastsum_df.append(sell_lastsum_temp)
            # print('3333')
            G_jy.set_sell_lastsum_df(sell_lastsum_df)
    else:
        #sell_lastsum_df空  那么创建他
        sell_lastsum_temp = pd.DataFrame(
            {"ts_code": [ts_code[0:6]], "last_sum100": [sum_100], "last_sum200": [sum_200],'is_awayed':[is_awayed],'is_zhaban':[is_zhaban]})
        sell_lastsum_df = sell_lastsum_df.append(sell_lastsum_temp)
        G_jy.set_sell_lastsum_df(sell_lastsum_df)

##################实盘中这块last_sum100、last_sum200的更新位置和内容都不变 end

#####################算法有变修改这块2 begin
    if can_sell:
        # 把is_awayed 的log记录查出来更新
        chiyou_sql_2 = "select ts_code,buyprice,buytime,sellprice,selltime,shouyi,selltype,state,is_awayed FROM jiaoyi_log"
        # 数据库中持有的股票df 有字段买入日期
        tscode = ts_code[0:6]
        chiyou_stock_sqldf_2 = db.query_data1(chiyou_sql_2)
        chiyou_stock_sqldf_2.loc[
            (chiyou_stock_sqldf_2.ts_code == tscode) & (chiyou_stock_sqldf_2.state == 'chiyou'),
            'is_awayed'] = is_awayed
        chiyou_stock_sqldf_2.loc[
            (chiyou_stock_sqldf_2.ts_code == tscode) & (chiyou_stock_sqldf_2.state == 'chiyou'),
            'is_zhaban'] = is_zhaban
        db.replace_todb(chiyou_stock_sqldf_2, 'jiaoyi_log')


        single_stock_df = single_stock_df[['trade_date','ts_code','Time','buy_sell','Price_x','Price_y','min_price']]
        # print(single_stock_df)
        # if (price_rate >= 0.4) :
        #     print(single_stock_df)
        return single_stock_df  # 'sell_time'

        cur_row += 1
    single_stock_df = single_stock_df[['trade_date', 'ts_code', 'Time' ,'buy_sell', 'Price_x', 'Price_y','min_price']]
    return single_stock_df
###########################算法有变修改这块2 end

############################################3
# 交易API部分
############################################
def ChiCang(var_stock):
    have_stock = 0
    zjqk = 0
    cgsl = 0
    print("1010请求持仓情况" + var_stock)
    try:
        res = urllib.request.urlopen('http://100.100.107.232:7777/api/v1.0/portfolios/?key=911911')

        resn = res.read()

        user_dic = json.loads(resn)
        zjqk = user_dic['subAccounts']['人民币']  # 资金情况 dict
        print('zjqk',zjqk)
        for user_dic_list in user_dic['dataTable']['rows']:
            if user_dic_list[0] == var_stock:
                have_stock = 1
                cgsl = user_dic_list  # 持仓情况 list
                break
        ppp = 1
    except Exception:
        zjqk = 0
        cgsl = 0
        ppp = 0
        qq = ppp + 1
        print("请求持仓情况!失败！！失败！！111" + var_stock)
    return have_stock, zjqk, cgsl


def shishijiage(var_stock):
    price = 0  # 实在出错就报价10000
    yesterdayClose = 0
    open_today = 0

    var_stock = var_stock[0:6]
    print("请求实时价格" + var_stock)
    for ii in range(1):  # 请求5次 得到结果就break
        try:
            res = urllib.request.urlopen('http://100.100.107.232:7777/api/v1.0/quotes/' + var_stock + '?key=911911')
            resn = res.read()
            price_dict = json.loads(resn)
            round_up_t = price_dict['bid1']  # 买一价格
            price = round_up(round_up_t)  # 四舍五入 这步很重要！
            yesterdayClose_t = price_dict['yesterdayClose']  #
            yesterdayClose = round_up(yesterdayClose_t)
            open_today_t = price_dict['yesterdayClose']
            open_today = round_up(open_today_t)
            ppp = 1
            break
        except Exception as e:
            ppp = 0
            qq = ppp + 1
            price = 0  # 实在出错就报价10000
            print("请求实时价格!失败！！失败！！" + var_stock)
    return price,yesterdayClose,open_today



def round_up(value):
    # 替换内置round函数,实现保留2位小数的精确四舍五入
    return round(value * 100) / 100.0


def chengjiao_q(var_stock):
    cj_q = 0
    print("1请求成交情况" + var_stock)
    try:
        res = urllib.request.urlopen('http://100.100.107.232:7777/api/v1.0/orders?status=FILLED?key=911911')
        resn = res.read()
        user_dic = json.loads(resn)
        for user_dic_list in user_dic['dataTable']['rows']:
            if user_dic_list[3] == var_stock:
                # have_stock = 1
                cj_qt = user_dic_list  # 一条成交情况 list
                cj_q = cj_q + int(cj_qt[9])  # 多条成交求和 list
                break
        ppp = 1
    except Exception:
        cj_q = 0
        ppp = 0
        qq = ppp + 1
        print("请求成交情况!失败！！失败！！" + var_stock)
    return cj_q


###############################################################
def chicangqk(today,zhunbei_flag):  #0--读取持仓情况   1--读并准备数据   2--测试专用
    global share_lock

    # zhunbei_flag = 1

    if zhunbei_flag == 2: #测试专用

        ts_code_list = ['300790']  #测试时手动改
        # sell_mission.sell_stock_jisuan_zhunbei(today, 30, ts_code_list,G_jy)  #准备测试数据
        chicang_df = pd.DataFrame({"证券代码": ['300790'], "证券名称": ['万年青'], "证券数量": ['100'],"库存数量": ['100'],
                                   "可卖数量": ['100'], "最新价": ['2.46'], "最新市值": ['4.2'], "成本价": ['15.34']})
        zjqk ={'可用': 150000, }  #测试时手动改
        #zjqk['可用'] = 150000   #测试时手动改
        G_jy.set_global_chichang_df(chicang_df)
        G_jy.set_zjqk(zjqk)  # 资金情况 dict
        return 100

    share_lock.acquire()
    G_jy.set_jiaoyi_locked(1)
    share_lock.release()

    #宏源
    chicang_df = pd.DataFrame(
         {"0": ['1'], "1": ['1'], "2": ['1'], "3": ['1'], "4": ['1'], "5": ['1'], "6": ['1'], "7": ['1'],
          "8": ['1'], "9": ['1'], "10": ['1'], "11": ['1'], "12": ['1'], "13": ['1'], "14": ['1'],
          "15": ['1'], "16": ['1']
          })

    #国泰
  #  chicang_df = pd.DataFrame(
   #     {"0": ['1'], "1": ['1'], "2": ['1'], "3": ['1'], "4": ['1'], "5": ['1'], "6": ['1'], "7": ['1'],
   #      "8": ['1'], "9": ['1'], "10": ['1'], "11": ['1'], "12": ['1'], "13": ['1'],
   #      })
    if chicang_df.empty == False:
        chicang_df.drop(chicang_df.index, inplace=True)  # 清空dataframe

    zjqk = 0
    cgsl = 0
    socket.setdefaulttimeout(8)   #3s chaoshi
    print("1请求持仓情况")
    try:
        res = urllib.request.urlopen('http://100.100.107.232:7777/api/v1.0/portfolios?key=911911')

        resn = res.read()

        user_dic = json.loads(resn)
        zjqk = user_dic['subAccounts']['人民币']  # 资金情况 dict

        column = user_dic['dataTable']['columns']
        # 宏源
        #olumn[9] = '盈亏比例'  # 去掉原字段的%号   原来是‘盈亏比例%’
        # 国泰
        column[9] = '盈亏比例(%)'  #

        # column[16] = '备用'
        chicang_df.columns = column
        # 证券代码   证券名称  证券数量  可卖数量 最新价 最新市值  成本价
        if user_dic['count'] != 0:
            for dict_list in user_dic['dataTable']['rows']:
                dict_row_df = pd.DataFrame([dict_list])
                #            dict_row_df1 = dict_row_df[['0','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16',]]
                dict_row_df.columns = column

                chicang_df = pd.concat([chicang_df,dict_row_df])
        #T
        # else:
        #     chicang_df = pd.DataFrame({"证券代码": ['000789'], "证券名称": ['万年青'], "证券数量": ['100'], "可卖数量": ['100'], "最新价": ['16.36'], "最新市值": ['1'], "成本价": ['16.29']})
        #     zjqk['可用'] = 150000
        #T

            if chicang_df.empty == False:
                chicang_df_t = chicang_df.loc[chicang_df.可用股份 >= '1']
                chichang_list = chicang_df_t.证券代码.tolist()
            else:
                chichang_list = []
            # chichang_list = chicang_df.证券代码.tolist()
            print('chichang_list', chichang_list)

            share_lock.acquire()
            G_jy.set_global_chichang_df(chicang_df)
            share_lock.release()

        ppp = 1
        share_lock.acquire()
        G_jy.set_zjqk(zjqk)# 资金情况 dict
        share_lock.release()

    except Exception as e:
        zjqk = 0
        cgsl = 0
        ppp = 0
        qq = ppp + 1
        print(e)
        share_lock.acquire()
        G_jy.set_jiaoyi_locked(0)
        share_lock.release()

        print("请求持仓情况!失败！！失败！！")

    #这个准备是将买粗选和持仓卖的list ftp到数据机
    if zhunbei_flag == 1 :
        zhunbei_sell_data(today)  #准备已持有的股票数据(today可能卖出) 和 昨天选出准备买入的股票


    share_lock.acquire()
    G_jy.set_jiaoyi_locked(0)
    share_lock.release()

    return 100

###############################################################

def strat_sell_mission(today,t900_or_t902,G_jy,shift1_date):
    # test = G_jy.get_jiaoyi_locked()
    # tt=00
    dt = datetime.datetime.now()  # 创建一个datetime类对象
    today = dt.strftime('%Y%m%d')

    # 次日卖出的股票全部卖完后  stop_5_sec
    time_now = dt.strftime('%H:%M:%S')  # H应该是24小时制
    ppp=777

    # sell_mission.L2_com_5sec_start(G_jy)
    print('now strat_sell_mission',time_now)
    sell_mission.stock_5sec_sell_start(today,G_jy,shift1_date)
    scheduler.add_job(func=sell_mission.stock_5sec_sell_start, id='stock_5sec_sell_start', args=(today,G_jy,shift1_date),
                          trigger='interval',
                          seconds=5,
                          replace_existing=True)
    #scheduler.start()

    # #持仓数量
    # stock_num = 40
    # buy_mission_file.stock_5sec_buy_start(today, stock_num,G_jy)
    # scheduler.add_job(func=buy_mission_file.stock_5sec_buy_start, id='stock_5sec_buy_start', args=(today, stock_num,G_jy),
    #                       trigger='interval',
    #                       seconds=5,
    #                       replace_existing=True)
    #
    # print('add_job  stock_5sec_buy  ' + time_now)
    return
####################################################

def strat_buy_mission930(today, shift1_date, G_jy,timeint,shift5_date):
    #持仓数量
    stock_num = 2

    G_jy.set_930_flag(0) #初始化930标志

    buy_list_new = []
    cnt_1_time = 40

    zp50_200df = pd.DataFrame()
    zp50_200df1 = pd.DataFrame()
    zp50_200df2 = pd.DataFrame()

    flag_df1 = 0
    flag_df2 = 0

#    dt = datetime.datetime.now()  # 创建一个datetime类对象
#    time_now_2 = dt.strftime('%H:%M:%S')  
#    if time_now_2>="09:31:00":
        #print('sellp 35s')
        #time.sleep(35)

    # time.sleep(30)
#    zp50_200df1 = myhttp.http_request_fromVc('zzz', 0, ['0'], 1)  # 请求1号机的zp50_200df
#    zp50_200df2 = myhttp.http_request_fromVc('zzz', 0, ['0'], 2)  # 请求2号机的zp50_200df
    while cnt_1_time>0:  #运行10次 即30秒
        print('等待',flag_df1,flag_df2,'号zp50_200df...剩余',cnt_1_time,zp50_200df1,zp50_200df2)
        if flag_df1 and flag_df2:
            break
        time.sleep(1.5)
        if zp50_200df1.empty==True :
            zp50_200df1 = myhttp.http_request_fromVc('zzz', 0, ['0'], 1)  # 请求1号机的zp50_200df
        if zp50_200df1.empty==False and flag_df1==0:
            print('获得成功zp50_200df111 剩余cnt_1_time=',cnt_1_time,)
            flag_df1 = 1

        time.sleep(1.5)
        if zp50_200df2.empty==True :
            zp50_200df2 = myhttp.http_request_fromVc('zzz', 0, ['0'], 2)  # 请求2号机的zp50_200df
        if zp50_200df2.empty == False and flag_df2==0:
            print('获得成功zp50_200df222 剩余cnt_1_time=', cnt_1_time,)
            flag_df2 = 1

        cnt_1_time = cnt_1_time - 1
        


    zp50_200df = pd.concat([zp50_200df1,zp50_200df2])
    print('时间到 zp50_200df', zp50_200df)
    zp50_200df_z = zp50_200df.loc[zp50_200df.zp50>=1500]
    buy_list_new_t1 = zp50_200df_z.tscode.to_list()
    buy_list_new_t = list(set(buy_list_new_t1))


    global share_lock
    share_lock.acquire()
    G_jy.set_buy_list(buy_list_new_t)
    share_lock.release()

    ts_code_forsell_list=[]
    chicang_df_t = pd.DataFrame()
    chicang_df = G_jy.get_global_chichang_df()
    if chicang_df.empty==False:
        chicang_df_t = chicang_df.loc[chicang_df.可用股份 >= '1']
        if chicang_df_t.empty==False:
            ts_code_forsell_list = chicang_df_t.证券代码.tolist()   #持仓list

    # up_txt_list_t = ts_code_forsell_list + buy_list_new_t
    # up_txt_list = list(set(up_txt_list_t))
    # up_txt_list.sort(key=up_txt_list_t.index)  # 保持原始序列

    have_5day_list=[]
    chiyou_sql = "select ts_code,buytime FROM jiaoyi_log "  
    jiaoyilog_sqldf = db.query_data1(chiyou_sql)
    if jiaoyilog_sqldf.empty ==False:

        have_5day_df = jiaoyilog_sqldf.loc[jiaoyilog_sqldf.buytime >=shift5_date]
        if have_5day_df.empty ==False:
            have_5day_list = have_5day_df.ts_code.to_list()
            have_5day_list = list(set( have_5day_list ))






    buy_mission_file.stock_5sec_buy_start(today, stock_num, G_jy,zp50_200df,have_5day_list)

    scheduler.add_job(func=buy_mission_file.stock_5sec_buy_start, id='stock_5sec_buy_start', args=(today, stock_num, G_jy,zp50_200df,have_5day_list),
                          trigger='interval',
                          seconds=5,
                          replace_existing=True)
    print('add_job stock_5sec_buy_start   pm')
    #scheduler.start()


####################################################


def strat_buy_mission930_2(today, shift1_date, G_jy,timeint,shift5_date):

    try:
        scheduler.remove_job('strat_buy_mission_930')
    except Exception as e:
        ret = 0
    #持仓数量
    stock_num = 2

    G_jy.set_930_flag(0) #初始化930标志

    buy_list_new = []
    cnt_1_time = 40

    zp50_200df = pd.DataFrame()
    zp50_200df1 = pd.DataFrame()
    zp50_200df2 = pd.DataFrame()

    flag_df1 = 0
    flag_df2 = 0

#    dt = datetime.datetime.now()  # 创建一个datetime类对象
#    time_now_2 = dt.strftime('%H:%M:%S')  
#    if time_now_2>="09:31:00":
        #print('sellp 35s')
        #time.sleep(35)

    # time.sleep(30)
#    zp50_200df1 = myhttp.http_request_fromVc('zzz', 0, ['0'], 1)  # 请求1号机的zp50_200df
#    zp50_200df2 = myhttp.http_request_fromVc('zzz', 0, ['0'], 2)  # 请求2号机的zp50_200df
    while cnt_1_time>0:  #运行10次 即30秒
        print('等待',flag_df1,flag_df2,'号zp50_200df...剩余',cnt_1_time,zp50_200df1,zp50_200df2)
        if flag_df1 and flag_df2:
            break
        time.sleep(1.5)
        if zp50_200df1.empty==True :
            zp50_200df1 = myhttp.http_request_fromVc('zzz', 0, ['0'], 1)  # 请求1号机的zp50_200df
        if zp50_200df1.empty==False and flag_df1==0:
            print('获得成功zp50_200df111 剩余cnt_1_time=',cnt_1_time,)
            flag_df1 = 1

        time.sleep(1.5)
        if zp50_200df2.empty==True :
            zp50_200df2 = myhttp.http_request_fromVc('zzz', 0, ['0'], 2)  # 请求2号机的zp50_200df
        if zp50_200df2.empty == False and flag_df2==0:
            print('获得成功zp50_200df222 剩余cnt_1_time=', cnt_1_time,)
            flag_df2 = 1

        cnt_1_time = cnt_1_time - 1
        


    zp50_200df = pd.concat([zp50_200df1,zp50_200df2])
    print('时间到 zp50_200df', zp50_200df)
    zp50_200df_z = zp50_200df.loc[zp50_200df.zp50>=1500]
    buy_list_new_t1 = zp50_200df_z.tscode.to_list()
    buy_list_new_t = list(set(buy_list_new_t1))


    global share_lock
    share_lock.acquire()
    G_jy.set_buy_list(buy_list_new_t)
    share_lock.release()

    ts_code_forsell_list=[]
    chicang_df_t = pd.DataFrame()
    chicang_df = G_jy.get_global_chichang_df()
    if chicang_df.empty==False:
        chicang_df_t = chicang_df.loc[chicang_df.可用股份 >= '1']
        if chicang_df_t.empty==False:
            ts_code_forsell_list = chicang_df_t.证券代码.tolist()   #持仓list

    # up_txt_list_t = ts_code_forsell_list + buy_list_new_t
    # up_txt_list = list(set(up_txt_list_t))
    # up_txt_list.sort(key=up_txt_list_t.index)  # 保持原始序列


    have_5day_list=[]
    chiyou_sql = "select ts_code,buytime FROM jiaoyi_log "  
    jiaoyilog_sqldf = db.query_data1(chiyou_sql)
    if jiaoyilog_sqldf.empty ==False:

        have_5day_df = jiaoyilog_sqldf.loc[jiaoyilog_sqldf.buytime >=shift5_date]
        if have_5day_df.empty ==False:
            have_5day_list = have_5day_df.ts_code.to_list()
            have_5day_list = list(set( have_5day_list ))

    buy_mission_file.stock_5sec_buy_start(today, stock_num, G_jy,zp50_200df,have_5day_list)

    scheduler.add_job(func=buy_mission_file.stock_5sec_buy_start, id='stock_5sec_buy_start', args=(today, stock_num, G_jy,zp50_200df,have_5day_list),
                          trigger='interval',
                          seconds=5,
                          replace_existing=True)
    print('add_job stock_5sec_buy_start   pm')
    #scheduler.start()


####################################################

def done_callback(pool_obj_row):
    global tempList
    res = pool_obj_row.result()
    tempList.append(res)

    return

#######################################################
def str2int(s):
    return reduce(lambda x,y:x*10+y, map(lambda s:{'0':0, '1':1, '2':2, '3':3, '4':4, '5':5, '6':6, '7':7, '8':8, '9':9}[s], s))

DIGITS={'0':0,'1':1,'2':2,'3':3,'4':4,'5':5,'6':6,'7':7,'8':8,'9':9}
def str2float(s):
    s=s.split('.')
    if s[0]==0:
        return 0+reduce(lambda x,y:x/10+y , map(lambda x:DIGITS[x],s[1][::-1]))/10
    else:
        return reduce(lambda x,y:x*10+y,map(lambda x:DIGITS[x],s[0]))+reduce(lambda x,y:x/10+y , map(lambda x:DIGITS[x],s[1][::-1]))/10








def end_all_trade(shangwu, today,shift1_date,shift5_date):
    dt = datetime.datetime.now()  # 创建一个datetime类对象
    time_now = dt.strftime('%H:%M:%S')  # H应该是24小时制
    jobs = scheduler.get_jobs()
    for job in jobs:
        #        print("id: %s,name: %s, trigger: %s, next run: %s, handler: %s" % (
        #            job.id,job.name, job.trigger, job.next_run_time, job.func))
        try:
            scheduler.remove_job(job.id)
            print('停止任务：' + str(job.id))
        except Exception as e:
            ret = 0

    if shangwu == 1:
        # 12:50:00读取持仓情况 剩余资金、 持仓stocks、持仓股数、买入价格、买入时间
        if (time_now < '12:50:00'):
            scheduler.add_job(func=chicangqk, id='chichangqk_afternoon', args=(today, 1),
                                  trigger='cron', day_of_week='0-4', hour=12, minute=50,
                                  second=0, replace_existing=True)
        else:
            chicangqk(today, 1)
            scheduler.add_job(func=chicangqk, id='chichangqk_afternoon', args=(today, 1),
                                  trigger='cron', day_of_week='0-4', hour=12, minute=50,
                                  second=0, replace_existing=True)

        # 13:00:01计算持仓股票 计算卖出   01秒是为了卖出效果
        if (time_now < '13:00:00'):


            scheduler.add_job(func=strat_sell_mission, id='strat_sell_buy_mission_pm', args=(today,1,G_jy,shift1_date),   #1--下午
                                  trigger='cron', day_of_week='0-4', hour=13, minute=0,
                                  second=1, replace_existing=True)
            print('add_job  strat_sell_buy_mission_pm ')
            scheduler.add_job(func=strat_buy_mission930, id='strat__buy_mission_930pm',
                                  args=(today, shift1_date, G_jy, 930,shift5_date),
                                  trigger='cron', day_of_week='0-4', hour=13, minute=00,
                                  second=0, replace_existing=True)
            print('add_job  strat_buy_mission1300 ')

            scheduler.add_job(func=end_buy_trade_1440, id='end_buy_trade_1440',
                                  args=(),
                                  trigger='cron', day_of_week='0-4', hour=14, minute=40,
                                  second=0, replace_existing=True)
            print('add_job  end_buy_trade_1440 ')
            # scheduler.add_job(func=strat_buy_mission1447, id='strat_buy_mission1447', args=(today, shift1_date, G_jy,1447),
            #                       trigger='cron', day_of_week='0-4', hour=14, minute=47,
            #                       second=0, replace_existing=True)
            # print('add_job  strat_buy_mission1447 ')
        else:

            strat_sell_mission(today,1,G_jy,shift1_date)
            scheduler.add_job(func=strat_sell_mission, id='strat_sell_buy_mission_pm', args=(today,1, G_jy,shift1_date),
                                  trigger='cron', day_of_week='0-4', hour=13, minute=0,
                                  second=1, replace_existing=True)
            print('now  strat_sell_buy_mission_pm ')
            strat_buy_mission930(today, shift1_date, G_jy, 1300,shift5_date)
            scheduler.add_job(func=strat_buy_mission930, id='strat__buy_mission_930pm',
                                  args=(today, shift1_date, G_jy, 930,shift5_date),
                                  trigger='cron', day_of_week='0-4', hour=13, minute=00,
                                  second=0, replace_existing=True)
            print('add_job  strat_buy_mission1300 ')
            # end_buy_trade_1440()
            scheduler.add_job(func=end_buy_trade_1440, id='end_buy_trade_1440',
                                  args=(),
                                  trigger='cron', day_of_week='0-4', hour=14, minute=40,
                                  second=0, replace_existing=True)
            print('add_job  end_buy_trade_1440 ')

            # if (time_now > '14:47:00') and (time_now < '14:50:00'):
            #     strat_buy_mission1447(today, 0, G_jy,)
            #     scheduler.add_job(func=strat_buy_mission1447, id='strat_buy_mission1447', args=(today, shift1_date, G_jy,1447),
            #                           trigger='cron', day_of_week='0-4', hour=14, minute=47,
            #                           second=0, replace_existing=True)
            #     print('now  strat_buy_mission1447 ')

        # 14:55停止所有任务
        if (time_now < '14:57:00'):
            scheduler.add_job(func=end_all_trade_15, id='end_all_trade_15', args=(0, today),
                                  trigger='cron', day_of_week='0-4', hour=14, minute=57,
                                  second=0, replace_existing=True)
        else:
            end_all_trade_15(0, today)
            scheduler.add_job(func=end_all_trade_15, id='end_all_trade_15', args=(0, today),
                                  trigger='cron', day_of_week='0-4', hour=14, minute=57,
                                  second=0, replace_existing=True)
    else:
        # 14:55停止所有任务
        if (time_now < '14:57:00'):
            scheduler.add_job(func=end_all_trade_15, id='end_all_trade_15', args=(0, today),
                                  trigger='cron', day_of_week='0-4', hour=14, minute=57,
                                  second=0, replace_existing=True)
        else:
            end_all_trade_15(0, today)
            print('end_all_trade_15 111')
            scheduler.add_job(func=end_all_trade_15, id='end_all_trade_15', args=(0, today),
                                  trigger='cron', day_of_week='0-4', hour=14, minute=57,
                                  second=0, replace_existing=True)

    #scheduler.start()
    return
###############################################
def end_all_trade_15(shangwu, today):

    jobs = scheduler.get_jobs()
    for job in jobs:
        #        print("id: %s,name: %s, trigger: %s, next run: %s, handler: %s" % (
        #            job.id,job.name, job.trigger, job.next_run_time, job.func))
        try:
            scheduler.remove_job(job.id)
            print('停止任务：' + str(job.id))
        except Exception as e:
            ret = 0
    return
###############################################
def end_buy_trade_1440():
    try:
        scheduler.remove_job('stock_5sec_buy_start')
        print('停止任务： stock_5sec_buy_start' )
    except Exception as e:
        ret = 0
    return

##test################################################
def sell_mission_test():
    today = '20220627'    #测试时手动改为卖出日期   并把对应的csv文件放入 策略机C:\from_ftp
    chicangqk(today,2)  #测试专用   进入函数中修改参数
    stock_5sec_sell_start_test(today, G_jy)  #测试时 在数据库jiaoyi_log中添加一行持仓数据

    return 100

##test##########################################
def stock_5sec_sell_start_test(today,G_jy):
    global tempList
    chicang_df = G_jy.get_global_chichang_df()  # 获得持仓情况   global_chichang_df %get_global_chichang_df()
#    print('chicang_df',chicang_df)
    chicang_df_t = chicang_df.loc[chicang_df.可用股份 >= '1']
    ts_code_list = chicang_df_t.证券代码.tolist()

    res_back = []
    tempList = []
    item_list =[]
    if chicang_df_t.empty == False:  # 如果非空

        buy_df = chicang_df_t.reset_index(drop=True)

        for t_index in buy_df.index.tolist():
            buy_df_row = buy_df.loc[(buy_df.index == t_index)]  # 取df中的一行
            tscode_T1 = buy_df_row.证券代码.tolist()[0]
        #    if tscode_T1[0] == '6':
        #        tscode_T = tscode_T1 + '.SH'
        #    else:
        #        tscode_T = tscode_T1 + '.SZ'
            chiyou_sql = "select ts_code,buyprice,buytime,sellprice,selltime,shouyi,selltype,state FROM jiaoyi_log WHERE state='chiyou' and ts_code=" + "'" + tscode_T1 + "'" + " ORDER BY buytime ASC LIMIT 0,1"  # DESC
            single_stock_sqldf = db.query_data1(chiyou_sql)
            single_stock_df = single_stock_sqldf.loc[(single_stock_sqldf.ts_code == tscode_T1)]
            #去掉次日卖出：
            # single_stock_df = single_stock_sqldf.loc[(single_stock_sqldf.ts_code == tscode_T1) and (single_stock_sqldf.buytime < today)]
            if single_stock_df.empty ==False:
                buyday_T = single_stock_df.buytime.tolist()[0]
                buyprice_T = str2float(buy_df_row.成本价.tolist()[0])
                # 宏源
                # buynum_T = str2int(buy_df_row.证券数量.tolist()[0])
                  # 中信
                buynum_T = str2int(buy_df_row.参考持股.tolist()[0])

                sellnum_T = str2int(buy_df_row.可用股份.tolist()[0])
                # var_path = 'd:\python\DATA'
                var_path = '/media/celue1/data'

                # 持仓天数have_days：
                sql = "select cal_date from cal_date where cal_date = " + '"' + today + '"'
                cal_today_df = db.query_data1(sql)
                if buyday_T == today:
                    chichang_days = 0
                else:
                    if cal_today_df.empty == False:  # 交易日历表里含今天
                        chichang_days = len(mystock.date2sqlTable(buyday_T, today)) - 1
                    else:  # 交易日历表里不含今天
                        chichang_days = len(mystock.date2sqlTable(buyday_T, today))

                item_temp = [tscode_T1, buyday_T, buyprice_T, buynum_T, sellnum_T, var_path, today, G_jy, chichang_days,1]
                #item_list.append(item_temp)


    reslist = []
    reslist.append(sell_mission.sellday101(item_temp))
    #测试标志 1 --测试
    test_0 = 1

    if (len(reslist) >= 1):
        maichu_df = pd.concat(reslist, axis=0, ignore_index=False, )
        # 1:'当天买入次日卖出'
        maichu_df1 = maichu_df.loc[maichu_df['sell_type'] ==1]
        if maichu_df1.empty == False:
            for row in maichu_df1.itertuples() : #遍历每一行
                ts_code_forsell = getattr(row, 'ts_code')
                sell_price = getattr(row, 'sell_price')
                if len(ts_code_forsell) > 5:   #ts_code_forsell 不为空，双重判断
                    sell_mission.ciri_sell_sql_append(ts_code_forsell,today)
            # 此条件卖出后，刷新持仓条件
            #chicangqk(today, 0)

        # 2:'大单跟随卖出
        maichu_df2 = maichu_df.loc[maichu_df['sell_type'] == 2]
        if maichu_df2.empty == False:
            for row in maichu_df2.itertuples():  # 遍历每一行
                ts_code_forsell = getattr(row, 'ts_code')
                sell_price = getattr(row, 'sell_price')
                if (len(ts_code_forsell) > 5) and (sell_price>0):  # ts_code_forsell 不为空，双重判断
                    sell_mission.sell_stock(ts_code_forsell,'大单跟随卖出',2,sell_price,test_0,G_jy)  # 还没改
            # 此条件卖出后，刷新持仓条件
            chicangqk(today, 0)


        # 3:'持有8天涨幅<10%'
        maichu_df3 = maichu_df.loc[maichu_df['sell_type'] == 3]
        if maichu_df3.empty == False:
            for row in maichu_df3.itertuples():  # 遍历每一行
                ts_code_forsell = getattr(row, 'ts_code')
                sell_price = getattr(row, 'sell_price')
                if (len(ts_code_forsell) > 5) and (sell_price>0):  # ts_code_forsell 不为空，双重判断
                    sell_mission.sell_stock(ts_code_forsell,'持有8天涨幅<10%',3,sell_price,test_0,G_jy)  # 还没改
            # 此条件卖出后，刷新持仓条件
            chicangqk(today, 0)

        # 4:'跌幅>35%'
        maichu_df4 = maichu_df.loc[maichu_df['sell_type'] == 4]
        if maichu_df4.empty == False:
            for row in maichu_df4.itertuples():  # 遍历每一行
                ts_code_forsell = getattr(row, 'ts_code')
                sell_price = getattr(row, 'sell_price')
                if (len(ts_code_forsell) > 5) and (sell_price>0):  # ts_code_forsell 不为空，双重判断
                    sell_mission.sell_stock(ts_code_forsell,'跌幅>35%',4,sell_price,test_0,G_jy)  # 还没改
            # 此条件卖出后，刷新持仓条件
            chicangqk(today, 0)

        # 21:'大单跟随当天卖出sellnum并且剩下的（buynum-sellnum）次日卖出
        maichu_df21 = maichu_df.loc[maichu_df['sell_type'] == 21]
        if maichu_df21.empty == False:
            for row in maichu_df21.itertuples():  # 遍历每一行
                ts_code_forsell = getattr(row, 'ts_code')
                sell_price = getattr(row, 'sell_price')
                if (len(ts_code_forsell) > 5) and (sell_price>0):  # ts_code_forsell 不为空，双重判断
                    sell_mission.sell_stock(ts_code_forsell,'大单跟随部分卖出+次日卖出',21,sell_price,test_0,G_jy)  # 还没改
            #此条件卖出后，刷新持仓条件
            chicangqk(today, 0)

        # 5:'股价超限卖出'
        # maichu_df5 = maichu_df.loc[maichu_df['sell_type'] == 5]
        # if maichu_df5.empty == False:
        #     for row in maichu_df5.itertuples():  # 遍历每一行
        #         ts_code_forsell = getattr(row, 'ts_code')
        #         sell_price = getattr(row, 'sell_price')
        #         if (len(ts_code_forsell) > 5) and (sell_price > 0):  # ts_code_forsell 不为空，双重判断
        #             sell_stock(ts_code_forsell, '股价超限卖出', 5, sell_price,test_0)  # 还没改
        #     # 此条件卖出后，刷新持仓条件
        #     chicangqk(today, 0)

    return
####################################################


#
# def shoudong_buy():
#     dt = datetime.datetime.now()  # 创建一个datetime类对象
#     today = dt.strftime('%Y%m%d')
#     # today = '20210922'
#     time_now = dt.strftime('%H:%M:%S')  # H应该是24小时制
#     print(today, time_now)
#     forword_day = 30  # 向前30天
#     sqlTables_forward = mystock.date2sqlTable('20210601', today)
#     if today == sqlTables_forward[-1][1:]:
#         shift1_date = sqlTables_forward[-2][1:]
#     else:
#         shift1_date = sqlTables_forward[-1][1:]
#
#         # 启动买入前计算
#         #  @b1
#     buy_df_sql = "select ts_code from buy" + shift1_date  # + ' where buy_or_not=1'
#     buy_df_from_sql = db.query_data1(buy_df_sql)
#
#     if buy_df_from_sql.empty == False:
#         # buy_df_from_sql_1 = buy_df_from_sql.loc[buy_df_from_sql.buy_or_not == 1]
#         buy_list_t = buy_df_from_sql['ts_code'].tolist()
#         buy_list = []
#         for ts_code in buy_list_t:
#             buy_list.append(ts_code[0:6])
#         print(shift1_date, '从数据库中读取待买股票', buy_list)
#     else:
#         print('启动买入前计算' + shift1_date)
#         # @b3
#         buy_list_t = buy_mission_file.buy_stock_jisuan(shift1_date, forword_day)
#
#         buy_df = pd.DataFrame()
#         # buy_list_t = ['300495']
#         buy_df['ts_code'] = buy_list_t
#         db.replace_todb(buy_df, 'buy' + shift1_date)
#
#         buy_list_t = buy_list_t[0:6]
#
#         buy_list = []
#         for ts_code in buy_list_t:
#             buy_list.append(ts_code[0:6])
#
#
#         ###Y    stock_list = list(set( buy_lst ))  # 去重   （会影响好坏排序）
#         print('通过分析', shift1_date, '选出次日待买股票', buy_list)
#
#
#     global share_lock
#     share_lock.acquire()
#     G_jy.set_buy_list(buy_list)
#     share_lock.release()
#
#     return buy_list

def shoudong_buy_sure():
    dt = datetime.datetime.now()  # 创建一个datetime类对象
    today = dt.strftime('%Y%m%d')
    # today = '20210922'
    time_now = dt.strftime('%H:%M:%S')  # H应该是24小时制
    print(today, time_now)
    forword_day = 30  # 向前30天
    sqlTables_forward = mystock.date2sqlTable('20210601', today)
    if today == sqlTables_forward[-1][1:]:
        shift1_date = sqlTables_forward[-2][1:]
    else:
        shift1_date = sqlTables_forward[-1][1:]

    buy_mission_file.stock_9351447_buy_start_sd(today, shift1_date, 7,G_jy)

if __name__ == '__main__':
    pd.set_option('display.max_columns', None)
    # 显示所有列
    pd.set_option('display.max_rows', None)

    window = tk.Tk()

    # 第2步，给窗口的可视化起名字
    window.title('114成库')

    # 第3步，设定窗口的大小(长 * 宽)
    window.geometry('1000x600')  # 这里的乘是小x

    #    list_test=[1,2,3,4,5,6,7,8,9,10,11]
    #    print(list_test[1:3])
    #    print(list_test[4:8])
    #    print(list_test[9:10])

    var_begin_date = tk.StringVar()
    var_begin_date.set('20221018')
    entry_begin_date = tk.Entry(window, textvariable=var_begin_date, font=('Arial', 14))
    entry_begin_date.place(x=0, y=0)

    var_end_date = tk.StringVar()
    var_end_date.set('20221018')  # 使用注意： 在10倍功能上var_end_date.set('20190821')data最新数据是0821那么要写0822要往后写一天
    entry_end_date = tk.Entry(window, textvariable=var_end_date, font=('Arial', 14))
    entry_end_date.place(x=100, y=0)
    # 第5步，在窗口界面设置放置Button按键
    # b = tk.Button(window, text='计算全部', font=('Arial', 12), width=10, height=1, command=jisuan)
    btn_jisuan = tk.Button(window, text='+开始交易', command=jiaoyi_main)
    btn_jisuan.place(x=400, y=0)
    btn_jisuan.pack()
    #   df3 = btn_jisuan.invoke()
 #   btn_up_one = tk.Button(window, text='上一个', command=up_one)
 #   btn_up_one.place(x=400, y=0)
 #   btn_down_one = tk.Button(window, text='下一个', command=down_one)
 #   btn_down_one.place(x=500, y=0)



    # 第6步，主窗口循环显示
    # 第4步，在图形界面上创建 500 * 300 大小的画布并放置各种元素


    canvas = tk.Canvas(window, bg='white', height=700, width=1600)
    canvas.place(x=300, y=60)
    # =====================================


    canvas2 = tk.Canvas(window, bg='white', height=400, width=1600)
    canvas2.place(x=300, y=600)



    window.mainloop()

