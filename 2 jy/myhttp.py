import sys
import os

curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import urllib.request
import http.client
import urllib.parse
import gzip, binascii, os
import io
import math
import time
import warnings
import datetime

warnings.filterwarnings('ignore')
from multiprocessing import Pool

import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
import pandas as pd
from pandas.core.frame import DataFrame
import json
import requests
import numpy as np



#首先声明此程序仅供学习交流使用，不可商用不可实盘否则后果自负！
#首先声明此程序仅供学习交流使用，不可商用不可实盘否则后果自负！
#首先声明此程序仅供学习交流使用，不可商用不可实盘否则后果自负！


###上面的老了不用了  使用以下的，以下的要配合mfc程序获得数据。两台数据机是为了高速，一台计算一部分
url_vc_2 = 'http://100.100.104.232:8171/echo'  #我的第一台MFC数据机
url_vc_1 = 'http://100.100.106.232:8171/echo' #我的第二台MFC数据机
# Json串：{
#      "cnt": "3",
#      "tscodes": [
#           "000012",
#           "000016",
#           "000020"
#      ]
# }

def http_request_fromVc_old(stock_list,time_str):  # 挂单

    L2_data_df_all = pd.DataFrame(
        {"tscode": ['1'], "Price_now": ['1'], "end_time": ['1'], "sum20_buy": ['1'], "sum20_sell": ['1'],
         "sum50_buy": ['1'], "sum50_sell": ['1'],
         "sum100_buy": ['1'], "sum100_sell": ['1'], "sum200_buy": ['1'], "sum200_sell": ['1'], })
    if L2_data_df_all.empty == False:
        L2_data_df_all.drop(L2_data_df_all.index, inplace=True)  # 清空dataframe

    stock_str_array =[]

    try:
        headers = {'Content-Type': 'application/json','Content-Length':'null','Connection':'close'}  #'Transfer-Encoding':'chunked',
        # proxies = {"http": None, "https": None}

        stock_num = len(stock_list)
        for st in stock_list:
            stock_str_array.append(st[0:6])

        # data = {'cnt': stock_num, 'tscodes': stock_str_array,'time':time_str}  #time_str为‘0’时请求实时转换结果
        data = {'cnt': stock_num, 'tscodes': stock_str_array,'time':'0'}
        dict = json.dumps(data)
        # print('dict:', dict)

        s = requests.session()
        s.keep_alive = False  # 关闭多余连接
        if http_testflag == 1:
            res = requests.post(url_vc_2, data=dict, headers=headers)#, proxies=proxies)
        else:
            res = requests.post(url_vc_1, data=dict, headers=headers)#, proxies=proxies)


        json_str = res.content
        L2_data_dict = json.loads(json_str)

        find_flag = L2_data_dict['find']
        if find_flag == 'TRUE':
            method_list = L2_data_dict['method']
            for one_ts in  method_list:
                one_L2_data_df_t = pd.DataFrame.from_dict(one_ts, orient='index')
                one_L2_data_df_t2 = one_L2_data_df_t.T
                L2_data_df_all  = pd.concat([L2_data_df_all,one_L2_data_df_t2])
        L2_data_df_all = L2_data_df_all.reset_index()
        # print(L2_data_df_all.loc[L2_data_df_all.tscode=='000905'])

    except Exception as e:
        print('str(Exception):\t', str(Exception))
        print('str(e):\t\t', str(e))
        print('repr(e):\t', repr(e))
        # print('e.message:\t', e.message)
        ppp = 0
        return L2_data_df_all

    dt = datetime.datetime.now()  # 创建一个datetime类对象
    today = dt.strftime('%Y%m%d')
    # today = '20210922'
    time_now = dt.strftime('%H:%M:%S')  # H应该是24小时制

    print('L2 load ok  ' ,time_now)
    return L2_data_df_all


def http_request_fromVc(stock,time_str,txt_list,whichone):  # 挂单

    single_df = pd.DataFrame()
    #     {"tscode": ['1'], "Price_now": ['1'], "end_time": ['1'], "sum20_buy": ['1'], "sum20_sell": ['1'],
    #      "sum50_buy": ['1'], "sum50_sell": ['1'],
    #      "sum100_buy": ['1'], "sum100_sell": ['1'], "sum200_buy": ['1'], "sum200_sell": ['1'], })
    # if L2_data_df_all.empty == False:
    #     L2_data_df_all.drop(L2_data_df_all.index, inplace=True)  # 清空dataframe

    price_now=0
    buy_v = 0
    sell_v = 0
    try:
        headers = {'Content-Type': 'application/json','Content-Length':'null','Connection':'close'}  #'Transfer-Encoding':'chunked',
        # proxies = {"http": None, "https": None}

        txt_list_str=""
        for tscode_t in txt_list:
            txt_list_str= txt_list_str+tscode_t+","

        # stock='000037'
        # data = {'cnt': stock_num, 'tscodes': stock_str_array,'time':time_str}  #time_str为‘0’时请求实时转换结果
        data = {'tscodes': [stock[0:6]],'time':'0','txt':[txt_list_str]}
        dict = json.dumps(data)

        s = requests.session()
        s.keep_alive = False  # 关闭多余连接
        if whichone == 1:
            res = requests.post(url_vc_1, data=dict, headers=headers)#, proxies=proxies)
        if whichone == 2:
            res = requests.post(url_vc_2, data=dict, headers=headers)#, proxies=proxies)


        result_data = res.content

        if len(result_data)>3:
            # 将result_data转换为字节流
            result_bytes = bytes(result_data)

            # 使用io.BytesIO将字节流封装为文件对象
            result_file = io.BytesIO(result_bytes)

            # 使用pandas的read_csv函数解析为DataFrame
            if stock=='zzz':
                #res_df = pd.read_csv(result_file, names=['tscode','zp50','zp50_BS'],dtype={'tscode':str})
                res_df = pd.read_csv(result_file, names=['tscode','zp50','z50_B','z50_S'],dtype={'tscode':str})
                single_df = res_df
            else:
                res_df = pd.read_csv(result_file,names=['Time','Price','buyVol', 'sellVol'])
                res_df['Price']  = res_df['Price'] /100
                res_df['buyVol'] = res_df['buyVol'] / 100
                res_df['sellVol'] = res_df['sellVol'] / 100

                single_df = res_df
                price_now = single_df.iloc[-1,1]
                buy_v = single_df.iloc[-1,2]
                sell_v = single_df.iloc[-1,3]
                # print(single_df.tail(1))

    except Exception as e:
        # print('str(Exception):\t', str(Exception))
        # print('str(e):\t\t', str(e))
        # print('repr(e):\t', repr(e))
        ppp = 0
        return single_df

    dt = datetime.datetime.now()  # 创建一个datetime类对象
    today = dt.strftime('%Y%m%d')
    # today = '20210922'
    time_now = dt.strftime('%H:%M:%S')  # H应该是24小时制

    print(stock,'L2 load ' ,time_now,'价格:',price_now,'buy_v',buy_v,'sell_v',sell_v)
    return single_df
