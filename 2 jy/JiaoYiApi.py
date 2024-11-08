import sys
import os
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]
sys.path.append(rootPath)
import urllib.request
from urllib.request import Request, urlopen
import http.client
import urllib.parse
import mystock
import io
from datetime import datetime
import json
import myhttp
import  s33 as app
import pandas as pd
import db
import time

import math

#首先声明此程序仅供学习交流使用，不可商用不可实盘否则后果自负！
#首先声明此程序仅供学习交流使用，不可商用不可实盘否则后果自负！
#首先声明此程序仅供学习交流使用，不可商用不可实盘否则后果自负！


def ChiCang(var_stock):
    have_stock = 0
    zjqk = 0
    cgsl = 0
    print("1请求持仓情况" + var_stock)
    try:
        res = urllib.request.urlopen('http://localhost:7777/api/v1.0/portfolios')

        resn = res.read()

        user_dic = json.loads(resn)
        zjqk = user_dic['subAccounts']['人民币']  # 资金情况 dict
        for user_dic_list in user_dic['dataTable']['rows']:
            if user_dic_list[0] == var_stock:
                have_stock = 1
                cgsl = user_dic_list  # 持仓情况 list
                break
        ppp=1
    except Exception:
        zjqk = 0
        cgsl =0
        ppp = 0
        qq = ppp + 1
        print("请求持仓情况!失败！！失败！！" + var_stock)
    return have_stock,zjqk,cgsl


def shishijiage(var_stock):
    price = 10000  # 实在出错就报价10000
    var_stock = var_stock[0:6]
    print("请求实时价格" + var_stock)
    for ii in range(4): #请求5次 得到结果就break
        try:
            res = urllib.request.urlopen('http://localhost:7777/api/v1.0/quotes/'+var_stock)
            resn = res.read()
            price_dict = json.loads(resn)
            round_up_t = price_dict['bid1'] #买一价格
            price = round_up(round_up_t)  #四舍五入 这步很重要！
            yesterdayClose_t = price_dict['yesterdayClose']  # 买一价格
            yesterdayClose = round_up(yesterdayClose_t)
            ppp=1
            break
        except Exception as e:
            ppp = 0
            qq = ppp + 1
            price = 10000  #实在出错就报价10000
            print("请求实时价格!失败！！失败！！" + var_stock)
    return price,yesterdayClose

#def sell_order(var_stock,price,order_num):
def sell_order(var_stock):
    var_stock = var_stock[0:6]
    url = 'http://localhost:7777/api/v1.0/orders'
    header = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0',
              'Content-Type': 'application/json'}
    post_data = {
        "action": "SELL",
        "symbol": var_stock,
        "type": "MARKET",
        "priceType": 3,             #即时成交剩余撤销委托  注意！！！3只用于深圳股
 #       "price": 19.00,
 #       "amount": 100,
        "amountProportion": "ALL",
        #"shareholderCode": "0123456789"
        }
    try:
        req = Request(url=url, data=json.dumps(post_data).encode('utf-8'), headers=header, method='POST')
        response = urlopen(req)
        html = response.read().decode('utf-8')
        print(html)
    except Exception as e:
        ppp = 0
        qq = ppp + 1
        price = 10000  # 实在出错就报价10000
        print("卖出!失败！！失败！！" + var_stock)
    return 1

def buy_order(var_stock,vol):
    var_stock = var_stock[0:6]
    url = 'http://localhost:7777/api/v1.0/orders'
    header = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0',
              'Content-Type': 'application/json'}
    post_data = {
        "action": "BUY",
        "symbol": var_stock,
        "type": "MARKET",
        "priceType": 3,             #即时成交剩余撤销委托  注意！！！3只用于深圳股
 #       "price": 19.00,
        "amount": vol,
        #"amountProportion": "ALL",
        #"shareholderCode": "0123456789"
        }
    try:
        req = Request(url=url, data=json.dumps(post_data).encode('utf-8'), headers=header, method='POST')
        response = urlopen(req)
        html = response.read().decode('utf-8')
        print(html)
    except Exception as e:
        ppp = 0
        qq = ppp + 1
        price = 10000  # 实在出错就报价10000
        print("买入!失败！！失败！！" + var_stock)
    return 1

def round_up(value):
  # 替换内置round函数,实现保留2位小数的精确四舍五入
    return round(value * 100) / 100.0


def chengjiao_q(var_stock):
    cj_q = 0
    print("1请求成交情况" + var_stock)
    try:
        res = urllib.request.urlopen('http://localhost:7777/api/v1.0/orders?status=FILLED')
        resn = res.read()
        user_dic = json.loads(resn)
        for user_dic_list in user_dic['dataTable']['rows']:
            if user_dic_list[3] == var_stock:
               # have_stock = 1
                cj_qt = user_dic_list  # 一条成交情况 list
                cj_q =cj_q+int(cj_qt[9])          # 多条成交求和 list
                break
        ppp=1
    except Exception:
        cj_q=0
        ppp = 0
        qq = ppp + 1
        print("请求成交情况!失败！！失败！！" + var_stock)
    return cj_q

###############################################################

def sell_mission(dadan):
    chicang_df = app.G_jy.global_chichang_df # 获得持仓情况   global_chichang_df %get_global_chichang_df()
    chicang_df_t = chicang_df.loc[chicang_df.可卖数量>='100']
    ts_code_list = chicang_df_t.证券代码.tolist()
    if len(ts_code_list)>0:
        stock_5sec_chunsell_start(ts_code_list, dadan)  # 启动定时任务  其中调用了  sell_stock(ts_code) 判断并执行卖出

def buy_mission(today):
    #?????这里可以优化   如果buy_list中的股今天9:33时差值为正买入
    buy_tscode =[]
    stock_buy_price ={}
    for ts_code in app.G_jy.buy_list:
        chichangqk()
        chichang_df = app.G_jy.get_global_chichang_df()
        chichang_df2 = chichang_df.loc[chichang_df.证券数量 != '0']  #当天卖出的也会在持仓中  只不过‘证券数量=0’
        stock_chi = chichang_df2.证券代码.tolist()
        zijin_dict = app.G_jy.zjqk  # 获得资金情况
        kyzj = zijin_dict['可用']
        zongzichang= zijin_dict['资产']
        stock_num = 10 #持仓股票数量
        price = shishijiage(ts_code)
        #获得today前一日close价格
        sqlsell_Tables = mystock.date2sqlTable('20200723', today)
        if (sqlsell_Tables.count('t' + today) > 0):  # 把today从list中去除
            sqlsell_Tables.remove('t' + today)
        sql = "select close from " + sqlsell_Tables[-1] + " where ts_code =" + "'" + ts_code + "'"
        temp_df_lastday = db.query_data1(sql)
        lastday_close = temp_df_lastday.iloc[0, 0]
        tscode = ts_code[0:6]
        if 0:   #正式时用
            # 股池小于10只股 & 股池里没有这支股票 & 有足够的钱买入股票 &开盘不是一字涨停
            if (len(stock_chi) < stock_num) & (stock_chi.count(tscode) == 0) & (
                    kyzj >= math.floor(zongzichang / stock_num / (price * 100)) * 100 *
                    price) & (price / lastday_close < 1.097):
                buy_vol = math.floor(zongzichang / stock_num / (price * 100)) * 100
                if buy_vol>0:
                    #T  buy_order(ts_code, 100) #测试
                    buy_order(ts_code, buy_vol)
                    print("#########################买入"+ts_code+ "  价格："+str(price) +" 数量："+str(buy_vol))

                buy_tscode.append(tscode)  #本次买入的股票列表
                stock_buy_price[tscode]  = price #买入价格存入dict
                time.sleep(2)#等待成交
            # 股池小于10只股 & 股池里没有这支股票 & 有足够的钱买入股票 &开盘不是一字涨停
        if (len(stock_chi) < stock_num) & (stock_chi.count(tscode) == 0) & (
                kyzj >= math.floor(zongzichang / stock_num / (price * 100)) * 100 *
                price) & (price / lastday_close < 1.097):

            buy_order(ts_code, 100)  # 测试
            print("#########################买入" + ts_code + "  价格：" + str(price) + " 数量：" + str(buy_vol))

            buy_tscode.append(tscode)  # 本次买入的股票列表
            stock_buy_price[tscode] = price  # 买入价格存入dict
            time.sleep(2)  # 等待成交

    jiaoyi_log_df = pd.DataFrame(
        {"ts_code": ['1'], "buyprice": [1], "buytime": ['1'], "sellprice": [1], "selltime": ['1'], "shouyi": [1],
         "selltype": ['1'], "state": ['1'], })  ##state:chiyou\selled\cirisell     次日sell
    if jiaoyi_log_df.empty == False:
        jiaoyi_log_df.drop(jiaoyi_log_df.index, inplace=True)  # 清空dataframe
    #再次读取持仓股  以这次读取为准
    chichangqk()
    chichang_df2 = app.G_jy.global_chichang_df
    stock_chi2 = chichang_df2.证券代码.tolist() #持仓的股票  其中可能有不是本次买入的
    #本次成功买入的股，与持仓的股的交集 （#持仓的股票  其中可能有不是本次买入的）
    stock_benci = list(set(buy_tscode).intersection(set(stock_chi2)))
    for tscode in stock_benci:
        temp_df = pd.DataFrame(
        {"ts_code": tscode, "buyprice": stock_buy_price[tscode], "buytime": today, "sellprice": 0, "selltime": '', "shouyi": 0,
         "selltype": '', "state": 'chiyou'},index=[0])
        jiaoyi_log_df = jiaoyi_log_df.append(temp_df)
    db.append_todb(jiaoyi_log_df, 'jiaoyi_log')

    return




def  chichangqk():
    app.G_jy.jiaoyi_locked = 1
    chicang_df = pd.DataFrame({"0": ['1'], "1": ['1'], "2": ['1'], "3": ['1'], "4": ['1'], "5": ['1'], "6": ['1'], "7": ['1'], "8": ['1'], "9": ['1'], "10": ['1'], "11": ['1'], "12": ['1'], "13": ['1'], "14": ['1'], "15": ['1'], "16": ['1']})
    if chicang_df.empty == False:
        chicang_df.drop(chicang_df.index, inplace=True)  # 清空dataframe

    zjqk = 0
    cgsl = 0
    print("1请求持仓情况" )
    try:
        res = urllib.request.urlopen('http://localhost:7777/api/v1.0/portfolios')

        resn = res.read()

        user_dic = json.loads(resn)
        zjqk = user_dic['subAccounts']['人民币']  # 资金情况 dict

        column = user_dic['dataTable']['columns']
        column[9] = '盈亏比例'  #去掉原字段的%号   原来是‘盈亏比例%’
        column[16]='备用'
        chicang_df.columns = column
#证券代码   证券名称  证券数量  可卖数量 最新价 最新市值
        if user_dic['count'] !=0 :
            for dict_list in user_dic['dataTable']['rows']:
                dict_row_df = pd.DataFrame([dict_list])
    #            dict_row_df1 = dict_row_df[['0','1','2','3','4','5','6','7','8','9','10','11','12','13','14','15','16',]]
                dict_row_df.columns =column
                chicang_df = chicang_df.append(dict_row_df)
                app.G_jy.set_global_chichang_df(chicang_df)
        ppp = 1
        app.G_jy.zjqk = zjqk
    #    app.G_jy.set_zjqk(zjqk)# 资金情况 dict

    except Exception as e:
        zjqk = 0
        cgsl = 0
        ppp = 0
        qq = ppp + 1
        app.G_jy.jiaoyi_locked = 0
        print("请求持仓情况!失败！！失败！！" )
   #     return 100
    app.G_jy.jiaoyi_locked = 0
    return 100

########################################3

#每6秒运行一次
def stock_5sec_chunsell(ts_code, dadan):
    sell_stock(ts_code, dadan)
 #   if app.G_jy.stock_5sec_df.loc[app.G_jy.stock_5sec_df.tscode == ts_code].locked ==0:
 #       app.G_jy.stock_5sec_df.loc[app.G_jy.stock_5sec_df.tscode == ts_code].locked = 1
 #       app.G_jy.stock_5sec_df.loc[app.G_jy.stock_5sec_df.tscode == ts_code].today_sell = today_sell
 #       app.G_jy.stock_5sec_df.loc[app.G_jy.stock_5sec_df.tscode == ts_code].locked = 0
    # 判断是否卖出
    return

############################################
def stock_5sec_chunsell_start(ts_code_list,dadan):

    seconds = len(ts_code_list)*6  #每个间隔6秒 乘以n个股
    for tscode in ts_code_list:
#        stock_5sec_chunsell(ts_code,dadan)
        #间隔seconds开始定时查询ts_code_list中的股票
        app.scheduler.add_job(func=sell_stock, id='stock_6sec_'+tscode, args=(tscode,dadan),
                          trigger='interval',
                          seconds=seconds,
                          replace_existing=True)

        time.sleep(6)

    return

#####################################是否卖出股票判断 每6秒执行一次
def sell_stock(ts_code,dadan):
    tscode = ts_code
    ts_code = ts_code + '.SZ'
    #查询买入时间
    # chiyou_stock_sqldf 可能会有同一只股票持有2次的情况 就是买入之后几天又买入了该股票 那么取第一次买入的数据行
    chiyou_sql = "select ts_code,buyprice,buytime,sellprice,selltime,shouyi,selltype,state FROM jiaoyi_log WHERE state='chiyou' and ts_code="+"'"+tscode+"'"+" ORDER BY buytime ASC LIMIT 0,1"  #DESC
    # 数据库中持有的股票df 有字段买入日期
    chiyou_stock_sqldf = db.query_data1(chiyou_sql)

    dt = datetime.now()  # 创建一个datetime类对象
    today = dt.strftime('%Y%m%d')
    time_now = dt.strftime('%H:%M:%S')  # H应该是24小时制

    # selltom_sql 可能会有同一只股票持有2次的情况 就是买入之后几天又买入了该股票 那么取第一次买入的数据行
    selltom_sql = "select ts_code,buyprice,buytime,sellprice,selltime,shouyi,selltype,state FROM jiaoyi_log WHERE state='selltom' and ts_code=" + "'" + tscode + "'" + " ORDER BY buytime ASC LIMIT 0,1"  # DESC
    # 数据库中持有的股票df 有字段买入日期
    selltom_stock_sqldf = db.query_data1(selltom_sql)
    if selltom_stock_sqldf.empty == False:
        selltom_buy_time = selltom_stock_sqldf.iloc[0][2]  # buytime 只记录日期
        ##################昨天满足差值卖出，今天卖出
        if (today > selltom_buy_time) & (time_now < '14:54:54'):
            app.G_jy.jiaoyi_locked = 1
            # 即时成交卖出all股
            sell_order(ts_code)
            # 停止tscode的卖出任务
            app.scheduler.remove_job('stock_6sec_' + tscode)
            price_now = shishijiage(ts_code)
            chiyou_sql_stom = "select ts_code,buyprice,buytime,sellprice,selltime,shouyi,selltype,state FROM jiaoyi_log"
            # 数据库中持有的股票df 有字段买入日期
            chiyou_stock_sqldf_2s = db.query_data1(chiyou_sql_stom)
            chiyou_stock_sqldf_2s.loc[
                (chiyou_stock_sqldf_2s.ts_code == tscode) & (chiyou_stock_sqldf_2s.state == 'selltom'), [
                    'sellprice']] = price_now
            chiyou_stock_sqldf_2s.loc[
                (chiyou_stock_sqldf_2s.ts_code == tscode) & (chiyou_stock_sqldf_2s.state == 'selltom'), [
                    'selltime']] = today + ' ' + time_now
            chiyou_stock_sqldf_2s.loc[
                (chiyou_stock_sqldf_2s.ts_code == tscode) & (chiyou_stock_sqldf_2s.state == 'selltom'), [
                    'selltype']] = '昨天满足差值今天卖出'
            chiyou_stock_sqldf_2s.loc[
                (chiyou_stock_sqldf_2s.ts_code == tscode) & (chiyou_stock_sqldf_2s.state == 'selltom'), [
                    'state']] = 'selled'
            db.replace_todb(chiyou_stock_sqldf_2s, 'jiaoyi_log')
            time.sleep(2)
            app.G_jy.jiaoyi_locked = 0
            return

    if chiyou_stock_sqldf.empty == False:
        buy_price = chiyou_stock_sqldf.iloc[0][1]
        buy_time = chiyou_stock_sqldf.iloc[0][2]  # buytime 只记录日期

        forward_df = pd.DataFrame(
            {"trade_date": ['1'], "ts_code": ['1'], "bbuys1": ['1'], "bsells1": ['1']})
        if forward_df.empty == False:
            forward_df.drop(forward_df.index, inplace=True)  # 清空dataframe

        sell_df = pd.DataFrame(
            {"trade_date": ['1'], "ts_code": ['1'], "bbuys1": ['1'], "bsells1": ['1']})
        if sell_df.empty == False:
            sell_df.drop(sell_df.index, inplace=True)  # 清空dataframe


        if(today > buy_time):
            sqlsell_Tables = mystock.date2sqlTable('20200601', today)
            if (sqlsell_Tables.count('t' + today) == 0):  # 把today 加入list中
                sqlsell_Tables.append('t' + today)

            buyday_index = sqlsell_Tables.index('t' + buy_time)  # buy_time 买入日期

            price_now = shishijiage(ts_code)
            chiyou_days = len(sqlsell_Tables[buyday_index:  ]) -1  #买入之后持有天数


            ##################最多持有29天卖出
            if (chiyou_days>29)&(app.G_jy.jiaoyi_locked ==0)&(time_now < '14:54:30'):
                app.G_jy.jiaoyi_locked = 1
                # 即时成交卖出all股
                sell_order(ts_code)
                #停止tscode的卖出任务
                app.scheduler.remove_job('stock_6sec_' + tscode)
                # 把所有tscode的log记录查出来更新
                chiyou_sql_2 = "select ts_code,buyprice,buytime,sellprice,selltime,shouyi,selltype,state FROM jiaoyi_log"
                # 数据库中持有的股票df 有字段买入日期
                chiyou_stock_sqldf_2 = db.query_data1(chiyou_sql_2)
                chiyou_stock_sqldf_2.loc[
                    (chiyou_stock_sqldf_2.ts_code == tscode) & (chiyou_stock_sqldf_2.state == 'chiyou'), [
                        'sellprice']] = price_now
                chiyou_stock_sqldf_2.loc[
                    (chiyou_stock_sqldf_2.ts_code == tscode) & (chiyou_stock_sqldf_2.state == 'chiyou'), [
                        'selltime']] = today + ' '+ time_now
                chiyou_stock_sqldf_2.loc[
                    (chiyou_stock_sqldf_2.ts_code == tscode) & (chiyou_stock_sqldf_2.state == 'chiyou'), [
                        'selltype']] = '大于29天卖出'
                chiyou_stock_sqldf_2.loc[
                    (chiyou_stock_sqldf_2.ts_code == tscode) & (chiyou_stock_sqldf_2.state == 'chiyou'), [
                        'state']] = 'selled'
                db.replace_todb(chiyou_stock_sqldf_2,'jiaoyi_log')
                time.sleep(2)
                app.G_jy.jiaoyi_locked = 0
                return
            ####################### 价格卖出条件 跌幅>5%卖出  ？？？？？？参数要调

            if (today > buy_time) & (price_now / buy_price < 0.965)&(app.G_jy.jiaoyi_locked ==0)&(time_now < '14:54:30'):
                app.G_jy.jiaoyi_locked = 1
                # 即时成交卖出all股
                sell_order(ts_code)
                # 停止tscode的卖出任务
                app.scheduler.remove_job('stock_6sec_' + tscode)
                # 把所有tscode的log记录查出来更新
                chiyou_sql_2 = "select ts_code,buyprice,buytime,sellprice,selltime,shouyi,selltype,state FROM jiaoyi_log"
                # 数据库中持有的股票df 有字段买入日期
                chiyou_stock_sqldf_2 = db.query_data1(chiyou_sql_2)
                chiyou_stock_sqldf_2.loc[
                    (chiyou_stock_sqldf_2.ts_code == tscode) & (chiyou_stock_sqldf_2.state == 'chiyou'), [
                        'sellprice']] = price_now
                chiyou_stock_sqldf_2.loc[
                    (chiyou_stock_sqldf_2.ts_code == tscode) & (chiyou_stock_sqldf_2.state == 'chiyou'), [
                        'selltime']] = today + ' '+ time_now
                chiyou_stock_sqldf_2.loc[
                    (chiyou_stock_sqldf_2.ts_code == tscode) & (chiyou_stock_sqldf_2.state == 'chiyou'), [
                        'selltype']] = '跌幅大于5%卖出'
                chiyou_stock_sqldf_2.loc[
                    (chiyou_stock_sqldf_2.ts_code == tscode) & (chiyou_stock_sqldf_2.state == 'chiyou'), [
                        'state']] = 'selled'
                db.replace_todb(chiyou_stock_sqldf_2, 'jiaoyi_log')
                time.sleep(2)

                app.G_jy.jiaoyi_locked = 0
                return
            ##################
            # 价格卖出条件 第5天  买入后5天涨幅<5%卖出  ？？？？？6天10% 10天10%？？参数要调
            if (today > buy_time) & (chiyou_days > 1) & (price_now / buy_price < 1.05)&(app.G_jy.jiaoyi_locked==0)&(time_now < '14:54:30'):
                app.G_jy.jiaoyi_locked = 1
                # 即时成交卖出all股
                sell_order(ts_code)
                # 停止tscode的卖出任务
                app.scheduler.remove_job('stock_6sec_' + tscode)
                # 把所有tscode的log记录查出来更新
                chiyou_sql_2 = "select ts_code,buyprice,buytime,sellprice,selltime,shouyi,selltype,state FROM jiaoyi_log"
                # 数据库中持有的股票df 有字段买入日期
                chiyou_stock_sqldf_2 = db.query_data1(chiyou_sql_2)
                chiyou_stock_sqldf_2.loc[
                    (chiyou_stock_sqldf_2.ts_code == tscode) & (chiyou_stock_sqldf_2.state == 'chiyou'), [
                        'sellprice']] = price_now
                chiyou_stock_sqldf_2.loc[
                    (chiyou_stock_sqldf_2.ts_code == tscode) & (chiyou_stock_sqldf_2.state == 'chiyou'), [
                        'selltime']] = today + ' '+ time_now
                chiyou_stock_sqldf_2.loc[
                    (chiyou_stock_sqldf_2.ts_code == tscode) & (chiyou_stock_sqldf_2.state == 'chiyou'), [
                        'selltype']] = '买入5天后涨幅小于5%卖出'
                chiyou_stock_sqldf_2.loc[
                    (chiyou_stock_sqldf_2.ts_code == tscode) & (chiyou_stock_sqldf_2.state == 'chiyou'), [
                        'state']] = 'selled'
                db.replace_todb(chiyou_stock_sqldf_2, 'jiaoyi_log')
                time.sleep(2)
                app.G_jy.jiaoyi_locked = 0
                return

            ###############为差值卖出准备数据
            if (today > buy_time):
                # ？？？买入前6天中为正的chazhi和作为计算依据之一  sellday_index-6   6好》5》7天
                for day1 in sqlsell_Tables[buyday_index - 2:buyday_index+1]:
                    sql = "select trade_date,ts_code,bbuys1,bsells1 from " + day1 + " where ts_code =" + "'" + ts_code + "'"
                    temp_df = db.query_data1(sql)
                    forward_df = forward_df.append(temp_df)
                forward_df['chazhi'] = forward_df['bbuys1'] - forward_df['bsells1']
                forward_df1 = forward_df.loc[(forward_df.chazhi > 0)]
                forward_buy = forward_df1.chazhi.sum()
                # 买入之后的值计算
                if chiyou_days >1: #持有天数>1
                    for dayse in sqlsell_Tables[buyday_index:-1]:  # ?????  [buyday_index-1:]
                        sqlse = "select trade_date,ts_code,bbuys1,bsells1 from " + dayse + " where ts_code =" + "'" + ts_code + "'"
                        temp_df_se = db.query_data1(sqlse)
                        sell_df = sell_df.append(temp_df_se)
                    sell_df['chazhi'] = sell_df['bbuys1'] - sell_df['bsells1']
                    sell_df1 = sell_df.loc[sell_df.chazhi < 0]  # 纯卖出的量   #??????  =sell_df.chazhi 差值
                    selldf_sellsum = abs(sell_df1.chazhi.sum())
                else:
                    selldf_sellsum = 0

                ########
                dt = datetime.now()  # 创建一个datetime类对象
                today = dt.strftime('%Y%m%d')
                time_now = dt.strftime('%H:%M:%S')  # H应该是24小时制
                #########'差值卖出'
                if (today > buy_time):
                    http_now_df = myhttp.get_l2data_from_http_5sec([tscode, ], 0, dadan)  # http获取当天的数据 190000
                    if len(http_now_df)!= 0:
                    ###if 1:  ###
                        today_sell = http_now_df[1]['sellVol'].sum()
                    ###    today_sell = 38000 ###
                        chun_sell = selldf_sellsum + today_sell
                    if (forward_buy < chun_sell) & (app.G_jy.jiaoyi_locked == 0) & (time_now < '14:54:50'):
                        app.G_jy.jiaoyi_locked = 1
                        # 即时成交卖出all股
                        sell_order(ts_code)
                        # 停止tscode的卖出任务
                        app.scheduler.remove_job('stock_6sec_' + tscode)
                        # 把所有tscode的log记录查出来更新
                        chiyou_sql_2 = "select ts_code,buyprice,buytime,sellprice,selltime,shouyi,selltype,state FROM jiaoyi_log"
                        # 数据库中持有的股票df 有字段买入日期
                        chiyou_stock_sqldf_2 = db.query_data1(chiyou_sql_2)
                        chiyou_stock_sqldf_2.loc[
                            (chiyou_stock_sqldf_2.ts_code == tscode) & (chiyou_stock_sqldf_2.state == 'chiyou'), [
                                'sellprice']] = price_now
                        chiyou_stock_sqldf_2.loc[
                            (chiyou_stock_sqldf_2.ts_code == tscode) & (chiyou_stock_sqldf_2.state == 'chiyou'), [
                                'selltime']] = today + ' '+ time_now
                        chiyou_stock_sqldf_2.loc[
                            (chiyou_stock_sqldf_2.ts_code == tscode) & (chiyou_stock_sqldf_2.state == 'chiyou'), [
                                'selltype']] = '差值卖出'   #'差值卖出'不能改这个字样
                        chiyou_stock_sqldf_2.loc[
                            (chiyou_stock_sqldf_2.ts_code == tscode) & (chiyou_stock_sqldf_2.state == 'chiyou'), [
                                'state']] = 'selled'     #'selled'不能改这个字样
                        db.replace_todb(chiyou_stock_sqldf_2, 'jiaoyi_log')
                        time.sleep(2)
                        chichangqk()
                        app.G_jy.jiaoyi_locked = 0
                        return
                    if (forward_buy < chun_sell) & (app.G_jy.jiaoyi_locked == 0) & (time_now >= '14:54:50'):
                        app.G_jy.jiaoyi_locked = 1
                        # 不能下单 下单也成费单
                        ##sell_order(ts_code)
                        # 停止tscode的卖出任务
                        app.scheduler.remove_job('stock_6sec_' + tscode)
                        # 把所有tscode的log记录查出来更新
                        chiyou_sql_2 = "select ts_code,buyprice,buytime,sellprice,selltime,shouyi,selltype,state FROM jiaoyi_log"
                        # 数据库中持有的股票df 有字段买入日期
                        chiyou_stock_sqldf_2 = db.query_data1(chiyou_sql_2)
                        chiyou_stock_sqldf_2.loc[
                            (chiyou_stock_sqldf_2.ts_code == tscode) & (chiyou_stock_sqldf_2.state == 'chiyou'), [
                                'sellprice']] = price_now
                        chiyou_stock_sqldf_2.loc[
                            (chiyou_stock_sqldf_2.ts_code == tscode) & (chiyou_stock_sqldf_2.state == 'chiyou'), [
                                'selltime']] = today + ' '+ time_now
                        chiyou_stock_sqldf_2.loc[
                            (chiyou_stock_sqldf_2.ts_code == tscode) & (chiyou_stock_sqldf_2.state == 'chiyou'), [
                                'selltype']] = '差值tom卖出'  #'差值tom卖出'不能改这个字样
                        chiyou_stock_sqldf_2.loc[
                            (chiyou_stock_sqldf_2.ts_code == tscode) & (chiyou_stock_sqldf_2.state == 'chiyou'), [
                                'state']] = 'selltom'     #'selltom'不能改这个字样
                        db.replace_todb(chiyou_stock_sqldf_2, 'jiaoyi_log')
                        time.sleep(2)
                        chichangqk()
                        app.G_jy.jiaoyi_locked = 0
                        return
    return

def end_all_trade(shangwu,dadan):
    dt = datetime.now()  # 创建一个datetime类对象
    time_now = dt.strftime('%H:%M:%S')  # H应该是24小时制
    jobs = app.scheduler.get_jobs()
    for job in jobs:
#        print("id: %s,name: %s, trigger: %s, next run: %s, handler: %s" % (
#            job.id,job.name, job.trigger, job.next_run_time, job.func))
        try:
            app.scheduler.remove_job(job.id)
            print('停止任务：'+str(job.id))
        except Exception as e:
            ret = 0

    if shangwu ==1:
        # 12:59:00读取持仓情况 剩余资金、 持仓stocks、持仓股数、买入价格、买入时间
        if (time_now < '12:58:00'):
            app.scheduler.add_job(func=chichangqk, id='chichangqk_buy1', args=(),
                                  trigger='cron', day_of_week='0-4', hour=12, minute=58,
                                  second=0, replace_existing=True)
        else:
            chichangqk()
            app.scheduler.add_job(func=chichangqk, id='chichangqk_buy1', args=(),
                                  trigger='cron', day_of_week='0-4', hour=12, minute=59,
                                  second=0, replace_existing=True)

        # 13:00计算持仓股票 计算卖出
        if (time_now < '12:59:54'):
            app.scheduler.add_job(func=sell_mission, id='sell_mission1', args=(dadan,),
                                  trigger='cron', day_of_week='0-4', hour=12, minute=59,
                                  second=54, replace_existing=True)
        else:
            sell_mission(dadan)
            app.scheduler.add_job(func=sell_mission, id='sell_mission1', args=(dadan,),
                                  trigger='cron', day_of_week='0-4', hour=12, minute=59,
                                  second=54, replace_existing=True)
        # 15:02停止所有任务
        if (time_now < '15:02:00'):
            app.scheduler.add_job(func=end_all_trade, id='end_all_trade151', args=(0, dadan),
                                  trigger='cron', day_of_week='0-4', hour=15, minute=2,
                                  second=0, replace_existing=True)
        else:
            end_all_trade(0, dadan)
            app.scheduler.add_job(func=end_all_trade, id='end_all_trade151', args=(0, dadan),
                                  trigger='cron', day_of_week='0-4', hour=15, minute=2,
                                  second=0, replace_existing=True)

    return



