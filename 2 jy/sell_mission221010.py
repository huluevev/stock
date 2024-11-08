from urllib.request import Request, urlopen
import mystock
from datetime import datetime
import json
import myhttp
import pandas as pd
import db
import datetime
import time

import s33 as shipan101

from concurrent.futures import ProcessPoolExecutor as Pool


#首先声明此程序仅供学习交流使用，不可商用不可实盘否则后果自负！
#首先声明此程序仅供学习交流使用，不可商用不可实盘否则后果自负！
#首先声明此程序仅供学习交流使用，不可商用不可实盘否则后果自负！


# 屏蔽掉 min_price ,卖点使用 2022-02-25
# 加入50万大单合计值，sum50  2022-03-12
def grp_cal_sell(single_stock_df, forward_day_num):
    single_stock_df = single_stock_df.sort_values('trade_date')
    single_stock_df['mean100'] = single_stock_df.rolling(forward_day_num, min_periods=1)['abs100'].mean()
    # single_stock_df['min_price'] = single_stock_df.rolling(10, min_periods=1)['close'].min()
    single_stock_df[['sum50', 'sum100', 'sum200']] = single_stock_df.rolling(20, min_periods=1).agg(
        {'chunmai50': 'sum', 'chunmai100': 'sum', 'chunmai200': 'sum'})  #  +50
    return single_stock_df
########################################


############################################
def stock_5sec_sell_start(today,G_jy,shift1_date):
    global tempList
    chicang_df = G_jy.get_global_chichang_df()  # 获得持仓情况   global_chichang_df %get_global_chichang_df()
    # print('chicang_df',chicang_df)
    # print('test111')
    #T 20221009
    
    chicang_df_t = pd.DataFrame()
    ts_code_list=[]
    if chicang_df.empty==False:
        chicang_df_t = chicang_df.loc[chicang_df.可用股份 >= '1']
        if chicang_df_t.empty==False:
            ts_code_list = chicang_df_t.证券代码.tolist()

   # p_back = MyPool()
    res_back = []
    tempList = []
    item_list =[]
    if chicang_df_t.empty == False:  # 如果非空

        buy_df = chicang_df_t.reset_index(drop=True)

        for t_index in buy_df.index.tolist():
            buy_df_row = buy_df.loc[(buy_df.index == t_index)]  # 取df中的一行
            tscode_T1 = buy_df_row.证券代码.tolist()[0]
            # if tscode_T1 == '601869':  ##
            #     continue
            if tscode_T1[0] == '6':
                ts_code = tscode_T1 + '.SH'
            else:
                ts_code = tscode_T1 + '.SZ'

            # print('ts:',tscode_T1)
            chiyou_sql = "select ts_code,buyprice,buytime,sellprice,selltime,shouyi,selltype,state FROM jiaoyi_log WHERE state='chiyou' and ts_code=" + "'" + tscode_T1 + "'" + " ORDER BY buytime ASC LIMIT 0,1"  # DESC
            single_stock_sqldf = db.query_data1(chiyou_sql)
           # print('single_stock_sqldf',single_stock_sqldf)
            single_stock_df = single_stock_sqldf.loc[(single_stock_sqldf.ts_code == tscode_T1)]
            #去掉次日卖出：
            # single_stock_df = single_stock_sqldf.loc[(single_stock_sqldf.ts_code == tscode_T1) and (single_stock_sqldf.buytime < today)]

            if single_stock_df.empty ==False:
                buyday_T = single_stock_df.buytime.tolist()[0]
                buyprice_T = shipan101.str2float(buy_df_row.成本价.tolist()[0])
                # 宏源
                # buynum_T = shipan101.str2int(buy_df_row.证券数量.tolist()[0])
                
                #print('参考持股',buy_df_row.参考持股.tolist()[0])
                #print('可用股份',buy_df_row.可用股份.tolist()[0][:-3])
                buynum_T = shipan101.str2int(buy_df_row.参考持股.tolist()[0])
                sellnum_T = shipan101.str2int(buy_df_row.可用股份.tolist()[0][:-3])  #100.00
                var_path = 'd:\python\DATA'

                # 持仓天数have_days：
                sql = "select cal_date from cal_date where cal_date = " + '"' + today + '"'
                cal_today_df = db.query_data1(sql)
                if buyday_T == today:
                    chichang_days = 0
                else:
                    if cal_today_df.empty == False:  # 交易日历表里含今天
                        chichang_days = len(mystock.date2sqlTable(buyday_T, today)) - 1
                        print('含今天chichang_days',chichang_days)
                    else:  # 交易日历表里不含今天
                        chichang_days = len(mystock.date2sqlTable(buyday_T, today))
                        print('不含今天chichang_days', chichang_days)

                #获取前几日交易数据
                faxian_day_t = mystock.date2sqlTable('20220601', buyday_T)
                # faxian_day = faxian_day_t[-2][1:]  # 买入前一天
                faxian_day = faxian_day_t[-4][1:]  # 买入前3天

                shift1day_t = mystock.date2sqlTable('20220601', today)
                shift1day = shift1_date
                #print('shift1day111',shift1day)
                faxian_to_shift1day = mystock.date2sqlTable(faxian_day, shift1day)

                sell_df = pd.DataFrame()
                for day_t in faxian_to_shift1day:  # sellday_index+1?   #买入天向前12天：回测止天   day--回测当天 这段代码放到这是为了只执行一遍 提速
                    sql = "select trade_date,ts_code,open,close,high,low,vol,pct_chg,free_share,bbuys50,bsells50,bbuys100,bsells100,bbuys200,bsells200,buy,sell,b50,s50,b100,s100,zb50,zs50,pe from " + day_t + " where ts_code = " + "'" + ts_code + "'"
                    temp_df = db.query_data1(sql)
                    # print(sql)
                   # print(temp_df)
                    if temp_df.empty == True:
                        continue
                    sell_df = pd.concat([sell_df,temp_df])



                item_temp = [tscode_T1, buyday_T, buyprice_T, buynum_T,sellnum_T, var_path, today,G_jy,chichang_days,sell_df]
                item_list.append(item_temp)
        # tempList = sellday101(item_temp) ###T
        #print('item_list',item_list)
        with Pool(max_workers=4) as outer_pool:
            for pool_res in outer_pool.map(sellday101,item_list):
                 print('start sell pool')
                 tempList.append(pool_res)

            # 实现目的：为了保证进程池里面任务全部运行完，后再统一拿返回值
            # outer_pool.shutdown(wait=True)  # 1.关闭进程池入口（确保统计时进程池不会再有数据进入）


    reslist = []

    for x in tempList:
        if x is None:
            pass
        else:
            reslist.append(x)


    # tempList.clear()   #读完tempList后要清空！ ##T

    #测试标志写0
    test_0=0

    if (len(reslist) >= 1):
        maichu_df = pd.concat(reslist, axis=0, ignore_index=False, )

        # # 1:'当天买入次日卖出'
        # maichu_df1 = maichu_df.loc[maichu_df['sell_type'] == 1]
        # if maichu_df1.empty == False:
        #     for row in maichu_df1.itertuples() : #遍历每一行
        #         ts_code_forsell = getattr(row, 'ts_code')
        #         sell_price = getattr(row, 'sell_price')
        #         if len(ts_code_forsell) > 5:   #ts_code_forsell 不为空，双重判断
        #             ciri_sell_sql_append(ts_code_forsell,today)
        #     # 此条件卖出后，刷新持仓条件
        #     #chicangqk(today, 0)

        # 2:'大单跟随卖出
        maichu_df2 = maichu_df.loc[maichu_df['sell_type'] == 2]
        if maichu_df2.empty == False:
            for row in maichu_df2.itertuples():  # 遍历每一行
                ts_code_forsell = getattr(row, 'ts_code')
                sell_price = getattr(row, 'sell_price')
                if (len(ts_code_forsell) > 5) and (sell_price>0):  # ts_code_forsell 不为空，双重判断
                    sell_stock(ts_code_forsell,'大单跟随卖出',2,sell_price,test_0,G_jy)  # 还没改
            # 此条件卖出后，刷新持仓条件
            shipan101.chicangqk(today, 0)


        # 3:'持有8天涨幅<10%'
        maichu_df3 = maichu_df.loc[maichu_df['sell_type'] == 3]
        if maichu_df3.empty == False:
            for row in maichu_df3.itertuples():  # 遍历每一行
                ts_code_forsell = getattr(row, 'ts_code')
                sell_price = getattr(row, 'sell_price')
                if (len(ts_code_forsell) > 5) and (sell_price>0):  # ts_code_forsell 不为空，双重判断
                    sell_stock(ts_code_forsell,'持有天涨幅<4%',3,sell_price,test_0,G_jy)  # 还没改
            # 此条件卖出后，刷新持仓条件
            shipan101.chicangqk(today, 0)

        # 4:'跌幅>35%'
        maichu_df4 = maichu_df.loc[maichu_df['sell_type'] == 4]
        if maichu_df4.empty == False:
            for row in maichu_df4.itertuples():  # 遍历每一行
                ts_code_forsell = getattr(row, 'ts_code')
                sell_price = getattr(row, 'sell_price')
                if (len(ts_code_forsell) > 5) and (sell_price>0):  # ts_code_forsell 不为空，双重判断
                    sell_stock(ts_code_forsell,'跌幅>9%',4,sell_price,test_0,G_jy)  # 还没改
            # 此条件卖出后，刷新持仓条件
            shipan101.chicangqk(today, 0)

        # # 21:'大单跟随当天卖出sellnum并且剩下的（buynum-sellnum）次日卖出
        # maichu_df21 = maichu_df.loc[maichu_df['sell_type'] == 21]
        # if maichu_df21.empty == False:
        #     for row in maichu_df21.itertuples():  # 遍历每一行
        #         ts_code_forsell = getattr(row, 'ts_code')
        #         sell_price = getattr(row, 'sell_price')
        #         if (len(ts_code_forsell) > 5) and (sell_price>0):  # ts_code_forsell 不为空，双重判断
        #             sell_stock(ts_code_forsell,'大单跟随部分卖出+次日卖出',21,sell_price,test_0,G_jy)  # 还没改
        #     #此条件卖出后，刷新持仓条件
        #     shipan101.chicangqk(today, 0)

        maichu_df0=maichu_df.loc[maichu_df['sell_type'] == 0]
        if len(maichu_df) == len(maichu_df0):
            print('这5秒没有卖出的股票, 持仓可卖: ',ts_code_list)
    return

##############################


# def L2_com_5sec_start(G_jy):
#     chicang_df = G_jy.get_global_chichang_df()  # 获得持仓情况   global_chichang_df %get_global_chichang_df()
#     #    print('chicang_df',chicang_df)
#     chicang_df_t = chicang_df.loc[chicang_df.证券数量 >= '1']
#     ts_code_list = chicang_df_t.证券代码.tolist()
#
#
#     guchi_buy_list = G_jy.get_buy_list()
#
#     tscode_list_t = ts_code_list + guchi_buy_list
#     tscode_list = list(set( tscode_list_t ))  # 去重   （会影响排序）
#     send_num = int(len(tscode_list) / 7)
#     # com.L2_com_zipdata(send_num,tscode_list,G_jy)
#     return





##############################################
#def sellday101(tscode, buyday, buyprice, buynum,sellnum, var_path, today,G_jy):
# 调用时 此函数负责单一股票的计算
def sellday101(itmlist):
    #       (tscode_T, buyday_T, buyprice_T, buynum_T,sellnum_T, var_path, today)
    # buynum---证券数量   sellnum---可卖数量
    tscode = itmlist[0]
    buyday = itmlist[1]
    buyprice = itmlist[2]
    buynum = itmlist[3]
    sellnum = itmlist[4]
    var_path = itmlist[5]
    today = itmlist[6]
    G_jy = itmlist[7]
    chichang_days = itmlist[8]
    sell_df_T = itmlist[9]
    # today_chazhi_max = itmlist[10]


    if tscode=='601608':
        ppp=999
    sell_df_row = pd.DataFrame({"ts_code": [tscode], "sell_type": [0], "sell_price": [0]})
   # print('#TT sell_df_row')
    chicang_df = G_jy.get_global_chichang_df()
    #T 20221009
    chicang_df_t = chicang_df.loc[chicang_df.可用股份 >= '1']
    #T
    # chicang_df_t = chicang_df.loc[chicang_df.证券数量 >= '0']     #调试卖出用
    chichang_list = chicang_df_t.证券代码.tolist()

    print('chichang_list22',chichang_list)
    if tscode not in chichang_list:
        return sell_df_row



    # try:
    if 1:
        print(tscode,'buyday ',buyday)
        # faxian_day_t = mystock.date2sqlTable('20221111', buyday)
        # # faxian_day = faxian_day_t[-2][1:]  # 买入前一天
        # faxian_day = faxian_day_t[-4][1:]  # 买入前3天
        #
        # shift1day_t = mystock.date2sqlTable('20221111', today)
        # shift1day = shift1day_t[-2][1:]
        #
        #
        #
        # # 买入前的发现时的chazhiH
        # faxian_to_shift1day = mystock.date2sqlTable(faxian_day, shift1day)
        # sell_df = pd.DataFrame()
        # #    print('faxian_day', faxian_day)
        # #    print('buy_to_today_t', buy_to_today_t)
        #
        # if tscode[0] == '6':
        #     ts_code = tscode + '.SH'
        # else:
        #     ts_code = tscode + '.SZ'
        #
        # # for day_t in faxian_to_shift1day:  # sellday_index+1?   #买入天向前12天：回测止天   day--回测当天 这段代码放到这是为了只执行一遍 提速
        # #     # sql = "select trade_date,ts_code,bbuys50,bsells50 from " + day_t + " where ts_code = " + "'" + tscode + "'"  ###+ " where ts_code =" + "'" + ts_code + "'"
        # #     # print('day_t',day_t)
        # #     sql = "select trade_date,ts_code,bbuys50,bsells50,pct_chg,b50,s50,b100,s100,close,open from " + day_t + " where ts_code = " + "'" + ts_code + "'"  ###+ " where ts_code =" + "'" + ts_code + "'"
        # #     temp_df = db.query_data1(sql)
        # #     # print(sql)
        # #     # print(temp_df)
        # #     if temp_df.empty == True:
        # #         continue
        # #     sell_df = sell_df.append(temp_df)
        #
        # cuxuan_all_df = G_jy.get_cuxuan_all_df()
        # sell_df = cuxuan_all_df.loc[(cuxuan_all_df.trade_date>=faxian_day) & (cuxuan_all_df.trade_date<=shift1day) & (cuxuan_all_df.ts_code == ts_code)]


        sell_df = sell_df_T
        open_sh1 = sell_df.iloc[-1, 2]
        close_sh1 = sell_df.iloc[-1, 3]
        low_sh1 = sell_df.iloc[-1, 5]
        high_sh1 = sell_df.iloc[-1, 4]


        close_buyday=0
        open_buyday=0
        
        buyday_df = sell_df.loc[sell_df.trade_date==buyday]
        if buyday_df.empty==False:
            # print('buyday_df',buyday_df)
            high_buyday = buyday_df.iloc[-1, 4]
            low_buyday = buyday_df.iloc[-1, 5]
            close_buyday = buyday_df.iloc[-1, 3]
            open_buyday = buyday_df.iloc[-1, 2]

        # print('sell_df',sell_df)
        Chazhi_He = sell_df['bbuys50'].sum() - sell_df['bsells50'].sum()


        if tscode[0] == '6':
            ts_code = tscode + '.SH'
        else:
            ts_code = tscode + '.SZ'
        print('sell计算',tscode)
        # sell_list = ["000012", "000016", "000301"]
        #   index  tscode Price_now end_time sum20_buy sum20_sell sum50_buy sum50_sell  \
        # 0      0  000008       254      NaN      4099       2566      2992       1224
        # 1      0  000010       332      NaN       181        462        50        203
        # 2      0  000031       376      NaN       567       2085       294       2251
        #
        #   sum100_buy sum100_sell sum200_buy sum200_sell B_100 B_50 S_100 S_50
        # 0       1731         768        643           0
        # 1          0           0          0           0
        # 2        135        2041          0         737

        sell_shishi_df = myhttp.http_request_fromVc(tscode, '0',['0'],1) #1号机
        # sec5_df = sell_shishi_df.columns = ['Time', 'Price', 'buyVol', 'sellVol', ]
        # print('sell_shishi_df', sell_shishi_df.tail(5))
        if sell_shishi_df.empty==True:
            return sell_df_row
        buy_50 = sell_shishi_df.buyVol.sum()
        sell_50 = sell_shishi_df.sellVol.sum()
        today_chazhi = buy_50 - sell_50
        price_now = sell_shishi_df.iloc[-1,1] ##???
        price_high = sell_shishi_df.Price.max()
        price1445_df = sell_shishi_df.loc[sell_shishi_df.Time>='14:45:00']
        price1445 = 0
        if price1445_df.empty==False:
            price1445 =price1445_df.Price.to_list()[-1]

        # print('sell_shishi_df',sell_shishi_df)
        # print('buy_50',buy_50)
        print(tscode,' Chazhi_He=',Chazhi_He,' Chazhi_He*255=',Chazhi_He*0.255,' today_chazhi ',today_chazhi)
        # print(tscode, ' cha_50=',cha_50,' cha_100=',cha_100,'  shift1_21price=',shift1_21price)
        sell_flag = 0
        sell_type = 0


        today_chazhi_max = 0
        chazhi_max_df = G_jy.get_chazhi_max_df()
        today_chazhi_max_df = chazhi_max_df.loc[chazhi_max_df.ts_code == ts_code]
        if today_chazhi_max_df.empty == False:
            today_chazhi_max = today_chazhi_max_df.today_chazhi_max.to_list()[0]
        else:
            chazhi_max_df.loc[(chazhi_max_df.ts_code == tscode), ['today_chazhi_max']] = 0
            G_jy.set_chazhi_max_df(chazhi_max_df)

        if today_chazhi > today_chazhi_max:
            chazhi_max_df.loc[(chazhi_max_df.ts_code == tscode), ['today_chazhi_max']] = today_chazhi
            G_jy.set_chazhi_max_df(chazhi_max_df)
            today_chazhi_max = today_chazhi


        # 当天回撤2% +7 % 0.55
       # print('test',ts_code,'price_now',price_now,'price_high',price_high)
        if ((price_now <= price_high * 0.98) and (price_now > buyprice * 1.065) and (price_now < buyprice * 1.18) and (
                (today_chazhi_max * 0.65) > today_chazhi) and (today_chazhi > 0) and (buyday != today)):
            sell_type_str = '2.大单跟随卖出 +7% 0.65'
            sell_type = 2  # 2:'大单跟随卖出'
            sell_flag = 1
            print(tscode+'2.大单跟随卖出 +7% 0.65')

        # 当天回撤3% price大于 -1%buyprice  （0.5 0.515  0.215）
        if ((price_now <= price_high * 0.97) and (price_now > buyprice * 0.99) and (
                (Chazhi_He * 0.25) < -today_chazhi) and (Chazhi_He > 0) and (today_chazhi < 0) and (
                    buyday != today)) or (
                ((price_now <= price_high * 0.97) and (price_now > buyprice * 0.99) and (
                        Chazhi_He * 0.25) > today_chazhi) and (Chazhi_He <= 0) and (today_chazhi < 0) and (
                        buyday != today)):  #
            sell_type_str = '2.大单跟随卖出  0.25'
            sell_type = 2  # 2:'大单跟随卖出'
            sell_flag = 1
            print(tscode+'2.大单跟随卖出  0.25')


        if ((price_now > buyprice * 1.01) and ((Chazhi_He * 0.395) < -today_chazhi) and (Chazhi_He > 0) and (today_chazhi < 0) and (buyday != today)) or (
                ((price_now > buyprice * 1.01) and (Chazhi_He * 0.395) > today_chazhi) and (Chazhi_He <= 0) and (today_chazhi < 0) and (buyday != today)):
            sell_type_str = '2.大单跟随卖出 0.395'
            sell_type = 2  # 2:'大单跟随卖出'
            sell_flag = 1
            print(tscode+'大单跟随卖出 0.395')

        # if ((price_now <= price_high * 0.98) and (price_now > buyprice * 0.99) and ((Chazhi_He * 0.135) < -today_chazhi) and (Chazhi_He > 0) and (today_chazhi < 0) and (
        #             buyday != today)) or (((price_now <= price_high * 0.98) and (price_now > buyprice * 0.99) and (
        #                 Chazhi_He * 0.135) > today_chazhi) and (Chazhi_He <= 0) and (today_chazhi < 0) and (
        #                 buyday != today)):
        #     sell_type_str = '2.大单跟随卖出 0.135'
        #     sell_type = 2  # 2:'大单跟随卖出'
        #     sell_flag = 1
        #     print(tscode+'大单跟随卖出 0.135')


        #20221005
        # 第二天价<1/2 算法
        shiti_buyday = abs(close_buyday - open_buyday)
        xx1 = price_now
        if open_buyday <= close_buyday:
            xx1 = (shiti_buyday / 2 + open_buyday) * 1.0
        else:
            xx1 = (shiti_buyday / 2 + close_buyday) * 1.0
        print('test第二天价<1/2','chichang_days>=1', chichang_days,'price_now<xx1',price_now,xx1)
        if (price_now < xx1) and (chichang_days>=1):  # chiyou_day_t>0
            print('(shiti_buyday/2+close_buyday)', (shiti_buyday / 2 + close_buyday))
            sell_type_str = '3.持有1天 小于x'
            sell_flag = 1
            sell_type = 3  # 3:'持有30天涨幅<10%'



        # 如果持有大于3天  小于x 卖出
        xian_sh1=high_sh1-low_sh1-(abs(close_sh1-open_sh1))
        shiti_sh1 = abs(close_sh1-open_sh1)
        if xian_sh1>shiti_sh1:
            x= round((high_sh1 - low_sh1)/2,3)+low_sh1
        else:
            if close_sh1>=open_sh1:
                x= round((close_sh1-open_sh1)/2,3)+open_sh1
            else:
                x= round((open_sh1-close_sh1)/2,3)+close_sh1
        print('test持有大于3天  小于x',ts_code,'chichang_days>3', chichang_days,'price1445<x',price1445,x)
        if (chichang_days > 3) and (price1445<x) and (price1445!=0) and (sell_flag == 0):
            sell_type_str = '3.持有3天 小于x'
            sell_flag = 1
            sell_type = 3  #3:'持有30天涨幅<10%'



        if (sellnum != 0)and (sell_flag == 0):
            if (((price_now * buynum) / (buyprice * sellnum)) < 0.89) and (sell_flag == 0):  # 35%
                # print(sellprice,dadan_seri['price'],adj_factor,buy_price_adj)
                sell_type = 4   #4:'跌幅>9%'
                sell_flag = 1
                sell_type_str = '4.跌幅>11%'


        if sell_flag:
            dt1 = datetime.datetime.now()
            selltime = dt1.strftime('%H:%M:%S')  # H应该是24小时制
            print('卖出股票：'+tscode + '   买入时间、价格：', buyday, buyprice, '  卖出类型：', sell_type_str,'   卖出时间:',selltime)

        sell_df_row = pd.DataFrame({"ts_code": [tscode], "sell_type": [sell_type], "sell_price": [price_now]})
        return sell_df_row
    # except Exception as e:
    #     ppp = 0
    #     print('3232',e)

    return sell_df_row

    # 1:'当天买入次日卖出'  #2:'大单跟随卖出' #3:'持有20天涨幅<5%' #4:'跌幅>35%'  #21:'大单跟随当天卖出sellnum并且剩下的（buynum-sellnum）次日卖出

##########################

########################################################
def ciri_sell_sql_append(ts_code_forsell,today):
    ts_code_forsell = ts_code_forsell[0:6]
    ciri_df = pd.DataFrame({"trade_date": [today], "ts_code": [ts_code_forsell],"selled":['0']})
    #数据表是否已添加记录
    cunzai_sql = "select ts_code,trade_date,selled FROM ciri_sell where selled = 0 and trade_date =" + "'" + today + "'"+ 'and ts_code = '+ "'"+ts_code_forsell +"'" + 'and selled = 0'
    cunzai_df = db.query_data1(cunzai_sql)
    if cunzai_df.empty == False:
        return
    else:
        db.append_todb(ciri_df, 'ciri_sell')
        print('数据库增加次日卖出',ts_code_forsell)
    return
#########################################################
def ciri_sell(today,G_jy):
    #测试标志为0--实盘
    test_0 = 0
    cirisell_sql = "select ts_code,trade_date,selled FROM ciri_sell where selled = 0 and trade_date!="+ "'" + today + "'"
    cirisell_df = db.query_data1(cirisell_sql)
    if cirisell_df.empty == False:
        for row in cirisell_df.itertuples():  # 遍历每一行
            ts_code_forsell = getattr(row, 'ts_code')
            if len(ts_code_forsell) > 5:  # ts_code_forsell 不为空，双重判断
                sell_stock(ts_code_forsell,'当天买入次日卖出',1,'open',test_0,G_jy)  # 还没改
                pass

#####################################是否卖出股票判断 每6秒执行一次
def sell_stock(tscode,str, sell_type,price_now,test,G_jy):

    dt = datetime.datetime.now()  # 创建一个datetime类对象
    today = dt.strftime('%Y%m%d')
    time_now = dt.strftime('%H:%M:%S')  # H应该是24小时制

    if (G_jy.get_jiaoyi_locked() == 0) and (time_now < '14:55:00'):
        # share_lock.acquire()
        G_jy.set_jiaoyi_locked(1)
        # share_lock.release()

        # 如果是测试 不链接交易机并把ret致1
        if test == 1:
            ret = 1
        else: #实盘交易   向交易机发指令
            # 即时成交卖出all股
            ret = sell_order(tscode)
            time.sleep(0.4)

        if ret == 1:
            # 把所有tscode的log记录查出来更新
            chiyou_sql_2 = "select ts_code,buyprice,buytime,sellprice,selltime,shouyi,selltype,state FROM jiaoyi_log"
            # 数据库中持有的股票df 有字段买入日期
            tscode = tscode[0:6]
            chiyou_stock_sqldf_2 = db.query_data1(chiyou_sql_2)
            chiyou_stock_sqldf_2.loc[
                (chiyou_stock_sqldf_2.ts_code == tscode) & (chiyou_stock_sqldf_2.state == 'chiyou'),
                    'sellprice'] = price_now
            chiyou_stock_sqldf_2.loc[
                (chiyou_stock_sqldf_2.ts_code == tscode) & (chiyou_stock_sqldf_2.state == 'chiyou'),
                    'selltime'] = today + ' ' + time_now
            chiyou_stock_sqldf_2.loc[
                (chiyou_stock_sqldf_2.ts_code == tscode) & (chiyou_stock_sqldf_2.state == 'chiyou'),
                    'selltype'] = sell_type
            chiyou_stock_sqldf_2.loc[
                (chiyou_stock_sqldf_2.ts_code == tscode) & (chiyou_stock_sqldf_2.state == 'chiyou'),'state'] = 'selled'
            db.replace_todb(chiyou_stock_sqldf_2, 'jiaoyi_log')

            # #如果是今日买入次日卖出：
            # if sell_type == 1:
            #     ciri_sql = "select ts_code,trade_date,selled FROM ciri_sell"
            #     ciri_df = db.query_data1(ciri_sql)
            #   #  ciri_df = ciri_df.reset_index(drop=True)
            #     if ciri_df.empty == False:
            #         ciri_df.loc[ciri_df['ts_code'] == tscode, 'selled'] = 1
            #         db.replace_todb(ciri_df, 'ciri_sell')   #@测试   ciri_sell表中加数据测

            print("卖出:", tscode, str, "卖价:", price_now)
            # share_lock.acquire()
            G_jy.set_jiaoyi_locked(0)
            # share_lock.release()
            return  1
        else:
            print("卖出失败:", tscode, str)
            # share_lock.acquire()
            G_jy.set_jiaoyi_locked(0)
            # share_lock.release()
            return 0

        # share_lock.acquire()
        G_jy.set_jiaoyi_locked(0)
        # share_lock.release()
    return  0

####################################


# def sell_order(var_stock,price,order_num):
def sell_order(var_stock):
#T    return 1
    var_stock = var_stock[0:6]
    url = 'http://100.100.107.232:7777/api/v1.0/orders?key=911911'
    header = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0',
              'Content-Type': 'application/json'}
    if var_stock[0] =='6':
        post_data = {
            "action": "SELL",
            "symbol": var_stock,
            "type": "MARKET",
            "priceType": 4,  # 5档成交剩余撤销委托  可用于上证股
            #       "price": 19.00,
            #       "amount": 100,
            "amountProportion": "ALL",
        }
    else:
        post_data = {
            "action": "SELL",
            "symbol": var_stock,
            "type": "MARKET",
            "priceType": 3,  # 即时成交剩余撤销委托  注意！！！3只用于深圳股
            #       "price": 19.00,
            #       "amount": 100,
            "amountProportion": "ALL",
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
    print("卖出指令下达完毕" + var_stock)
    return 1

####################################
