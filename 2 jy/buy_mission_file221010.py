from urllib.request import Request, urlopen
import mystock
import json
import myhttp
import pandas as pd
import db
import time
import math
import operator
import numpy as np
import s33 as shipan
import datetime
from concurrent.futures import ProcessPoolExecutor as Pool  #新多进程方法

#首先声明此程序仅供学习交流使用，不可商用不可实盘否则后果自负！
#首先声明此程序仅供学习交流使用，不可商用不可实盘否则后果自负！
#首先声明此程序仅供学习交流使用，不可商用不可实盘否则后果自负！
###########################################  ok 20220316
def stock_5sec_buy_start(today, stock_num,G_jy,zp50_200df,have_5day_list):

    guchi_cuxuan_list = G_jy.get_buy_list()
    tempList = []
    item_list =[]
    buylist1= G_jy.get_buy_list1()
    buylist2= G_jy.get_buy_list2()

    cuxuan_all_df = G_jy.get_cuxuan_all_df()
    #print('guchi_cuxuan_list',guchi_cuxuan_list)
    print('5sec++',have_5day_list)

    for tscode in guchi_cuxuan_list:
        if tscode not in have_5day_list:
            ts_code = ''
            if tscode[0] == '6':
                ts_code = tscode + '.SH'
            if tscode[0] == '0':
                ts_code = tscode + '.SZ'
            if tscode[0] == '3':
                ts_code = tscode + '.SZ'

            m = 1
            if tscode in buylist1:
                m = 1
            else:
                m = 2

            zp50_200df_single = zp50_200df.loc[zp50_200df.tscode == tscode]
            item_temp = [today, ts_code, cuxuan_all_df, G_jy,zp50_200df_single,m]
            item_list.append(item_temp)
        else:
            aa=1;
            print(tscode,'in have_5day_list,out')
    print('启动买进程')
    with Pool(max_workers=11) as outer_pool:
       for pool_res in outer_pool.map(buy_ornot_single_stock, item_list):
            tempList.append(pool_res)


    reslist = []

    for x in tempList:
        if x is None:
            pass
        else:
            reslist.append(x)
    # 测试标志写0
    test_0 = 0
    print('现在只需分析股票:', G_jy.get_buy_list())

    if (len(reslist) >= 1):
        buy_sure_df = pd.concat(reslist, axis=0, ignore_index=False, )  #list组到df
        
        # 确定要买入的股
        # print('buy_sure_df',buy_sure_df)
        buy_sure_df_t = buy_sure_df.loc[buy_sure_df.buy_status ==1]
        buy_sure_list = buy_sure_df_t.ts_code.tolist()
        G_jy.set_buy_sure_list(buy_sure_list)
        if len(buy_sure_list) > 0:
            buy_mission_for5sec(today, stock_num,G_jy, buy_sure_list,buy_sure_df_t)
        else:
            print('这5秒没有要买入的股票。')
    else:
        print('这5秒没有要买入的股票。')
    return

############################################# ok 20220316
# 返回结果： [ts_code,buyday,buytime,buyprice,buynum,buy_or_not]     #buy_or_not 默认0不买 1买入
# buy_stocks_df结构： {"ts_code": ['0'], 'trade_date': ['0'], 'Time': ['0'], 'Price': [0],'buy_status': [0]}
#原  buy_stock_t()函数
def buy_ornot_single_stock(itmlist):
    today = itmlist[0]
    ts_code = itmlist[1]
    tscode = ts_code[:6]
    cuxuan_all_df = itmlist[2]
    G_jy = itmlist[3]
    zp50_200df_single = itmlist[4]
    m = itmlist[5]


    buy_df = pd.DataFrame({"ts_code": ['0'], 'buy_status': [0], 'price_now': [0]})

    chicang_df = G_jy.get_global_chichang_df()
    # 宏源
    # chicang_df_t = chicang_df.loc[chicang_df.证券数量 >= '1']
    # 国泰
    if chicang_df.empty==False:
        chicang_df_t = chicang_df.loc[chicang_df.可用股份 >= '1']
        chichang_list = chicang_df_t.证券代码.tolist()
    else:
        chichang_list = []

    if tscode in chichang_list:
        debug=1
        return buy_df

    # if ts_code == '300492.SZ':#T
    #     print('find find')
    #     return buy_df
    # if ts_code == '003023.SZ':#T
    #     print('find find')
    # else:
    #     return buy_df

    try:
        single_stock_df_qt = cuxuan_all_df.loc[cuxuan_all_df.ts_code == ts_code]
        single_stock_df_q = single_stock_df_qt.reset_index(drop=True)

        # print('single_stock_df_q',single_stock_df_q.tail(2))
        if (single_stock_df_q.empty == False):
            close_sh1 = single_stock_df_q.iloc[-1, 2]
            #dadan = 500000
            # sec5_df=sec5_df.columns=['Time','Price','buyVol','sellVol',]
            # ts_code = '000766.SZ'
            sec5_df = myhttp.http_request_fromVc(ts_code, 0, ['0'],m) #号机
            # print(ts_code,sec5_df.tail(10))
            if (len(sec5_df) > 0):
                # sec5_df_930 = sec5_df.loc[sec5_df.Time<='09:30:04']
                # sec5_df_930['buy_cum'] = sec5_df_930['buyVol'].cumsum()
                # sec5_df_930['sell_cum'] = sec5_df_930['sellVol'].cumsum()
                # # print('sec5_df_930',sec5_df_930.tail(10))
                # if sec5_df_930.empty==True:
                #     zp50 = 0
                # else:
                #     zp50 = sec5_df_930.iloc[-1,4]-sec5_df_930.iloc[-1,5]
                # print(ts_code,'zp50',zp50)
                zp50 = 0
                if zp50_200df_single.empty==False:
                    zp50 = zp50_200df_single.iloc[0,1]
                print(ts_code,'zp50=',zp50)
                if zp50>=1900:    # 1500
                    # print(ts_code,'zp50>2000',zp50 )
                    # print(huice_today,'需要计算++ ',ts_code)
                    sec5_df['chazhi50'] = sec5_df['buyVol'] - sec5_df['sellVol']
                    sec5_df['price_sh'] = sec5_df.Price.shift(1)  ##??
                    sec5_df['wg_zf'] = round((sec5_df['Price'] / sec5_df['price_sh'] - 1) * 100, 3)

                    sec5_df['wg_zfbl'] = round(
                        ((sec5_df['buyVol'] - sec5_df['sellVol']) / sec5_df['wg_zf']), 1)
                    # new 20230816
                    sec5_df['wg_zfbl_new'] = round((sec5_df['wg_zf'] / sec5_df['chazhi50']) * 10000, 1)

                    # print('single_stock_df_n+++4',single_stock_df.tail(2))

                    sec5_df['zf'] = round((sec5_df['Price'] / close_sh1 - 1) * 100, 3)

                    sec5_df['chazhi50_cum'] = sec5_df.chazhi50.cumsum()

                    sec5_10_df = sec5_df.loc[sec5_df.buyVol >= 10]
                    temp_df = sec5_df.loc[(sec5_df.sellVol > 0)]  # & (sec5_df.zf>0)]
                    # sec5_df_02 = sec5_df.loc[sec5_df.wg_zf >= 0.2]
                    # new 20230825
                    sec5_df_02 = sec5_df.loc[
                        (sec5_df.wg_zf >= 0.2) & (sec5_df.chazhi50_cum > -1550)]  # 240 改100
                    if sec5_df_02.empty == False:
                        # sec5_df_02['wg_zfbl_new'] = sec5_df_02['wg_zfbl_new'].replace(np.inf,np.nan)
                        temp = sec5_df_02.iloc[0, 7]
                        for i, row in sec5_df_02.iterrows():
                            if row['wg_zf'] > 0 and ~np.isinf(row['wg_zfbl_new']):
                                sec5_df_02.at[i, 'zfbl_new_shift'] = temp
                                temp = row['wg_zfbl_new']
                            elif (np.isinf(row['wg_zfbl_new'])):
                                sec5_df_02.at[i, 'zfbl_new_shift'] = temp

                        bz_df_new = sec5_df_02.loc[(sec5_df_02.chazhi50 < 700) &
                                                   (sec5_df_02.chazhi50 >= 0) &

                                                   (
                                                           ((
                                                                        sec5_df_02.wg_zfbl_new > sec5_df_02.zfbl_new_shift) &
                                                            (sec5_df_02.wg_zfbl_new < np.inf) & (
                                                                        sec5_df_02.zfbl_new_shift >= 0)) |
                                                           ((
                                                                        sec5_df_02.wg_zfbl_new > sec5_df_02.zfbl_new_shift) &
                                                            (sec5_df_02.wg_zfbl_new <= np.inf) & (
                                                                        sec5_df_02.zfbl_new_shift < 0)) |
                                                           ((
                                                                        sec5_df_02.wg_zfbl_new >= sec5_df_02.zfbl_new_shift) &
                                                            (sec5_df_02.zfbl_new_shift >= 0) &
                                                            (sec5_df_02.chazhi50_cum < 0) & (
                                                                        sec5_df_02.zf > 2))
                                                   ) &

                                                   (sec5_df_02.zf > -2.0) &
                                                   (sec5_df_02.wg_zf >= 0.21)  # &
                            # (sec5_df_02.wg_zfbl_new>100) #???
                                                   ]

                        if bz_df_new.empty == False:
                            buy_time11 = bz_df_new.Time.to_list()[0]
                            # print('buy_time11', buy_time11, type(buy_time11))
                            buy_price11 = bz_df_new.Price.to_list()[0]
                            #
                            zheng_price_mean = single_stock_df_q.iloc[-1,19]     #????
                            # print('zheng_price_mean',zheng_price_mean,type(zheng_price_mean))
                            buy_p_zheng_p = round(buy_price11 / zheng_price_mean, 3)
                            # print('buy_p_zheng_p',buy_p_zheng_p,type(buy_p_zheng_p),(buy_p_zheng_p <= 1.7))
                            # zf_11 = bz_df_new.zf.to_list()[0]
                            # wg_zf_11 = bz_df_new.wg_zf.to_list()[0]

                            # buy_time111=buy_time11.strftime('%H:%M:%S')

                            # if buy_p_zheng_p<=1.12 and zp50>=1000 or 1:
                            # if buy_p_zheng_p <= 1.7 and zp50 >= 1500 and fengshu>110:
                            if (buy_p_zheng_p <= 1.7)  :

                                # 获取当前系统时间：
                                current_time = datetime.datetime.now().time()
                                given_time = datetime.datetime.strptime(buy_time11, "%H:%M:%S").time()
                                # 4. 计算当前时间与给定时间之间的差距，以分钟为单位：
                                time_diff = ( (datetime.datetime.combine(datetime.date.today(),current_time)
                                                                        - datetime.datetime.combine(datetime.date.today(), given_time)).total_seconds() )/ 60
                                print("原来准备买入：",ts_code, ' 时间相差大于5分钟:', time_diff, ' buy_time22:', buy_time11,'price_now',buy_price11)
                                if time_diff <= 5:
                                    buy_df = pd.DataFrame(
                                        {"ts_code": [ts_code], 'buy_status': [1], 'price_now': [buy_price11]})

                                    print("实时准备买入：", ts_code, ' price_now:', buy_price11)

                                    return buy_df




                else:
                    # 获取当前系统时间：
                    current_time_t = datetime.datetime.now().time()
                    # print('current_time_t',current_time_t,type(current_time_t))
                    current_time_s = current_time_t.strftime( "%H:%M:%S")
                    # print('current_time_s',current_time_s)
                    if (current_time_s > '09:32:00'):   #???
                        guchi_cuxuan_list_q = G_jy.get_buy_list()
                        guchi_cuxuan_list_q.remove(tscode)
                        G_jy.set_buy_list(guchi_cuxuan_list_q)

                    print(ts_code,'zp50<1900 buy list 去除:',tscode,'现在只需分析股票数量',len(G_jy.get_buy_list()))
                    return buy_df

        return  buy_df
    except Exception as e:
        print(ts_code,' eee111')
        print(e)

    return  buy_df
####################################


###################################优化前
# def buy_ornot_single_stock(itmlist):
#     today = itmlist[0]
#     ts_code = itmlist[1]
#     cuxuan_all_df = itmlist[2]
#     G_jy = itmlist[3]
#     single_stock_shishi_df = itmlist[4]
#
#     not_buy_stocks_df = pd.DataFrame({"ts_code": ['0'], 'buy_status': [0]})
#
#     if (single_stock_df.empty == False) and (single_stock_shishi_df.empty == False):
#         # 获取待买股票实时数据
#         # calc_buy_status返回结构    {"ts_code": ['0'], 'trade_date': ['0'], 'Time': ['0'], 'Price': [0], 'open': [0], 'buy_status': [0]})
#         # buy_stocks_df = calc_buy_status(today, ts_code, G_jy, 0)
#
#         c100_std2_last = single_stock_df.iloc[-1,14]
#
#         bb100 = single_stock_shishi_df.iloc[-1, 7]
#         bs100 = single_stock_shishi_df.iloc[-1, 8]
#         chazhi100 = round((bb100 - bs100), 3)   #???  是否要除以10000
#
#         single_temp_df = pd.DataFrame(
#             {"trade_date": [today], "ts_code": [ts_code], "pct_chg": [0], "chazhi100": [chazhi100]})
#
#         single_stock_df_temp = single_stock_df[['trade_date', 'ts_code', 'pct_chg', 'chazhi100']]
#         single_stock_df_temp = single_stock_df_temp.tail(17)
#         single_stock_df_temp = single_stock_df_temp.append(single_temp_df)
#         single_stock_df_temp['c100_mean'] = single_stock_df_temp.chazhi100.rolling(window=5, center=False).mean()  # 14
#         single_stock_df_temp['c100_std'] = single_stock_df_temp.chazhi100.rolling(window=14, center=False).std()
#         single_stock_df_temp['c100_std2'] = single_stock_df_temp.c100_std.rolling(window=3, center=False).std()
#         c100_mean_last_t = single_stock_df_temp.iloc[-1, 4]
#         c100_std2_last_t = single_stock_df_temp.iloc[-1, 6]
#
#         # if2222
#         if (c100_std2_last_t > 100) and (c100_std2_last_t >= c100_std2_last * 10) and (c100_std2_last_t < 920) and \
#                 (c100_mean_last_t > 0) and (chazhi100 > 100) and (zp50>-200):
#
#             #买入并去除cuxuan中的ts_code
#             guchi_buy_list = G_jy.get_buy_list()
#             guchi_buy_list1 = guchi_buy_list.remove(ts_code)
#             G_jy.set_buy_list(guchi_buy_list1)
#
#             buy_single_df = pd.DataFrame({"ts_code": ['ts_code'], 'buy_status': [1]})
#             return buy_single_df
#
#     return  not_buy_stocks_df
####################################3


################################# ok 20220316
    # 个股结构
    # trade_date,ts_code,bbuys100,bsells100,bbuys200,bsells200,open,high,low,close,pct_chg,pe
    # chunmai100，chunmai200，abs100，mean100，sum100，sum200，buy_sell,buy_rate,sell_mean_100,sell_rate
    # sell_time,buy_status,chazhi100,chazhi200,Time,Price_x,Price_y

def get_buy_status(single_stock_df) :
    cur_row = len(single_stock_df)-1
    ts_code = single_stock_df.iloc[cur_row, 0]

    single_stock_df = single_stock_df.sort_values('trade_date')
    #single_stock_df 的列名
    last_row = 0
    sell_flag = 0
    #  for stock in single_stock_df.iterrows():
    #      if cur_row > 0:
    last_row =  cur_row -1
    if single_stock_df.iloc[0, -3] != 0:  # Price_x
        price = single_stock_df.iloc[cur_row, 6]  # Price_x
    else:
        price = single_stock_df.iloc[cur_row, 7]  # Price_y

    # high = single_stock_df.iloc[cur_row, 7]  # high
    # min_price = single_stock_df.iloc[cur_row ,  -1]  # 'min_price'
    # price_rate = 0
    # if (min_price != 0):
    #     price_rate = (price - min_price) / min_price
    sum_100 = single_stock_df.iloc[cur_row , 10 ]  # 'sum100'
    sum_200 = single_stock_df.iloc[cur_row , 11 ]  #'sum200'
    last_sum_100 = single_stock_df.iloc[last_row , 12 ]  # 用'high'字段存取 last_sum100
    last_sum_200 = single_stock_df.iloc[last_row , 13 ]  #  用'low'字段存取 last_sum200
    chazhi_100 = single_stock_df.iloc[cur_row , 3 ]  # 'chunmai100'
    chazhi_200 = single_stock_df.iloc[cur_row , 4 ]  # 'chunmai200'
    mean_100 = single_stock_df.iloc[cur_row , 8 ]  # 'mean100'

    # 处理第一行 last_sum_100 == 0 and last_sum_200 == 0  2022-03-05
    if (last_sum_100 == 0) and (last_sum_200 == 0) :
        if chazhi_100 > 0 :
            last_sum_100 = sum_100 -1
        if chazhi_100 < 0 :
            last_sum_100 = sum_100 + 1
        if chazhi_200 > 0 :
            last_sum_200 = sum_200 - 1
        if chazhi_200 < 0 :
            last_sum_200 = sum_200  + 1

    rate_sum100_sell0 = 0
    rate_sum200_sell0 = 0
    if (last_sum_100 != 0) :
        rate_sum100_sell0 = abs(chazhi_100 / last_sum_100)
    if ( last_sum_200 != 0):
        rate_sum200_sell0 =  abs( chazhi_200 / last_sum_200 )

    # 依据buy_status的当前值做判断  2022-03-05
    # zhuli_come = ( single_stock_df.iloc[cur_row, -6] == 2)
    # dadan_away = ( single_stock_df.iloc[cur_row, -6] == 1)
    # print( ts_code, single_stock_df.iloc[cur_row, -3], chazhi_100, chazhi_200, last_sum_100, sum_100, last_sum_200, rate_sum100_sell0,rate_sum200_sell0,single_stock_df.iloc[cur_row, -6] )

    # 更新buy_status，不含第一行 2022-02-24
    if (last_sum_100 != 0) and (last_sum_200 != 0) and ((sum_100 != last_sum_100) or (sum_200 != last_sum_200)):
        # 判断主力是否进场  2022-02-24
        zhuli_come = ((chazhi_100 > 300) and (chazhi_200 >= 0) and (rate_sum100_sell0 > 0.15) and (
                    sum_100 > last_sum_100)) \
                     or ((chazhi_200 > 300) and (chazhi_100 > 0) and (rate_sum200_sell0 > 0.15) and (
                    sum_200 > last_sum_200))

        # 判断主力是否正在抛盘标志 dadan_away   2022-01-11
        dadan_away = ((chazhi_100 < -100) and (chazhi_200 <= 0) and (rate_sum100_sell0 > 0.03) and (
                    last_sum_100 > sum_100)) \
                     or ((chazhi_200 < -100) and (chazhi_100 < 0) and (rate_sum200_sell0 > 0.03) and (
                    last_sum_200 > sum_200))

        # # 炸板等类似情况判断  2022-01-26
        # is_zhaban = ((chazhi_100 > 300) and ( chazhi_200 >= 0 ) and (sum_100 > 1000) and (rate_sum100_sell0 > 0.3) and (sum_100 < last_sum_100)) \
        #             or ((chazhi_200 > 300) and ( chazhi_100 > 0)and (sum_200 > 1000) and (rate_sum200_sell0 > 0.3) and (sum_200 < last_sum_200))

        if zhuli_come:
            single_stock_df.iloc[cur_row, 14] = 2   #last_status
            # print( '主力入场',ts_code, single_stock_df.iloc[cur_row, -3], chazhi_100, chazhi_200, last_sum_100, sum_100, last_sum_200, sum_200, rate_sum100_sell0,  rate_sum200_sell0 )
        else:
            if (not dadan_away):
                single_stock_df.iloc[cur_row, 14] = 0  # 恢复初始转态  #last_status

        if dadan_away:
            single_stock_df.iloc[cur_row, 14] = 1  #last_status
            # print( '抛盘',ts_code, single_stock_df.iloc[cur_row, -3], chazhi_100, chazhi_200, last_sum_100, sum_100, last_sum_200, sum_200, rate_sum100_sell0,  rate_sum200_sell0 )
        else:
            if (not zhuli_come):
                # print('1',single_stock_df.iloc[cur_row, -3])
                single_stock_df.iloc[cur_row, 14] = 0  # 恢复初始转态  #last_status

    single_rst_df = single_stock_df[['ts_code','last_status']]

    return single_rst_df
########################################

#######################################
#buy_mission 在买完所需股票后，再查询持仓情况后再次启动sell_mission
def buy_mission_for5sec(today,stock_num,G_jy,buy_sure_list,buy_sure_df_t):
    buy_lst = buy_sure_list

    maihou_chichanglist_old = G_jy.get_maihou_chichanglist()
    maihou_chichanglist_old.sort()
    buy_list1=[]
    for ts_code1 in buy_sure_list:
        buy_list1.append(ts_code1[0:6])
    buy_list1.sort()
    if operator.eq(maihou_chichanglist_old, buy_list1):
        print('买不上，返回.这5秒没有要买入的股票')
        return


    shipan.chicangqk(today,0)   #获取持仓 但不准备数据
    zjqk_dic = G_jy.get_zjqk()
    zichanky = zjqk_dic['可用金额']

    chicang_df = G_jy.get_global_chichang_df()
    # 宏源
    # chicang_df_t = chicang_df.loc[chicang_df.证券数量 >= '1']
    # 国泰
    if chicang_df.empty==False:
        chicang_df_t = chicang_df.loc[chicang_df.可用股份 >= '1']
        chichang_list = chicang_df_t.证券代码.tolist()
    else:
        chichang_list = []

    print('chichang_list11',chichang_list)
    if (len(buy_lst) > 0) and (len(chichang_list)<stock_num) :#and (zichanky > 0):
        for tscode_t in buy_lst:
            #已持仓的不再买入
            tscode = tscode_t[0:6]
            maihou_chichanglist = G_jy.get_maihou_chichanglist()
            if( tscode not in chichang_list) and (tscode not in  maihou_chichanglist) and (stock_num > len(chichang_list)):
                # 每只股票买入金额 = 可用资金/（计划持仓数-持仓数量）
                zichan1_n = zichanky / (stock_num - len(chichang_list))

                # price_now ,yesterdayClose,open_today = shipan230128.shishijiage(tscode) # open_today加到这个函数里
                price_now_df = buy_sure_df_t.loc[buy_sure_df_t.ts_code == tscode_t]
                price_now = price_now_df.iloc[-1,2]
                print(tscode,' 实时价格：',price_now)
                if price_now != 0:   #查询股价成功
                    # 股池小于10只股 and 股池里没有这支股票 and 有足够的钱买入股票 and开盘不是一字涨停（创业板20%）
                    buy_num = math.floor(zichan1_n/ (price_now * 100*1.044)) * 100
                    #TTT
                    #if buy_num>=100:
                    #    buy_num = 100  #测试时只买100股
                    # else:
                    #     buy_num = 0
                    #TTT

                    maibushang = G_jy.get_global_maibushang()
                    if maibushang == 1:
                        buy_num = math.floor(zichan1_n/ (price_now * 100*1.2)) * 100

                    buy_amount = buy_num * price_now
                    if ((buy_num > 0) and (len(chichang_list) < stock_num) and (
                            chichang_list.count(tscode) == 0) and (zichanky >= buy_amount)):
                            # and
                            #  (((tscode[0] == '3') and (
                            #         price_now / yesterdayClose <= 1.2)) or (
                            #         (tscode[0] != '3') and (
                            #              price_now / yesterdayClose <=1.1)))):
                        # open价格买入  向下取整 3.15得3
                        # 买入股数 100股的整数倍：
                        print('开始买入股票'+tscode+"   ",buy_num, "股")
                        #TTT
                        temp_buy_df = pd.DataFrame(
                                    {"ts_code": tscode, "buyprice": price_now, "buytime": today, "sellprice": 0, "selltime": '',
                                     "shouyi": 0, "selltype": 0, "state": 'chiyou',}, index=[0])
                        
                        #buy_num = 1  ###T
                        success = buy_order(tscode, buy_num)
                        db.append_todb(temp_buy_df, 'jiaoyi_log')
                        #TTT time.sleep(3)  # 等待成交
                        if success:
                            zichanky = zichanky - buy_amount

                            # 加入持仓股list，这里只是为了for判断买入了几只股票了，chichang_list会在下一个日期会重新读取MaiChuHou_df后赋值
                            maihou_chichanglist = G_jy.get_maihou_chichanglist()
                            maihou_chichanglist.append(tscode)  # tscode    对应stock_chi[i][0]
                            G_jy.set_maihou_chichanglist(maihou_chichanglist)
                            print('剩余可用资金' + tscode + "   ", zichanky, "元")
                            #jiaoyi_log_df = pd.DataFrame(
                            #    {"ts_code": ['1'], "buyprice": [1], "buytime": ['1'], "sellprice": [1], "selltime": ['1'], "shouyi": [1],
                            #     "selltype": ['1'], "state": ['1'],})  ##state:chiyou\selled\cirisell     次日sell
                            #if jiaoyi_log_df.empty == False:
                            #    jiaoyi_log_df.drop(jiaoyi_log_df.index, inplace=True)  # 清空dataframe

                            #temp_df = pd.DataFrame(
                            #        {"ts_code": tscode, "buyprice": price_now, "buytime": today, "sellprice": 0, "selltime": '',
                            #         "shouyi": 0, "selltype": '', "state": 'chiyou',}, index=[0])
                            #jiaoyi_log_df = pd.concat([jiaoyi_log_df,temp_df])
                            #db.append_todb(jiaoyi_log_df, 'jiaoyi_log')

                            # 买入所有所需股票后1、更新持仓情况 2、启动每5秒卖出：
                            print('买入所有所需股票后 更新持仓情况')
                            mbs_flag = 0
                            G_jy.set_global_maibushang(mbs_flag)
                            shipan.chicangqk(today, 0)  # 获取持仓
                        else:
                            print('买入失败，等5秒后再买 ' ,tscode)
                            mbs_flag = 1
                            G_jy.set_global_maibushang(mbs_flag)

    # #买入所有所需股票后1、更新持仓情况 2、启动每5秒卖出：
    # print('买入所有所需股票后 更新持仓情况')
    # shipan230128.chicangqk(today, 0)   #获取持仓

    return



################################ ok 20220316
# #b_f_Mean*ben_mean/amo_mean排序
# 为了提高回测效率，一次性计算所有的mean100
# --@b3
def buy_stock_jisuan(shift1_date, today,G_jy):
    print('buy数据准备中... shift1_date=',shift1_date)
    sqlTables_forward = mystock.date2sqlTable('20220707', shift1_date)  # 数据只有从20190301起
    len_n = 115
    selljs_double_begin_date = sqlTables_forward[-len_n][1:]
    sqlTable_t = mystock.date2sqlTable(selljs_double_begin_date, shift1_date)

    sell_df = pd.DataFrame()
    try:
        sell_df = pd.read_csv(selljs_double_begin_date + '_' + today + '_'+str(len_n) + 'shipan.csv',dtype={'trade_date':str})  # 读取训练数据
        print('读取all_data数据成功,注意数据天数！！')
    except Exception as e:
        pass
        print('zheng4 准备数据中.......')
        for day_all in sqlTable_t:  # sellday_index+1?   #买入天向前12天：回测止天   day--回测当天 这段代码放到这是为了只执行一遍 提速
            if day_all >= 't20221201':
                sql = "select trade_date,ts_code,open,bbuys100,bsells100,bbuys50,bsells50,bbuys200,bsells200,pct_chg,close,free_share,zb50,zs50,zb100,zs100,low,high from " + day_all  ###+ " where ts_code =" + "'" + ts_code + "'"
                temp_df = db.query_data1(sql)
            else:
                sql ="select trade_date,ts_code,open,bbuys100,bsells100,bbuys200,bsells200,pct_chg,close,free_share,zb50,zs50,zb100,zs100,low,high from "  + day_all  ###+ " where ts_code =" + "'" + ts_code + "'"
                temp_df = db.query_data1(sql)
                temp_df['bbuys50'] = 1
                temp_df['bsells50'] =1

            if temp_df.empty == True:
                continue
            sell_df = pd.concat([sell_df,temp_df])

        sell_df.to_csv(selljs_double_begin_date + '_' + today + '_'+str(len_n) + 'shipan.csv', index=False)
    df_all_data = sell_df  # 所有的数据


    #####################去除ST用
    sql_ts_name = "select ts_code,name from stock_basic"
    name_df = db.query_data1(sql_ts_name)
    name_df.set_index('ts_code', inplace=True)

    # @b4
    single_stock_df_new = df_all_data.groupby('ts_code',as_index=False).apply(cuxuan18)

    buy_zhunbei = single_stock_df_new.loc[single_stock_df_new.buy_zhunbei==1]
    # print(buy_zhunbei.index)
    ##########过滤st股票
    cuxuan_list=[]
    if buy_zhunbei.empty == False:
        data_xs_t = pd.merge(buy_zhunbei, name_df, how='inner',
                             left_on='ts_code',
                             right_index=True, sort=False)
        # print('data_xs_t', data_xs_t)
        data_xs_t1 = data_xs_t[~ data_xs_t['name'].str.contains('ST')]  # 删除st股
        data_xs_ok = data_xs_t1[~ data_xs_t1['name'].str.contains('退')]  # 删除退市股
        # print('data_xs_ok', data_xs_ok)
        ##############################
        zhangfu_df4 = data_xs_ok  # stock_basic 空了

        cuxuan_list = zhangfu_df4.ts_code.tolist()
        cuxuan_list = list(set(cuxuan_list))   #去重


    # 准备前40天数据
    # print('to_csv',data_xs_t.tail(10))
    data_xs_t.to_csv(today+'_cuxuan_all_df_shipan.csv', index=False)
    G_jy.set_cuxuan_all_df(data_xs_t)

    return cuxuan_list

#####################################ok

# 屏蔽掉 min_price ,买、卖 都使用 2022-02-25
# --@b4
def grp_cal_1( single_stock_df,forward_day_num) :
    single_stock_df = single_stock_df.sort_values('trade_date')
    single_stock_df[['mean100','min_price']] = single_stock_df.rolling(forward_day_num, min_periods=1).agg({'abs100':'mean','low':'min'})
    # single_stock_df['mean100'] = single_stock_df.rolling(forward_day_num, min_periods=1)['abs100'].mean()
    # single_stock_df['min_price'] = single_stock_df.rolling(10, min_periods=1)['low'].min()
    single_stock_df[['sum100','sum200']] = single_stock_df.rolling(20, min_periods=1).agg({'chunmai100':'sum','chunmai200':'sum'})
    return single_stock_df

###################################


def buy_order(var_stock, vol):

    var_stock = var_stock[0:6]
    url = 'http://100.100.107.232:7777/api/v1.0/orders?key=911911'
    header = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0',
              'Content-Type': 'application/json'}
    post_data = {
        "action": "BUY",
        "symbol": var_stock,
        "type": "MARKET",
        "priceType": 4,  # 4-五档即时成交剩余撤销委托  3-即时成交剩余撤销委托  注意！！！3只用于深圳股4沪深都能用
        # "price": 19.00,
        "amount": vol,

    }
    try:
        req = Request(url=url, data=json.dumps(post_data).encode('utf-8'), headers=header, method='POST')
        response = urlopen(req)
        html = response.read().decode('utf-8')
        print(html)
    except Exception as e:
        ppp = 0
        qq = ppp + 1
        price = 0  # 实在出错就报价000
        print(e)
        print("买入!失败！！失败！！" + var_stock)
        return 0
    return 1

#############################
def cuxuan18(self):
    pd.set_option('display.max_rows', None)  # 设置显示最大行
    pd.set_option('display.max_columns', None)  # 设置显示最大列，None为显示所有列
    single_stock_df = self
    ts_code = single_stock_df.iloc[0, 1]

    not_buy_df = pd.DataFrame()

    if ts_code[0] == '6' or ts_code[0] == '4' or ts_code[0] == '8':
        return not_buy_df

    single_stock_df = single_stock_df.drop(
        index=single_stock_df.trade_date[single_stock_df.trade_date == 0].index)  # 很关键 要去除trade_date == 0的行
    single_stock_df = single_stock_df.sort_values(by='trade_date', axis=0, ascending=1, inplace=False, kind='quicksort',
                                                  na_position='last')  # 升序
    single_stock_df = single_stock_df.reset_index(drop=True)
    # print(single_stock_df.tail(1))
    # single_df_today
    # ['trade_date', 'ts_code', 'open', 'close', 'high', 'low', 'vol',
    #    'pct_chg', 'pe', ],

    single_df_len = len(single_stock_df)


    # if ts_code!='002870.SZ':
    # 20230515
    # if ts_code != '301193.SZ':
    # if ts_code != '300882.SZ':
    # if ts_code != '300809.SZ':
    # if ts_code != '000931.SZ':
    # if ts_code != '000973.SZ':
    # if ts_code != '002076.SZ':
    # if ts_code != '300076.SZ':

    # 20230928 测试用
    # if ts_code in ["001366.SZ", "300391.SZ", "300440.SZ",'300562.SZ','300942.SZ','301105.SZ']:
    #     debug = 1
    #     buy_df = single_stock_df.tail(1)
    #     buy_df['buy_zhunbei'] = 1
    #     buy_df['zheng_price_mean'] = 10
    #     return buy_df



        # single_df_len
        # ['trade_date', 'ts_code', 'open', 'close', 'high', 'low', 'vol',
        #  'pct_chg', 'pe', ]
    if single_df_len > 57:  #上市大于57天
        #20231009
        single_df_pct = single_stock_df[['pct_chg']]
        # 从frist头便利
        cnt_max = 0
        cnt_new = 0
        idx_end = 0
        zheng_max = 0
        df = pd.DataFrame()
        idx_frist = single_stock_df.head(1).index.to_list()[0]
        # print('idx_frist日期',single_stock_df.iloc[idx_frist,0])
        idx_last = single_stock_df.tail(1).index.to_list()[0]
        for idx in range(idx_frist, idx_last, 1):
            cnt_new = 0
            for idx_l in range(idx_last - 20, idx_last + 1, 1):
                df = single_stock_df.iloc[idx:idx_l, ]
                if df.empty == False:
                    # print('idx日期', single_stock_df.iloc[idx, 0])
                    # print('idx_l日期', single_stock_df.iloc[idx_l, 0])
                    # print('df.pct_chg.max()',df.pct_chg.max())
                    # print('df.pct_chg.min()',df.pct_chg.min())
                    pct_max_flag = df.pct_chg.max() < 4.7
                    pct_min_flag = df.pct_chg.min() > -4.7
                    if pct_max_flag and pct_min_flag:

                        zheng = (df.high.max() - df.low.min()) / df.low.min() * 100
                        # print('zhengfu>=20 ??', zheng)
                        # print('cnt_new', cnt_new)
                        if zheng < 20.7:
                            cnt_new = idx_l - idx + 1
                            if cnt_max < cnt_new:
                                cnt_max = cnt_new
                                idx_end = idx_l
                                # print('起点日期',single_stock_df.iloc[idx,0])
                                # print('中点日期', single_stock_df.iloc[idx_l, 0])
                                # print('zhengfu',zheng)
                            if zheng_max < zheng:
                                zheng_max = zheng
                        else:
                            cnt_new = 0
                    else:
                        cnt_new = 0

        #20230927 pct2日大于
        pct2_flag = 0
        pct2_df = single_df_pct.tail(2)
        pct2_sum = pct2_df.pct_chg.sum()
        if ts_code[0]=='3' and pct2_sum>24:
            pct2_flag=1
        if ts_code[0]!='3' and pct2_sum>11:
            pct2_flag=1

        # 20231003 pct1日大于
        pct1_flag = 0
        pct1_df = single_df_pct.tail(1)
        pct1_sum = pct1_df.pct_chg.to_list()[0]
        if ts_code[0] == '3' and pct1_sum > 19:
            pct1_flag = 1
        if ts_code[0] != '3' and pct1_sum > 9.3:
            pct1_flag = 1




        # print('single_stock_df',single_stock_df.head(2))
        #single_stock_df
        #   trade_date    ts_code  close  chazhi50  chazhi100  pct_chg  free_share  \(6)
        # 0   20220609  000665.SZ   8.61   -1419.0    -1415.0  -0.6920  61408.4993
        # 1   20220610  000665.SZ   8.59   -1799.0    -1211.0  -0.2323  61408.4993
        #
        #    bbuys100  bsells100  chazhi200  mean100_abs  mean100  sum100  sum200 (13)
        # 0    2686.0     4101.0     -707.0       1415.0   1415.0 -1415.0  -707.0
        # 1    1839.0     3050.0       74.0       1211.0   1313.0 -2626.0  -633.0

        #   zb50, zs50, zb100, zs100,

        free_share = single_stock_df.iloc[-2, 6]
        close_sh1 = single_stock_df.iloc[-1,2]
        shizhi = free_share * close_sh1

        zheng_price_mean = df.close.mean()
        close_sh1_zheng_p = round(close_sh1 / zheng_price_mean, 3)
        close_sh1_zheng_p_flag=0
        if ts_code[0] == '3':
            close_sh1_zheng_p_flag = close_sh1_zheng_p >= 1.7 * 0.84  #大于就排除
        if ts_code[0] != '3':
            close_sh1_zheng_p_flag = close_sh1_zheng_p >= 1.7 * 0.91

        # if (cnt_max <= 30) or (single_df_len - idx_end > 20) or (pct2_flag == 1 ) or (shizhi>1100000):
        if (cnt_max <= 30) or ((single_df_len - idx_end) > 25) or (pct2_flag == 1) or (pct1_flag == 1) or (
                    shizhi > 1200000) or (close_sh1_zheng_p_flag == 1):
            return not_buy_df

        else:
            single_stock_df['buy_zhunbei'] = 1
            single_stock_df['zheng_price_mean'] = zheng_price_mean
            return single_stock_df
    return not_buy_df

#################################
