
import pandas as pd

#首先声明此程序仅供学习交流使用，不可商用不可实盘否则后果自负！
#首先声明此程序仅供学习交流使用，不可商用不可实盘否则后果自负！
#首先声明此程序仅供学习交流使用，不可商用不可实盘否则后果自负！
class Global_value(object):

    def __init__(self):
        df_temp = pd.DataFrame({"Time": ['09:30:00', '09:31:00', '09:32:00'],
                                "buyVol": ['0', '0', '0'],
                                "sellVol": ['1', '1', '1'],
                                "buyVolsum": ['1', '1', '1'],
                                "sellVolsum": ['1', '1', '1'],
                                "sumchazhi": ['1', '1', '1'],
                                "stock": ['600', '600', '600']
                                })
        self.global_1 =df_temp
        self.global_2 =df_temp
        self.global_3 =df_temp

    #在类里面改变全局变量的值
    def get_global_1(self):
#        print("get_global_1:", self.global_1)
        return self.global_1

    def set_global_1(self,global_1):
        self.global_1=global_1
 #       print("set_global_1:", self.global_1)
        return

    def get_global_2(self):
       # print("get_global_1:", self.global_1)
        return self.global_2

    def set_global_2(self,global_2):
        self.global_2=global_2
        #print("set_global_1:", self.global_1)
        return
    def get_global_3(self):
        #print("get_global_1:", self.global_3)
        return self.global_3

    def set_global_3(self,global_3):
        self.global_3=global_3
        #print("set_global_1:", self.global_1)
        return

class Stocks_data():
    def __init__(self,stock_code1,stock_code2,stock_code3,dadan1,dadan2,dadan3,
                 sellvol1,sellvol2,sellvol3,state1,state2,state3,name1,name2,name3,st_num1,st_num2,st_num3):
        self.stock_code1 = stock_code1
        self.stock_code2 = stock_code2
        self.stock_code3 = stock_code3
        self.dadan1 = dadan1
        self.dadan2 = dadan2
        self.dadan3 = dadan3
        self.sellvol1 = sellvol1
        self.sellvol2 = sellvol2
        self.sellvol3 = sellvol3
        self.state1 = state1  # 状态'待连接'
        self.state2 = state2  # 状态'待连接'
        self.state3 = state3  # 状态'待连接'
        self.name1 = name1  # 股名'待连接'  # 名称
        self.st_num1 = st_num1  # 当前股数
        self.state2 = state2# 状态'待连接'
        self.name2 = name2#股名'待连接'  # 名称
        self.st_num2 = st_num2#当前股数
        self.state3 = state3  # 状态'待连接'
        self.name3 = name3  # 股名'待连接'  # 名称
        self.st_num3 = st_num3  # 当前股数

    def reset(self,stock1,stock2,stock3,da1,da2,da3,svol1,svol2,svol3,sta1,sta2,sta3,na1,na2,na3,st_n1,st_n2,st_n3):
        self.stock_code1 = stock1
        self.stock_code2 = stock2
        self.stock_code3 = stock3
        self.dadan1 = da1
        self.dadan2 = da2
        self.dadan3 = da3
        self.sellvol1 = svol1
        self.sellvol2 = svol2
        self.sellvol3 = svol3
        self.state1 = sta1  # 状态'待连接'
        self.state2 = sta2  # 状态'待连接'
        self.state3 = sta3  # 状态'待连接'
        self.name1 = na1  # 股名'待连接'  # 名称
        self.st_num1 = st_n1  # 当前股数
        self.state2 = sta2  # 状态'待连接'
        self.name2 = na2  # 股名'待连接'  # 名称
        self.st_num2 = st_n2  # 当前股数
        self.state3 = sta3  # 状态'待连接'
        self.name3 = na3  # 股名'待连接'  # 名称
        self.st_num3 = st_n3  # 当前股数

    def get_stock_code1(self):
        return self.stock_code1

    def set_stock_code1(self,stock_code1):
        self.stock_code1=stock_code1
        return

    def get_stock_code2(self):
        return self.stock_code2

    def set_stock_code2(self, stock_code2):
        self.stock_code2 = stock_code2
        return

    def get_stock_code3(self):
        return self.stock_code3

    def set_stock_code3(self, stock_code3):
        self.stock_code3 = stock_code3
        return

    def get_dadan1(self):
        return self.dadan1
    def set_dadan1(self,dadan1):
        self.dadan1=dadan1
        return
    def get_dadan2(self):
        return self.dadan2
    def set_dadan2(self,dadan2):
        self.dadan2=dadan2
        return
    def get_dadan3(self):
        return self.dadan3
    def set_dadan3(self,dadan3):
        self.dadan3=dadan3
        return

    def get_sellvol1(self):
        return self.sellvol1
    def set_sellvol1(self,sellvol1):
        self.sellvol1=sellvol1
        return
    def get_sellvol2(self):
        return self.sellvol2
    def set_sellvol2(self,sellvol2):
        self.sellvol2=sellvol2
        return
    def get_sellvol3(self):
        return self.sellvol3
    def set_sellvol3(self,sellvol3):
        self.sellvol3=sellvol3
        return

    def getState1(self):
        return self.state1

    def setState1(self,state1):
        self.state1 = state1

    def getState2(self):
        return self.state2

    def setState2(self, state2):
        self.state2 = state2

    def getState3(self):
        return self.state3

    def setState3(self, state3):
        self.state3 = state3

    def getName1(self):
        return self.name1

    def setName1(self, name1):
        self.name1 = name1

    def getSt_num1(self):
        return self.st_num1

    def setSt_num1(self, st_num1):
        self.st_num1 = st_num1

    def getState2(self):
        return self.state2

    def setState2(self, state2):
        self.state2 = state2

    def getName2(self):
        return self.name2

    def setName2(self, name2):
        self.name2 = name2

    def getSt_num2(self):
        return self.st_num2

    def setSt_num2(self, st_num2):
        self.st_num2 = st_num2

    def getState3(self):
        return self.state3

    def setState3(self, state3):
        self.state3 = state3

    def getName3(self):
        return self.name3

    def setName3(self, name3):
        self.name3 = name3

    def getSt_num3(self):
        return self.st_num3

    def setSt_num3(self, st_num3):
        self.st_num3 = st_num3



class Global_jiaoyi():

    def __init__(self):
        self.buy_list = []
        self.buy_list1 = []
        self.buy_list2 = []
        self.buy_sure_list=[]
        self.mission930_20_cnt=0
        self.jiaoyi_locked = 0
        self.maibushang = 0
        self.zjqk = {}  # 资金情况 dict
        self.maihou_chichanglist=[]
        self.selljisuan_df = pd.DataFrame()

        self.sell_lastsum_df = pd.DataFrame()

        self.chazhi_max_df = pd.DataFrame({'ts_code':[000],'today_chazhi_max':[0]})
        #print('G_jy  init')
        df_temp = pd.DataFrame({"证券代码": ['000'],
                                "证券名称": ['0'],
                                "证券数量": ['0'],
                                "可卖数量": ['0'],
                                "最新价": ['0'],
                                "最新市值": ['0'],
                                "成本价": ['0'],
                                "成本金额": ['0'],
                                "参考盈亏": ['0'],
                                "盈亏比例": ['0'],
                                "买入冻结": ['0'],
                                "卖出冻结": ['0'],
                                "在途买入": ['0'],
                                "在途卖出": ['0'],
                                "股东代码": ['0'],
                                "交易市场": ['0'],
                                "备用": ['0']
                                })
        self.global_chichang_df =df_temp
        if self.global_chichang_df.empty == False:
            self.global_chichang_df.drop(self.global_chichang_df.index, inplace=True)  # 清空dataframe
        df_temp2 = pd.DataFrame({"tscode": ['000'],
                                "today_sell": ['0'],
                                "locked": ['0']
                                })
        self.stock_5sec_df = df_temp2
        if self.stock_5sec_df.empty == False:
            self.stock_5sec_df.drop(self.stock_5sec_df.index, inplace=True)  # 清空dataframe

    #在类里面改变全局变量的值
    def get_global_chichang_df(self):
        return self.global_chichang_df

    def set_global_chichang_df(self,global_chichang_df):
        self.global_chichang_df=global_chichang_df
        return

    def get_maihou_chichanglist(self):
        return self.maihou_chichanglist

    def set_maihou_chichanglist(self,maihou_chichanglist):
        self.maihou_chichanglist=maihou_chichanglist
        return

    def get_selljisuan_df(self):
 #       print('get_selljisuan_df')
        return self.selljisuan_df

    def set_selljisuan_df(self,selljisuan_df):
        self.selljisuan_df=selljisuan_df
  #      print('set_selljisuan_df')
        ppp=9
        return

    def get_sell_lastsum_df(self):
        return self.sell_lastsum_df

    def set_sell_lastsum_df(self, sell_lastsum_df):
        self.sell_lastsum_df = sell_lastsum_df
        return

    def get_buy_sure_list(self):
        return self.buy_sure_list

    def set_buy_sure_list(self, buy_sure_list):
        self.buy_sure_list = buy_sure_list
        return


    def get_mission930_20_cnt(self):
        return self.mission930_20_cnt

    def set_mission930_20_cnt(self, mission930_20_cnt):
        self.mission930_20_cnt = mission930_20_cnt
        return

    def get_datas0_2(self):
        return self.datas0_2_df
    def set_datas0_2(self, datas0_2_df):
        self.datas0_2_df = datas0_2_df
        return

    def get_buy_list(self):
        return self.buy_list
    def set_buy_list(self,buy_list):
        self.buy_list=buy_list
        return

    def get_buy_list1(self):
        return self.buy_list1
    def set_buy_list1(self,buy_list1):
        self.buy_list1=buy_list1
        return

    def get_buy_list2(self):
        return self.buy_list2
    def set_buy_list2(self,buy_list2):
        self.buy_list2=buy_list2
        return

    def get_zjqk(self):  # 资金情况 dict
        return self.zjqk
    def set_zjqk(self,zjqk):
        self.zjqk=zjqk
        return

    def get_jiaoyi_locked(self):  # 交易锁
        return self.jiaoyi_locked
    def set_jiaoyi_locked(self,jiaoyi_locked):
        self.jiaoyi_locked=jiaoyi_locked
        return

    def get_L2_COM_DATA_DF(self):  # L2_com_data
        return self.L2_COM_DATA_DF
    def set_L2_COM_DATA_DF(self,L2_COM_DATA_DF):
        self.L2_COM_DATA_DF=L2_COM_DATA_DF
        return

    def get_all_data_df(self):  # L2_com_data
        return self.All_data_df
    def set_all_data_df(self,all_data_df):
        self.all_data_df=all_data_df
        return

    def get_cuxuan_all_df(self):
        return self.cuxuan_all_df

    def set_cuxuan_all_df(self,cuxuan_all_df):
        self.cuxuan_all_df=cuxuan_all_df
        return

    def get_930_flag(self):
        return self.g930_flag

    def set_930_flag(self, g930_flag):
        self.g930_flag = g930_flag
        return

    def get_930_df(self):
        return self.g930_df

    def set_930_df(self,g930_df):
        self.g930_df=g930_df
        return

    def set_global_maibushang(self,maibushang):
        self.maibushang=maibushang
        return

    def get_global_maibushang(self):
        return self.maibushang

    def get_chazhi_max_df(self):
        return self.chazhi_max_df

    def set_chazhi_max_df(self,chazhi_max_df):
        self.chazhi_max_df=chazhi_max_df
        return
