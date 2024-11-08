# stock
量化交易

#首先声明此程序仅供学习交流使用，不可商用不可实盘否则后果自负！
#首先声明此程序仅供学习交流使用，不可商用不可实盘否则后果自负！
#首先声明此程序仅供学习交流使用，不可商用不可实盘否则后果自负！

这是一套量华系统 
1、数据准备
2、较易
3、数据库结构
4、mfc数据源

1、ruku.py是基础

2、
sp11_old_now文件夹中的是一套程序，这是个测试程序 使用python3.11（我记得是3.11） 打开
buy_mission_file221010    买功能
sell_mission221010.py    卖功能
db   数据库连接
data_def    全局变量
getL2Data  从ftp中获得L2数据（已经不用）
globalvar.py  保留就好 功能忘了
mystock.py  使用了该文件中的大单计算功能
JiaoYiApi.py  对接jy的api（我这个api已经不用了  你可以改造为  “策略易”的api 自己百度） 
ss33.py相当于 main（）从这里开始运行 def buy_mission(today)

3、mysql数据库结构
4、mfc多线程程序 用于数据加工 

运行大致顺序：
1请求持仓情况
2懒得写了 从ss33 的  buy_mission(today)函数中自己看吧



