#def _init():、

#首先声明此程序仅供学习交流使用，不可商用不可实盘否则后果自负！
#首先声明此程序仅供学习交流使用，不可商用不可实盘否则后果自负！
#首先声明此程序仅供学习交流使用，不可商用不可实盘否则后果自负！
#    global _global_dict
#    _global_dict = {}
#    print('_global_dict_inited')

_global_dict = {}
print('_global_dict_inited')
def set_value(name, value):
    global _global_dict
    _global_dict[name] = value

def get_value(name, defValue=None):
    global _global_dict
    try:
        return _global_dict[name]
    except KeyError:
        return defValue