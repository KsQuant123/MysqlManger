import os
import sys
import json

sys.path.append(os.path.abspath(os.path.join(os.path.abspath(__file__), '../../../..', )))
from ExecutableFile.Config.config import main_path

title_list = []
title_list += [
    '美国-GDP综合指标', '美国-物价', '美国-市场指标', '美国-联准会',
    '中国-GDP综合指标', '中国-市场指标', '中国-物价',
    '美国-股市', '中國-股市', '香港-股市',
    '美元', '欧元', '澳币', '加币', '英镑', '日圆',
    '原油', '黄金', '白银', '铁矿砂', '铜', '黄豆', '玉米', '小麦', '棉花', '咖啡', '天然气',
    '美债', '美国-公司债', '新兴市场-股债市',
    '美國-S&P500各板塊EPS', '航运', '汽车', '科技', '半导体', '工业/制造业', '美国-房地产', '美国-金融',
    '美国-科技巨头', '加密货币', '利差高频数据', '波动率', '地缘政治', '全球-景气衰退'
]
title_list += ['行情-债券', '行情-外汇', '行情-股市', '行情-CDF', '行情-波动率']
title_list = ['行情-债券']
py_file = os.path.join(main_path, r'ExecutableFile\Script\macro\macro_mm_update_by_table_id.py')
# title_list = json.dumps(title_list)
command = rf'python {py_file} -title_list="{title_list}"'
print(command)
os.system(command)