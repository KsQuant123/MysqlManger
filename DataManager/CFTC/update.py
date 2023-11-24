from datetime import datetime
import pytz
from ._func import *
import DataManager.CFTC._data_clean as cpd
from loguru import logger

"""
更新COT数据并处理
"""

# 获取美国东部时间
# logger.add("log/CFTC数据更新_{time:YYYY_MM_DD}.log")
now = datetime.now(tz=pytz.timezone('US/Eastern'))
current_year = now.year  # 可自行输入年份
# current_year = 2021
# 获取更新的URL:
url_base = 'https://www.cftc.gov/files/dea/history/'
cot_file_name_list_base = ['fut_disagg_xls_{}.zip', 'com_disagg_xls_{}.zip',
                           'fut_fin_xls_{}.zip', 'com_fin_xls_{}.zip',
                           'dea_fut_xls_{}.zip', 'dea_com_xls_{}.zip'
                           ]
# cot_file_name_list = [x.format(current_year) for x in cot_file_name_list_base]
# cot_url_list = [url_base + x for x in cot_file_name_list]

# database_path_list = ['FutureOptions/Disagg/', 'FutureOptions/Report/', 'FutureOptions/Traders/']
database_path_list = ['FutureOptions/Disagg', 'FutureOptions/Report/', 'FutureOptions/Traders/',
                      'Future/Disagg', 'Future/Report', 'Future/Traders']
data_type_list = ['Future', 'FutureOptions', ]
cot_type_list = ['Report', 'Traders', 'Disagg']
file_dict = {'FutureOptions': 'com', 'Future': 'fut', 'Report': 'dea', 'Traders': 'fin', 'Disagg': 'disagg'}
CFTC_path = r'D:\CFTC'


def update(year=2023, main_path=r'D:\CFTC'):
    if year is None:
        year = current_year
    cot_file_name_list = [x.format(year) for x in cot_file_name_list_base]
    for i in data_type_list:
        for t in cot_type_list:
            filename = [x for x in cot_file_name_list if file_dict.get(i) in x and file_dict.get(t) in x][0]
            # 加载zip到指定位置
            database_path = os.path.join(main_path, i, t)
            ori_path = os.path.join(database_path, 'data', filename)
            update_path = load_CFTC_zip(path=os.path.join(database_path, 'data'), cot_filename=filename)
            # update_path = r'FutureOptions\Disagg\data\update.zip'
            logger.info(f'处理{filename}')
            logger.info(f'下载更新文件{update_path}')
            logger.info(f'对比原始文件{ori_path}')

            # 判断zip是否相同
            if check_zip_same(update_path, ori_path):
                logger.info(f"{update_path}, {ori_path}, '文件相同'")
            else:
                if clone_file(update_path, ori_path, delete=True):
                    update_zip(ori_path)
                    code_list = cpd.data_to_raw_factor_yearly(database_path, update_zip=ori_path)
                    cpd.raw_data_combine(database_path, code_list=code_list)
            # 更新这个zip
