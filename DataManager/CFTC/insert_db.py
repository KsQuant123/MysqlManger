"""
将CFTC csv插入数据库，（历史数据）

"""

import os
import pandas as pd
from loguru import logger
import calendar
from datetime import timedelta
from dateutil.parser import parse
from ..utils.TableStructure.__4cftc import *
from ..utils.mysql_peewee import insert_data2table, get_table

database_path = 'D:\CFTC'
data_type_list = ['FutureOptions', 'Future']
cot_type_list = ['Report', 'Traders', 'Disagg']
table_dict = {
    'FutureOptions': {'Report': CFTCReportFutureOption, 'Traders': CFTCTradersFutureOption,
                      'Disagg': CFTCDisaggFutureOption},
    'Future': {'Report': CFTCReportFuture, 'Traders': CFTCTradersFuture, 'Disagg': CFTCDisaggFuture}}

data = get_table(CFTCReportFuture).dropna()
symbol2code = dict(zip(data['symbol'], data['contract_market_code']))


def load_cot_data(code, data_type, cot_type, *args, **kwargs) -> pd.DataFrame:
    """
    获取COT 原始数据()
    :param code: str 标的代码
    :param data_type: str 数据种类(Future:期货, FutureOptions:期货期权)
    :param cot_type: str COT种类('Report', 'Traders', 'Disagg')
    :return:cot data
    """
    filename = os.path.join(database_path, data_type, cot_type, 'cot_data', 'cot_data_{}.csv'.format(code))
    raw_data = pd.read_csv(filename, parse_dates=True)
    raw_data['Contract_Market_Code'] = code
    raw_data['symbol'] = symbol2code.get(code, None)
    raw_data.columns = [x.lower() for x in raw_data.columns]
    return raw_data


def cftc_insert_db_all():
    for data_type in data_type_list:
        for cot_type in cot_type_list:
            for file in os.listdir(os.path.join(database_path, data_type, cot_type, 'cot_data')):
                code = file.split('_')[-1].split('.')[0]
                df = load_cot_data(code, data_type, cot_type)
                table = table_dict[data_type][cot_type]
                insert_data2table(table, df, conflict='ignore')


def cftc_insert_db(year: int = 2023):
    for data_type in data_type_list:
        for cot_type in cot_type_list:
            for dirs in os.listdir(os.path.join(database_path, data_type, cot_type, 'raw_cot_data')):
                code = dirs
                file = os.path.join(database_path, data_type, cot_type, 'raw_cot_data', dirs, f'raw_factor_{year}.csv')
                if os.path.exists(file):
                    print(code, file)
                    df = load_cot_data2(file, code)
                    table = table_dict[data_type][cot_type]
                    insert_data2table(table, df, conflict='ignore')


def load_cot_data2(filename, code, *args, **kwargs) -> pd.DataFrame:
    """
    获取COT 原始数据()
    :param code: str 标的代码
    :param data_type: str 数据种类(Future:期货, FutureOptions:期货期权)
    :param cot_type: str COT种类('Report', 'Traders', 'Disagg')
    :return:cot data
    """
    raw_data = pd.read_csv(filename, parse_dates=True)
    raw_data['Contract_Market_Code'] = code
    raw_data['symbol'] = symbol2code.get(code, None)
    raw_data.columns = [x.lower() for x in raw_data.columns]
    return raw_data


if __name__ == '__main__':
    # print(help(os.path))
    cftc_insert_db()
