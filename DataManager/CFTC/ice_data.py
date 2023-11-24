import os
from datetime import datetime

import pandas as pd
import pytz
from ._func import *
import DataManager.CFTC._data_clean as cpd
from loguru import logger
from ..utils.TableStructure.__4cftc import *
from ..utils.mysql_peewee import insert_data2table, get_table

os.environ["http_proxy"] = "http://127.0.0.1:7890"
os.environ["https_proxy"] = "http://127.0.0.1:7890"
database_path = r'D:\CFTC\Future\Disagg\ICE'

keep_columns = ['date', 'contract_market_code',
                'change_in_m_money_long_all', 'change_in_m_money_short_all',
                'change_in_m_money_spread_all', 'change_in_nonrept_long_all',
                'change_in_nonrept_short_all',
                'change_in_other_rept_long_all', 'change_in_other_rept_short_all',
                'change_in_other_rept_spread_all', 'change_in_prod_merc_long_all',
                'change_in_prod_merc_short_all', 'change_in_swap_long_all',
                'change_in_swap_short_all', 'change_in_swap_spread_all',
                'change_in_tot_rept_long_all', 'change_in_tot_rept_short_all',
                'conc_gross_le_4_tdr_long_all', 'conc_gross_le_4_tdr_short_all',
                'conc_gross_le_8_tdr_long_all', 'conc_gross_le_8_tdr_short_all',
                'conc_net_le_4_tdr_long_all', 'conc_net_le_4_tdr_short_all',
                'conc_net_le_8_tdr_long_all', 'conc_net_le_8_tdr_short_all',
                'm_money_positions_long_all', 'm_money_positions_short_all',
                'm_money_positions_spread_all', 'nonrept_positions_long_all',
                'nonrept_positions_short_all',
                'other_rept_positions_long_all', 'other_rept_positions_short_all',
                'other_rept_positions_spread_all', 'pct_of_oi_m_money_long_all',
                'pct_of_oi_m_money_short_all', 'pct_of_oi_m_money_spread_all',
                'pct_of_oi_nonrept_long_all', 'pct_of_oi_nonrept_short_all',
                'pct_of_oi_other_rept_long_all', 'pct_of_oi_other_rept_short_all',
                'pct_of_oi_other_rept_spread_all', 'pct_of_oi_prod_merc_long_all',
                'pct_of_oi_prod_merc_short_all', 'pct_of_oi_swap_long_all',
                'pct_of_oi_swap_short_all', 'pct_of_oi_swap_spread_all',
                'pct_of_oi_tot_rept_long_all', 'pct_of_oi_tot_rept_short_all',
                'prod_merc_positions_long_all', 'prod_merc_positions_short_all',
                'swap_positions_long_all', 'swap_positions_short_all',
                'swap_positions_spread_all', 'tot_rept_positions_long_all',
                'tot_rept_positions_short_all', 'traders_m_money_long_all',
                'traders_m_money_short_all', 'traders_m_money_spread_all',
                'traders_other_rept_long_all', 'traders_other_rept_short_all',
                'traders_other_rept_spread_all', 'traders_prod_merc_long_all',
                'traders_prod_merc_short_all', 'traders_swap_long_all',
                'traders_swap_short_all', 'traders_swap_spread_all', 'traders_tot_all',
                'traders_tot_rept_long_all', 'traders_tot_rept_short_all', 'futonly_or_combined']
rename_dict = {'As_of_Date_Form_MM/DD/YYYY': 'date', 'CFTC_Commodity_Code': 'contract_market_code',
               }


def _ice_data_format(path):
    data = pd.read_csv(path)
    data.rename(columns=rename_dict, inplace=True)
    data.columns = [x.lower() for x in data.columns]
    data['date'] = pd.to_datetime(data['date'])
    return data


def load_history_data():
    data_list = []
    for file in os.listdir(os.path.join(database_path, 'history')):
        data_ = _ice_data_format(os.path.join(database_path, 'history', file))
        data_list.append(data_)
    data = pd.concat(data_list, axis=0)
    data = data[keep_columns].reset_index(drop=True)
    data_f = data.query("futonly_or_combined=='FutOnly'").drop(['futonly_or_combined'], axis=1)
    data_c = data.query("futonly_or_combined=='Combined'").drop(['futonly_or_combined'], axis=1)
    return data_f, data_c


def insert_history_data():
    table_f = CFTCDisaggFuture
    table_c = CFTCDisaggFutureOption
    data_f, data_c = load_history_data()

    insert_data2table(table_f, data_f, conflict='ignore')
    insert_data2table(table_c, data_c, conflict='ignore')


def update_ice_week():
    load_ice_cot_week_csv(os.path.join(database_path, 'history'), date='')


def update_ice_data(year='2023'):
    table_f = CFTCDisaggFuture
    table_c = CFTCDisaggFutureOption
    file = load_ice_cot_year_csv(os.path.join(database_path, 'history'), year=year)
    old_file = os.path.join(database_path, 'history', f'COTHist{year}.csv')
    if check_file_same(file, old_file):
        return None
    else:
        clone_file(file, old_file, delete=False)
        data = _ice_data_format(file)
        data = data[keep_columns].reset_index(drop=True)
        data_f = data.query("futonly_or_combined=='FutOnly'").drop(['futonly_or_combined'], axis=1)
        data_c = data.query("futonly_or_combined=='Combined'").drop(['futonly_or_combined'], axis=1)
        insert_data2table(table_f, data_f, conflict='ignore')
        insert_data2table(table_c, data_c, conflict='ignore')
