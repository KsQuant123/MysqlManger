from fredapi import Fred
import numpy as np
import pandas as pd
from datetime import datetime
from ...utils.mysql_peewee import insert_data2table
from ...utils.TableStructure.__2fs_macro import table_macro_fs_info, table_macro_fs_data, table_macro_fs_data_r

api_key = '4dd97a659913639fbad81046ce9ea8b3'

fred = Fred(api_key)


def parse(date_str, format='%Y-%m-%d'):
    """
    helper function for parsing FRED date string into datetime
    """
    print(date_str)
    try:
        rv = pd.to_datetime(date_str, format=format)
    except:
        try:
            rv = datetime.strptime('1270-01-01 00:00:00', "%Y-%m-%d %H:%M:%S")
        except:
            return None
    print(rv)
    if hasattr(rv, 'to_pydatetime'):
        rv = rv.to_pydatetime()
    return rv


def get_fs_info(code):
    # info = fred.search(code, limit=1)
    info = fred.get_series_info(code)
    info = pd.DataFrame([info])
    for field in ["realtime_start", "realtime_end", "observation_start", "observation_end", "last_updated"]:
        info[field] = info[field].apply(parse, format=None)
    info['tz'] = info['last_updated'].apply(lambda x: str(x.tz))
    return info


def get_fs_data(code, all_s=False):
    if all_s:
        data = fred.get_series_all_releases(code, realtime_start='2020-01-01', realtime_end='2023-04-04')
        data = data.rename(columns={'realtime_start': 'real_date'})
    else:
        data = fred.get_series_latest_release(code)
        data = data.reset_index()
        data.columns = ['date', 'value']
    data['id'] = code
    data = data.where(data.notnull(), None)
    return data


def update_fs_data(code, table_info=table_macro_fs_info, table_data=table_macro_fs_data,
                   table_data_all=table_macro_fs_data_r, all_series=False):
    if isinstance(code, str):
        code = [code]

    for _code in code:
        if table_info.get_or_none(table_info.id == _code) is None:
            info = get_fs_info(_code)
            insert_data2table(table=table_info, data=info, conflict='replace')
        else:
            pass
        # if info.loc[0, 'realtime_end'] == info.loc[0, 'realtime_start'] and not all_series:
        if not all_series:
            data = get_fs_data(_code, all_s=False)
            print(data)
            insert_data2table(table=table_data, data=data, conflict='ignore')
        else:
            data = get_fs_data(_code, all_s=True)
            insert_data2table(table=table_data_all, data=data, conflict='ignore')


if __name__ == '__main__':
    test_code = 'CPIAUCSL'
    test_data = get_fs_data(test_code, True)
    print(test_data)
    # test_info = get_fs_info(test_code)

    # print(test_data)

    # code_list = ['BAMLH0A0HYM2EY', 'BAMLC0A4CBBBEY','DGS10']
    # code_dict = {'BAMLH0A0HYM2EY': '美国投资级债券收益率', 'BAMLC0A4CBBBEY': '美国高收益债券收益率'}
    # code_dict = {'BAMLEMEBCRPIEEY': '欧洲投资级债券收益率', 'BAMLHE00EHYIEY': '欧洲高收益债券收益率'}
    # code_list = list(code_dict.keys())
    # update_fs_data(code=[code_list])
    # code_list2 = ['CPIAUCSL']
    # update_fs_data(code=code_list2, all_series=True)
