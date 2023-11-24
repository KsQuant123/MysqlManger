import pandas as pd
import os
from glob import glob
# import data_process as dp
import DataManager.CFTC._data_process as dp
from loguru import logger
from tqdm import tqdm
"""
对已有COT数据进行处理
"""


def data_process(temp_df, database_path):
    # print(temp_df['CFTC_Contract_Market_Code'].iloc[0])
    code = str(temp_df['CFTC_Contract_Market_Code'].iloc[0])
    raw_data_path = os.path.join(database_path, 'raw_data', code)
    dp.create_file(raw_data_path)
    raw_data = dp.raw_data_load(term_df=temp_df, data_path=raw_data_path)

    raw_factor_path = os.path.join(database_path, 'raw_cot_data', code)
    dp.create_file(raw_factor_path)
    raw_factor = dp.raw_factor_load(term_df=raw_data, data_path=raw_factor_path,
                                    columns_list=dp.columns_dict[os.path.split(database_path)[1]])
    return None


def data_to_raw_factor_yearly(path, update_zip=None):
    file_path = path + '/data/'
    if update_zip is not None:
        zip_list = [update_zip]
    else:
        zip_list = glob(f'{file_path}*' + '*' + '.zip', recursive=True)
        zip_list = [x for x in zip_list if 'update' not in x]
    print(zip_list)
    for file in tqdm(zip_list):
        data_dict = dp.read_zip_excel(file)
        for i in data_dict:
            data = data_dict[i]
            data.columns = [x.split('\'')[-1] for x in data.columns]
            data['CFTC_Contract_Market_Code'] = data['CFTC_Contract_Market_Code'].astype(str)
            data.groupby('CFTC_Contract_Market_Code').apply(data_process, database_path=path)
    return data['CFTC_Contract_Market_Code'].unique().tolist()


def raw_data_combine(path, code_list=None):
    raw_data_path = os.path.join(path, 'raw_cot_data')
    if code_list is None:
        code_list = os.listdir(raw_data_path)
    for code in tqdm(code_list):
        df_combine = pd.DataFrame()
        for file_name in os.listdir(os.path.join(raw_data_path, code)):
            temp_df = pd.read_csv(os.path.join(raw_data_path, code, file_name), index_col='date')
            df_combine = pd.concat([df_combine, temp_df], axis=0)
        df_combine = df_combine.sort_values('date', ascending=True)
        cot_data_path = os.path.join(path, 'cot_data', 'cot_data_{}.csv'.format(code))
        df_combine.to_csv(cot_data_path, date_format='%Y-%m-%d')


# def cot_data_to_factor(path):
#     return None
#     cot_data_path = os.path.join(path, 'cot_data')
#     for file_name in os.listdir(cot_data_path):
#         cot_data = pd.read_csv(os.path.join(cot_data_path, file_name), index_col='date')
#         cot_data = dp.
#         cot_data_path = os.path.join(path, 'cot_factor', file_name.replace('cot_data', 'cot_factor'))
#         cot_data.to_csv(cot_data_path, date_format='%Y-%m-%d')


if __name__ == '__main__':
    py_path = (os.path.abspath(__file__))
    # database_path = os.path.abspath(os.path.join(py_path, "..", 'FutureOptions'))

    # database_path_list = ['FutureOptions/Disagg', ]  # 'FutureOptions/Report/', 'FutureOptions/Traders/']
    # database_path_list = ['Future/Disagg', 'Future/Report', 'Future/Traders']
    database_path_list = ['FutureOptions/Disagg', 'FutureOptions/Report/', 'FutureOptions/Traders/',
                          'Future/Disagg', 'Future/Report', 'Future/Traders']
    for database_path in database_path_list:
        # print(database_path)
        database_path = os.path.abspath(os.path.join(py_path, "..", database_path))
        logger.info(f'处理{database_path}')
        # continue
        data_to_raw_factor_yearly(database_path, update_zip=None)
        raw_data_combine(database_path)
        # cot_data_to_factor(database_path)
