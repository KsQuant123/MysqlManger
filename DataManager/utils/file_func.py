import os
import pandas as pd
from itertools import chain
from tqdm import tqdm


def fast_flatten(input_list):
    return list(chain.from_iterable(input_list))


def fast_concat(data_list):
    """
    快速运算 pd.concat axis=1, 要求数据列相同,index会被忽略
    :param data_list:
    :return:
    """
    COLUMN_NAMES = data_list[0].columns
    df_dict = dict.fromkeys(COLUMN_NAMES, [])
    for col in tqdm(COLUMN_NAMES):
        extracted = (frame[col] for frame in data_list)  # Use a generator to save memory
        df_dict[col] = fast_flatten(extracted)  # Flatten and save to df_dict
    ori_data = pd.DataFrame.from_dict(df_dict)[COLUMN_NAMES]
    return ori_data


def list_all_files(root_dir, func=None, args=[]):
    """
    列出文件夹下所有的目录与文件
    :param root_dir: str 路径地址
    :param func: function 正则公式
    :return: list
    """
    if func is None:
        func = lambda x: True
    _files = []
    file_list = os.listdir(root_dir)
    for i in range(0, len(file_list)):
        # 构造路径
        path = os.path.join(root_dir, file_list[i])
        # 判断路径是否为文件目录或者文件
        # 如果是目录则继续递归
        if os.path.isdir(path):
            _files.extend(list_all_files(path, func, args=args))
        if os.path.isfile(path) and func(path, *args):
            _files.append(path)
    return _files


def read_csv(path, query_str=None, counts=None):
    data = pd.read_csv(path, chunksize=50000)
    df_list = []
    count = 0
    for i in tqdm(data):
        count += 1
        # 返回所要数据
        if query_str is not None:
            temp = i.query(query_str)
        else:
            temp = i  # .query("SecurityID==510050")
        if temp.shape[0] > 1:
            df_list.append(temp)
            if count==counts:
                break
    ori_data = fast_concat(df_list)
    return ori_data
