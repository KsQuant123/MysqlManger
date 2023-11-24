import os
import sys
import fire
import json
from loguru import logger

# 脚本路径
py_path = (os.path.abspath(__file__))
# 主路径
main_path = os.path.abspath(os.path.join(py_path, '../../../..', ))
sys.path.append(main_path)
from DataManager.Macro.Fred.fred_stlouisfed_org_api import update_fs_data
from ExecutableFile.Config.config import config_logger, config

fred_fred_id = config('data', 'Macro', 'Fred', 'fred_id_path')


def split_list(lst, chunk_size=10):
    """将列表分割成多个子列表，每个子列表最多包含 chunk_size 个元素。"""
    for i in range(0, len(lst), chunk_size):
        yield lst[i:i + chunk_size]


@logger.catch
def main(code_list=None, all_series=False):
    split_lists = list(split_list(code_list, 10))
    for lists in split_lists:
        update_fs_data(lists, all_series=all_series)


if __name__ == '__main__':
    log_dir = fred_fred_id
    # print(log_dir)
    logger = config_logger(dirs=log_dir, file_name='fred_update')
    fire.Fire(main)
