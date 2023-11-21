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
from DataManager.Macro.MM.MM_update import insert_mm_data
from ExecutableFile.Config.config import config_logger, config

mm_path = config('data', 'Macro', 'MM', 'mm_path')


@logger.catch
def main(title_list=None, idx_list=None):
    auth = input("请输入auth: ")
    cookie = input("请输入cookie: ")
    print(auth, cookie)
    insert_mm_data(title_list=title_list, idx_list=idx_list, auth=auth, cookie=cookie)


if __name__ == '__main__':
    fire.Fire(main)
    logger = config_logger(dirs=mm_path, file_name='mm_update')

