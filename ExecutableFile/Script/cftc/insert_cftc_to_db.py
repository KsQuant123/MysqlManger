import os
import sys
import fire
from loguru import logger

# 脚本路径
py_path = (os.path.abspath(__file__))
# 主路径
main_path = os.path.abspath(os.path.join(py_path, '../../../..', ))
sys.path.append(main_path)

from DataManager.CFTC.insert_db import cftc_insert_db
from ExecutableFile.Config.config import config, config_logger
from ExecutableFile.Config.config import current_year_us


@logger.catch
def main(year=current_year_us):
    """更新cftc数据至数据库"""
    cftc_insert_db(year)


if __name__ == '__main__':
    CFTC_Database_path = config('data', 'CFTC', 'database')
    # 配置日志
    logger = config_logger(dirs=CFTC_Database_path, file_name='cftc_update')

    fire.Fire(main)
