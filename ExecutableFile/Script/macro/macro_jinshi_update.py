import os
import sys
import fire
from loguru import logger

# 脚本路径
py_path = (os.path.abspath(__file__))
# 主路径
main_path = os.path.abspath(os.path.join(py_path, '../../../..', ))
sys.path.append(main_path)
from DataManager.Macro.JInshi.jinshi_update import record_jinshi_data, record_jinshi_describe
from ExecutableFile.Config.config import config_logger, config, current_time, ten_days_ago

jinshi_path = config('data', 'Macro', 'Jinshi', 'jinshi_path')


def main():
    record_jinshi_data(start_date=str(ten_days_ago.date()), end_date=str(current_time.date()))
    record_jinshi_describe()


if __name__ == '__main__':
    logger = config_logger(dirs=jinshi_path, file_name='jinshi_update')
    fire.Fire(main)