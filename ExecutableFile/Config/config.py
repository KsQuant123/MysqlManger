import os
import io
import sys
import warnings
from yaml import safe_load
import datetime
import pytz

class Config:
    def __init__(self, config_path):
        self._config_info = self.load_config(config_path)

    @staticmethod
    def load_config(file_name: str):
        """
        加载配置文件
        :param file_name: 配置文件名称
        """
        assert file_name.endswith("yaml"), "配置文件格式不对"
        with open(file_name, "r", encoding="utf-8") as f:
            config = safe_load(f.read())
        return config

    def get_config(self, *keys):

        value = self._config_info

        for k in keys:
            try:
                value = value[k]
            except KeyError:
                warnings.warn(f"key of {k} not in config")
                value = None
                break

        return value

    __call__ = get_config

    def __getitem__(self, item):
        return self._config_info[item]


# 当前文件路径
py_path = (os.path.abspath(__file__))
# 主路径
main_path = os.path.abspath(os.path.join(py_path, '../../..', ))
# 配置文件路径
config_path = os.path.abspath(os.path.join(main_path, 'ExecutableFile', 'Config', 'config_main.yaml'))

# 配置文件
config = Config(config_path)

from loguru import logger

# 日志配置
log_dir = config('log', 'dir_path')
if log_dir is None:
    log_dir = os.path.join(main_path, 'Log')
log_name = config('log', 'log_name')
if log_name is None:
    log_name = 'log'
rotation = config('log', 'rotation')

update_path = os.path.abspath(os.path.join(main_path, 'ExecutableFile', 'Config', 'update.yaml'))
update_config = Config(update_path)


def config_logger(dirs='log', file_name=log_name, ro=rotation):
    """ 配置日志 """
    logger.remove()
    file_path = "%s_{time:YYYY_MM}.log" % (file_name)
    logger_path = os.path.join(log_dir, dirs, file_path)
    print("logger_path: ", logger_path)
    logger.add(logger_path, rotation=ro)


def log_print(func):
    def wrapper(*args, **kwargs):
        # Redirect stdout to a buffer
        output_buffer = io.StringIO()
        sys.stdout = output_buffer

        # Call the function
        result = func(*args, **kwargs)

        # Restore stdout and write buffer contents to log
        sys.stdout = sys.__stdout__
        logger.info(output_buffer.getvalue())
        return func(*args, **kwargs)


# 时间管理
current_time_us = datetime.datetime.now(tz=pytz.timezone('US/Eastern'))
ten_days_ago_us = current_time_us - datetime.timedelta(days=10)
current_year_us = current_time_us.year


current_time = datetime.datetime.now(tz=pytz.timezone('Asia/Shanghai'))
current_year = current_time.year
ten_days_ago = current_time - datetime.timedelta(days=10)


if __name__ == '__main__':
    # print(config)
    print(log_dir, type(log_dir))
    print(log_name)
    print(rotation)