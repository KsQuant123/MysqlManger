import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.abspath(__file__), '../../../..', )))
from ExecutableFile.Config.config import main_path

# 更新数据
py_file = os.path.join(main_path, r'ExecutableFile\Script\cftc\load_cftc_to_csv.py')
command = rf"python {py_file} --year=2023"
os.system(command)
#
# 插入数据库
py_file = os.path.join(main_path, r'ExecutableFile\Script\cftc\insert_cftc_to_db.py')
command = rf"python {py_file} --year=2023"
os.system(command)
#
# 插入数据库
py_file = os.path.join(main_path, r'ExecutableFile\Script\cftc\insert_cftc_to_db.py')
command = rf"python {py_file} --year=2023"
os.system(command)
