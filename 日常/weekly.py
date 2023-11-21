import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.abspath(__file__), '../../../..', )))
from ExecutableFile.Config.config import main_path


# 更新CFTC
py_file = os.path.join(main_path, r'Shell\Update\Macro\cftc数据更新.py')
os.system(rf"python {py_file}")

# 更新宏观数据
# 更新Fred数据
py_file = os.path.join(main_path, r'Shell\Update\Macro\fred数据更新.py')
os.system(rf"python {py_file}")

# 更新金十数据
py_file = os.path.join(main_path, r'Shell\Update\Macro\jinshi数据更新.py')
os.system(rf"python {py_file}")

# 更新MM数据
py_file = os.path.join(main_path, r'Shell\Update\Macro\mm数据更新.py')
os.system(rf"python {py_file}")