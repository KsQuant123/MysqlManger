import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.abspath(__file__), '../../../..', )))
from ExecutableFile.Config.config import main_path


py_file = os.path.join(main_path, r'ExecutableFile\Script\macro\macro_jinshi_update.py')
command = rf"python {py_file}"
os.system(command)
