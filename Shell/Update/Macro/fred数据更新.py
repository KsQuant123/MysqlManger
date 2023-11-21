import os
import sys
import json

sys.path.append(os.path.abspath(os.path.join(os.path.abspath(__file__), '../../../..', )))
from ExecutableFile.Config.config import main_path

code_list = []
code_list += ['BAMLH0A0HYM2EY', 'BAMLC0A4CBBBEY', 'DGS10']
code_list += ['PAYEMS']  # 非农就业人数
code_list += ['PPIACO']  # ppi
code_list += ['T10Y3M', 'T10Y2Y', 'T10YFF', 'T1YFF', 'TB3SMFFM']
code_list += ['DGS10', 'DGS2', 'DGS30', 'DGS5', 'DGS3MO', 'DGS1MO', 'DGS20', 'DGS7', 'DGS1', 'DGS6MO', 'DGS3', ]

# 更新Fred数据
py_file = os.path.join(main_path, r'ExecutableFile\Script\macro\macro_fred_record_by_id.py')
code_list = json.dumps(code_list)
command = rf'python {py_file} -code_list="{code_list}"'
# print(command)
os.system(command)
