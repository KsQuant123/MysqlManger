import os
import hashlib
from zipfile import ZipFile

import pandas as pd
import requests
import shutil

url_base = 'https://www.cftc.gov/files/dea/history/'


def load_ice_cot_week_csv(path, date='20230815'):
    url_date = pd.to_datetime(date)
    url_date = url_date.strftime('%d%m%y')
    url = f'https://www.theice.com/publicdocs/cot_report/automated/COT_{url_date}.csv'
    r = requests.get(url)
    update_file_path = os.path.join(path, f'COT_{url_date}.csv')
    with open(update_file_path, "wb") as code:
        code.write(r.content)
    return update_file_path


def load_ice_cot_year_csv(path, year='2023'):
    url = f'https://www.ice.com/publicdocs/futures/COTHist{year}.csv'
    r = requests.get(url)
    update_file_path = os.path.join(path, f'update.csv')
    with open(update_file_path, "wb") as code:
        code.write(r.content)
    return update_file_path


def load_CFTC_zip(path, cot_filename):
    r = requests.get(url_base + cot_filename)
    update_file_path = os.path.join(path, 'update.zip')
    with open(update_file_path, "wb") as code:
        code.write(r.content)
    return update_file_path


def get_file_md5(f):
    m = hashlib.md5()
    while True:
        data = f.read(1024)
        if not data:
            break
        m.update(data)
    return m.hexdigest()


def get_zip_hash(path):
    with ZipFile(path, 'r') as z:
        for file_info in z.infolist():
            f = z.open(file_info)
    hash = get_file_md5(f)
    f.close()
    return hash


def get_file_hash(path):
    with open(path, 'rb' ) as f:
        hash = get_file_md5(f)
    return hash


def check_zip_same(path1, path2):
    if os.path.exists(path2) and os.path.exists(path1):
        hash1 = get_zip_hash(path1)
        hash2 = get_zip_hash(path2)
        return hash1 == hash2
    else:
        print('文件1{},文件2{}'.format(os.path.exists(path2), os.path.exists(path1)))
        return False


def check_file_same(path1, path2):
    if os.path.exists(path2) and os.path.exists(path1):
        hash1 = get_file_hash(path1)
        hash2 = get_file_hash(path2)
        return hash1 == hash2
    else:
        print('文件1{},文件2{}'.format(os.path.exists(path2), os.path.exists(path1)))
        return False


def clone_file(path, rename_path, delete=False):
    if os.path.exists(rename_path):
        print('文件已存在')
        if delete:
            pass
        else:
            print('不覆盖文件')
            return False
    shutil.copyfile(path, rename_path)
    # os.rename(path, rename_path)
    print('文件已覆盖')
    return True


def update_zip(path):
    pass

# Disaggregated Futures Only Reports:
# url = 'https://www.cftc.gov/files/dea/history/fut_disagg_xls_{}.zip'.format(current_year)
# Disaggregated Futures-and-Options Combined Reports:
# url = 'https://www.cftc.gov/files/dea/history/com_disagg_xls_2021.zip'
# Traders in Financial Futures ; Futures Only Reports:
# url = 'https://www.cftc.gov/files/dea/history/fut_fin_xls_2021.zip'
# Traders in Financial Futures ; Futures-and-Options Combined Reports:
# url = 'https://www.cftc.gov/files/dea/history/com_fin_xls_2021.zip'
# Futures Only Reports:
# url = 'https://www.cftc.gov/files/dea/history/dea_fut_xls_2021.zip'
# Futures-and-Options Combined Reports
# url = 'https://www.cftc.gov/files/dea/history/dea_com_xls_2021.zip'
