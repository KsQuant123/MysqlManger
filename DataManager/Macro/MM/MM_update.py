import re
import json
import time
import brotli
# import requests
import httpx as requests
import pandas as pd
import datetime
import numpy as np
import backoff
from tqdm import tqdm
from loguru import logger
from bs4 import BeautifulSoup
from DataManager.utils.TableStructure.__11mm_macro import MacroMMInfo, MacroMMDetail, MacroMMdata
from DataManager.utils.mysql_peewee import insert_data2table, get_table


headers1 = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
}


def find_urls_with_keyword(text, keyword=None):
    """从文本中提取 URL"""
    # 更新正则表达式以匹配 URL，直到遇到引号或空格
    url_pattern = r'https?://[^\s"]+'
    # 查找所有匹配的 URL
    urls = re.findall(url_pattern, text)

    # 过滤出包含关键词的 URL
    if keyword:
        filtered_urls = [url for url in urls if keyword in url]

    return filtered_urls


@backoff.on_exception(backoff.expo, requests.ReadTimeout, max_tries=5)
def get_trader_insights_table_url():
    """获取交易者视野的表url"""
    url = 'https://sc.macromicro.me/trader-insights'
    r = requests.get(url, headers=headers1)
    r.content.decode('utf8')
    soup = BeautifulSoup(r.content, 'html.parser')
    url_dict = {}
    for i in soup.find_all(class_='title'):
        url_dict[i.text] = i.a.attrs['href']
    return url_dict


@backoff.on_exception(backoff.expo, requests.ReadTimeout, max_tries=5)
def get_area_table_url():
    """获取地区数据表的url"""
    url_dict = {}
    url_list = [f'https://sc.macromicro.me/macro/{c}' for c in ['us', 'eu', 'cn', 'jp', 'tw', 'em', 'an']]
    url_name = ['美国', '欧洲', '中国', '日本', '台湾', '新兴市场', '东南亚']
    url_dict_macro = dict(zip(url_name, url_list))
    for k, v in tqdm(url_dict_macro.items()):
        url = v
        r = requests.get(url, headers=headers1)
        r.content.decode('utf8')
        soup = BeautifulSoup(r.content, 'html.parser')
        charts_str = re.findall('collectionChart =.+', str(soup))[0]
        charts_list = eval(
            charts_str.replace('collectionChart =', '').
                replace('null', 'None').replace('true', 'True').replace('false', 'False').replace(';', ''))
        charts = pd.DataFrame(charts_list)
        url_dict_ = dict(zip(charts['name'], charts['url']))
        url_dict.update(url_dict_)
    return url_dict


@backoff.on_exception(backoff.expo, requests.ReadTimeout, max_tries=5)
def get_central_bank_table_url():
    url_dict = {}
    url = 'https://sc.macromicro.me/central_bank/overview'
    r = requests.get(url, headers=headers1)
    soup = BeautifulSoup(r.content, 'html.parser')
    script = soup.find_all('script')
    charts_str = re.findall('relatedCollections.+', str(script))[0]
    # 正则表达式来匹配JSON对象
    pattern = r'\{.*?\"luv\":null\},'

    # 查找所有匹配的JSON对象
    matches = re.findall(pattern, charts_str)
    # print(matches)
    for match in matches:
        match = match.replace('null', 'None').replace('true', 'True').replace('false', 'False')
        match = eval(match)[0]
        url_dict[match['name']] = match['url']
    return url_dict


def insert_MM_info():
    """更新MM财经平方的信息表"""
    url_dict = {}
    # 加入交易者视野的表
    # url_dict.update(get_trader_insights_table_url())

    # 加入行业数据表
    # url_dict = {'汽车': 'https://sc.macromicro.me/collections/29/mm-car',
    #             '科技': 'https://sc.macromicro.me/collections/2932/info-tech',
    #             '半导体': 'https://sc.macromicro.me/collections/4345/mm-semiconductor',
    #             '工业/製造业': 'https://sc.macromicro.me/collections/3261/sector-industrial',
    #             '美国-房地产': 'https://sc.macromicro.me/collections/7/us-housing-relative',
    #             '美国-金融': 'https://sc.macromicro.me/collections/5499/us-financial-stocks',
    #             '美国-科技巨头': 'https://sc.macromicro.me/collections/4093/us-big-tech',
    #             '加密货币': 'https://sc.macromicro.me/collections/3785/crypto',
    #             '肺炎疫情': 'https://sc.macromicro.me/collections/2340/covid19',
    #             '疫苗': 'https://sc.macromicro.me/collections/3587/vaccination-data',
    #             '利差': 'https://sc.macromicro.me/collections/384/spreads',
    #             '高频数据': 'https://sc.macromicro.me/collections/3208/high-frequency-data',
    #             '波动率': 'https://sc.macromicro.me/collections/4536/volatility',
    #             '地缘政治': 'https://sc.macromicro.me/collections/6447/geopolitical-risk',
    #             '全球-景气衰退': 'https://sc.macromicro.me/collections/8231/global-recession'}
    # 加入地区数据表
    # url_dict_macro = get_area_table_url()
    # url_dict.update(url_dict_macro)

    # 加入央行数据表
    url_dict_central_bank = get_central_bank_table_url()
    url_dict.update(url_dict_central_bank)
    print(url_dict)
    for k, v in tqdm(url_dict.items()):
        get_state = True
        j = 0
        while get_state and j < 5:
            try:
                url = v
                r = requests.get(url, headers=headers1)
                soup = BeautifulSoup(r.content, 'html.parser')
                script = soup.find_all('script')
                charts_str = re.findall('charts =.+', str(script))[0]
                charts_list = eval(
                    charts_str.replace('charts =', '').
                        replace('null', 'None').replace('true', 'True').replace('false', 'False').replace(';', ''))
                charts = pd.DataFrame(charts_list)
                charts['title'] = k
                charts['home_page'] = url
                charts['img_updated'] = charts['img_updated'].astype(float)
                charts['img_updated'] = charts['img_updated'].apply(
                    lambda x: datetime.datetime.fromtimestamp(x) if not np.isnan(x) else np.nan)
                insert_data2table(MacroMMInfo, data=charts, conflict='replace')
                get_state = False
            except requests.ReadTimeout or requests.ConnectTimeout as e:
                time.sleep(6)
                j += 1

    # Authorization_value = soup.find('p')['data-stk']


def insert_market_info_single(url='https://sc.macromicro.me/global/cds/data', title='行情-CDF'):
    """更新MM财经平方的信息表--行情表 单独"""
    # url = 'https://sc.macromicro.me/global/cds/data'
    get_state = True
    j = 0
    while get_state and j < 5:
        try:
            r = requests.get(url, headers=headers1, timeout=30.0)
            get_state = False

        except requests.ReadTimeout or requests.ConnectTimeout as e:
            time.sleep(6)
            j += 1
    market_info = json.loads(r.content)
    market_info_list = []
    for k, group in market_info['data'].items():
        for i in group:
            tmp = i.get('chart', {'name': i.get('name', {})})
            tmp['stats'] = json.dumps([i['stat']], ensure_ascii=False)
            market_info_list.append(tmp)

    market_charts = pd.DataFrame(market_info_list).drop(columns=['count_booked', 'count_viewed', 'updated'],
                                                        errors='ignore')
    market_charts['title'] = title
    market_charts['home_page'] = url
    market_charts['img_updated'] = None
    insert_data2table(MacroMMInfo, data=market_charts, conflict='replace')


def insert_market_info():
    """更新MM财经平方的信息表-行情表"""
    market_dict = {'行情-外汇': 'https://sc.macromicro.me/global/forex/data',
                   '行情-股市': 'https://sc.macromicro.me/global/stocks/data',
                   '行情-ETF': 'https://sc.macromicro.me/global/etf/data',
                   '行情-商品': 'https://sc.macromicro.me/global/commodities/data',
                   '行情-债券': 'https://sc.macromicro.me/global/bonds/data',
                   '行情-加密货币': 'https://sc.macromicro.me/global/crypto/data',
                   '行情-波动率': 'https://sc.macromicro.me/global/volatility/data',
                   '行情-CDF': 'https://sc.macromicro.me/global/cds/data'}
    for title, url in market_dict.items():
        insert_market_info_single(url=url, title=title)


# def insert_MM_info_central_bank():
#     """插入更新MM财经平方的信息表（央行） 单独获取"""
#     url = 'https://sc.macromicro.me/central_bank/overview'
#     headers = {
#         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
#     }
#     get_state = True
#     j = 0
#     while get_state and j < 5:
#         try:
#             r = requests.get(url, headers=headers)
#             soup = BeautifulSoup(r.content, 'html.parser')
#             script = soup.find_all('script')
#             # 全球央行关注图表
#             charts_str = re.findall('aiCharts =.+', str(script))[0]
#             charts_list = eval(
#                 charts_str.replace('aiCharts =', '').
#                     replace('null', 'None').replace('true', 'True').replace('false', 'False').replace(';', ''))
#             charts = pd.DataFrame(charts_list)
#             charts['title'] = 'MM AI 央行鹰鸽指数'
#             charts['home_page'] = url
#             charts['img_updated'] = charts['img_updated'].astype(float)
#             charts['img_updated'] = charts['img_updated'].apply(
#                 lambda x: datetime.datetime.fromtimestamp(x) if not np.isnan(x) else np.nan)
#             insert_data2table(MacroMMInfo, data=charts, conflict='replace')
#             get_state = False
#
#         except requests.ReadTimeout as e:
#             time.sleep(6)
#             j += 1


def get_authorization():
    """获取Authorization的值"""
    global Authorization_value
    if Authorization_value is None:
        table_info = get_table(MacroMMInfo)
        url = table_info['home_page'][0]
        r = requests.get(url)
        soup = BeautifulSoup(r.content, 'html.parser')
        script = soup.find_all('script')
        Authorization_value = soup.find('p')['data-stk']
    return Authorization_value


def get_m2_info(idx: str, Authorization_value = None, Cookie=None):
    """获取M2数据"""
    # get_A()
    base_url = 'https://sc.macromicro.me/charts/data/'
    if Cookie is None:
        Cookie = '_ga_4CS94JJY2M=GS1.1.1700185376.41.1.1700186114.0.0.0; _ga=GA1.2.608335812.1683335932; _hjSessionUser_1543609=eyJpZCI6ImJmYmRmMzIyLTc5ZWItNTc5MC1iOTU3LTZmNGVmYzZiYTU3MCIsImNyZWF0ZWQiOjE2ODMzMzU5MzM0NjksImV4aXN0aW5nIjp0cnVlfQ==; mmu=fca5d578e8e252dd48220aea0b7abbf7; __lt__cid=ccb59980-730d-4aa1-b862-886ff4f28dc6; cf_clearance=3I1Vx43JflZbK9kFGbHQhZA0KvCiu2s2syYh0x9.59E-1700185376-0-1-23625122.cf1eaaba.74630ec2-0.2.1700185376; _fbp=fb.1.1692611913122.1089766470; bvt=0; _gid=GA1.2.1442489518.1700124144; globalinvestor_43=1700125908; 2024forward=1700128556; app_ui_newbie_btn=5; mm_sess_pages=43; PHPSESSID=pl3ssm4fk8bq7iu80ekjitm1ml; mmt=300004%7C2e447ce2160203ff7a5d7c; _hjIncludedInSessionSample_1543609=0; _hjSession_1543609=eyJpZCI6IjJmNzUxMWIzLWJlM2UtNGFkMS04NWM2LWY4MmM0NzgzNjIxNSIsImNyZWF0ZWQiOjE3MDAxODI3MjAwMTgsImluU2FtcGxlIjpmYWxzZSwic2Vzc2lvbml6ZXJCZXRhRW5hYmxlZCI6ZmFsc2V9; _hjAbsoluteSessionInProgress=0; __lt__sid=39e82c9a-698dff69; _gat_gtag_UA_66285376_3=1'
    if Authorization_value is None:
        Authorization_value = 'd42a049dc2f6cdff3298a1c2a5464948'
    Authorization = f'Bearer {Authorization_value}'
    base_header = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2',
        'Authorization': Authorization,
        'Connection': 'keep-alive',
        'Cookie': Cookie,
        # 'Docref':'',
        'Host': 'sc.macromicro.me',
        'Referer': 'https://sc.macromicro.me/collections/51/us-treasury-bond/763/mm-us-bond-index',
        # 'Sec-Fetch-Dest':'empty',
        # 'Sec-Fetch-Mode':'cors',
        # 'Sec-Fetch-Site':'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/112.0',
        'X-Requested-With': 'XMLHttpRequest', }

    url = base_url + idx
    header = base_header
    #     header['Referer'] = f'https://sc.macromicro.me/collections/4093/us-big-tech/23581/nasdaq-composite'
    res = requests.get(url, headers=header)
    try:
        info = json.loads(brotli.decompress(res.content).decode('utf8'))
    except brotli.error:
        info = json.loads(res.content.decode('utf8'))
    return info


def replace_formula(s, values):
    # 字母顺序对应的字典
    letters = {letter: num for letter, num in zip('ABCDEFGHIJK', values)}

    # 替换公式中的每个字母
    replaced_formula = ''.join([letters[char] if char in letters else char for char in s])

    return replaced_formula


def parse_m2_data(info, idx):
    """解析M2数据"""
    data_info_list = []
    data_list = []
    column_info_list = info['data'][f'c:{idx}']['info']['chart_config']['seriesConfigs']

    for i in range(len(column_info_list)):

        column_info = column_info_list[i]
        info_dict = {}
        info_dict['id'] = idx
        info_dict['name'] = column_info.get('name', column_info['name_sc'])
        info_dict['name_sc'] = column_info['name_sc']
        info_dict['name_tc'] = column_info['name_tc']
        info_dict['name_en'] = column_info['name_en']
        info_dict['aggn'] = column_info['aggn']
        info_dict['currency'] = column_info.get('currency', '')
        info_dict['category'] = column_info.get('category', '')
        info_dict['base_date'] = column_info.get('base_date', '')
        info_dict['u'] = column_info['u']
        info_dict['old_u'] = column_info['old_u']
        info_dict['freq'] = column_info['freq']
        info_dict['old_freq'] = column_info['old_freq']
        info_dict['ytype'] = column_info['ytype']
        info_dict['arithmetic'] = column_info['arithmetic']
        info_dict['opn'] = column_info['opn']
        info_dict['opn_n'] = column_info.get('opn_n', '')
        info_dict['opn_m'] = column_info.get('opn_m', '')
        info_dict['lineType'] = column_info.get('lineType', '')
        stats = column_info['stats']
        if len(stats) == 1:
            info_dict['currency'] = stats[0].get('currency', '')
            info_dict['category'] = stats[0].get('category', '')
            info_dict['stat_id'] = stats[0]['stat_id']
        else:
            stats_list = [str(s['stat_id']) for s in stats]
            info_dict['stat_id'] = replace_formula(info_dict['arithmetic'], stats_list)

        data_info_list.append(info_dict)
        stat_id = info_dict['stat_id']
        if info_dict['freq'] != info_dict['old_freq']:
            continue
        else:
            df = pd.DataFrame(info['data'][f'c:{idx}']['series'][i], columns=['date', stat_id])
            df.set_index(['date'], inplace=True)
            data_list.append(df)

    info = pd.DataFrame(data_info_list).drop(columns=['stat_name'], errors='ignore')
    if len(data_list) == 0:
        data = pd.DataFrame()
    else:
        data = pd.concat(data_list, axis=1)

    # info.rename(columns={'idx': 'id', }, inplace=True)
    return data, info


def insert_mm_data(title_list=None):
    table_info = get_table(MacroMMInfo)
    print(table_info)
    if title_list is not None:
        table_info = table_info[table_info['title'].isin(title_list)]
    idx_list = table_info['id'].unique()
    print(idx_list)
    for idx in tqdm(idx_list):
        print(idx)
        info_ = get_m2_info(str(idx))
        time.sleep(1)
        if not info_['success']:
            print(f"failed to load {idx}")
            continue
        df, info = parse_m2_data(info_, idx)
        insert_data2table(MacroMMDetail, info, conflict='ignore')
        for i in df.columns:
            data = df[i].reset_index().dropna(subset='value')
            data['stat_id'] = i
            data['stat_id_first_digit'] = i[0]
            data.rename(columns={i: 'value'}, inplace=True)
            insert_data2table(MacroMMdata, data, conflict='replace')


def insert_mm_data(title_list=None, idx_list=None, auth=None, cookie=None):
    table_info = get_table(MacroMMInfo)

    if title_list is None and idx_list is None:
        table_d = get_table(MacroMMDetail)
        idx_list = [x for x in table_info['id'].unique() if x not in table_d['id'].unique()]
    else:
        if title_list is None:
            title_list = []
        if idx_list is None:
            idx_list = []
        table_info = table_info[table_info['title'].isin(title_list) | table_info['id'].isin(idx_list)]
        idx_list = table_info['id'].unique()

    logger.info("update_idx_list:{}".format(idx_list))
    download_list = []
    for idx in tqdm(idx_list):
        info_ = get_m2_info(str(idx), Authorization_value=auth, Cookie=cookie)
        time.sleep(1)
        if not info_['success']:
            logger.warning(f"failed to load idx_{idx}")
            continue
        df, info = parse_m2_data(info_, idx)
        insert_data2table(MacroMMDetail, info, conflict='ignore')
        logger.success(f"success to load idx_{idx}, including: {df.columns.tolist()}")
        for i in df.columns:
            if i not in download_list:
                download_list.append(i)
                data = df[i].reset_index().dropna(subset=i)
                data['stat_id'] = i
                data.rename(columns={i: 'value'}, inplace=True)
                insert_data2table(MacroMMdata, data, conflict='update')


if __name__ == '__main__':
    title_list = [
        '美国-GDP综合指标', '美国-物价', '美国-市场指标', '美国-联准会',
        '中国-GDP综合指标', '中国-市场指标', '中国-物价',
        '美国-股市', '中國-股市', '香港-股市',
        '美元', '欧元', '澳币', '加币', '英镑', '日圆',
        '原油', '黄金', '白银', '铁矿砂', '铜', '黄豆', '玉米', '小麦', '棉花', '咖啡', '天然气',
        '美债', '美国-公司债', '新兴市场-股债市',
        '美國-S&P500各板塊EPS', '航运', '汽车', '科技', '半导体', '工业/制造业', '美国-房地产', '美国-金融',
        '美国-科技巨头', '加密货币', '利差高频数据', '波动率', '地缘政治', '全球-景气衰退'
    ]
    # load_list = ['香港-股市']
    # insert_mm_data(title_list=load_list)
    # insert_market_info()
    insert_mm_data(title_list=['香港-股市'], idx_list=['25489'])
