import json
import parsel
import requests
import pandas as pd
from datetime import datetime, timedelta
from loguru import logger
from dateutil.parser import parse
from tqdm import tqdm
from ...utils.mysql_peewee import insert_data2table, get_table
from ...utils.time_func import time_switch
from ...utils.TableStructure.__10jinshi import table_jinshi_info, table_macro_jinshi_data

now = datetime.now()


def get_jinshi_daily(date):
    """获取金十日数据"""
    date_str = date.strftime("%Y/daily/%m/%d/")
    url = f"https://cdn-rili.jin10.com/web_data/{date_str}/economics.json"
    print(url)
    r = requests.get(url=url)
    info = json.loads(r.text)
    return pd.DataFrame(info)


def get_jinshi_data(start_date, end_date):
    """获取金十数据"""
    data_list = []
    for i in tqdm(pd.date_range(start_date, end_date)):
        # time.sleep(1)
        try:
            _data = get_jinshi_daily(i)
            data_list.append(_data)
        except:
            print(f"error_date{str(i)}")
    return pd.concat(data_list)


def get_jinshi_describe(idx):
    """获取金十数据描述"""
    url = f"https://rili.jin10.com/detail/{idx}"
    r = requests.get(url=url)
    selector = parsel.Selector(r.text)
    info = [x.strip() for x in selector.css('td.detail-page__table-content::text').getall()] + [idx]
    key = ['describe', 'affect', 'method', 'source', "idx"]
    return dict(zip(key, info))


def insert_jinshi_data(jinshi_data, silence=False):
    """
    录入 金十财经 数据
    """
    jinshi_data['date_time'] = jinshi_data['pub_time_unix'].apply(
        lambda x: time_switch(x, sourcetz="utc", totype='str'))
    jinshi_data['date'] = jinshi_data['date_time'].apply(lambda x: x.split(' ')[0])
    jinshi_data['idx'] = jinshi_data['id']
    jinshi_data['id'] = jinshi_data['indicator_id']
    jinshi_data.rename(columns={'actual': 'value', 'pub_time_unix': 'timestamp'}, inplace=True)
    jinshi_data['name_'] = jinshi_data['name']
    jinshi_data['name'] = jinshi_data['country'] + jinshi_data['name_']
    # keep_columns = ['id', 'idx', 'date_time', 'name', 'value', 'previous', 'revised', 'consensus',
    #                 'timestamp', 'date', 'unit']
    # jinshi_data = jinshi_data[keep_columns]
    keep_columns = ['id', 'idx', 'date_time', 'name_', 'name', 'country', 'value', 'previous', 'revised', 'consensus',
                    'affect', 'star', 'timestamp', 'indicator_id', 'time_period', 'video_url', 'date', 'unit']
    jinshi_data = jinshi_data[keep_columns]
    # print(jinshi_data)
    insert_data2table(table_macro_jinshi_data, jinshi_data, conflict='replace', silence=silence)


def record_jinshi_data(start_date='2000-01-01', end_date=None):
    if end_date is None:
        end_date = str(datetime.now().date() - timedelta(days=1))
    else:
        end_date = str(parse(end_date).date())

    data = get_jinshi_data(start_date, end_date)
    logger.success(f"获取金十数据：{start_date}至{end_date}")
    insert_jinshi_data(data)


def record_jinshi_describe():
    df = get_table(table_macro_jinshi_data)
    df = df.drop_duplicates(['id'], keep='last')
    info = get_table(table_jinshi_info)

    drop_list = []
    if not info.empty:
        drop_list = info['id'].tolist()
    for i, idx in tqdm(zip(df['id'], df['idx'])):
        if i in drop_list:
            continue
        describe_dict = get_jinshi_describe(idx)
        if len(describe_dict) < 3:
            logger.warning(f'无法录入金十数据描述：{i}, {idx}, {describe_dict}')
            continue
        describe_dict['id'] = i
        logger.success(f'录入金十数据描述：{i}, {idx}')
        table_macro_jinshi_data.insert_many(describe_dict).on_conflict_replace().execute()


if __name__ == '__main__':
    record_jinshi_data(start_date='2023-02-01', end_date='2023-10-26')
    # record_jinshi_describe()
    # df = get_table(table_macro_jinshi_data_total)
    # df = df.drop_duplicates(['id'], keep='last')
    # idx = df.query("id==952")['idx'].values[0]
    # describe_dict = get_jinshi_describe(idx)
    # describe_dict['id'] = 952
    # print(describe_dict)
    # table_jinshi_info_list.insert_many(describe_dict).on_conflict_replace().execute()

    # print(time_switch(1644480000, sourcetz='utc', totype='str', targettz='cn'))

    # df = get_table(table_macro_jinshi_data_total)
    # df.index = df['date_time']
    # df['date_time'] = df['timestamp']
    # print(df['date_time'])
    # print(df['timestamp'].tail(20))
    # # df.to_csv('test.csv')
    # print(df['date_time'])
    # insert_data2table(table_macro_jinshi_data_total, df, conflict='replace', silence=False)

    # _data = get_jinshi_daily(datetime('2022-08-20'))