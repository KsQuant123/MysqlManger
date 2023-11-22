import warnings
from sqlalchemy import create_engine
from peewee import *
import json
import os
import pandas as pd
from io import StringIO
from datetime import datetime

# 配置文件
Script_path = os.path.dirname(__file__)
with open(os.path.join(Script_path, 'config_db.json'), 'r', encoding='utf-8') as f:
    config_db = json.load(f)
# 数据库地址
base_host = config_db['host']
# 数据库端口
base_port = config_db['port']
# 数据库用户名
base_user = config_db['user']
# 密码
base_password = config_db['password']
# 数据库名称
base_database = 'ks_index'
# 字符集
base_charset = config_db['charset']
# 当前时间
now = datetime.now()


#
# 配置数据库
def connect_mysql(host=base_host, database=base_database, user=base_user, password=base_password, charset=base_charset):
    db = MySQLDatabase(host=host, database=database, user=user, passwd=password, charset=charset)
    db.connect()
    return db


def mysql_engine(host=base_host, database=base_database, user=base_user, password=base_password, port=base_port):
    con = create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{database}')
    return con


# 连接数据库
# mysql_db = connect_mysql()
# con = mysql_engine()

def create_table(table: Model):
    """
    如果table不存在，新建table
    """
    if not table.table_exists():
        table.create_table()
        print(f'表{table._meta.table_name}建立')
    else:
        print(f'表{table._meta.table_name}已存在')


def drop_table(table: Model):
    """
    table 存在，就删除
    """
    if table.table_exists():
        table.drop_table()
        print(f'表{table._meta.table_name}已删')
    else:
        print(f'表{table._meta.table_name}不存在')


def init_table(table: Model):
    """
    table 初始化
    """
    drop_table(table)
    create_table(table)


def handle_table(table: Model, type_=1):
    """
    table 处理
    1: 创建
    2: 删除
    3: 初始化
    """
    if type_ == 1:
        create_table(table)
    elif type_ == 2:
        drop_table(table)
    elif type_ == 3:
        init_table(table)
    else:
        warnings.warn("未找到类型，请重新处理")


def get_table(table):
    return pd.DataFrame(list(table.select().dicts()))


def get_table_comment(db, table_name=None):
    # 查询mysql表名和注释
    # 获取游标
    database_name = db.database
    if table_name is None:
        query_term = "select table_name,table_comment from information_schema.TABLES " \
                     "where TABLE_SCHEMA='%s' order by table_name" % database_name
    else:
        query_term = "select table_name,table_comment from information_schema.TABLES " \
                     "where TABLE_SCHEMA='%s' and table_name='%s'" % (database_name, table_name)
    cursor = db.execute_sql(query_term)
    return_tables = cursor.fetchall()
    cursor.close()
    return_dict = {}
    for table_data in return_tables:
        return_dict[table_data[0]] = json2dict(table_data[1])
    return return_dict


def get_table_info(table):
    info = get_table_comment(db=table._meta.database, table_name=table._meta.table_name)[
        table._meta.table_name]
    return info


def get_data_by_code(table, code, column):
    if hasattr(table, column):
        code_column = getattr(table, column)
        query = table.select().where(code_column == code)
        return pd.DataFrame(list(query.dicts()))


def get_latest_date(table, date_name='date', op_time=True):
    if not table.table_exists():
        print(f"Table '{table._meta.database.database}.{table._meta.table_name}' doesn't exist")
        return None, None
    if hasattr(table, date_name):
        query_date = getattr(table, date_name)
        query = table.select(fn.max(table.op_time).alias('op_time'),
                             fn.max(query_date).alias('last_time')).get_or_none()
        if op_time:
            return query.last_time, query.op_time
        return query.last_time, None
    else:
        print(f'{date_name} not in {table._meta.table_name}')
        return None, None


def update_data(table, data):
    base = pd.DataFrame(list(table.select().dicts()))
    # base.to_csv('test_base.csv')
    # data.to_csv('test_data.csv')
    if base.empty:
        return data
    total = pd.concat([data, base], axis=0)
    if 'date_time' in data.columns:
        total['date_time'] = pd.to_datetime(total['date_time'])
    elif 'date' in data.columns:
        total['date'] = pd.to_datetime(total['date'])

    result = total.drop_duplicates(
        subset=data.columns,
        keep=False
    ).query("op_time=='NaT'").dropna(how='all', axis=1)
    total.drop_duplicates(
        subset=data.columns,
        keep=False
    ).query("op_time=='NaT'")#.to_csv('test.csv')
    return result


def insert_dict2table(table, dict_term):
    pass


def insert_data2table(table, data, conflict, comment=None, silence=False):
    data = data.astype(object)
    data = data.where(data.notnull(), None)
    if not silence:
        print(data)
    database = table._meta.database
    dicts = data.to_dict(orient="records")
    if conflict == 'replace':
        with database.atomic():
            for c in chunked(dicts, 50):
                table.insert_many(
                    c).on_conflict_replace().execute()
    elif conflict == 'ignore':
        with database.atomic():
            for c in chunked(dicts, 50):
                table.insert_many(
                    c).on_conflict_ignore().execute()
    elif conflict == 'update':
        data = update_data(table, data)
        dicts = data.to_dict(orient="records")
        with database.atomic():
            for c in chunked(dicts, 50):
                table.insert_many(
                    c).on_conflict_replace().execute()
    if not silence:
        print(dicts)
    if comment is not None:
        insert_table_comment(table, comment)


def insert_table_comment(table, comment):
    database = table._meta.database
    if isinstance(comment, dict):
        sql_str = "ALTER TABLE %s.`%s` COMMENT = '%s'" % (
            database.database, table._meta.table_name, dict2str(comment))
    elif isinstance(comment, str):
        sql_str = "ALTER TABLE %s.`%s` COMMENT = '%s'" % (
            database.database, table._meta.table_name, comment)
    database.execute_sql(sql_str)
    database.commit()


def update_table_comment(table, comment: dict):
    comment_dict = get_table_comment(table._meta.database, table._meta.table_name)
    comment_dict.update(comment)
    insert_table_comment(comment_dict)


def insert_single_table(table, data_func=None, data=None, database=None, conflict='ignore', comment_info={}):
    if database is None:
        database = table._meta.database
    if not table.table_exists():
        print('表不存在')
        create_table(table)

    table_info = get_table_info(table)
    update_time = table_info.get('更新时间', '')
    if update_time < str(now.date()) or data is not None:
        if data is None:
            if data_func:
                data = data_func()
            # else:
            #     raise TypeError("missing required positional argument: 'data' or 'data_func'")
            else:
                print(f'{now.date()}, {table._meta.table_name}, 无数据')
                return None
        table_info['更新时间'] = str(now.date())
        table_info.update(comment_info)
        insert_data2table(table, data, conflict=conflict, comment=table_info)
        print('更新时间：%s' % str(now.date()))
    else:
        print('无需更新')
        return None


# def query_latest_op_time(table):
#     query = table.select(fn.max(table.op_time).alias('op_time')).get_or_none()
#     op_time = query.op_time
#     return op_time


def dict2str(dicts: dict):
    """
    将dict转json信息
    :param dicts:
    :return:
    """
    json_str = str(json.dumps(dicts, ensure_ascii=False))
    return json_str


def json2dict(comment: str):
    """
    将comment信息转dict
    :param comment:
    :return:
    """
    pass
    dicts = json.loads(comment)
    return dicts


def doc2dict(doc):
    result = {}
    doc = doc.replace('\n    ', '')
    for i in doc.split(';\n'):
        term = i.strip().split(':')
        result[term[0]] = ':'.join(term[1:])
    return result


def content2csv(content):
    return pd.read_csv(StringIO(content.decode()))


def content2excel(content):
    return pd.read_excel(content)


def check_tb_name(table_name):
    table_name = table_name.replace(':', '_')
    table_name = table_name.replace(';', '_')
    table_name = table_name.replace(',', '_')
    table_name = table_name.replace('.', '_')
    table_name = table_name.replace(' ', '_')
    table_name = table_name.replace('=', '_')
    table_name = table_name.replace('(', '_')
    table_name = table_name.replace(')', '_')
    table_name = table_name.replace('[', '_')
    table_name = table_name.replace(']', '_')
    table_name = table_name.replace('|', '_')
    table_name = table_name.replace('"', '_')
    table_name = table_name.replace('/', '_')
    table_name = table_name.replace('{', '_')
    table_name = table_name.replace('}', '_')
    table_name = table_name.replace('?', '_')
    return table_name


if __name__ == '__main__':
    print(config_db)
    # info = get_table_comment(db=connect_mysql())
    # info2 = get_table_info()
    # print(info['期货信息'], type(info['期货信息']))
    # test_dict = json2dict(info['期货信息'])
    # print(test_dict, type(test_dict))
    # test_dict = {"描述": "所有期货合约信息", "更新时间": "2022-04-28", }
    # print(dict2str(test_dict))
    # print(json.loads(info['期货信息']))
    # config = {
    #     "host": "localhost",
    #     "port": 3306,
    #     "user": "root",
    #     "password": '123456',
    #     "charset": ''
    # }
    # config_json = json.dumps(config)
    # f = open('config_db.json', 'w')
    # f.write(config_json)
    # f.close()
