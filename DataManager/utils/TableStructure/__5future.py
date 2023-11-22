from peewee import *
from datetime import datetime
from ..mysql_peewee import connect_mysql, dict2str, handle_table
# from DataManager.Script.mysql_peewee import connect_mysql, dict2str, handle_table
#
# 当前时间
now = datetime.now()
# 链接数据库
mysql_db = connect_mysql(database='ks_db')


class table_future_kbar(Model):
    """
    table_trade_view base model
    """
    date = DateField(column_name='date', null=False, help_text='交易日')
    date_time = DateTimeField(column_name='date_time', null=False, help_text='时间')
    symbol = CharField(column_name='symbol', null=True, help_text='代码')
    open = DoubleField(column_name='open', null=True, help_text='开盘价')
    high = DoubleField(column_name='high', null=True, help_text='最高价')
    low = DoubleField(column_name='low', null=True, help_text='最低价')
    close = DoubleField(column_name='close', null=True, help_text='收盘价')
    volume = DoubleField(column_name='volume', null=True, help_text='交易量')
    oi = BigIntegerField(column_name='oi', null=True, help_text='总持仓')
    turnover = DoubleField(column_name='turnover', null=True, help_text='换手率')
    interval = CharField(column_name='interval', null=True, help_text='时间间隔')
    op_time = DateTimeField(column_name="op_time", default=datetime.now, help_text='操作时间')

    class Meta:
        primary_key = CompositeKey('date_time', 'symbol')
        indexes = (
            # create a unique on from/to/date
            (('date', 'date_time', 'symbol',), True),)
        table_name = 'future_1m'
        database = mysql_db
        info = {'描述': '米筐分钟期货数据', }
        table_settings = ["COMMENT='%s'" % (dict2str(info))]


class table_future_wh(Model):
    """
    table_trade_view base model
    """
    date = DateField(column_name='date', null=False, help_text='交易日')
    wh_code = CharField(column_name='wh_code', null=True, help_text='代码')
    open = DoubleField(column_name='open', null=True, help_text='开盘价')
    high = DoubleField(column_name='high', null=True, help_text='最高价')
    low = DoubleField(column_name='low', null=True, help_text='最低价')
    close = DoubleField(column_name='close', null=True, help_text='收盘价')
    vol = DoubleField(column_name='vol', null=True, help_text='交易量')
    oi = BigIntegerField(column_name='oi', null=True, help_text='总持仓')
    settle = DoubleField(column_name='settle', null=True, help_text='结算价')
    interval = CharField(column_name='interval', null=True, help_text='时间间隔')
    last_record = BooleanField(column_name='last_record', null=True, help_text='最后一挑记录')
    op_time = DateTimeField(column_name="op_time", default=datetime.now, help_text='操作时间')

    class Meta:
        primary_key = CompositeKey('date', 'wh_code', 'interval')
        indexes = (
            # create a unique on from/to/date
            (('interval', 'wh_code',), False),)
        table_name = 'wh_future'
        database = mysql_db
        info = {'描述': '文华财经期货价格数据', }
        table_settings = ["COMMENT='%s'" % (dict2str(info))]


class table_wh_info(Model):
    """
    table_wh_info
    """
    wh_code = CharField(column_name='wh_code', primary_key=True, help_text='文华代码')
    symbol = CharField(column_name='symbol', null=True, help_text='代码')
    op_time = DateTimeField(column_name="op_time", default=datetime.now, help_text='操作时间')

    class Meta:
        table_name = 'wh_future_info'
        database = mysql_db
        info = {'描述': '文华财经期货代码', }
        table_settings = ["COMMENT='%s'" % (dict2str(info))]


if __name__ == '__main__':
    handle_table(table_future_wh, 1)
