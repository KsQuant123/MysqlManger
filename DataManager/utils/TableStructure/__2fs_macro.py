from peewee import *
from datetime import datetime
# from ..mysql_peewee import connect_mysql, dict2str, handle_table
from DataManager.utils.mysql_peewee import connect_mysql, dict2str, handle_table
# 当前时间
now = datetime.now()
# 链接数据库
mysql_db = connect_mysql(database='ks_db')


class table_macro_fs_info(Model):
    """
    fred_stlouisfed 数据信息
    """
    id = CharField(column_name="id", null=False, unique=True, primary_key=True, help_text='指标ID')
    title = CharField(column_name="title", null=True, help_text='指标名称')
    frequency = CharField(column_name="frequency", null=True, help_text='频率')
    frequency_short = CharField(column_name="frequency_short", null=True, help_text='频率 简称')
    units = CharField(column_name="units", null=True, help_text='单位')
    units_short = CharField(column_name="units_short", null=True, help_text='单位 简称')
    seasonal_adjustment = CharField(column_name="seasonal_adjustment", default=None, null=True, help_text='季节调整')
    seasonal_adjustment_short = CharField(column_name="seasonal_adjustment_short", default=None, null=True, help_text='季节调整 简称')
    notes = TextField(column_name="notes", null=True, help_text='信息')
    observation_start = DateField(column_name="observation_start", null=True, help_text='起始日期')
    observation_end = DateField(column_name="observation_end", null=True, help_text='结束日期')
    realtime_end = DateField(column_name="realtime_end", null=True, help_text='起始日期')
    realtime_start = DateField(column_name="realtime_start", null=True, help_text='结束日期')
    popularity = IntegerField(column_name="popularity", null=True, help_text='流行排名')
    tz = CharField(column_name="tz", null=True, help_text='时区')
    last_updated = TimestampField(column_name="last_updated", null=True, help_text='数据更新时间')
    op_time = DateTimeField(column_name="op_time", default=datetime.now, help_text='操作时间')
    is_valid = BooleanField(column_name="is_valid", default=True, help_text='是否有效')

    class Meta:
        database = mysql_db
        table_name = 'macro_fs_info'
        table_settings = ["COMMENT='%s'" % (dict2str({'描述': '美国圣路易斯联储联邦银行数据列表'}))]


class table_macro_fs_data(Model):
    """
    fred_stlouisfed 数据
    """
    date = DateField(column_name='date', help_text='日期')
    id = CharField(column_name='id', help_text='指标名')
    value = FloatField(column_name='value', null=True, help_text='值')
    op_time = DateTimeField(column_name="op_time", default=datetime.now)

    class Meta:
        primary_key = CompositeKey('id', 'date')
        indexes = (
            (('id', 'date', 'value'), True),
        )
        table_name = 'macro_fs_data'
        database = mysql_db
        table_settings = ["COMMENT='%s'" % (dict2str({'描述': '美国圣路易斯联储联邦银行数据', }))]


class table_macro_fs_data_r(Model):
    """
    fred_stlouisfed 数据
    """
    date = DateField(column_name='date', help_text='日期')
    real_date = DateField(column_name='real_date', help_text='发布日')
    id = CharField(column_name='id', help_text='指标名')
    value = FloatField(column_name='value', null=True, help_text='值')
    op_time = DateTimeField(column_name="op_time", default=datetime.now)

    class Meta:
        primary_key = CompositeKey('id', 'real_date', 'value')
        # indexes = (
        #     (('id', 'real_date', 'value'), True),
        # )
        table_name = 'macro_fs_data_r'
        database = mysql_db
        table_settings = ["COMMENT='%s'" % (dict2str({'描述': '美国圣路易斯联储联邦银行数据', }))]


if __name__ == '__main__':
    # handle_table(table_macro_fs_info, 1)
    # handle_table(table_macro_fs_data, 1)
    # handle_table(table_macro_fs_data_r, 1)
    pass