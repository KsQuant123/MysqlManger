from peewee import *
from datetime import datetime
from ..mysql_peewee import connect_mysql, dict2str, handle_table

# 当前时间
now = datetime.now()
# 链接数据库
mysql_db = connect_mysql(database='ks_db')


class table_macro_jinshi_data(Model):
    """
    金十财经 数据
    """
    id = IntegerField(column_name='id', help_text='指标id')
    date_time = DateTimeField(column_name='date_time', help_text='公布时间')
    country = CharField(column_name="country", null=True, help_text='国家')
    name = CharField(column_name="name", null=True, help_text='指标名称')
    name_ = CharField(column_name="name_", null=True, help_text='指标名称')
    value = FloatField(column_name='value', null=True, help_text='公布值')
    previous = FloatField(column_name='previous', null=True, help_text='前值')
    revised = FloatField(column_name='revised', null=True, help_text='修正后前值')
    consensus = FloatField(column_name='consensus', null=True, help_text='预期值')
    unit = CharField(column_name="unit", null=True, help_text='时间区间')
    time_period = CharField(column_name="time_period", null=True, help_text='单位')
    affect = IntegerField(column_name='affect', null=True, help_text='影响')
    star = IntegerField(column_name='star', null=True, help_text='重要程度')
    indicator_id = IntegerField(column_name='indicator_id', null=True, help_text='指示标识')
    time_status = CharField(column_name='time_status', null=True, help_text='公布状态')
    video_url = CharField(column_name="video_url", null=True, help_text='视频地址')
    timestamp = TimestampField(column_name='timestamp', null=True, help_text='公布时间戳')
    date = DateField(column_name='date', help_text='公布日期')
    idx = IntegerField(column_name='idx', primary_key=True, help_text='金十自增id')
    op_time = DateTimeField(column_name="op_time", default=datetime.now)

    class Meta:
        # primary_key = CompositeKey('id', 'date_time', 'value')
        indexes = (
            (('id', 'date', 'value'), True),
        )
        table_name = 'macro_jinshi_data'
        database = mysql_db
        table_settings = ["COMMENT='%s'" % (dict2str({'描述': '金十财经数据', }))]


class table_jinshi_info(Model):
    ['数据释义', '数据影响', '统计方法', '公布机构', "id"]
    id = IntegerField(column_name='id', help_text='指标id')
    idx = IntegerField(column_name='idx', primary_key=True, help_text='金十自增id')
    describe = TextField(column_name="describe", null=True, help_text='数据释义')
    affect = CharField(column_name="affect", null=True, help_text='数据影响')
    method = TextField(column_name="method", null=True, help_text='统计方法')
    source = CharField(column_name="source", null=True, help_text='公布机构')
    op_time = DateTimeField(column_name="op_time", default=datetime.now, help_text='操作时间')

    class Meta:
        database = mysql_db
        table_name = 'macro_jinshi_info'
        info = {'描述': '金十宏观数据信息表', }
        table_settings = ["COMMENT='%s'" % (dict2str(info))]


if __name__ == '__main__':
    handle_table(table_macro_jinshi_data, 1)
    handle_table(table_jinshi_info, 1)
