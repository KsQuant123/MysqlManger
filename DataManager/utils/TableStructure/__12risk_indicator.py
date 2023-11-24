from peewee import *
from datetime import datetime
from ..mysql_peewee import connect_mysql, dict2str, handle_table
# from DataManager.utils.mysql_peewee import connect_mysql, dict2str, handle_table
#
# 当前时间
now = datetime.now()
# 链接数据库
mysql_db = connect_mysql(database='ks_db')


class RiskIndicatorInfo(Model):
    """
    指标信息表
    """
    id = CharField(column_name='id', help_text='指标id')
    country = CharField(column_name="country", null=True, help_text='国家')
    name = CharField(column_name="name", null=True, help_text='指标名称')
    unit = CharField(column_name="unit", null=True, help_text='单位')
    freq = CharField(column_name="freq", null=True, help_text='频率')
    source = CharField(column_name="source", null=True, help_text='来源')
    number = IntegerField(column_name="number", null=True, help_text='样本数列')
    start_time = DateField(column_name="start_time", null=True, help_text='数据起始时间')
    end_time = DateField(column_name="end_time", null=True, help_text='数据结束时间')
    description = TextField(column_name='description', null=True, help_text='简介')
    tz = CharField(column_name="tz", null=True, help_text='时区')
    level1 = CharField(column_name="level1", null=True, help_text='一级标签')
    level2 = CharField(column_name="level2", null=True, help_text='二级标签')
    level3 = CharField(column_name="level3", null=True, help_text='三级标签')
    op_time = DateTimeField(column_name="op_time", default=datetime.now)

    class Meta:
        primary_key = CompositeKey('id')

        table_name = 'risk_indicator_info'
        database = mysql_db
        table_settings = ["COMMENT='%s'" % (dict2str({'描述': '指标信息表', }))]


class RiskIndicatorData(Model):
    """
    指标数据
    """
    date = DateField(verbose_name='date', help_text='日期')
    id = CharField(verbose_name='id', help_text='指标名')
    value = DoubleField(verbose_name='value', null=True, help_text='值')
    op_time = DateTimeField(verbose_name='op_time', default=datetime.now)

    class Meta:
        primary_key = CompositeKey('id', 'date')
        table_name = 'risk_indicator_data'
        database = mysql_db
        table_settings = ["COMMENT='%s'" % (dict2str({'描述': '指标数据', }))]


if __name__ == '__main__':

    handle_table(RiskIndicatorInfo, type_=1)
    handle_table(RiskIndicatorData, type_=1)