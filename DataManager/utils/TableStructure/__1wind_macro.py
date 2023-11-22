from peewee import *
from datetime import datetime
from ..mysql_peewee import connect_mysql, dict2str, handle_table

# 当前时间
now = datetime.now()
# 链接数据库
mysql_db = connect_mysql(database='ks_db')


class table_wind_info(Model):
    nation = CharField(column_name="nation", null=True, help_text='国家')
    wind_table = CharField(column_name="wind_table", null=True, help_text='表名')
    wind_id = CharField(column_name="wind_id", null=False, unique=True, primary_key=True, help_text='指标ID')
    name = CharField(column_name="name", null=True, help_text='指标名称')
    freq = CharField(column_name="freq", null=True, help_text='频率')
    unit = CharField(column_name="unit", null=True, help_text='单位')
    source = CharField(column_name="source", null=True, help_text='来源')
    number = IntegerField(column_name="number", null=True, help_text='样本数列')
    time_interval_old = CharField(column_name="time_interval_old", null=True, help_text='时间区间(不更新)')
    update_time_old = DateField(column_name="update_time_old", null=True, help_text='初次更新时间')
    delta_time_old = IntegerField(column_name="delta_time_old", null=True, help_text='初次更新时间差')
    start_time = DateField(column_name="start_time", null=True, help_text='数据起始时间')
    end_time = DateField(column_name="end_time", null=True, help_text='数据结束时间')
    is_update = IntegerField(column_name="is_update", null=True, help_text='是否更新')
    is_abandon = IntegerField(column_name="is_abandon", null=True, help_text='是否停止更新')
    level1 = CharField(column_name="level1", null=True, help_text='一级标签')
    level2 = CharField(column_name="level2", null=True, help_text='二级标签')
    level3 = CharField(column_name="level3", null=True, help_text='三级标签')
    level4 = CharField(column_name="level4", null=True, help_text='四级标签')
    update_time = DateField(column_name="update_time", null=True, help_text='更新时间')
    op_time = DateTimeField(column_name="op_time", default=datetime.now, help_text='操作时间')

    class Meta:
        database = mysql_db
        table_name = 'macro_wind_info'
        info = {'描述': '万德宏观数据信息表', }
        table_settings = ["COMMENT='%s'" % (dict2str(info))]


class table_wind_data(Model):
    """
    wind 数据
    """
    date = DateField(verbose_name='date', help_text='日期')
    wind_id = CharField(verbose_name='wind_id', help_text='指标名')
    value = DoubleField(verbose_name='value', null=True, help_text='值')
    op_time = DateTimeField(verbose_name='op_time', default=datetime.now)

    class Meta:
        primary_key = CompositeKey('wind_id', 'date')
        indexes = (
            (("wind_id", "date", "value"), True),
        )
        table_name = 'macro_wind_data'
        database = mysql_db
        table_settings = ["COMMENT='%s'" % (dict2str({'描述': '万德宏观数据', }))]


class table_wind_data_xlsx_path(Model):
    """
    wind 数据
    """
    wind_id = AutoField()
    path = CharField(verbose_name='date', unique=True, help_text='文件路径')
    file_name = CharField(verbose_name='file_name', unique=False, help_text='文件名字')
    op_time = DateTimeField(verbose_name="op_time", default=datetime.now)
    column = TextField(verbose_name='column', help_text='因子列表')

    class Meta:
        table_name = 'macro_wind_data_xlsx'
        database = mysql_db
        table_settings = ["COMMENT='%s'" % (dict2str({'描述': '万德宏观单独下载数据', }))]


if __name__ == '__main__':

    handle_table(table_wind_info, type_=1)
    handle_table(table_wind_data, type_=1)
    handle_table(table_wind_data_xlsx_path, type_=1)

