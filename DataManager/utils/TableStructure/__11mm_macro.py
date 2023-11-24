from peewee import *
from datetime import datetime
from ..mysql_peewee import connect_mysql, dict2str, handle_table
# from DataManager.utils.mysql_peewee import connect_mysql, dict2str, handle_table
# from DataManager.Script.mysql_peewee import connect_mysql, dict2str, handle_table
#
# 当前时间
now = datetime.now()
# 链接数据库
mysql_db = connect_mysql(database='ks_db')


class MacroMMInfo(Model):
    title = CharField(column_name='title', null=False, help_text='标题、分类')
    home_page = CharField(column_name='home_page', help_text='主页面url')
    id = CharField(column_name='id', help_text='id')
    user_id = CharField(column_name='user_id', help_text='')
    type = CharField(column_name='type', null=True, help_text='')
    country = CharField(column_name='country', null=True, help_text='国家')
    name = CharField(column_name='name', null=False, help_text='数据集名称（一系列指标）')
    slug = CharField(column_name='slug', null=True, help_text='数据集名称-英文')
    description = TextField(column_name='description', null=True, help_text='简介')
    count_comments = CharField(column_name='count_comments', null=True, help_text='')
    commented = CharField(column_name='commented', null=True, help_text='')
    img_updated = DateTimeField(column_name='img_updated', null=True, help_text='更新时间')
    is_membersonly = CharField(column_name='is_membersonly', null=True, help_text='是否会员专享')
    state = TextField(column_name='state', null=True, help_text='是否持续有效')
    url = CharField(column_name='url', null=True, help_text='')
    luv = CharField(column_name='luv', null=True, help_text='')
    is_on = CharField(column_name='is_on', null=True, help_text='')
    settings = TextField(column_name='settings', null=True, help_text='')
    relations = TextField(column_name='relations', null=True, help_text='相关数据信息')
    blog_tags = CharField(column_name='blog_tags', null=True, help_text='')
    related_charts = TextField(column_name='related_charts', null=True, help_text='相关主题')
    sc_liked = CharField(column_name='sc_liked', null=True, help_text='')
    user = CharField(column_name='user', null=True, help_text='')
    srcs = CharField(column_name='srcs', null=True, help_text='数据来源')
    stats = TextField(column_name='stats', null=True, help_text='子数据信息')
    cc_url = CharField(column_name='cc_url', null=True, help_text='数据集页面')
    op_time = DateTimeField(column_name="op_time", default=datetime.now, help_text='操作时间')

    class Meta:
        primary_key = CompositeKey('title', 'id')
        table_name = 'macro_mm_info'
        database = mysql_db
        info = {'描述': 'MM平方财经分类表', }
        table_settings = ["COMMENT='%s'" % (dict2str(info))]


class MacroMMDetail(Model):
    id = CharField(column_name='id', help_text='macro_mm_info 中数据组的id')
    stat_id = CharField(column_name='stat_id', null=False, help_text='数据id')
    name = CharField(column_name='name', null=True, help_text='数据名称')
    name_tc = CharField(column_name='name_tc', null=True, help_text='繁体中文名')
    name_sc = CharField(column_name='name_sc', null=True, help_text='简体中文名')
    name_en = CharField(column_name='name_en', null=True, help_text='英文名')
    country = CharField(column_name='country', null=True, help_text='国家')
    ytype = CharField(column_name='ytype', null=True, help_text='数据类型')
    arithmetic = CharField(column_name='arithmetic', null=True, help_text='算法')
    lineType = CharField(column_name='lineType', null=True, help_text='线条类型')
    currency = CharField(column_name='currency', null=True, help_text='货币')
    freq = CharField(column_name='freq', null=True, help_text='数据频率')
    old_freq = CharField(column_name='old_freq', null=True, help_text='数据频率（旧）')
    category = CharField(column_name='category', null=True, help_text='分类')
    u = CharField(column_name='u', null=True, help_text='')
    old_u = CharField(column_name='old_u', null=True, help_text='')
    aggn = CharField(column_name='aggn', null=True, help_text='')
    opn = CharField(column_name='opn', null=True, help_text='')
    opn_n = CharField(column_name='opn_n', null=True, help_text='')
    opn_m = CharField(column_name='opn_m', null=True, help_text='')
    base_date = CharField(column_name='base_date', null=True, help_text='')
    op_time = DateTimeField(column_name="op_time", default=datetime.now, help_text='操作时间')

    class Meta:
        primary_key = CompositeKey('id', 'stat_id')
        table_name = 'macro_mm_detail'
        database = mysql_db
        info = {'描述': 'MM平方财经数据信息', }
        table_settings = ["COMMENT='%s'" % (dict2str(info))]


class MacroMMdata(Model):
    date = DateField(column_name='date', null=True, help_text='日期')
    stat_id = CharField(column_name='stat_id', null=False, help_text='数据id')
    value = CharField(column_name='value', null=True, help_text='数值')
    stat_id_first_digit = IntegerField(column_name='stat_id_first_digit', null=False, help_text='id首位数字')
    op_time = DateTimeField(column_name="op_time", default=datetime.now, help_text='操作时间')

    class Meta:
        primary_key = CompositeKey('date', 'stat_id', 'stat_id_first_digit')
        table_name = 'macro_mm_data'
        database = mysql_db
        info = {'描述': 'MM平方财经数据', }
        table_settings = ["COMMENT='%s'" % (dict2str(info))]


if __name__ == '__main__':
    # handle_table(MacroMMInfo, 1)
    # handle_table(MacroMMDetail, 1)
    handle_table(MacroMMdata, 1)
