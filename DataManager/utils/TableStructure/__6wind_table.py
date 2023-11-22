from peewee import *
from datetime import datetime
from ..mysql_peewee import connect_mysql, dict2str, create_table, drop_table, check_tb_name, init_table

# 当前时间
now = datetime.now()
# 链接数据库
mysql_db = connect_mysql(database='ks_db')


def mk_wind_name(model_class):
    """
    table_wind_stock_idx 表名制定
    """
    symbol = 'test'
    if hasattr(model_class, '_symbol'):
        symbol = model_class._symbol
    symbol = check_tb_name(symbol)
    return f'wind_idx_{symbol}'


class _table_wind_stock_idx(Model):
    date = DateField(column_name='date', primary_key=True, help_text='交易时间')
    symbol = CharField(column_name='symbol', help_text='代码')
    pre_close = DoubleField(column_name='pre_close', null=True, help_text='前收盘价')
    open = DoubleField(column_name='open', null=True, help_text='开盘价')
    high = DoubleField(column_name='high', null=True, help_text='最高价')
    low = DoubleField(column_name='low', null=True, help_text='最低价')
    close = DoubleField(column_name='close', null=True, help_text='收盘价')
    volume = DoubleField(column_name='volume', null=True, help_text='成交量')
    amt = DoubleField(column_name='amt', null=True, help_text='成交额')
    turn = DoubleField(column_name='turn', null=True, help_text='换手率')
    mkt_cap_ard = DoubleField(column_name='mkt_cap_ard', null=True, help_text='总市值')
    pe_ttm = DoubleField(column_name='pe_ttm', null=True, help_text='市盈率PE(TTM')
    ps_ttm = DoubleField(column_name='ps_ttm', null=True, help_text='市销率PS(TTM')
    pcf_ocf_ttm = DoubleField(column_name='pcf_ocf_ttm', null=True, help_text='市现率PCF(经营现金流,TTM)')
    pb_lf = DoubleField(column_name='pb_lf', null=True, help_text='市净率PB(LF,内地)')
    dividendyield2 = DoubleField(column_name='dividendyield2', null=True, help_text='股息率(LF,内地)')
    op_time = DateTimeField(column_name="op_time", default=datetime.now, help_text='操作时间')

    class Meta:
        table_function = mk_wind_name
        database = mysql_db
        info = {'描述': '万德股票指数数据', }
        table_settings = ["COMMENT='%s'" % (dict2str(info))]


def table_wind_stock_idx(symbol, create=False):
    class table(_table_wind_stock_idx):
        _symbol = str(symbol).lower()

    if create:
        create_table(table)
    return table


class table_wind_number_of_change_sh_sz(Model):
    date = DateField(column_name='date', primary_key=True, help_text='交易时间')
    risenumberofshandsz = IntegerField(help_text='沪深两市上涨家数')
    noriseorfallnumberofshandsz = IntegerField(help_text='沪深两市平盘家数')
    fallnumberofshandsz = IntegerField(help_text='沪深两市下跌家数')
    limitupnumofshandsz = IntegerField(help_text='沪深两市涨停家数')
    limitdownnumofshandsz = IntegerField(help_text='沪深两市跌停家数')
    risenumberofsh = IntegerField(help_text='沪市上涨家数')
    noriseorfallnumberofsh = IntegerField(help_text='沪市平盘家数')
    fallnumberofsh = IntegerField(help_text='沪市下跌家数')
    pctchangeofsh = DoubleField(help_text='沪市上综指涨跌幅')
    closeofsh = DoubleField(help_text='沪市收盘点位')
    limitupnumofsh = IntegerField(help_text='沪市涨停家数')
    limitdownnumofsh = IntegerField(help_text='沪市跌停家数')
    risenumberofsz = IntegerField(help_text='深市上涨家数')
    noriseorfallnumberofsz = IntegerField(help_text='深市平盘家数')
    fallnumberofsz = IntegerField(help_text='深市下跌家数')
    pctchangeofsz = DoubleField(help_text='深市上综指涨跌幅')
    closeofsz = DoubleField(help_text='深市收盘点位')
    limitupnumofsz = IntegerField(help_text='深市涨停家数')
    limitdownnumofsz = IntegerField(help_text='深市跌停家数')
    risenumberofsmallbusiness = IntegerField(help_text='中小企业板上涨家数')
    noriseorfallnumberofsmallbusiness = IntegerField(help_text='中小企业板平盘家数')
    fallnumberofsmallbusiness = IntegerField(help_text='中小企业板下跌家数')
    pctchangeofsmallbusiness = DoubleField(help_text='中小企业板上综指涨跌幅')
    closeofsmallbusiness = DoubleField(help_text='中小企业板收盘点位')
    limitupnumofsmallbusiness = IntegerField(help_text='中小企业板涨停家数')
    limitdownnumofsmallbusiness = IntegerField(help_text='中小企业板跌停家数')
    risenumofchinext = IntegerField(help_text='创业板上涨家数')
    noriseorfallnumofchinext = IntegerField(help_text='创业板平盘家数')
    fallnumofchinext = IntegerField(help_text='创业板下跌家数')
    changeofchinext = DoubleField(help_text='创业板上综指涨跌幅')
    closeofchinext = DoubleField(help_text='创业板收盘点位')
    limitupnumofchinext = IntegerField(help_text='创业板涨停家数')
    limitdownnumofchinext = IntegerField(help_text='创业板跌停家数')
    symbol = CharField(column_name='symbol', help_text='代码')
    op_time = DateTimeField(column_name="op_time", default=datetime.now, help_text='操作时间')

    class Meta:
        table_name = "wind_table_numberofchangeinshandsz"
        database = mysql_db
        info = {'描述': '沪深涨跌家数统计,提示：该表统计正常交易和停牌的股票，暂停上市公司不计入涨跌家数统计。', }
        table_settings = ["COMMENT='%s'" % (dict2str(info))]


class table_wind_szhk_transaction_tatistics(Model):
    date = DateField(column_name='date', primary_key=True, help_text='交易时间')
    sz_total_amount = FloatField(help_text='深股通-总成交金额（亿元，人民币）')
    sz_buy_amount = FloatField(help_text='深股通-买入成交金额（亿元，人民币）')
    sz_sell_amount = FloatField(help_text='深股通-卖出成交金额（亿元，人民币）')
    sz_net_purchases = FloatField(help_text='深股通-成交净买入（亿元，人民币）')
    sz_cumulative_net_bought = FloatField(help_text='深股通-累计净买入（亿元，人民币）')

    hk_total_amount = FloatField(help_text='港股通-总成交金额（亿元，人民币）')
    hk_buy_amount = FloatField(help_text='港股通-买入成交金额（亿元，人民币）')
    hk_sell_amount = FloatField(help_text='港股通-卖出成交金额（亿元，人民币）')
    hk_net_purchases = FloatField(help_text='港股通-成交净买入（亿元，人民币）')
    hk_cumulative_net_bought = FloatField(help_text='港股通-累计净买入（亿元，人民币）')

    op_time = DateTimeField(column_name="op_time", default=datetime.now, help_text='操作时间')

    class Meta:
        table_name = "wind_table_szhktransactionstatistics"
        database = mysql_db
        info = {'描述': '深港通成交统计'}
        table_settings = ["COMMENT='%s'" % (dict2str(info))]


class table_wind_shhk_transaction_tatistics(Model):
    date = DateField(column_name='date', primary_key=True, help_text='交易时间')
    sh_total_amount = FloatField(help_text='沪股通-总成交金额（亿元，人民币）')
    sh_buy_amount = FloatField(help_text='沪股通-买入成交金额（亿元，人民币）')
    sh_sell_amount = FloatField(help_text='沪股通-卖出成交金额（亿元，人民币）')
    sh_net_purchases = FloatField(help_text='沪股通-成交净买入（亿元，人民币）')
    sh_cumulative_net_bought = FloatField(help_text='沪股通-累计净买入（亿元，人民币）')

    hk_total_amount = FloatField(help_text='港股通-总成交金额（亿元，人民币）')
    hk_buy_amount = FloatField(help_text='港股通-买入成交金额（亿元，人民币）')
    hk_sell_amount = FloatField(help_text='港股通-卖出成交金额（亿元，人民币）')
    hk_net_purchases = FloatField(help_text='港股通-成交净买入（亿元，人民币）')
    hk_cumulative_net_bought = FloatField(help_text='港股通-累计净买入（亿元，人民币）')

    op_time = DateTimeField(column_name="op_time", default=datetime.now, help_text='操作时间')

    class Meta:
        table_name = "wind_table_shhktransactionstatistics"
        database = mysql_db
        info = {'描述': '沪港通成交统计', }
        table_settings = ["COMMENT='%s'" % (dict2str(info))]


class table_wind_margin_short_size_analysis_value(Model):
    date = DateField(column_name='date', primary_key=True, help_text='截止日')
    total_balance = DoubleField(column_name='total_balance', help_text='融资融券余额（沪深两市）')
    sh_balance = DoubleField(column_name='sh_balance', help_text='融资融券余额（沪市）')
    sz_balance = DoubleField(column_name='sz_balance', help_text='融资融券余额（深市）')
    total_balance_ratio_a_share_negmktcap = FloatField(column_name='total_balance_ratio_a_share_negmktcap',
                                                       help_text='两融余额占A股流通市值（%%）')
    margin_balance = DoubleField(column_name='margin_balance', help_text='融资余额')
    short_balance = DoubleField(column_name='short_balance', help_text='融券余额')
    balance_difference = DoubleField(column_name='balance_difference', help_text='余额插值（融资-融券）')
    total_trade_amount = DoubleField(column_name='total_trade_amount', help_text='两融交易额')
    total_balance_ratio_a_share_amount = FloatField(column_name='total_balance_ratio_a_share_amount',
                                                    help_text='两融交易额占A股流通市值（%%）')
    margin_purchase_amount = DoubleField(column_name='margin_purchase_amount', help_text='融资买入额')
    margin_sell_amount = DoubleField(column_name='margin_sell_amount', help_text='融券卖出额')
    trade_difference = DoubleField(column_name='trade_difference', help_text='交易额差值（融资-融券）')
    op_time = DateTimeField(column_name="op_time", default=datetime.now, help_text='操作时间')

    class Meta:
        table_name = "wind_table_marginshortsizeanalysisvalue"
        database = mysql_db
        info = {'描述': '融资融券数值分析', }
        table_settings = ["COMMENT='%s'" % (dict2str(info))]


def mk_wind_option_name(model_class):
    """
    table_wind_stock_idx 表名制定
    """
    symbol = 'test'
    if hasattr(model_class, '_symbol'):
        symbol = model_class._symbol
    symbol = check_tb_name(symbol)
    return f'wind_option_{symbol}'


class _table_wind_option(Model):
    date = DateField(column_name='date', help_text='交易时间')
    symbol = CharField(column_name='symbol', help_text='资产名称')
    option_code = CharField(column_name='option_code', help_text='期权代码')
    tradecode = CharField(column_name='tradecode', null=True, help_text='交易代码')
    exercise = FloatField(column_name='exercise', null=True, help_text='执行价')
    option_name = CharField(column_name='option_name', null=True, help_text='期权名称')
    change = DoubleField(column_name='change', null=True, help_text='涨跌幅')
    pre_settle = FloatField(column_name='pre_settle', null=True, help_text='前结算价')
    open = FloatField(column_name='open', null=True, help_text='开盘价')
    high = FloatField(column_name='high', null=True, help_text='最高价')
    low = FloatField(column_name='low', null=True, help_text='最低价')
    close = FloatField(column_name='close', null=True, help_text='收盘价')
    settle = FloatField(column_name='settle', null=True, help_text='结算价')
    volume = DoubleField(column_name='volume', null=True, help_text='成交量')
    amt = DoubleField(column_name='amt', null=True, help_text='成交额')
    position = DoubleField(column_name='position', null=True, help_text='持仓量')
    up_limit = FloatField(column_name='up_limit', null=True, help_text='涨停价格')
    down_limit = FloatField(column_name='down_limit', null=True, help_text='跌停价格')
    delta = FloatField(column_name='delta', null=True, help_text='delta')
    gamma = FloatField(column_name='gamma', null=True, help_text='gamma')
    vega = FloatField(column_name='vega', null=True, help_text='vega')
    theta = FloatField(column_name='theta', null=True, help_text='theta')
    rho = FloatField(column_name='rho', null=True, help_text='rho')
    op_time = DateTimeField(column_name="op_time", default=datetime.now, help_text='操作时间')

    class Meta:
        primary_key = CompositeKey('date', 'option_code')
        table_function = mk_wind_option_name
        database = mysql_db
        info = {'描述': '期权日行情数据', }
        table_settings = ["COMMENT='%s'" % (dict2str(info))]


def table_wind_option(symbol, create=False):
    class table(_table_wind_option):
        _symbol = str(symbol).lower()

    if create:
        create_table(table)
    return table


if __name__ == '__main__':
    pass
    # create_table(table_wind_margin_short_size_analysis_value)
    # create_table(table_wind_info_list)
    # create_table(table_wind_data)
    # create_table(table_wind_data_xlsx_path)
    # test = table_wind_stock_idx(symbol='test', create=True)
    test = table_wind_option(symbol='510050.SH', create=True)