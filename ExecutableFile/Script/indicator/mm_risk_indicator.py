import os
import sys
import fire
from loguru import logger
from tqdm import tqdm

# 脚本路径
py_path = (os.path.abspath(__file__))
# 主路径
main_path = os.path.abspath(os.path.join(py_path, '../../../..', ))
sys.path.append(main_path)

from DataManager.Indicator.update_indicator import IndicatorHandler, get_data_by_columns, RiskIndicatorData
from ExecutableFile.Config.config import config, config_logger
from DataManager.Macro.MM.MM_update import MacroMMInfo, MacroMMdata, MacroMMDetail


@logger.catch
def vix():
    """更新指标数据至数据库"""
    info = get_data_by_columns(table=MacroMMInfo, title='行情-波动率')
    for i in info.itertuples():
        detail = get_data_by_columns(table=MacroMMDetail, id=i.id).iloc[0]
        idx = IndicatorHandler(ID=i.slug,
                               country=i.country,
                               name=i.name,
                               unit=detail.u,
                               freq=detail.freq,
                               source='MM',
                               description=i.description,
                               tz='UTC+4',
                               level1='风险',
                               level2='市场风险',
                               level3='波动率')
        old_data = get_data_by_columns(table=RiskIndicatorData, id=idx.ID)
        data = get_data_by_columns(table=MacroMMdata, stat_id=detail.stat_id, stat_id_first_digit=detail.stat_id[0])
        if not old_data.empty:
            data = data[data['date'] >= old_data['date'].max()]
        if not data.empty:
            idx.run(data=data, conflict='ignore')


@logger.catch
def bond():
    """更新指标数据至数据库"""
    info = get_data_by_columns(table=MacroMMInfo, title='行情-债券')
    for i in info.itertuples():
        detail = get_data_by_columns(table=MacroMMDetail, id=i.id).iloc[0]
        idx = IndicatorHandler(ID=i.slug,
                               country=i.country,
                               name=i.name,
                               unit=detail.u,
                               freq=detail.freq,
                               source='MM',
                               description=i.description,
                               tz='UTC+8',
                               level1='风险',
                               level2='市场风险',
                               level3='利率')
        old_data = get_data_by_columns(table=RiskIndicatorData, id=idx.ID)
        data = get_data_by_columns(table=MacroMMdata, stat_id=detail.stat_id, stat_id_first_digit=detail.stat_id[0])
        if not old_data.empty:
            data = data[data['date'] >= old_data['date'].max()]
        if not data.empty:
            idx.run(data=data, conflict='ignore')


@logger.catch
def commodity():
    """更新指标数据至数据库"""
    info = get_data_by_columns(table=MacroMMInfo, title='行情-商品')
    for i in info.itertuples():
        detail = get_data_by_columns(table=MacroMMDetail, id=i.id).iloc[0]
        idx = IndicatorHandler(ID=i.slug,
                               country=i.country,
                               name=i.name,
                               unit=detail.u,
                               freq=detail.freq,
                               source='MM',
                               description=i.description,
                               tz='UTC+8',
                               level1='风险',
                               level2='市场风险',
                               level3='通胀')
        old_data = get_data_by_columns(table=RiskIndicatorData, id=idx.ID)
        data = get_data_by_columns(table=MacroMMdata, stat_id=detail.stat_id, stat_id_first_digit=detail.stat_id[0])
        if not old_data.empty:
            data = data[data['date'] >= old_data['date'].max()]
        if not data.empty:
            idx.run(data=data, conflict='ignore')


@logger.catch
def forex():
    """更新指标数据至数据库"""
    info = get_data_by_columns(table=MacroMMInfo, title='行情-外汇')
    for i in info.itertuples():
        detail = get_data_by_columns(table=MacroMMDetail, id=i.id).iloc[0]
        idx = IndicatorHandler(ID=i.slug,
                               country=i.country,
                               name=i.name,
                               unit=detail.u,
                               freq=detail.freq,
                               source='MM',
                               description=i.description,
                               tz='UTC+8',
                               level1='风险',
                               level2='市场风险',
                               level3='汇率')
        old_data = get_data_by_columns(table=RiskIndicatorData, id=idx.ID)
        data = get_data_by_columns(table=MacroMMdata, stat_id=detail.stat_id, stat_id_first_digit=detail.stat_id[0])
        if not old_data.empty:
            data = data[data['date'] >= old_data['date'].max()]
        if not data.empty:
            idx.run(data=data, conflict='ignore')


@logger.catch
def equity():
    """更新指标数据至数据库"""
    info = get_data_by_columns(table=MacroMMInfo, title='行情-股市')
    for i in info.itertuples():
        detail = get_data_by_columns(table=MacroMMDetail, id=i.id).iloc[0]
        idx = IndicatorHandler(ID=i.slug,
                               country=i.country,
                               name=i.name,
                               unit=detail.u,
                               freq=detail.freq,
                               source='MM',
                               description=i.description,
                               tz='UTC+8',
                               level1='风险',
                               level2='市场风险',
                               level3='权益')
        old_data = get_data_by_columns(table=RiskIndicatorData, id=idx.ID)
        data = get_data_by_columns(table=MacroMMdata, stat_id=detail.stat_id, stat_id_first_digit=detail.stat_id[0])
        if not old_data.empty:
            data = data[data['date'] >= old_data['date'].max()]
        if not data.empty:
            idx.run(data=data, conflict='ignore')


@logger.catch
def CDS():
    """更新指标数据至数据库"""
    info = get_data_by_columns(table=MacroMMInfo, title='行情-CDS')
    for i in tqdm(info.itertuples()):
        detail = get_data_by_columns(table=MacroMMDetail, id=i.id).iloc[0]
        idx = IndicatorHandler(ID=i.slug,
                               country=i.country,
                               name=i.name,
                               unit=detail.u,
                               freq=detail.freq,
                               source='MM',
                               description=i.description,
                               tz='UTC+8',
                               level1='风险',
                               level2='信用风险',
                               level3='CDS')
        old_data = get_data_by_columns(table=RiskIndicatorData, id=idx.ID)
        data = get_data_by_columns(table=MacroMMdata, stat_id=detail.stat_id, stat_id_first_digit=detail.stat_id[0])
        if not old_data.empty:
            data = data[data['date'] >= old_data['date'].max()]
        if not data.empty:
            idx.run(data=data, conflict='ignore')
            # pass


if __name__ == '__main__':
    Indicator_Database_path = config('data', 'Indicator', 'indicator_path')
    # 配置日志
    logger = config_logger(dirs=Indicator_Database_path, file_name='indicator_update')
    #
    bond()
    commodity()
    forex()
    equity()
    CDS()
    vix()
