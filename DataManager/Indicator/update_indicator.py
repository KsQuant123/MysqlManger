from ..utils.TableStructure.__12risk_indicator import RiskIndicatorData, RiskIndicatorInfo
from ..utils.mysql_peewee import get_data_by_columns, insert_data2table, insert_dict2table
from loguru import logger


class IndicatorHandler:
    def __init__(self, **kwargs):
        self.set_attributes(kwargs)

    def set_attributes(self, attributes):
        for key, value in attributes.items():
            setattr(self, key, value)

    def insert_indicator_info(self, conflict='ignore'):
        """
        插入指标信息
        """
        info_dict = [{'id': self.ID,
                      'country': self.country,
                      'name': self.name,
                      'unit': self.unit,
                      'freq': self.freq,
                      'source': self.source,
                      'number': None,
                      'start_time': None,
                      'end_time': None,
                      'description': self.description,
                      'tz': self.tz,
                      'level1': self.level1,
                      'level2': self.level2,
                      'level3': self.level3,
                      }]
        insert_dict2table(dict_term=info_dict, table=RiskIndicatorInfo, conflict=conflict)

    def update_indicator_info(self):
        """
        更新指标信息
        :param data: 指标信息
        :return: None
        """
        data = get_data_by_columns(table=RiskIndicatorData, id=self.ID)
        if data.empty:
            logger.warning(f'指标{self.ID}数据为空')
            return
        number = len(data)
        start_time = data['date'].min()
        end_time = data['date'].max()
        update_query = RiskIndicatorInfo.update(number=number, start_time=start_time, end_time=end_time) \
            .where(RiskIndicatorInfo.id == self.ID)
        update_query.execute()
        logger.info(f'指标{self.ID}信息更新成功')

    def insert_indicator_data(self, data, conflict='ignore'):
        """
        插入指标数据
        :param data: 指标数据
        :param conflict: 冲突处理方式
        :return: None
        """
        data['id'] = self.ID
        insert_data2table(RiskIndicatorData, data[['id', 'date', 'value']], conflict=conflict)

    def run(self, data=None, conflict='ignore'):
        self.insert_indicator_info(conflict=conflict)
        if data is not None:
            self.insert_indicator_data(data=data, conflict=conflict)
            self.update_indicator_info()


if __name__ == '__main__':
    test = IndicatorHandler(ID='test',
                            country='中国',
                            name='测试指标',
                            unit='元',
                            freq='月',
                            source='测试来源',
                            description='测试描述',
                            tz='UTC+8',
                            level1='测试一级标签', level2='测试二级标签', level3='测试三级标签')
    test.run()
