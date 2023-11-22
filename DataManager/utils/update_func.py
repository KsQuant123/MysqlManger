from functools import wraps
from datetime import datetime, timedelta
from pytz import timezone
from .mysql_peewee import get_latest_date

# 当前时间
cn = timezone('Asia/Shanghai')
now = datetime.now(cn)


def update_data(func):
    """
    func(start_time, end_time, *args, **kwargs)
    start_time, end_time import params
    table: optional if None, direct return func
    :param func:
    :return:
    """

    @wraps(func)
    def query_data(*args, **kwargs):
        if kwargs.get('update', False):
            table = kwargs.get('table', None)
            if table is None:
                return func(*args, **kwargs)
            last_time_columns = kwargs.get('last_time_columns', 'date')
            last_time, op_time = get_latest_date(table=table, date_name='date', op_time=True)
            if op_time is None:
                pass
            elif op_time.date() < now.date():
                kwargs['start_time'] = str(last_time + timedelta(days=-10))
            else:
                print('无需更新')
                return None
        return func(*args, **kwargs)

    return query_data
