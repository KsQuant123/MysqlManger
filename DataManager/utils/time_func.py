import datetime
import time

import pytz


timezone = {
    "cn": datetime.timezone(datetime.timedelta(hours=8)),
    "utc": datetime.timezone.utc,
    "ny": datetime.timezone(datetime.timedelta(hours=-4)),
    'cc': datetime.timezone(datetime.timedelta(hours=-5)),
    "0": datetime.timezone.utc,
    "+1": datetime.timezone(datetime.timedelta(hours=1)),
    "+2": datetime.timezone(datetime.timedelta(hours=2)),
    "+3": datetime.timezone(datetime.timedelta(hours=3)),
    "+4": datetime.timezone(datetime.timedelta(hours=4)),
    "+5": datetime.timezone(datetime.timedelta(hours=5)),
    "+6": datetime.timezone(datetime.timedelta(hours=6)),
    "+7": datetime.timezone(datetime.timedelta(hours=7)),
    "+8": datetime.timezone(datetime.timedelta(hours=8)),
    "+9": datetime.timezone(datetime.timedelta(hours=9)),
    "+10": datetime.timezone(datetime.timedelta(hours=10)),
    "+11": datetime.timezone(datetime.timedelta(hours=11)),
    "+12": datetime.timezone(datetime.timedelta(hours=12)),
    "-1": datetime.timezone(datetime.timedelta(hours=-1)),
    "-2": datetime.timezone(datetime.timedelta(hours=-2)),
    "-3": datetime.timezone(datetime.timedelta(hours=-3)),
    "-4": datetime.timezone(datetime.timedelta(hours=-4)),
    "-5": datetime.timezone(datetime.timedelta(hours=-5)),
    "-6": datetime.timezone(datetime.timedelta(hours=-6)),
    "-7": datetime.timezone(datetime.timedelta(hours=-7)),
    "-8": datetime.timezone(datetime.timedelta(hours=-8)),
    "-9": datetime.timezone(datetime.timedelta(hours=-9)),
    "-10": datetime.timezone(datetime.timedelta(hours=-10)),
    "-11": datetime.timezone(datetime.timedelta(hours=-11)),
    "-12": datetime.timezone(datetime.timedelta(hours=-12)),

}


def time_switch(t, sourcetz="cn", totype="str", targettz="cn", strformat="%Y-%m-%d %H:%M:%S"):
    """
        t: int/str 需要转换的时间表示，秒级时间戳或格式化的字符串
        sourcetz: t所关联的时区别名，在timezone字典里自定义别名，如“cn”表示东八区,"utc"为世界标准时间
        targettz: 输出时间的时区别名
        totype: "str" or "timestamp" 输出时间表示类型
        strformat：输入或输出的字符串格式，如果t为字符串，则strformat需要和它对应。
    """
    # 常用的两个时区定义在字典里，可以添加其他时区

    sourcetz = timezone.get(sourcetz) if sourcetz else None
    targettz = timezone.get(targettz) if targettz else sourcetz

    # transfer to datetime
    if isinstance(t, str):
        ctime = datetime.datetime.strptime(t, strformat).replace(tzinfo=sourcetz)
    elif isinstance(t, int) or isinstance(t, float):
        if t > 1e10: t = t / 1000
        ctime = datetime.datetime.fromtimestamp(t)#.replace(tzinfo=sourcetz)
    elif isinstance(t, datetime.datetime):
        ctime = t.replace(tzinfo=sourcetz)
    # from datetime to target type
    if totype == "str":

        newtime = ctime.astimezone(targettz).strftime(strformat)
    elif totype == 'timestamp':
        newtime = ctime.astimezone(targettz).timestamp()
        newtime = int(newtime)
    return newtime


if __name__ == '__main__':
    print(time_switch(1644480000, sourcetz='utc', totype='str', targettz='cn'))
    print(time_switch('2022-05-25 18:18:00', sourcetz='ny', totype='timestamp', targettz='ny'))
