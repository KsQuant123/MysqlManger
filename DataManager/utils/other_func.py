import re
import inspect


def var2name(p):
    """
    将变量转为str
    :param p: var 变量
    :return: str 变量名
    """
    print(inspect.getframeinfo(inspect.currentframe().f_back))
    for line in inspect.getframeinfo(inspect.currentframe().f_back)[3]:
        m = re.search(r'\bvar2name\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)', line)
        if m:
            return m.group(1)


if __name__ == '__main__':
    a = 1
    import re
    import inspect


    def varname(p):
        """将变量转为str"""
        print(inspect.getframeinfo(inspect.currentframe().f_back)[3])
        for line in inspect.getframeinfo(inspect.currentframe().f_back)[3]:
            print(line)
            m = re.search(r'\bvarname\s*\(\s*([A-Za-z_][A-Za-z0-9_]*)\s*\)', line)
        if m:
            return m.group(1)


    print(varname(a))