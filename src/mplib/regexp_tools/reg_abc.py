#!/usr/bin/python
# -*- coding: UTF-8 -*-
import re

def reg_abc(regexp_para):
    # 赋值
    m = regexp_para["head"]["distance"]
    n = regexp_para["tail"]["distance"]
    o = regexp_para["center"]["distance"]
    a = regexp_para["head"]["value"]
    aa = regexp_para["head"]["logic"]
    b = regexp_para["center"]["value"]
    bb = regexp_para["center"]["logic"]
    c = regexp_para["tail"]["value"]
    cc = regexp_para["tail"]["logic"]
    # 参数检验_中心词
    if b == "":
        print "中心词不能为空"
        raise TypeError
    for line in list(b):
        if line in ["^", "["]:
            print "元字符只能有|和()"
            raise TypeError
    if bb not in ["包含", "不包含"]:
        print "中心词逻辑只能有包含或者不包含"
        raise TypeError
    if o != "":
        print "中心词没有距离"
        raise TypeError
    # 参数检验_前后缀
    for line in list(a+c):
        if line in ["^", "["]:
            print "元字符只能有|和()"
            raise TypeError
    if aa not in ["包含", "不包含", ""] or cc not in ["包含", "不包含", ""]:
        print "前后缀逻辑只能有包含或者不包含或者空"
        raise TypeError
    for line in [m, n]:
        if line != "":
            try:
                if int(line) >= 0:
                    continue
                else:
                    print "前后缀距离不能为负"
                    raise TypeError
            except Exception:
                print "前后缀距离只能是数字"
        else:
            continue
    # 参数检验_逻辑
    for line in [a, aa, m]:
        if line == '' and len(a+aa+m) != 0:
            print "前缀参数不全"
            raise TypeError
    for line in [c, cc, n]:
        if line == '' and len(c+cc+n) != 0:
            print "后缀参数不全"
            raise TypeError
    # 分状态
    # 参数处理
    a = '(%s)' % a
    b = '(%s)' % b
    c = '(%s)' % c
    # print a
    # 不包含中心词
    if bb == "不包含":
        regexp_out = '^(?!.*%s)' % b
    # 包含中心词，只需考虑前后缀
    else:
        # 无前后缀
        if aa == "" and cc == "":
            regexp_out = '%s' % b
        # 无前有后
        elif aa == "" and cc != "":
            n = int(n)
            if cc == "包含":
                regexp_out = '%s([^%s]{,%s})%s' % (b, c, n-1, c)
            if cc == "不包含":
                regexp_out = '%s([^%s]{%s,}|[^%s]*$)' % (b, c, n, c)
        # 有前无后
        elif aa != "" and cc == "":
            m = int(m)
            if aa == "包含":
                regexp_out = '%s[^%s]{,%s}%s' % (a, a, m-1, b)
            if aa == "不包含":
                regexp_out = '([^%s]{%s,}|^[^%s]*)%s' % (a, m, a, b)
        # 有前有后
        elif aa != "" and cc != "":
            m = int(m)
            n = int(n)
            if aa == "包含" and cc == "包含":
                regexp_out = '(%s[^%s]{,%s})%s([^%s]{,%s})%s' % (a, a, m-1, b, c, n-1, c)
            elif aa == "不包含" and cc == "不包含":
                regexp_out = '([^%s]{%s,}|^[^%s]*)%s([^%s]{%s,}|[^%s]*$)' % (a, m, a, b, c, n, c)
            elif aa == "包含" and cc == "不包含":
                regexp_out = '(%s[^%s]{,%s})%s([^%s]{%s,}|[^%s]*$)' % (a, a, m-1, b, c, n, c)
            elif aa == "不包含" and cc == "包含":
                regexp_out = '([^%s]{%s,}|^[^%s]*)%s([^%s]{,%s})%s' % (a, m, a, b, c, n-1, c)
        else:
            print "this is a bug!"
    # 不包含中心词，有前后缀
    pass
    # 返回正则表达式
    regexp_out = regexp_out.decode('utf-8')
    return regexp_out

def unit_test(regexp_para,test_case):
    # 正则实现
    regexp_out = reg_abc(regexp_para)
    print regexp_out
    for line in test_case:
        sample_text, r = line
        # print sample_text
        try:
            if re.search(regexp_out, sample_text) is None:
                rr = False
            else:
                rr = True
            if rr != r:
                print sample_text, r
        except Exception as e:
            # print e
            print "reg_abc怎么还有bug？"

if __name__ == "__main__":
    # 字典结构：{中心词(值、距离、逻辑)，前缀词(值、距离、逻辑)，后缀词(值、距离、逻辑)}，中心距离只能为空，前后缀值/距离/逻辑应该同时存在或不存在。
    # 测试文本：[(字符串,Y/N),(字符串,Y/N),(字符串,Y/N)]，对同一个字典做一系列测试，输出匹配错误的case
    regexp_para = dict(
        center=dict(
            value="中心",
            distance="",
            logic="包含",
        ),
        head=dict(
            value="前缀|前面",
            distance="3",
            logic="不包含",
        ),
        tail=dict(
            value="后缀|后面",
            distance="2",
            logic="不包含",
        ),
    )
    test_case = [
        (u"中心", True),
        (u"中心后缀", False),
        (u"前面是是中心的后缀", False),
        (u"前缀是是是中心不不后缀", True),
        (u"前缀是是是中心不后缀的", False),
        (u"前缀是是中心不不后面", False)
    ]
    unit_test(regexp_para, test_case)
