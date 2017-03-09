#!/usr/bin/python
# -*- coding: UTF-8 -*-
from __future__ import unicode_literals
from mplib.common.unicode_tools import smart_decode


def reg_abc(regexp_para):
    # 赋值
    regexp_para = smart_decode(regexp_para)
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
        if line == "" and len(a+aa+m) != 0:
            print "前缀参数不全"
            raise TypeError
    for line in [c, cc, n]:
        if line == "" and len(c+cc+n) != 0:
            print "后缀参数不全"
            raise TypeError
    # 分状态
    # 参数处理
    a = "(%s)" % a
    b = "(%s)" % b
    c = "(%s)" % c
    # print a
    # 不包含中心词
    if bb == "不包含":
        regexp_out = "^(?!.*%s)" % b
    # 包含中心词，只需考虑前后缀
    else:
        # 无前后缀
        if aa == "" and cc == "":
            regexp_out = "%s" % b
        # 无前有后
        elif aa == "" and cc != "":
            n = int(n)
            if cc == "包含":
                regexp_out = "%s([^%s]{,%s})%s" % (b, c, n-1, c)
            if cc == "不包含":
                regexp_out = "%s([^%s]{%s,}|[^%s]*$)" % (b, c, n, c)
        # 有前无后
        elif aa != "" and cc == "":
            m = int(m)
            if aa == "包含":
                regexp_out = "%s[^%s]{,%s}%s" % (a, a, m-1, b)
            if aa == "不包含":
                regexp_out = "([^%s]{%s,}|^[^%s]*)%s" % (a, m, a, b)
        # 有前有后
        elif aa != "" and cc != "":
            m = int(m)
            n = int(n)
            if aa == "包含" and cc == "包含":
                regexp_out = "(%s[^%s]{,%s})%s([^%s]{,%s})%s" % (a, a, m-1, b, c, n-1, c)
            elif aa == "不包含" and cc == "不包含":
                regexp_out = "([^%s]{%s,}|^[^%s]*)%s([^%s]{%s,}|[^%s]*$)" % (a, m, a, b, c, n, c)
            elif aa == "包含" and cc == "不包含":
                regexp_out = "(%s[^%s]{,%s})%s([^%s]{%s,}|[^%s]*$)" % (a, a, m-1, b, c, n, c)
            elif aa == "不包含" and cc == "包含":
                regexp_out = "([^%s]{%s,}|^[^%s]*)%s([^%s]{,%s})%s" % (a, m, a, b, c, n-1, c)
        else:
            print "this is a bug!"
    # 不包含中心词，有前后缀
    pass
    # 返回正则表达式
    return regexp_out


