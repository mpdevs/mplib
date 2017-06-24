# coding: utf-8
# __author__: u"John"
"""
This module contains debug / print tools
"""
from __future__ import unicode_literals, absolute_import, print_function, division
from mplib.common.unicode_tool import to_unicode, smart_decode
from mplib.common.setting import DEBUG
from mplib.IO import PostgreSQL
from os.path import dirname, join
from collections import OrderedDict
from datetime import datetime
from pprint import pprint
from mplib import *

import pickle
import numpy


def print_line(placeholder="-", repeat=50, center_word=""):
    """
    打印一行分隔符
    :param placeholder: 分割符基本元素
    :param repeat: 分隔符基本元素分别在前面和后面出现的次数, 必须是int
    :param center_word: 中心显示的内容
    :return:
    """
    if not DEBUG:
        return

    if isinstance(repeat, long) or isinstance(repeat, int):
        pass
    elif isinstance(repeat, float):
        repeat = int(repeat)
    elif isinstance(repeat, basestring):
        try:
            repeat = int(repeat)
        except ValueError:
            return
    else:
        return

    repeat = repeat if repeat <= 80 else 80
    center_word = " {0} ".format(to_unicode(center_word)) if center_word else center_word
    print("{0}{1}{0}".format(to_unicode(placeholder) * repeat, center_word))


def print_var_str(var_name_str, ptype="print", caller_locals=locals()):
    var_val = {var_name_str: caller_locals.get(var_name_str)}
    if ptype == "print":
        print(var_val)
    elif ptype == "pprint":
        pprint(var_val)


def print_var(var_name, ptype="print", caller_locals=locals()):
    """
    https://stackoverflow.com/questions/2553354/how-to-get-a-variable-name-as-a-string-in-python
    """
    names = [k for k, v in iteritems(caller_locals) if v is var_name]
    for name in names:
        print_var_str(name, ptype, caller_locals)


def get_print_var(var_dict, var_list):
    od = OrderedDict()

    for i in var_list:
        od[i] = None

    for k, v in iteritems(var_dict):
        if k in list(od):
            od[k] = v

    return od


def print_var_info(var_dict, var_list):
    for k, v in iteritems(get_print_var(var_dict, var_list)):
        d("{0}: {1}".format(k, v))


def d(string):
    print("{0} {1}".format(datetime.now(), string))


def save_model_to_pickle(model_obj, model_name, path=None):
    from sklearn.externals import joblib
    path = path if path else __file__
    joblib.dump(model_obj, "{0}.pickle".format(join(dirname(path), model_name)))


def save_model_to_pg(model_obj, model_name):
    sql = """
    INSERT INTO text_value
    (name, value)
    VALUES
    ('{model_name}', %s)
    """.format(model_name=model_name)
    PostgreSQL("v2_uat").execute(sql, (pickle.dumps(model_obj),))


def update_model_to_pg(model_obj, model_name):
    sql = """
    UPDATE text_value
    SET value = %s
    WHERE name = '{model_name}'
    """.format(model_name=model_name)
    PostgreSQL("v2_uat").execute(sql, (pickle.dumps(model_obj),))


def load_model_from_pickle(model_name, path=None):
    from sklearn.externals import joblib
    path = path if path else __file__
    return joblib.load("{0}.pickle".format(join(dirname(path), model_name)))


def load_model_from_pg(model_name):
    sql = """
        SELECT value
        FROM text_value
        WHERE name = '{model_name}'
        """.format(model_name=model_name)
    return pickle.loads(PostgreSQL("v2_uat").query(sql)[0].get("value"))


def exists_model_in_pg(model_name):
    sql = """
        SELECT 1
        FROM text_value
        WHERE name = '{model_name}'
        """.format(model_name=model_name)
    ret = PostgreSQL("v2_uat").query(sql)
    return True if ret else False


def vector_reshape_to_matrix(v):
    return v.reshape(len(v), 1)


def normalize(data):
    data -= numpy.mean(data)
    data /= numpy.std(data)
    return data


def remove_crlf(string):
    return smart_decode(string).replace("\n", "").replace("\r", "")


if __name__ == "__main__":
    print_var_str("normalize")
    print_var(normalize)

