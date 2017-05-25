# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals, absolute_import, print_function, division
from mplib import *
import numbers
import chardet


def smart_decode(obj, cast=False):
    """
    处理中文编码，根据要求返回unicode
    目前仅能处理 basestring, numeric, list, tuple, dict类型
    :param obj:
    :param cast: 是否需要将数值类型的元素都转换成unicode
    :return:
    """
    if isinstance(obj, numbers.Number):
        if cast:
            return unicode(obj)
        else:
            return obj
    elif isinstance(obj, unicode):
        return obj
    elif isinstance(obj, str):
        try:
            return obj.decode("utf8")
        except UnicodeDecodeError:
            pass
        try:
            return obj.decode("gbk")
        except UnicodeDecodeError:
            pass
        try:
            return obj.decode("gb2312")
        except UnicodeDecodeError:
            pass
        try:
            return obj.decode(chardet.detect(obj).get("encoding"))
        except UnicodeDecodeError:
            return obj
    elif isinstance(obj, list):
        return [smart_decode(i, cast=cast) for i in obj]
    elif isinstance(obj, dict):
        return {smart_decode(k, cast=cast): smart_decode(v, cast=cast) for k, v in iteritems(obj)}
    elif isinstance(obj, tuple):
        return tuple([smart_decode(i, cast=cast) for i in obj])
    else:
        return obj


def smart_encode(obj, charset="utf8", cast=False):
    """
    处理Unicode编码，根据需求返回str
    :param obj:
    :param charset:
    :param cast: 是否需要将数值类型的元素转换成字符串
    :return:
    """
    if isinstance(obj, unicode):
        return obj.encode(charset)
    elif isinstance(obj, numbers.Number):
        if cast:
            return str(obj)
        else:
            return obj
    elif isinstance(obj, str):
        try:
            obj.decode(charset)
            return obj
        except UnicodeDecodeError:
            return smart_decode(obj, cast=cast).encode(charset)
    elif isinstance(obj, list):
        return [smart_encode(i, cast=cast) for i in obj]
    elif isinstance(obj, dict):
        return {smart_encode(k, cast=cast): smart_encode(v, cast=cast) for k, v in iteritems(obj)}
    elif isinstance(obj, tuple):
        return tuple([smart_encode(i, cast=cast) for i in obj])


def change_charset(obj, to_charset="utf8"):
    """
    将对象的编码转换成指定的编码
    目前只处理basestring, list, tuple, dict对象, 数值类型不作处理
    :param obj:
    :param to_charset:
    :return:
    """
    if isinstance(obj, unicode):
        return obj.encode(to_charset)

    elif isinstance(obj, str):
        try:
            return smart_decode(obj).encode(to_charset)
        except UnicodeDecodeError:
            return obj

    elif isinstance(obj, numbers.Number):
        return obj

    elif isinstance(obj, list):
        return [change_charset(i) for i in obj]

    elif isinstance(obj, dict):
        return {change_charset(k): change_charset(v) for k, v in iteritems(obj)}

    elif isinstance(obj, tuple):
        return tuple([change_charset(i) for i in obj])
    
    else:
        return obj


def to_unicode(obj):
    """
    打印需要
    注意! list/tuple/dict类如果只是需要转换内部元素为unicode, 则需用smart_decode函数
    :param obj:
    :return:
    """
    if isinstance(obj, unicode):
        return obj

    elif isinstance(obj, str):
        return smart_decode(obj)

    elif isinstance(obj, numbers.Number) or isinstance(obj, list) or isinstance(obj, dict) or isinstance(obj, tuple):
        return unicode(obj)

    else:
        return obj

if __name__ == "__main__":
    test_objs = [
        3,
        311241231245123,
        2.5,
        "it's a unicode",
        ["a", "你好".encode("utf8"), "世界".encode("utf8")],
        {b"key1": b"value1", b"key2": "值2", "键3": b"value3", "key4": 5.3},
        ("tuple[0]", "元组".encode("utf8"), "奇怪的编码".encode("gb2312"))
    ]
    for test_obj in test_objs:
        print(smart_decode(test_obj, cast=True), type(smart_decode(test_obj, cast=True)))
    for test_obj in test_objs:
        print(smart_encode(test_obj, cast=True), type(smart_encode(test_obj, cast=True)))
    print(smart_decode(test_objs, cast=True))
