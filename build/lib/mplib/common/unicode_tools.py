# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
import chardet


def smart_decode(obj, cast=False):
    """
    处理中文编码，根据要求返回unicode
    目前仅能处理 basestring, numeric, list, tuple, dict类型
    :param obj:
    :param cast: 是否需要将所有元素都转换成unicode
    :return:
    """
    if isinstance(obj, int) or isinstance(obj, long) or isinstance(obj, float) or isinstance(obj, unicode):
        return unicode(obj) if cast else obj
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
        return [smart_decode(i) for i in obj]
    elif isinstance(obj, dict):
        return {smart_decode(k): smart_decode(v) for k, v in obj.iteritems()}
    elif isinstance(obj, tuple):
        return tuple([smart_decode(i) for i in obj])
    else:
        return obj


def smart_encode(obj, charset="utf8"):
    """
    处理Unicode编码，根据需求返回str
    :param obj:
    :param charset:
    :return:
    """
    try:
        return obj.encode(charset)
    except UnicodeEncodeError:
        return obj


def change_charset(obj, to_charset="utf8"):
    """
    将对象的编码转换成指定的编码
    目前只处理basestring, list, tuple, dict对象, numeric类型不作处理
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

    elif isinstance(obj, int) or isinstance(obj, float) or isinstance(obj, long):
        return obj

    elif isinstance(obj, list):
        return [change_charset(i) for i in obj]

    elif isinstance(obj, dict):
        return {change_charset(k): change_charset(v) for k, v in obj.iteritems()}

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

    elif isinstance(obj, int) or isinstance(obj, list) or isinstance(obj, dict) or isinstance(obj, tuple):
        return unicode(obj)

    else:
        return obj

if __name__ == "__main__":
    test_objs = [
        3,
        3L,
        2.5,
        "it's a unicode",
        ["a", b"你好", b"世界"],
        {b"key1": b"value1", b"key2": "值2", "键3": b"value3", "key4": 5.3},
        ("tuple[0]", b"元组", "奇怪的编码".encode("gb2312"))
    ]
    for test_obj in test_objs:
        print smart_decode(test_obj)

