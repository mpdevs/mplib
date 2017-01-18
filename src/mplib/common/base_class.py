# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals


# region 属性访问定义——字典后接'.' + key名，即可得到字典中key对应的内容
class AttributeDict(dict):
    """
    能够把dict的key当作class的attribute
    """
    def __getattr__(self, attr):
        return self[attr]

    def __setattr__(self, attr, value):
        self[attr] = value
# endregion
