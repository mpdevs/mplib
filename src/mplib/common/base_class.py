# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals, absolute_import, print_function, division
from abc import abstractmethod
from datetime import datetime


class AttributeDict(dict):
    """
    能够把dict的key当作class的attribute
    """
    def __getattr__(self, attr):
        return self[attr]

    def __setattr__(self, attr, value):
        self[attr] = value


class DateTime(datetime):
    def __init__(self, *args, **kwargs):
        datetime.__init__(*args, **kwargs)
        self.quarter = (self.month - 1) // 3 + 1


class Database(object):
    def __init__(self):
        self.connection = None
        self.cursor = None
        self.env_dict = None

    @abstractmethod
    def query(self):
        pass

    @abstractmethod
    def execute(self):
        pass

    @staticmethod
    def show_env_dict():
        pass


if __name__ == "__main__":
    ad = AttributeDict()
    ad["a"] = "value of key: a"
    print(ad.a)
    for i in list(range(1, 13)):
        print(DateTime(2017, i, 1).quarter)
