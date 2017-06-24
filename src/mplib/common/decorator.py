# coding: utf-8
# __author__: u"John"
"""
This module contains global decorators
"""
from __future__ import unicode_literals, absolute_import, print_function, division
from datetime import datetime
import time


def time_elapse(function):
    def wrapper(*args, **kwargs):
        start = time.time()
        print("{0} executing '{1}'".format(datetime.now(), function.__name__))
        ret = function(*args, **kwargs)
        print("function '{0}' elapse {1} seconds".format(function.__name__, time.time() - start))
        return ret
    return wrapper


def clock_elapse(function):
    def wrapper(*args, **kwargs):
        start = time.clock()
        print("{0} executing '{1}'".format(datetime.now(), function.__name__))
        ret = function(*args, **kwargs)
        print("function '{0}' elapse {1} clock seconds".format(function.__name__, time.clock() - start))
        return ret
    return wrapper
