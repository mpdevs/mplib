# coding: utf-8
# __author__: u"John"
"""
This module contains global decorators
"""
from __future__ import unicode_literals
import time


def time_elapse(function):
    def wrapper(*args, **kwargs):
        start = time.time()
        ret = function(*args, **kwargs)
        print "function '{0}' elapse {1} seconds".format(function.__name__, time.time() - start)
        return ret
    return wrapper


def clock_elapse(function):
    def wrapper(*args, **kwargs):
        start = time.clock()
        ret = function(*args, **kwargs)
        print "function '{0}' elapse {1} clock seconds".format(function.__name__, time.clock() - start)
        return ret
    return wrapper
