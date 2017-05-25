# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals, absolute_import, print_function, division
from six import iteritems, itervalues, next

"""
http://www.rfk.id.au/blog/entry/preparing-pyenchant-for-python-3/
"""

try:
    unicode = unicode
except NameError:
    str = str
    unicode = str
    bytes = bytes
    basestring = (str, bytes)
else:
    str = str
    unicode = unicode
    bytes = str
    basestring = basestring
