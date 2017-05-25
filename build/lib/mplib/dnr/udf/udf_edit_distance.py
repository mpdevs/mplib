# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals, absolute_import, print_function, division
from mplib.common import smart_decode
import editdistance
import traceback
import sys


reload(sys)
sys.setdefaultencoding("utf8")


try:
    for line in sys.stdin:
        # content1, content2, id1, id2
        line = smart_decode(line).replace("\n", "").replace("\r", "").replace("\\N", "").split("\t")
        distance = str(editdistance.eval(line[0], line[1]))
        print("\t".join(line[2:] + [distance]))

except Exception as e:
    print("\t".join([str(e), '', '', '', traceback.format_exc()]))
