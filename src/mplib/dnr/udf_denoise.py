# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
from mplib.dnr.factory import DataDenoiser
from datetime import datetime
import traceback
import pandas
import sys
import csv


try:
    reader = pandas.read_table(sys.stdin, header=None, sep='\t', quoting=csv.QUOTE_NONE, encoding='utf-8', index_col=None, iterator=True)
    while True:
        try:
            chunk = reader.get_chunk(2000)
            dd = DataDenoiser(chunk)
            dd.use_keywords = True
            dd.use_tag = False
            dd.use_length = True
            dd.use_series = False
            dd.use_client = False
            dd.use_special_characters = False
            dd.use_edit_distance = False
            dd.run()
        except StopIteration:
            break

except Exception as e:
    with open('/tmp/udf_denoise.log', 'a') as f:
        f.write('{0}: {1}'.format(datetime.now(), traceback.format_exc()))

