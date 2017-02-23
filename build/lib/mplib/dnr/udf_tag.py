# coding: utf-8
# __author__: 'Rich'
from __future__ import unicode_literals
import sys, re
import json
import pandas as pd
import traceback
import MySQLdb
import datetime
import csv
import gc

reload(sys)
sys.setdefaultencoding("utf-8")

class Tagging:
    def __init__(self, tags, dimensions):
        #运算符
        self.tags = json.loads(tags)
        self.dimensions = json.loads(dimensions)

        self.opts = {
            '[&]': '&',
            '[|]': '|',
            '[-]': '-',
            '(': '(',
            ')': ')'
        }

    def __opsplit(self, exp):
        for k, v in self.opts.iteritems():
            exp = exp.replace(k, '\t{0}\t'.format(v))
        return [_ for _ in exp.split('\t') if _]

    def __match(self, pattern, x):
        try:
            return 1 if re.search(pattern, x) else 0
        except:
            return 0

    def __get_columns(self, x):
        for tag in self.dimensions.keys():
            if x[tag] > 0:
                print '\t'.join([x['id'], tag])

    def get(self, df):
        for tag, keywords in self.tags.iteritems():
            df[tag] = df.content.apply(lambda x: self.__match(keywords, x))

        for name, rule in self.dimensions.iteritems():
            if name in self.tags.keys():
                continue

            opt_lst = [_ for _ in self.__opsplit(rule)]
            for i in range(len(opt_lst)):
                if opt_lst[i] in self.opts.values():
                    continue

                #如果带*号则需要处理
                if '*' in opt_lst[i]:
                    columns = [_ for _ in self.tags.keys() if _.startswith(opt_lst[i].replace('*', ''))]
                    opt_lst[i] = 'df.loc[:, ["{0}"]].any(axis=1).apply(lambda x: int(x))'.format('","'.join(columns))
                else:
                    opt_lst[i] = 'df["{0}"]'.format(opt_lst[i])

            df[name] = eval(''.join(opt_lst))

        df.apply(lambda x: self.__get_columns(x), axis=1)

        #释放内存
        del df
        gc.collect()

connection_name = {
    'db': 'das',
    'user': 'das',
    'passwd': '123',
    'host': '192.168.110.121',
    'port': 3306,
    'charset': 'utf8'
}

def search(rules_name):
    dbs = MySQLdb.connect(**connection_name)
    cursor = dbs.cursor()
    cursor.execute("select tags, dimensions, objs from rules where name='{0}'".format(rules_name))
    columns = [_[0].lower() for _ in cursor.description]
    rows = [dict(zip(columns, _)) for _ in cursor]
    cursor.close()
    return rows[0] if len(rows) > 0 else None

if __name__ == '__main__':
    try:
        data = search(sys.argv[1])
        tagging = Tagging(data['tags'], data['dimensions'])

        reader = pd.read_table(sys.stdin, header=None, sep='\t', quoting=csv.QUOTE_NONE, encoding='utf-8', index_col=None, iterator=True)
        while True:
            try:
                chunk = reader.get_chunk(100000)
                chunk.columns = ['id', 'content']
                tagging.get(chunk)
            except StopIteration:
                break

    except Exception as e:
        with open('/tmp/tagging_udf.log', 'a') as f:
            f.write('{0}: {1}'.format(datetime.datetime.now(), traceback.format_exc()))
