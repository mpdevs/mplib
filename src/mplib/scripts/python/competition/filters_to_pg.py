# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals, absolute_import, print_function, division
from collections import OrderedDict
from mplib.IO import PostgreSQL
import pandas
import json


def parse(df):
    od = OrderedDict()
    for row in df.values.tolist():
        if row[0] in list(od):
            if row[1] in list(od[row[0]]):
                od[row[0]][row[1]].append(row[2])
            else:
                od[row[0]][row[1]] = [row[2]]
        else:
            dimension_value = OrderedDict()
            dimension_value[row[1]] = [row[2]]
            od[row[0]] = dimension_value
    return od


def essential_csv_to_db():
    od = parse(pandas.read_table("essential_tag_data.csv", encoding="utf8", sep=","))
    sql = """
    INSERT INTO json_value (value, name)
    VALUES (%s, 'essential_dict')
    """
    PostgreSQL().execute(sql, (json.dumps(od),))


def essential_read_db():
    sql = "SELECT value FROM json_value WHERE name = 'essential_dict'"
    return PostgreSQL().query(sql)[0].get("value")


def important_csv_to_db():
    od = parse(pandas.read_table("important_tag_data.csv", encoding="utf8", sep=","))
    sql = """
    INSERT INTO json_value (value, name)
    VALUES (%s, 'important_dict')
    """
    PostgreSQL().execute(sql, (json.dumps(od),))


def important_read_db():
    sql = "SELECT value FROM json_value WHERE name = 'important_dict'"
    return PostgreSQL().query(sql)[0].get("value")

if __name__ == "__main__":
    d = important_read_db()
    print(type(d), len(d))
    # important_csv_to_db()
