# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import print_function
from __future__ import division
from mplib.IO import PostgreSQL
from io import open
import pandas
import json


def get_attr_pnv():
    db = PostgreSQL()
    sql = """
    SELECT
      attr_name, attr_value
    FROM attr_value
    WHERE industry_id = 16
    AND attr_name <> '品牌'
    """
    ret = db.query(sql)
    s = list()
    for line in ret:
        s.append(line.get("attr_name"))
        for attr_value in line.get("attr_value").split(","):
            s.append(attr_value)
    s = list(set(s))
    print("get_attr_pnv.s:", len(s))
    return s


def get_word_vec():
    d = []
    with open("vectors.txt", encoding="utf8") as f:
        for line in f:
            d.append(line.replace("\r", "").replace("\n", "").split(" "))
    print("get_word_vec.d:", len(d))
    return d


def process_wv():
    d = get_word_vec()
    s = get_attr_pnv()
    d = filter(lambda x: x[0] in s, d)
    print("process_wv.d:", len(d))
    pandas.DataFrame(d, columns=None).to_csv("filtered_vectors.txt", sep=b" ", encoding="utf8", index=False, header=False)


def to_csv():
    df = pandas.read_table("filtered_vectors.txt", sep=b" ", encoding="utf8")
    df.to_csv("filtered_vectors.csv", encoding="utf8", index=False, header=False)


def to_json():
    d = dict()
    df = pandas.read_csv("filtered_vectors.csv", encoding="utf8")
    for line in df.values.tolist():
        d[line[0]] = line[1:]
    with open("vectors.json", mode="w") as f:
        json.dump(d, f, encoding="utf8")


def to_db():
    with open("vectors.json", encoding="utf8") as f:
        d = json.load(f, encoding="utf8")

    d = json.dumps(d)
    db = PostgreSQL()
    sql = """
    INSERT INTO json_value
    (value, name)
    VALUES
    (%s, 'word_vector')
    """
    db.execute(sql, (d,))


def read_from_db():
    db = PostgreSQL()
    sql = "SELECT value FROM json_value WHERE name = 'word_vector'"
    ret = db.query(sql, fetchone=True)[0].get("value")
    return ret


if __name__ == "__main__":
    j = read_from_db()
    print(len(list(j)))
