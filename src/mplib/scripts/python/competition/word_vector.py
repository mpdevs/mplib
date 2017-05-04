# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals, absolute_import, print_function, division
from mplib.common import time_elapse
from mplib.IO import PostgreSQL
from io import open
import pandas
import json


@time_elapse
def get_attr_pnv():
    sql = """
    SELECT
      DISTINCT attr_value
    FROM attr_value
    WHERE industry_id = 16
    AND attr_name <> '品牌'
    """
    return list(map(lambda x: x.get("attr_value"), PostgreSQL(env="v2_uat").query(sql)))


@time_elapse
def get_word_vec():
    with open("vectors.txt", encoding="utf8") as f:
        d = [line.replace("\r", "").replace("\n", "").split(" ") for line in f]
    return d


@time_elapse
def process_wv():
    word_vector = get_word_vec()  # d[0]词, d[1]向量
    word = list(map(lambda x: x[0], word_vector))  # 所有词
    print("word_vector length:", len(word))
    attr_value = get_attr_pnv()  # 去重属性值
    print("attr_value length:", len(attr_value))
    d = list(filter(lambda x: x[0] in attr_value, word_vector))  # 在属性值中出现的词向量
    print("word_vector in attr_value length:", len(d))
    o = list(filter(lambda x: x not in word, attr_value))  # 没有在词向量中出现的属性值
    print("attr_value not in word_vector length:", len(o))
    pandas.DataFrame(d, columns=None).to_csv("filtered_vectors.txt", sep=b" ", encoding="utf8", index=False, header=False)
    print("vector exported")
    pandas.DataFrame(o, columns=None).to_csv("oov.txt", sep=b" ", encoding="utf8", index=False, header=False)


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
    # j = read_from_db()
    # print(len(list(j)))
    # print(len(get_attr_pnv()))
    process_wv()

