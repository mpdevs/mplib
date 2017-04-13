# coding: utf8
# __author__: "John"
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import print_function
from __future__ import division
from mplib.IO import PostgreSQL, pickle_load
from os.path import dirname, join
import pickle


def get_bytes_value():
    return PostgreSQL().query("SELECT value FROM bytes_value WHERE name = 'test'")[0].get("value")


def save_pickle_to_pg(category_id):
    path = join(join(join(dirname(dirname(dirname(__file__))), "pricing"), "models"), "sklearn_randomforest")
    print(path)
    d = pickle.dumps(pickle_load(path, "pkl"))
    sql = """
    INSERT INTO bytes_value
    (id, name, value)
    VALUES
    (2, 'pricing_sklearn_randomforest_{category_id}', %s)
    """.format(category_id=category_id)
    PostgreSQL().execute(sql, (d,))


if __name__ == "__main__":
    # print(pickle.loads(get_bytes_value()))
    save_pickle_to_pg(50008899)
