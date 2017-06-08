# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals, absolute_import, print_function, division
from mplib.common.setting import IMPALA_CONNECTION
from mplib.common import smart_decode, smart_encode
from impala import dbapi


def get_env(env):
    return get_env_dict().get(env, IMPALA_CONNECTION)


def get_env_dict():
    return dict(
        local=IMPALA_CONNECTION
    )


class Impala(object):
    def __init__(self, env="local"):
        self.conn = dbapi.connect(**get_env(env))

    def query(self, sql):
        with self.conn.cursor() as cursor:
            cursor.execute(smart_encode(sql))
            columns = smart_decode([_[0] for _ in cursor.description])
            rows = smart_decode([dict(zip(columns, _)) for _ in cursor])
            cursor.close()
            self.conn.close()
            return rows

    def execute(self, sql):
        with self.conn.cursor() as cursor:
            for line in [_.strip() for _ in sql.split(";") if _.strip()]:
                cursor.execute(smart_encode(line))
            cursor.close()
            self.conn.close()
            return True

    @staticmethod
    def get_env_dict():
        return get_env_dict()

    @staticmethod
    def show_env_dict():
        from pprint import pprint
        pprint(get_env_dict())


if __name__ == "__main__":
    # print(Impala().query("SHOW TABLES;"))
    # print(Impala().execute("SHOW TABLES;"))
    print(Impala.get_env_dict())
