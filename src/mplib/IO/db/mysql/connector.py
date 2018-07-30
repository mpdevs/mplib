# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals, absolute_import, print_function, division
from mplib.common.setting import *
from mplib.common import smart_decode
from mplib import *
import MySQLdb


def get_env(env):
    return get_env_dict().get(env, DAS_PRO_MYSQL_CONNECTION)


def get_env_dict():
    return dict(
        das_pro=DAS_PRO_MYSQL_CONNECTION,
        lj_test=LJ_TEST_MYSQL_CONNECTION,
        mpportal=MYSQL_SETTINGS,
        local_hive=LOCAL_HIVE_MYSQL_CONNECTION,
        das_api=DAS_API_MYSQL_CONNECTION,
        das_api_dev=DAS_API_DEV_MYSQL_CONNECTION,
    )


def db_connect(env="das_pro"):
    return MySQLdb.Connect(**get_env(env))


def db_cursor(env="das_pro"):
    return MySQLdb.Connect(**get_env(env)).cursor()


class MPMySQL(object):

    def __init__(self, env="das_pro"):
        self.env = env
        self.conn = db_connect(env=self.env)
        self.cursor = None

    def query(self, sql, dict_cursor=True, fetchone=False, cast=True):
        self.cursor = self.conn.cursor(MySQLdb.cursors.DictCursor) if dict_cursor else conn.cursor()
        self.cursor.execute(sql)
        ret = [self.cursor.fetchone()] if fetchone else list(self.cursor.fetchall())
        if cast:
            return smart_decode(ret)
        else:
            return ret

    def execute(self, sql):
        self.cursor = self.conn.cursor()
        self.cursor.execute(sql if isinstance(sql, str) else sql.encode("utf8"))
        self.conn.commit()

    def execute_many(self, sql, args):
        self.cursor = self.conn.cursor()
        self.cursor.executemany(sql, args)
        self.conn.commit()

    @staticmethod
    def get_connect(self):
        return db_connect(env=self.env)

    @staticmethod
    def get_env_dict():
        return get_env_dict()

    @staticmethod
    def show_env_dict():
        from pprint import pprint
        pprint(get_env_dict())

    def __del__(self):
        self.conn.close()


if __name__ == "__main__":
    print(MPMySQL().query("SELECT now() AS time;", fetchone=True))
    print(MPMySQL().query("SELECT now() AS time;"))
    print(MPMySQL(env="das_api").query("select now() AS time;"))
    MPMySQL.show_env_dict()
