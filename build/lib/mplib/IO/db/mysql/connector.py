# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
from mplib.common.settings import *
from mplib.common import smart_decode
import MySQLdb


def get_settings(env):
    env_dict = dict(
        das_pro=DAS_MYSQL_CONNECTION,
        mpportal=MYSQL_SETTINGS,
    )
    return env_dict.get(env)


def db_connect(env="das_pro"):
    connection = MySQLdb.Connect(**get_settings(env))
    return connection


def db_cursor(env="das_pro"):
    cursor = MySQLdb.Connect(**get_settings(env)).cursor()
    return cursor


class MPMySQL(object):

    def __init__(self, env="das_pro"):
        self.settings = get_settings(env)

    def get_settings(self):
        env_dict = dict(
            das=DAS_MYSQL_CONNECTION,
            mpportal=MYSQL_SETTINGS,
        )
        self.settings = env_dict.get(self.env)

    def query(self, sql, dict_cursor=True, fetchone=False):
        conn = MySQLdb.connect(**self.settings)
        cursor = conn.cursor(MySQLdb.cursors.DictCursor) if dict_cursor else conn.cursor()
        cursor.execute(sql)
        try:
            ret = [cursor.fetchone()] if fetchone else list(cursor.fetchall())
        except Exception as e:
            print "error message:{0}".format(e)
            return False
        else:
            return [smart_decode(r) for r in ret]
        finally:
            cursor.close()
            conn.close()

    def execute(self, sql):
        conn = MySQLdb.connect(**self.settings)
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
            conn.commit()
        except Exception as e:
            print "error message:{0}".format(e)
            return False
        else:
            return True
        finally:
            cursor.close()
            conn.close()

    def execute_many(self, sql, args):
        conn = MySQLdb.connect(**self.settings)
        cursor = conn.cursor()
        try:
            cursor.executemany(sql, args)
            conn.commit()
        except Exception as e:
            print "error message:{0}".format(e)
            return False
        else:
            return True
        finally:
            cursor.close()
            conn.close()


if __name__ == "__main__":
    print MPMySQL().query("SELECT now() AS time;", fetchone=True)
    print MPMySQL().query("SELECT now() AS time;")
