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

    def query(self, sql, dict_cursor=True, fetchone=False, cast=True):
        conn = db_connect(env=self.env)
        cursor = conn.cursor(MySQLdb.cursors.DictCursor) if dict_cursor else conn.cursor()
        cursor.execute(sql)
        try:
            ret = [cursor.fetchone()] if fetchone else list(cursor.fetchall())
        except Exception as e:
            print("error message:{0}".format(e))
            return False
        else:
            if cast:
                return smart_decode(ret)
            else:
                return ret
        finally:
            cursor.close()
            conn.close()

    def execute(self, sql):
        conn = db_connect(env=self.env)
        cursor = conn.cursor()
        try:
            cursor.execute(sql if isinstance(sql, str) else sql.encode("utf8"))
            conn.commit()
        except Exception as e:
            print("error message:{0}".format(e))
            return False
        else:
            return True
        finally:
            cursor.close()
            conn.close()

    def execute_many(self, sql, args):
        conn = db_connect(env=self.env)
        cursor = conn.cursor()
        try:
            cursor.executemany(sql, args)
            conn.commit()
        except Exception as e:
            print("error message:{0}".format(e))
            return False
        else:
            return True
        finally:
            cursor.close()
            conn.close()

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


if __name__ == "__main__":
    print(MPMySQL().query("SELECT now() AS time;", fetchone=True))
    print(MPMySQL().query("SELECT now() AS time;"))
    print(MPMySQL(env="das_api").query("select now() AS time;"))
    MPMySQL.show_env_dict()
