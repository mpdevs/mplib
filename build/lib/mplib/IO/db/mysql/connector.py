# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import print_function
from __future__ import division
from mplib.common.settings import MYSQL_SETTINGS, DAS_PRO_MYSQL_CONNECTION
from mplib.common import smart_decode
import MySQLdb


def get_env(env="das_pro"):
    env_dict = dict(
        das_pro=DAS_PRO_MYSQL_CONNECTION,
        mpportal=MYSQL_SETTINGS,
    )
    return env_dict.get(env, DAS_PRO_MYSQL_CONNECTION)


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


if __name__ == "__main__":
    print(MPMySQL().query("SELECT now() AS time;", fetchone=True))
    print(MPMySQL().query("SELECT now() AS time;"))
