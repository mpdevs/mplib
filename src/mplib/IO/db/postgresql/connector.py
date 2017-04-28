# coding: utf-8
# __author__: "John"
from __future__ import unicode_literals, absolute_import, print_function, division
from mplib.common.setting import PG_CONNECTION, PG_UAT_CONNECTION
from mplib.common import smart_decode
import psycopg2.extras
import psycopg2.pool
import traceback


def get_env(env="pro"):
    env_dict = dict(
        pro=PG_CONNECTION,
        uat=PG_UAT_CONNECTION,
    )
    return env_dict.get(env, PG_CONNECTION)


class MPPG(object):
    def __init__(self, env="pro"):
        self.env = get_env(env)
        self.pool = None
        self.conn = None
        self.cursor = None
        self.in_transaction = False
        self.set_pool()

    def set_pool(self):
        dsn = "host={host} port={port} dbname={dbname} user={user} password={password}".format(**self.env)
        self.pool = psycopg2.pool.SimpleConnectionPool(self.env.get("minconn"), self.env.get("maxconn"), dsn=dsn)

    def execute(self, operation, parameters=None):
        try:
            if not self.in_transaction:
                self.begin()

            self.cursor.execute(operation, parameters) if parameters else self.cursor.execute(operation)
            self.commit()
            self.cursor.close()

        except:
            traceback.print_exc()
            self.cursor.close()
            self.rollback()
            return False

        else:
            return True

    def executemany(self, operation, parameters=None):
        try:
            if not self.in_transaction:
                self.begin()

            self.cursor.executemany(operation, parameters) if parameters else self.cursor.executemany(operation)
            self.commit()

        except:
            traceback.print_exc()
            self.cursor.close()
            self.rollback()
            return False
        else:
            self.cursor.close()
            return True

    def query(self, operation, parameters=None, fetchone=False):
        if not self.in_transaction:
            self.begin()
        try:
            self.cursor.execute(operation, parameters) if parameters else self.cursor.execute(operation)
            rows = [self.cursor.fetchone()] if fetchone else self.cursor.fetchall()

        except:
            traceback.print_exc()
            self.cursor.close()
            self.pool.putconn(self.conn, close=True)
            return False
        else:
            self.cursor.close()
            self.pool.putconn(self.conn)
            return [smart_decode(dict(r)) for r in rows]

    def begin(self):
        self.in_transaction = True
        self.conn = self.pool.getconn()
        self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def commit(self):
        if self.in_transaction and self.conn:
            try:
                self.conn.commit()
            except:
                self.conn.rollback()
                self.pool.putconn(self.conn, close=True)
                traceback.print_exc()
            else:
                self.pool.putconn(self.conn)
            finally:
                self.in_transaction = False

    def rollback(self):
        if self.in_transaction and self.conn:
            try:
                self.conn.rollback()
            except:
                traceback.print_exc()
            finally:
                self.pool.putconn(self.conn, close=True)
                self.in_transaction = False


if __name__ == "__main__":
    print(MPPG().query(
        "SELECT '你好' AS method, 'test' AS name UNION ALL SELECT '你好' AS method, 'test' AS name;", fetchone=True))
    print(MPPG(env="uat").query("select now();"))
