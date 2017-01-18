# coding: utf-8
# __author__: "John"
from __future__ import unicode_literals
from mplib.common.settings import PG_CONNECTION
from mplib.common import smart_decode
import psycopg2.extras
import psycopg2.pool
import traceback


class DatabaseSingleton(object):
    def __new__(cls, settings=PG_CONNECTION):
        if not hasattr(cls, "_instance"):
            orig = super(DatabaseSingleton, cls)
            cls._instance = orig.__new__(cls, settings)

            minconn = PG_CONNECTION.get("minconn")
            maxconn = PG_CONNECTION.get("maxconn")

            dsn = "host={host} port={port} dbname={dbname} user={user} password={password}".format(**settings)
            cls._instance.dbpool = psycopg2.pool.SimpleConnectionPool(minconn, maxconn, dsn=dsn)

        return cls._instance

    def __del__(self):
        self.dbpool.closeall()
        object.__del__(self)


class MPPG(DatabaseSingleton):
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.in_transaction = False

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
            self.dbpool.putconn(self.conn, close=True)
            return False
        else:
            self.cursor.close()
            self.dbpool.putconn(self.conn)
            return [smart_decode(dict(r)) for r in rows]

    def begin(self):
        self.in_transaction = True
        self.conn = self.dbpool.getconn()
        self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    def commit(self):
        if self.in_transaction and self.conn:
            try:
                self.conn.commit()
            except:
                self.conn.rollback()
                self.dbpool.putconn(self.conn, close=True)
                traceback.print_exc()
            else:
                self.dbpool.putconn(self.conn)
            finally:
                self.in_transaction = False

    def rollback(self):
        if self.in_transaction and self.conn:
            try:
                self.conn.rollback()
            except:
                traceback.print_exc()
            finally:
                self.dbpool.putconn(self.conn, close=True)
                self.in_transaction = False


if __name__ == "__main__":
    print MPPG().query("SELECT '你好' AS method, 'test' AS name UNION ALL SELECT '你好' AS method, 'test' AS name;", fetchone=True)

