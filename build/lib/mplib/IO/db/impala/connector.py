# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
from mplib.common.settings import IMPALA_CONNECTION
from mplib.common import smart_decode, smart_encode
from impala import dbapi


class Impala(object):
    def __init__(self):
        self.conn = dbapi.connect(**IMPALA_CONNECTION)

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


if __name__ == "__main__":
    print Impala().query("SHOW TABLES;")
    print Impala().execute("SHOW TABLES;")
