# coding: utf8
from __future__ import unicode_literals, print_function
import sqlite3


class SQLite(object):
    def __init__(self, file_path, *args, **kwargs):
        self.connection = sqlite3.connect(file_path)
        self.cursor = None
        self.get_cursor()

    def get_cursor(self):
        self.cursor = self.connection.cursor()

    def query(self, sql):
        self.get_cursor()
        self.cursor.execute(sql)
        columns = [description[0] for description in self.cursor.description]
        return [dict(zip(columns, _)) for _ in self.cursor.fetchall()]
    
    def values_list(self, sql):
        self.get_cursor()
        self.cursor.execute(sql)
        return [_[0] for _ in self.cursor.fetchall()]

    def execute(self, sql):
        self.get_cursor()
        self.cursor.execute(sql)
        self.connection.commit()

    def __del__(self):
        self.cursor.close()
        self.connection.close()


if __name__ == "__main__":
    sqlite3_instance = SQLite("~/Users/panjunjun/PycharmProjects/Lab/adhoc/lijie/das_tool/db.sqlite3")
    print(sqlite3_instance.query("SELECT COUNT(1) FROM progress"))
    
