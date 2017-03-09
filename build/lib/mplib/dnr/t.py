# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
import pandas


class Luna(object):
    def __init__(self, data):
        self.data = data
        self.rl = len(df)
        self.cl = len(df.columns.tolist())

    @staticmethod
    def f(x):
        return ["a", x[1], x[2]] if x[2] == 1 else x


    def run(self):
        self.data = self.data.apply(self.f, axis=1)

if __name__ == "__main__":
    data = [
        dict(a=1, b=2, c=0),
        dict(a=3, b=4, c=1),
        dict(a=5, b=6, c=0),
        dict(a=7, b=8, c=1),
    ]
    df = pandas.DataFrame(data)
    l = Luna(df)
    l.run()
    print l.data
