# coding: utf8
# __author__: "John"
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import print_function
from __future__ import division
from mplib.pricing.helper import split_id_feature, get_items
from mplib.pricing.factory import SKAssess


def foo():
    print("bar")


def train_and_predict():
    a = SKAssess()
    a.x_train, a.x_test, a.y_train, a.y_test = get_items(nrows=100, cid=50008899, path=__file__)
    a.train()
    a.path = __file__
    a.save_model()
    a.load_model()
    a.predict()
    a.print_info()


def just_train():
    a = SKAssess()
    a.x_train, a.x_test, a.y_train, a.y_test = get_items(nrows=100, cid=50008899, path=__file__)
    a.train()
    a.path = __file__
    a.save_model()
    a.print_info()


def just_predict():
    a = SKAssess()
    a.x_train, a.x_test, a.y_train, a.y_test = get_items(nrows=100, cid=50008899, path=__file__)
    a.path = __file__
    a.load_model()
    a.predict()
    a.print_info()


if __name__ == "__main__":
    # udf_data = [
    #     ["2333", "1,0,0,1"],
    #     ["233", "1,1,0,1"],
    #     ["23", "1,0,1,1"],
    #     ["6666", "1,1,1,1"],
    #     ["666", "1,0,0,0"],
    #     ["66", "1,0,1,0"],
    # ]
    # item, feature = split_id_feature(udf_data)
    # print(item, feature)
    # train_and_predict()
    # just_train()
    just_predict()
