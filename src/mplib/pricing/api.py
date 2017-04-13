# coding: utf8
# __author__: "John"
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import print_function
from __future__ import division
from mplib.pricing.helper import get_items, split_id_feature
from mplib.pricing.factory import SKAssess
from mplib.common import time_elapse


@time_elapse
def text_file_train_and_predict():
    a = SKAssess()
    a.x_train, a.x_predict, a.y_train, a.y_predict = get_items(nrows=100, cid=50008899, path=__file__)
    a.train()
    a.path = __file__
    a.save_model()
    a.load_model()
    a.predict()
    a.print_info()


@time_elapse
def text_file_train():
    import pickle
    a = SKAssess()
    a.x_train, a.x_predict, a.y_train, a.y_predict = get_items(nrows=100, cid=50008899, path=__file__)
    a.train()
    a.path = __file__
    print(pickle.dumps(a.model))
    # a.save_model()
    # a.print_info()


@time_elapse
def text_file_predict():
    a = SKAssess()
    a.x_train, a.x_predict, a.y_train, a.y_predict = get_items(nrows=100, cid=50008899, path=__file__)
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
    # data = (zip(item, feature.astype("unicode").tolist()))
    # for line in data:
    #     print("\t".join([line[0], ",".join(line[1])]))
    # text_file_train_and_predict()
    text_file_train()
    # text_file_predict()
