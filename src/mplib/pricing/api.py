# coding: utf8
# __author__: "John"
from __future__ import unicode_literals, absolute_import, print_function, division
from mplib.pricing.helper import get_items, get_hive_train_data, execute_hive_predict_data
from mplib.pricing.factory import SKAssess
from mplib.common import time_elapse


@time_elapse
def text_file_train_and_predict(data_dir=__file__, models_dir=__file__):
    a = SKAssess()
    a.x_train, a.x_predict, a.y_train, a.y_predict = get_items(nrows=100, cid=50008899, path=data_dir)
    a.train()
    a.path = models_dir
    a.save_model()
    a.load_model()
    a.predict()
    a.print_info()


@time_elapse
def text_file_train(data_dir=__file__, models_dir=__file__):
    a = SKAssess()
    a.x_train, a.x_predict, a.y_train, a.y_predict = get_items(nrows=100, cid=50008899, path=data_dir)
    a.train()
    a.path = models_dir
    a.save_model()
    a.print_info()


@time_elapse
def text_file_predict(data_dir=__file__, models_dir=__file__):
    a = SKAssess()
    a.x_train, a.x_predict, a.y_train, a.y_predict = get_items(nrows=100, cid=50008899, path=data_dir)
    a.path = models_dir
    a.load_model()
    a.predict()
    a.print_info()


@time_elapse
def hive_data_train(category_id, is_event=False, show_log=False):
    from copy import deepcopy
    a = SKAssess()
    a.category_id, a.interval = category_id, "event" if is_event else "daily"
    a.x_train, a.y_train = get_hive_train_data(category_id)
    a.x_predict, a.y_predict = deepcopy(a.x_train), deepcopy(a.y_train)
    a.train()
    a.save_model()
    if show_log:
        a.predict()
        a.print_info()


@time_elapse
def hive_data_predict(category_id, is_event=False):
    execute_hive_predict_data(category_id, is_event)


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
    # text_file_train()
    # text_file_predict()
    hive_data_train(162103)

