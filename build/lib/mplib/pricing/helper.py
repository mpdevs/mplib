# coding: utf8
# __author__: "John"
from __future__ import unicode_literals, absolute_import, print_function, division
from mplib.common.helper import get_print_var
from mplib.common import smart_encode
from os.path import dirname, join, splitext
from os import listdir
from sklearn import model_selection
from mplib import *
import pandas
import numpy


def split_id_feature(data):
    item_list = []
    feature_list = []
    for line in data:
        item_list.append(line[0])
        feature_list.append(numpy.array(line[1].split(",")).astype(float))
    return item_list, numpy.array(feature_list)


def split_x_y(data):
    return data[:, 1:], data[:, 0]


def get_file_name(file_name):
    return splitext(file_name)[0]


def get_items(nrows=1000, cid=50008899, path=None):
    if train_test_splited(exists_source(cid, path)):
        return get_train_test(nrows, cid, path)
    else:
        return split_train_test(nrows, cid, path)


def exists_source(cid, path):
    path = path if path else __file__
    sources = list(filter(lambda x: "{0}_pricing.csv".format(cid), listdir(join(dirname(path), "data"))))
    if sources:
        return sources
    else:
        raise IOError("File {0}_pricing.csv does not exist".format(cid))


def train_test_splited(sources):
    train = list(filter(lambda x: "train" in x, sources))
    test = list(filter(lambda x: "test" in x, sources))
    if train and test:
        return True
    else:
        return False


def get_train_test(nrows, cid, path=None):
    path = path if path else __file__
    prefix = join(dirname(path), join("data", "{0}_pricing".format(cid)))
    x_train = pandas.read_table("{0}_x_train.csv".format(prefix), sep=",", nrows=nrows).values
    x_test = pandas.read_table("{0}_x_test.csv".format(prefix), sep=",", nrows=nrows).values
    y_train = pandas.read_table("{0}_y_train.csv".format(prefix), sep=",", nrows=nrows).values
    y_test = pandas.read_table("{0}_y_test.csv".format(prefix), sep=",", nrows=nrows).values

    train_size = int(nrows * 0.8)
    test_size = int(nrows * 0.2)

    return x_train[:train_size], x_test[:test_size], y_train[:train_size], y_test[:test_size]


def split_train_test(nrows, cid, path=None):
    path = path if path else __file__
    df = pandas.read_table(
        filepath_or_buffer=join(dirname(path), join("data", "{0}_pricing.csv".format(cid))),
        sep=",",
        nrows=nrows,
        header=None,
        error_bad_lines=False
    ).fillna(0)
    df = df[df[0] >= 100].values
    x_train, x_test, y_train, y_test = model_selection.train_test_split(
        df[:, 1:], df[:, 0], test_size=0.2, random_state=42)
    del df

    prefix = join(dirname(path), join("data", "{0}_pricing".format(cid)))
    numpy.savetxt("{0}_x_train.csv".format(prefix), x_train, delimiter=b",")
    numpy.savetxt("{0}_x_test.csv".format(prefix), x_test, delimiter=b",")
    numpy.savetxt("{0}_y_train.csv".format(prefix), y_train, delimiter=b",")
    numpy.savetxt("{0}_y_test.csv".format(prefix), y_test, delimiter=b",")
    return x_train, x_test, y_train, y_test


def gen_print_var():
    return [
        "m",
        "n",
        "y_definition",
        "train_size",
        "predict_size",
        "framework_model",
        "train_elapse",
        "predict_elapse",
        "metric_mae",
        "metric_mse",
        "metric_r2",
        "metric_e5",
        "metric_e10",
        "metric_e15",
        "metric_e20",
        "run_time",
    ]


def logging_process(locals_var):
    var_dict = get_print_var(locals_var, gen_print_var())
    info = ",".join([str(v) for k, v in iteritems(var_dict)])
    with open(get_experiment_logs_path("experiment_logs.csv"), mode="a") as f:
        f.write(smart_encode(info + "\n"))


def get_experiment_logs_path(file_name):
    return join(dirname(__file__), join("logs", file_name))


def gen_experiment_logs_head():
    info = ",".join(gen_print_var())
    with open(get_experiment_logs_path("experiment_head.csv"), mode="w") as f:
        f.write(smart_encode(info + "\n"))


def gen_experiment_logs_table():
    with open(get_experiment_logs_path("experiment_head.csv")) as fh:
        head = fh.readline().replace("\n", "").replace("\r", "").split(",")
    with open(get_experiment_logs_path("experiment_logs.csv")) as fl:
        logs = [line.replace("\n", "").replace("\r", "").split(",") for line in fl]
    df = pandas.DataFrame(data=logs, columns=head)
    df.to_excel(get_experiment_logs_path("experiment_table.xls"), encoding="utf8", index=False)


def drop_exp_columns(array, feature_number=7, pow_number=5):
    for f in range(feature_number):
        for p in range(1, pow_number):
            array = numpy.delete(array, f + 1, 1)
    return array


def drop_zero_columns(array):
    return array[:, (array != 0).sum(axis=0) >= 1]


def y_approximation(vector):
    return 10 * numpy.ceil(vector / 10)


if __name__ == "__main__":
    print(__file__)
