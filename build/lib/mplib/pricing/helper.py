# coding: utf8
# __author__: "John"
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import print_function
from __future__ import division
from os.path import dirname, join, splitext
from sklearn.externals import joblib
from sklearn import model_selection
from datetime import datetime
from os import listdir
import matplotlib.pyplot as plt
import collections
import pandas
import numpy
import pylab


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
    print("filter y >= 100 data shape: ", df.shape)
    df = drop_zero_columns(df)
    print("drop zero data shape: ", df.shape)
    x_train, x_test, y_train, y_test = model_selection.train_test_split(df[:, 1:], df[:, 0], test_size=0.2, random_state=42)
    del df

    prefix = join(dirname(__file__), join("data", "{0}_pricing".format(cid)))
    numpy.savetxt("{0}_x_train.csv".format(prefix), x_train, delimiter=",")
    numpy.savetxt("{0}_x_test.csv".format(prefix), x_test, delimiter=",")
    numpy.savetxt("{0}_y_train.csv".format(prefix), y_train, delimiter=",")
    numpy.savetxt("{0}_y_test.csv".format(prefix), y_test, delimiter=",")
    return x_train, x_test, y_train, y_test


def normalize(data):
    data -= numpy.mean(data)
    data /= numpy.std(data)
    return data


def vector_reshape_to_matrix(v):
    return v.reshape(len(v), 1)


def d(string):
    print("{0} {1}".format(datetime.now(), string))


def gen_print_var():
    var_list = [
        "M",
        "N",
        "y_definition",
        "train_size",
        "test_size",
        "framework_model",
        "train_time",
        "predict_time",
        "MAE",
        "MSE",
        "r2",
        "error_5p",
        "error_10p",
        "error_15p",
        "error_20p",
        "run_time",
        "plot_file_name"
    ]
    return var_list


def get_print_var(locals_var):
    var_list = gen_print_var()
    var_dict = collections.OrderedDict()

    for i in var_list:
        var_dict[i] = None

    for k, v in locals_var.items():
        if k in var_dict.keys():
            var_dict[k] = v

    return var_dict


def print_all_info(locals_var):
    for k, v in get_print_var(locals_var).items():
        d("{0}: {1}".format(k, v))


def plot_scatter(p, y, file_name=None):
    fig, ax = plt.subplots()
    ax.scatter(y, p)
    ax.plot([y.min(), y.max()], [y.min(), y.max()], "k--", lw=3)
    ax.set_xlabel("Measured")
    ax.set_ylabel("Predicted")
    if file_name:
        pylab.savefig(join(join(dirname(__file__), "plots"), file_name))
    else:
        plt.show()


def plot_line(p, y, file_name):
    fig, ax = plt.subplots()
    ax.scatter(y, p)
    ax.plot([y.min(), y.max()], [y.min(), y.max()], "k--", lw=3)
    ax.set_xlabel("Measured")
    ax.set_ylabel("Predicted")
    if file_name:
        pylab.savefig(join(join(dirname(__file__), "plots"), file_name))
    else:
        plt.show()


def save_model(model_obj, file_name, path=None):
    path = path if path else __file__
    joblib.dump(model_obj, "{0}.pkl".format(join(dirname(path), join("models", file_name))))


def load_model(file_name, path=None):
    path = path if path else __file__
    return joblib.load("{0}.pkl".format(join(dirname(path), join("models", file_name))))


def logging_process(locals_var):
    var_dict = get_print_var(locals_var)
    info = ",".join([str(v) for k, v in var_dict.items()])
    with open(get_experiment_logs_path("experiment_logs.csv"), mode="a") as f:
        f.write(info + "\n")


def get_experiment_logs_path(file_name):
    return join(dirname(__file__), join("logs", file_name))


def gen_experiment_logs_head():
    info = ",".join(gen_print_var())
    with open(get_experiment_logs_path("experiment_head.csv"), mode="w") as f:
        f.write(info + "\n")


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
