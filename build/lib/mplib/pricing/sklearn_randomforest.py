# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
from __future__ import print_function
from sklearn.ensemble import RandomForestRegressor
from treeinterpreter import treeinterpreter as ti
from sklearn.metrics import r2_score
from helper import *
import time
import os


def run(nrows=10000, cid=50008899):
    run_time = datetime.now()

    rf = RandomForestRegressor()

    x_train, x_test, y_train, y_test = get_items(nrows=nrows, cid=cid)

    y_test = y_approximation(y_test)
    y_train = y_approximation(y_train)

    train_start = time.time()
    rf.fit(x_train, y_train)
    train_time = time.time() - train_start

    predict_start = time.time()
    prediction, bias, contributions = ti.predict(rf, x_test)
    predict_time = time.time() - predict_start

    prediction = vector_reshape_to_matrix(prediction)
    MAE = numpy.mean(numpy.abs(prediction - y_test))
    MSE = numpy.mean(numpy.square(prediction - y_test))

    e = numpy.abs(prediction - y_test) / y_test
    l = len(e)

    error_5p = "{0}%".format(round(len(e[e < 0.05]) / l * 100, 2))
    error_10p = "{0}%".format(round(len(e[e < 0.10]) / l * 100, 2))
    error_15p = "{0}%".format(round(len(e[e < 0.15]) / l * 100, 2))
    error_20p = "{0}%".format(round(len(e[e < 0.20]) / l * 100, 2))

    train_size = len(x_train)
    test_size = len(x_test)

    M = train_size + test_size
    N = x_train.shape[1]

    y_definition = "SUM(salesamt) / SUM(salesqty)"

    framework_model = get_file_name(os.path.basename(__file__))
    plot_file_name = "{0}_{1}_{2}.png".format(framework_model, cid, run_time.strftime("%Y%m%d%H%M%S"))

    r2 = r2_score(y_test, prediction, multioutput="variance_weighted")
    save_model(rf, framework_model)
    logging_process(locals())
    print_all_info(locals())
    plot_scatter(prediction, y_test, plot_file_name)


def predict(nrows, cid):
    x_train, x_test, y_train, y_test = get_items(nrows=nrows, cid=cid)
    rf = load_model(get_file_name(os.path.basename(__file__)))

    y_test = y_approximation(y_test)
    y_train = y_approximation(y_train)

    prediction, bias, contributions = ti.predict(rf, x_test)
    prediction = vector_reshape_to_matrix(prediction)
    e = numpy.abs(prediction - y_test) / y_test
    l = len(e)
    error_5p = "{0}%".format(round(len(e[e < 0.05]) / l * 100, 2))
    error_10p = "{0}%".format(round(len(e[e < 0.10]) / l * 100, 2))
    error_15p = "{0}%".format(round(len(e[e < 0.15]) / l * 100, 2))
    error_20p = "{0}%".format(round(len(e[e < 0.20]) / l * 100, 2))
    r2 = r2_score(y_test, prediction, multioutput="variance_weighted")
    print_all_info(locals())


if __name__ == "__main__":
    run(nrows=71244, cid=50008899)
    # predict(nrows=80164, cid=50008899)
    # run(nrows=100, cid=50008899)
    # pass
