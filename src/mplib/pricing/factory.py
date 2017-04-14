# coding: utf8
# __author__: "John"
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import print_function
from __future__ import division
from sklearn.ensemble import RandomForestRegressor
from treeinterpreter import treeinterpreter as ti
from sklearn.metrics import r2_score
from datetime import datetime
from mplib.pricing.helper import gen_print_var
from mplib.common.helper import save_model_to_pg, load_model_from_pg, load_model_from_pickle, save_model_to_pickle
from mplib.common.helper import print_var_info, exists_model_in_pg, vector_reshape_to_matrix
import numpy
import time


class SKAssess(object):
    def __init__(self):
        self.category_id = 50008899
        self.interval = "201612A"
        self.x_train = None
        self.x_predict = None
        self.y_train = None
        self.y_predict = None
        self.model = None
        self.model_name = None
        self.prediction = None
        self.path = None  # extension for pickle file
        # region Expose to outside
        self.m = None
        self.n = None
        self.y_definition = "SUM(salesamt) / SUM(salesqty)"
        self.train_size = None
        self.predict_size = None
        self.framework_model = "sklearn_randomforest"
        self.train_elapse = None
        self.predict_elapse = None
        self.metric_mae = None
        self.metric_mse = None
        self.metric_r2 = None
        self.metric_e5 = None
        self.metric_e10 = None
        self.metric_e15 = None
        self.metric_e20 = None
        self.run_time = datetime.now().strftime("%Y%m%d%H%M%S")
        self.plot_file_name = None
        # endregion

    def update_info(self):
        self.train_size = len(self.x_train) if self.x_train is not None else 0
        self.predict_size = len(self.x_predict) if self.x_predict is not None else 0
        self.m = self.train_size + self.predict_size
        self.n = self.x_train.shape[1] if self.x_train is not None else self.x_predict.shape[1]

    def train(self):
        self.update_info()
        self.model = RandomForestRegressor()
        start = time.time()
        self.model.fit(self.x_train, self.y_train)
        self.train_elapse = time.time() - start

    def predict(self):
        self.update_info()
        start = time.time()
        self.prediction, bias, contributions = ti.predict(self.model, self.x_predict)
        self.predict_elapse = time.time() - start

        self.prediction = vector_reshape_to_matrix(self.prediction)
        self.metric_mae = numpy.mean(numpy.abs(self.prediction - self.y_predict))
        self.metric_mse = numpy.mean(numpy.square(self.prediction - self.y_predict))
        e = numpy.abs(self.prediction - self.y_predict) / self.y_predict
        l = len(e)
        self.metric_e5 = "{0}%".format(round(len(e[e < 0.05]) / l * 100, 2))
        self.metric_e10 = "{0}%".format(round(len(e[e < 0.10]) / l * 100, 2))
        self.metric_e15 = "{0}%".format(round(len(e[e < 0.15]) / l * 100, 2))
        self.metric_e20 = "{0}%".format(round(len(e[e < 0.20]) / l * 100, 2))
        self.metric_r2 = r2_score(self.y_predict, self.prediction, multioutput="variance_weighted")

    def save_model(self):
        if self.model_name is None:
            self.gen_model_name()

        if self.path is None:
            if not exists_model_in_pg(self.model_name):
                save_model_to_pg(self.model, self.model_name)
        else:
            save_model_to_pickle(self.model, self.model_name, self.path)

    def load_model(self):
        if self.model_name is None:
            self.gen_model_name()

        if self.path is None:
            self.model = load_model_from_pg(self.model_name)
        else:
            load_model_from_pickle(self.model_name, self.path)

    def print_info(self):
        print_var_info(self.__dict__, gen_print_var())

    def gen_model_name(self):
        self.model_name = "{0}_{1}_{2}".format(self.framework_model, self.category_id, self.interval)


if __name__ == "__main__":
    from mplib.pricing.helper import get_items
    a = SKAssess()
    a.x_train, a.x_predict, a.y_train, a.y_predict = get_items(nrows=100, cid=50008899, path=__file__)
    a.path = __file__
    a.train()
    a.save_model()
    a.load_model()
    a.predict()
    a.print_info()
