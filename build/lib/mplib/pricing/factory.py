# coding: utf8
# __author__: "John"
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import print_function
from __future__ import division
from sklearn.ensemble import RandomForestRegressor
from treeinterpreter import treeinterpreter as ti
from sklearn.metrics import r2_score
from collections import OrderedDict
from datetime import datetime
from six import iteritems

from mplib.pricing.helper import vector_reshape_to_matrix, save_model, load_model
from mplib.IO import PostgreSQL

import pickle
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
        self.info_list = self.gen_info_list()
        self.info_detail = None
        self.model = None
        self.model_name = None
        self.prediction = None
        self.path = None  # extension for pickle file
        # region Expose to outside
        self.m = None
        self.n = None
        self.y_definition = "SUM(salesamt) / SUM(salesqty)"
        self.train_size = None
        self.test_size = None
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

    @staticmethod
    def gen_info_list():
        return [
            "m",
            "n",
            "y_definition",
            "train_size",
            "test_size",
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

    def process_info(self):
        self.info_detail = OrderedDict()
        for i in self.info_list:
            self.info_detail[i] = None

        for k, v in iteritems(self.__dict__):
            if k in self.info_list:
                self.info_detail[k] = v

    def update_info(self):
        self.train_size = len(self.x_train)
        self.test_size = len(self.x_predict)
        self.m = self.train_size + self.test_size
        self.n = self.x_train.shape[1]

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
        if self.model_name is None: self.gen_model_name()

        if self.path is None:
            if not self.exists_model():
                bytes_obj = pickle.dumps(self.model)
                sql = """
                INSERT INTO text_value
                (name, value)
                VALUES
                ('{model_name}', %s)
                """.format(model_name=self.model_name)
                PostgreSQL().execute(sql, (bytes_obj,))
        else:
            save_model(self.model, self.model_name, self.path)

    def load_model(self):
        if self.model_name is None: self.gen_model_name()

        if self.path is None:
            sql = """
            SELECT value
            FROM text_value
            WHERE name = '{model_name}'
            """.format(model_name=self.model_name)
            self.model = pickle.loads(PostgreSQL().query(sql)[0].get("value"))
        else:
            load_model(self.model_name, self.path)

    def print_info(self):
        self.process_info()
        for k, v in iteritems(self.info_detail):
            print("{0}: {1}".format(k, v))

    def gen_model_name(self):
        self.model_name = "sklearn_randomforest_{0}_{1}".format(self.category_id, self.interval)

    def exists_model(self):
        sql = """
        SELECT 1
        FROM text_value
        WHERE name = '{model_name}'
        """.format(model_name=self.model_name)
        ret = PostgreSQL().query(sql)
        return True if ret else False

if __name__ == "__main__":
    from mplib.pricing.helper import get_items
    a = SKAssess()
    a.x_train, a.x_predict, a.y_train, a.y_predict = get_items(nrows=100, cid=50008899, path=__file__)
    a.train()
    a.save_model()
    a.load_model()
    a.predict()
    a.print_info()
