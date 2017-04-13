# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import print_function
from __future__ import division
from mplib.common.helper import print_var_info
from mplib.competition.helper import *
from datetime import datetime
import time


class Hound(object):
    def __init__(self):
        self.category_id = 1623
        self.interval = "201612"
        self.x_train = None  # word_vec, attr_name distance mixin
        self.y_train = None  # True or False
        self.x_predict = None
        self.y_predict = None
        self.model = None
        self.model_name = None
        self.prediction = None
        self.path = None  # extension for pickle file
        self.word_vectors = get_word_vector()
        self.essential_dict = get_essential_dict()
        self.important_dict = get_important_dict()
        self.tag_dict = tag_to_dict(get_attribute_meta())
        # region Expose to outside
        self.m = None
        self.n = None
        self.y_definition = "Similarity(item1, item2)"
        self.train_size = None
        self.predict_size = None
        self.framework_model = "sklearn_ensemble"
        self.train_elapse = None
        self.predict_elapse = None
        self.metric_accuracy = None
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
        start = time.time()
        self.model.fit(self.x_train, self.y_train)
        self.train_elapse = time.time() - start

    def predict(self):
        pass

    def save_model(self):
        pass

    def load_model(self):
        pass

    def print_info(self):
        print_var_info(self.__dict__, gen_print_var())

    def gen_model_name(self):
        self.model_name = "{0}_{1}_{2}".format(self.framework_model, self.category_id, self.interval)

if __name__ == "__main__":
    Hound().print_info()

