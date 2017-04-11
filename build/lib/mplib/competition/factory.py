# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from helper import *


class Hound(object):
    def __init__(self):
        self.data = None
        self.word_vectors = get_word_vector()
        self.train_x = None
        self.train_y = None
        self.test_x = None
        self.test_y = None
        self.model_dict = None
        self.essential_dict = get_essential_dict()
        self.important_dict = get_important_dict()
        self.attribute_meta = get_attribute_meta()

    def load_data(self, data):
        self.data = data

    def preprocess(self):
        pass

    def build_features(self):
        pass

    def train(self):
        self.data = get_train_data()
        pass

    def predict(self):
        pass

    def observer(self):
        pass

    def run(self):
        pass


if __name__ == "__main__":
    Hound().run()
