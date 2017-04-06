# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division


class Hound(object):
    def __init__(self):
        self.data = None
        self.word_vectors = None
        self.train_x = None
        self.train_y = None
        self.test_x = None
        self.test_y = None
        self.model_dict = None

    def load_data(self):
        pass

    def preprocess(self):
        pass

    def build_features(self):
        pass

    def train(self):
        pass

    def predict(self):
        pass

    def observer(self):
        pass

    def run(self):
        pass


if __name__ == "__main__":
    h = Hound()
    h.run()
