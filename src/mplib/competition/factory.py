# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import print_function
from __future__ import division
from .helper import *


class Hound(object):
    def __init__(self, category_id):
        self.data = None
        self.word_vectors = get_word_vector()
        self.train_x = None  # word_vec, attr_name distance
        self.train_y = None  # True or False
        self.test_x = None
        self.test_y = None
        self.model_dict = None
        self.essential_dict = get_essential_dict()
        self.important_dict = get_important_dict()
        self.category_id = category_id
        self.tag_dict = tag_to_dict(get_attribute_meta())
        self.data_type = "train"

    def load_data(self):
        if self.data_type == "train":
            self.data = get_train_data(self.category_id)
        else:
            self.data = get_predict_data(self.category_id)

    def preprocess(self):
        self.load_data()
        tag_dict = self.tag_dict.get(self.category_id)
        return construct_train_feature(self.data.values.tolist(), tag_dict)

    def build_features(self):
        self.distance = generate_distance_df(
            self.attr,
            self.dummy,
            self.train,
            self.column_word_vector_dict,
            self.word_vectors, self.word_ids,
            size=self.size,
            is_training_set=True
        )

    def train(self):
        self.train_x, self.train_y, id1, id2 = self.preprocess()

    def predict(self):
        self.data_type = "predict"
        self.test_x, self.test_y, id1, id2 = self.preprocess()

    def observer(self):
        pass

    def run(self):
        self.train()
        # self.predict()
        pass

if __name__ == "__main__":
    Hound(1623).run()
