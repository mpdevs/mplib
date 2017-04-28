# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals, absolute_import, print_function, division
from mplib.competition.helper import gen_print_var, get_word_vector, get_essential_dict, get_important_dict
from mplib.competition.helper import get_attribute_meta, attributes_to_dict, gen_model_dict, do_dimension_trick
from mplib.competition.helper import make_similarity_feature, get_dummy_head, get_distance
from mplib.common.helper import print_var_info, save_model_to_pickle, load_model_from_pickle, vector_reshape_to_matrix
from mplib.common.helper import save_model_to_pg, load_model_from_pg, exists_model_in_pg
from sklearn.ensemble import VotingClassifier
from sklearn.metrics import accuracy_score
from six import iteritems, itervalues, next
from functools import partial
from datetime import datetime
import time


class Hound(object):
    def __init__(self):
        self.category_id = "1623"
        self.interval = "201612"
        self.x_train = None  # word_vec, attr_name distance mixin
        self.y_train = None  # True or False
        self.x_predict = None
        self.y_predict = None
        self.model = None
        self.model_name = None
        self.prediction = None
        self.path = None  # extension for pickle file
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
        self.model = VotingClassifier(estimators=[tuple([k, v]) for k, v in iteritems(gen_model_dict())])
        self.model.fit(self.x_train, self.y_train)
        self.train_elapse = time.time() - start

    def predict(self):
        self.update_info()
        start = time.time()
        self.prediction = vector_reshape_to_matrix(self.model.predict(self.x_predict))
        self.predict_elapse = time.time() - start
        self.metric_accuracy = accuracy_score(vector_reshape_to_matrix(self.y_predict), self.prediction)

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
            self.model = load_model_from_pickle(self.model_name, self.path)

    def print_info(self):
        print_var_info(self.__dict__, gen_print_var())

    def gen_model_name(self):
        self.model_name = "{0}_{1}_{2}".format(self.framework_model, self.category_id, self.interval)


class GoldMiner(object):
    def __init__(self):
        self.category_id = "1623"
        self.data = None

    def pan(self):
        """
        维度法筛选
        :return:
        """
        essential_dict = get_essential_dict().get(self.category_id)
        important_dict = get_important_dict().get(self.category_id)
        self.data = list(filter(partial(do_dimension_trick, important_dict, essential_dict), self.data))

    def refine(self):
        """
        维度字典构建相似度向量
        :return:
        """
        tag_dict = attributes_to_dict(get_attribute_meta()).get(self.category_id)
        self.data = list(map(partial(make_similarity_feature, tag_dict), self.data))

    def mold(self):
        """
        用词向量、标签、相似度、距离公式生成最终特征
        :return:
        """
        dummy_head = get_dummy_head()
        word_vector = get_word_vector()
        size = next(itervalues(word_vector)).shape[0]
        self.data = list(map(partial(get_distance, dummy_head, word_vector, size), self.data))

    def smelt(self):
        """
        UDF stdout
        :return:
        """
        for line in self.data:
            print("\t".join(line))

if __name__ == "__main__":
    h = Hound()
    # h.x_train, h.x_predict, h.y_train, h.y_predict = what?
    # h.path = __file__
    # h.train()
    # h.save_model()
    # h.load_model()
    # h.predict()
    h.print_info()
    # gm = GoldMiner()
    # gm.data = get_data
    # gm.pan().smelt()
