# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import AdaBoostClassifier
from sklearn.naive_bayes import GaussianNB
from mplib.common import time_elapse
from sklearn.svm import SVC
from db_api import *
from helper import *
# import xgboost
import pandas
import numpy


class CalculateCompetitiveItems(object):
    # region 内部所有属性的初始化
    # 舊的 source table: "itemmonthlysales2015"
    @time_elapse
    def __init__(
            self,
            industry="mp_women_clothing",
            source_table="itemmonthlysales2015",
            target_table="itemmonthlyrelation_2015",
            shop_id=None,
            cid=None,
            use_essential_tag_dict=True,
            use_important_tag_dict=True
    ):
        # region 必要
        self.industry = industry
        self.source_table = source_table
        self.target_table = target_table
        self.customer_shop_id = shop_id
        self.date_range = get_max_date_range(db=self.industry, table=self.source_table)
        self.date_range_list = None
        self.category_dict = {int(row[0]): row[1] for row in get_categories(db=self.industry)}
        self.category_id = cid
        # 用来将商品标签向量化
        self.attribute_meta = get_attribute_meta(db=self.industry)
        self.feature_type = "Jaccard"
        # key=CategoryID value=CategoryName
        # CID: CNAME
        self.tag_dict = tag_to_dict(df=transform_attr_value_dict(self.attribute_meta))
        self.training_data = None
        self.new_training_data = None
        self.train_x = None
        self.train_x_positive = dict()
        self.train_x_negative = dict()
        self.test_x = dict()
        self.train_y = None
        # key=category value=model
        self.model = OrderedDict()
        self.predict_x = None
        self.predict_y = None
        self.message = None
        self.customer_shop_items = None
        self.competitor_items = None
        self.item_pairs = None
        self.es_item_pairs = None
        self.data_to_db = None
        self.construct_prediction_feature = construct_prediction_feature
        # endregion
        # region 可选
        # 必要维度法
        self.use_essential_tag_dict = use_essential_tag_dict
        self.essential_tag_data = pandas.read_csv("essential_tag_data.csv", encoding="utf8")
        self.essential_tag_dict_all = parse_essential_dimension(self.essential_tag_data) if self.use_essential_tag_dict else None
        self.essential_tag_dict = None

        # 重要維度法
        self.use_important_tag_dict = use_important_tag_dict
        self.important_tag_data = pandas.read_csv("important_tag_data.csv", encoding="utf8")
        self.important_tag_dict_all = parse_essential_dimension(self.important_tag_data) if self.use_important_tag_dict else None
        self.important_tag_dict = None
        self.statistic_info = []
        self.SIZE = 100
        self.wordID, self.word_vectors = get_word_vector(self.SIZE)
        self.column_word_vector_dict = {}
        self.model_dict = dict(
            LR=dict(model=LogisticRegression()),
            GBDT=dict(model=GradientBoostingClassifier()),
            KNN=dict(model=KNeighborsClassifier()),
            RF=dict(model=RandomForestClassifier()),
            NB=dict(model=GaussianNB()),
            Ada=dict(model=AdaBoostClassifier()),
            SVM=dict(model=SVC()),
            # XGB=dict(model=xgboost.XGBClassifier()),
        )
        self.RATIO = 1.0
        self.is_random = True
        self.sampling_times = 15
        # endregion
        return

    # endregion

    # region print实例的值
    def __str__(self):
        """
        调试程序
        :return:
        """
        string = "industry={0}\ncustomer_shop_id={1}\nCategoryName={2}\n".format(
            self.industry, self.customer_shop_id, self.category_dict[self.category_id])
        return string

    # endregion

    @time_elapse
    def build_train_raw_feature(self):
        """
        崴
        輸出訓練數據中正例的原始數據
        :return:
        """
        self.training_data = get_training_data(cid=self.category_id)
        self.new_training_data = get_new_training_data(cid=self.category_id)

        self.training_data = pandas.concat([self.training_data, self.new_training_data], ignore_index=True)
        self.training_data.columns = ["attr1", "attr2", "score", "ItemID", "ItemID2"]
        self.new_training_data.columns = ["attr1", "attr2", "score", "ItemID", "ItemID2"]

        self.training_data = self.training_data.sort_values(
            # [self.training_data.ItemID, self.training_data.ItemID2]
            ["ItemID", "ItemID2"], ascending=[True, True]
        )

        self.training_data = self.training_data.loc[self.training_data[["ItemID", "ItemID2"]].drop_duplicates().index]
        # 未打標籤 labeling = False
        attr_train_full = construct_train_raw_feature(
            raw_data=self.training_data.values.tolist(),
            tag_dict=self.tag_dict[self.category_id],
            labeling=False
        )
        return None

    @time_elapse
    def get_result(self):
        """
        获取人工标注结果
        :return:
        """
        ret = get_result_data(cid=self.category_id)
        return

    # region 特征(feature)构造
    @time_elapse
    def build_train_feature(self):
        """
        构建所有训练数据(人工标注数据)的特征
        輸出正例的Jaccard距離特徵
        :return:
        """
        self.training_data = get_training_data(cid=self.category_id)
        self.new_training_data = get_new_training_data(cid=self.category_id)

        self.training_data = pandas.concat([self.training_data, self.new_training_data], ignore_index=True)
        self.training_data.columns = ["attr1", "attr2", "score", "ItemID", "ItemID2"]

        self.training_data = self.training_data.sort_values(
            [self.training_data.columns[3], self.training_data.columns[4]]
        )

        self.training_data.iloc[:, 3:5].astype(long)
        self.training_data = self.training_data.loc[self.training_data[["ItemID", "ItemID2"]].drop_duplicates().index]
        self.train_x, self.train_y, ID1, ID2 = construct_train_feature(
            raw_data=self.training_data.values.tolist(),
            tag_dict=self.tag_dict[self.category_id],
            demo=False
        )

        training_set = pandas.concat(
            [pandas.DataFrame(zip(ID1, ID2)), pandas.DataFrame(self.train_x), pandas.DataFrame(self.train_y)],
            axis=1
        )

        col = ["ID_customer", "ID_competitor"] + [i for i in self.tag_dict[self.category_id].keys()] + ["Label"]
        return

    @time_elapse
    def build_train_negative_feature(self):
        """
        构建没有标注过的负例数据, 默认所有未标注数据都为负例数据
        輸出測試集和負例Jaccard距離特徵
        DataFrame: TaggedItemAttr, item_id, shop_id
        :return:
        """
        self.customer_shop_items = get_customer_shop_items(
            db=self.industry,
            table=self.source_table,
            shop_id=self.customer_shop_id,
            date_range=self.date_range,
            category_id=self.category_id
        )  # 构造预测数据

        self.competitor_items = get_competitor_shop_items(
            db=self.industry,
            table=self.source_table,
            category_id=self.category_id,
            date_range=self.date_range
        )  # 获取客户店铺之外所有对应品类下的商品信息

        self.essential_tag_dict = None  # 如果没有必要维度的品类

        # 崴 extract prediction set raw features to one csv
        # 未打標籤 labeling = False
        attr1_train_negative = construct_prediction_raw_feature(
            raw_data=self.customer_shop_items.values,
            tag_dict=self.tag_dict[self.category_id],
            labeling=False
        )

        attr2_train_negative = construct_prediction_raw_feature(
            raw_data=self.competitor_items.values,
            tag_dict=self.tag_dict[self.category_id],
            labeling=False
        )

        train_negative = pandas.concat([attr1_train_negative, attr2_train_negative], ignore_index=True)
        train_negative, self.item_pairs, self.es_item_pairs = construct_prediction_feature(
            customer_data=self.customer_shop_items.values,
            competitor_data=self.competitor_items.values,
            tag_dict=self.tag_dict[self.category_id],
            essential_tag_dict=self.essential_tag_dict,
            important_tag_dict=self.important_tag_dict
        )

        train_negative = pandas.concat([pandas.DataFrame(self.item_pairs), pandas.DataFrame(train_negative)], axis=1)

        col = ["ID_customer", "ID_competitor"] + [i for i in self.tag_dict[self.category_id].keys()]
        return

    @time_elapse
    def build_prediction_feature(self):
        """
        随机抽取用于预测的数据(非人工标注)
        輸出預測集Jaccard距離特徵
        DataFrame: TaggedItemAttr, item_id, shop_id
        :return:
        """
        self.source_table = "itemmonthlysales_201607"
        self.date_range = "20160701"
        self.customer_shop_items = get_customer_shop_items(
            db=self.industry,
            table=self.source_table,
            shop_id=self.customer_shop_id,
            date_range=self.date_range,
            category_id=self.category_id
        )

        self.competitor_items = get_competitor_shop_items(
            db=self.industry,
            table=self.source_table,
            category_id=self.category_id,
            date_range=self.date_range
        )

        try:
            self.essential_tag_dict = self.essential_tag_dict_all[self.category_id]
            self.important_tag_dict = self.important_tag_dict_all[self.category_id]
        except KeyError:
            pass
        except TypeError:
            pass

        # 崴 extract prediction set raw features to one csv
        # 未打標籤 labeling = False
        attr1_predict = construct_prediction_raw_feature(
            raw_data=self.customer_shop_items.values,
            tag_dict=self.tag_dict[self.category_id],
            labeling=False
        )

        attr2_predict = construct_prediction_raw_feature(
            raw_data=self.competitor_items.values,
            tag_dict=self.tag_dict[self.category_id],
            labeling=False
        )
        attr_predict = pandas.concat([attr1_predict, attr2_predict], ignore_index=True)

        attr_predict, self.item_pairs, self.es_item_pairs = construct_prediction_feature(
            customer_data=self.customer_shop_items.values,
            competitor_data=self.competitor_items.values,
            tag_dict=self.tag_dict[self.category_id],
            essential_tag_dict=self.essential_tag_dict,
            important_tag_dict=self.important_tag_dict
        )

        attr_predict = pandas.concat([pandas.DataFrame(self.item_pairs), pandas.DataFrame(attr_predict)], axis=1)

        col = ["ID_customer", "ID_competitor"] + [i for i in self.tag_dict[self.category_id].keys()]

        # Extract item pairs excluded by Essential Tag method
        es_df = pandas.DataFrame(self.es_item_pairs)
        return

    # endregion

    # region 計算word vector距離
    @time_elapse
    def get_train_distance(self):
        """
        生成正例基於word vector的距離特徵
        一次性的程序
        """
        create_dummy_attr("attr_train_full_", self.category_id)

        attr, dummy, train = read_csv_data(
            is_training_set=True,
            category=self.category_id
        )

        train_distance = generate_distance_df(
            attr,
            dummy,
            train,
            self.column_word_vector_dict,
            self.word_vectors, self.wordID,
            size=self.SIZE,
            is_training_set=True
        )

        train_distance.to_csv("train_distance_" + str(self.category_id) + ".csv", encoding="utf8")

        threshold = 0.5
        train_positive = separate_positive_negative(train, threshold)
        return

    @time_elapse
    def get_prediction_distance(self):
        """
        生成預測數據的基於word vector距離特徵
        一次性的程序
        """
        create_dummy_attr("attr_prediction_full_", self.category_id)

        prediction_set = pandas.read_csv("prediction_" + str(self.category_id) + ".csv", encoding="utf8")
        attr, dummy, train_set = read_csv_data(is_training_set=True, category=self.category_id)

        prediction_set["is_train"] = 0
        train_set["is_train"] = 1

        prediction_set = pandas.concat([train_set, prediction_set], ignore_index=True)
        prediction_set = prediction_set.loc[prediction_set[["ID_customer", "ID_competitor"]].drop_duplicates().index]
        prediction_set = prediction_set[prediction_set.is_train == 0]

        # construct prediction features
        attr_name = "attr_prediction_full_"
        attr = pandas.read_csv("{0}{1}.csv".format(attr_name, self.category_id), encoding="utf-8")  # 原本是 utf-8-sig
        dummy = pandas.read_csv("{0}{1}_dummy.csv".format(attr_name, self.category_id), encoding="utf-8")  # 原本是 utf-8-sig

        # 限制預測集大小以控制訓練時間
        max_size = 400000
        if len(prediction_set) > max_size:
            random_index = set(numpy.random.randint(0, len(prediction_set), max_size))
            sampled_prediction = prediction_set.iloc[list(random_index), :]
            sampled_prediction = sampled_prediction.reset_index(drop=True)
        else:
            sampled_prediction = prediction_set.copy()

        prediction_distance = generate_distance_df(
            attr,
            dummy,
            sampled_prediction,
            self.column_word_vector_dict,
            self.word_vectors,
            self.wordID,
            size=self.SIZE,
            is_training_set=False)
    # end region

    # region 模型训练和预测
    @time_elapse
    def easyensemble_prediction(self):
        """
        隨機負採樣，並集成八個模型預測概率
        :return:
        """
        prediction_proba_dict = {}

        # Construct training distance metrics
        attr, dummy, train = read_csv_data(is_training_set=True, category=self.category_id)
        train_distance = pandas.read_csv("train_distance_{0}.csv".format(self.category_id), encoding="utf8")

        threshold = 0.5
        train_distance_positive = separate_positive_negative(train_distance, threshold)
        train_positive = separate_positive_negative(train, threshold)
        train_positive["Label"] = 1.0
        train_distance_positive["Label"] = 1.0

        # construct prediction features
        prediction_set = pandas.read_csv("prediction_{0}.csv".format(self.category_id), encoding="utf8")
        prediction_proba = pandas.DataFrame()

        prediction_distance = pandas.read_csv("prediction_distance_{0}.csv".format(self.category_id), encoding="utf8")
        prediction_full_features = pandas.concat([prediction_set.iloc[:, 3:], prediction_distance.iloc[:, 2:]], axis=1)
        prediction_full_features.fillna(0, inplace=True)

        attr, dummy, test = read_csv_data(
            is_training_set=False,
            category=self.category_id
        )

        self.sampling_times = 15

        for iteration in range(self.sampling_times):
            sampled_test = down_sample_testset(
                train_distance_positive,
                test,
                self.RATIO,
                self.is_random
            )

            test_distance = generate_distance_df(
                attr,
                dummy,
                sampled_test,
                self.column_word_vector_dict,
                self.word_vectors,
                self.wordID,
                size=self.SIZE,
                is_training_set=False
            )

            # Construct features
            X, y = generate_model_input(
                train_positive,
                sampled_test,
                train_distance_positive,
                test_distance
            )

            # 去除在prediction set中有 但training set中沒有的特徵

            X = X[filter(lambda x: x in prediction_full_features.columns.tolist(), X.columns.tolist())]
            prediction_full_features = prediction_full_features[X.columns]

            # 記綠每個模型的預測
            for key in self.model_dict.keys():
                model = self.model_dict[key]["model"]
                # Predict
                model.fit(X, y)
                try:
                    # keep the predicted probability for class == 1
                    prob = pandas.Series(model.predict_proba(prediction_full_features)[:, 1])  # 0-1分类, 只取1的概率
                except:
                    prob = pandas.Series(model.predict(prediction_full_features))

                prediction_proba = pandas.concat([prediction_proba, prob], axis=1)

                # Print Feature Importance
                if key == "XGB" and iteration == 0:
                    features_importance = pandas.Series(model.feature_importances_, index=X.columns.values)

            prediction_proba_dict[self.category_id] = prediction_proba
    # endregion

    # region 程序入口
    @time_elapse
    def run(self):
        """
        1. 读数据库
        2. 处理不同品类的标签, 构造特征
        3. 训练数据
        4. 预测数据
        5. 写入数据库
        6. 数据会进行分割，先按CategoryID，再按DaterRange进行双重循环
        :return:
        """
        for cid in (self.category_dict.keys()):
            self.category_id = cid
            # self.get_result()
            self.build_train_raw_feature()
            # self.build_train_feature()
            self.get_train_distance()
            # self.build_train_negative_feature()
            self.build_prediction_feature()
            self.get_prediction_distance()
            self.easyensemble_prediction()
        return
    # endregion


if __name__ == "__main__":
    c = CalculateCompetitiveItems()
    c.run()
