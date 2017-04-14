# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import print_function
from __future__ import division
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import AdaBoostClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.svm import SVC
from sklearn.externals import joblib
from scipy.spatial.distance import cosine, cityblock, euclidean, chebyshev, canberra, braycurtis
from collections import OrderedDict
from os.path import dirname, join
from six import iteritems
import pickle
import pandas
import numpy
import re

try:
    from mplib.IO import PostgreSQL
except ImportError:
    pass
try:
    from mplib.IO import Hive
except ImportError:
    pass


def get_external_json():
    return PostgreSQL().query("SELECT value FROM json_value")


def get_word_vector():
    ret = PostgreSQL().query("SELECT value FROM json_value WHERE name = 'word_vector'")[0].get("value")
    return {k: numpy.array(v) for k, v in ret.iteritems()}


def get_essential_dict():
    return PostgreSQL().query("SELECT value FROM json_value WHERE name = 'essential_dict'")[0].get("value")


def get_important_dict():
    return PostgreSQL().query("SELECT value FROM json_value WHERE name = 'important_dict'")[0].get("value")


def get_attribute_meta():
    sql = """
    SELECT
        category_id,
        attr_name,
        attr_value
    FROM attr_value
    WHERE industry_id = 16
    AND attr_name != '品牌'
    """
    return pandas.DataFrame(PostgreSQL().query(sql))


def get_train_data(category_id):
    sql = """
    SELECT
        a1.attr AS attr1,
        a2.attr AS attr2,
        c.score,
        c.SourceItemID AS itemid1,
        c.TargetItemID AS itemid2
    FROM elengjing.train_data_for_competitive_items AS c
    JOIN elengjing.women_clothing_item_attr_t AS a1
    ON c.SourceItemID = a1.itemid
    JOIN elengjing.women_clothing_item_attr_t AS a2
    ON c.TargetItemID = a2.itemid
    WHERE c.CategoryID = {category_id}
    """.format(category_id=category_id)
    return pandas.DataFrame(Hive().query(sql))


def get_predict_data(category_id):
    return


def do_word_to_vec():
    return


def get_distance():
    return


def construct_train_feature(raw_data, tag_dict):
    x = []
    y = []

    id1 = []
    id2 = []

    for row in raw_data:
        attr1 = row[0]
        attr2 = row[1]

        feature_vector = construct_feature(attr1=attr1, attr2=attr2, tag_dict=tag_dict)

        if len(feature_vector) != len(tag_dict):
            print("not ok")

        x.append(feature_vector)
        y.append(row[2])

        id1.append(row[3])
        id2.append(row[4])

    return numpy.array(x), numpy.array(y), numpy.array(id1), numpy.array(id2)


def construct_feature(attr1, attr2, tag_dict):
    attr1, attr2 = attributes_to_dict(attr1), attributes_to_dict(attr2)
    return make_similarity_feature(attr1=attr1, attr2=attr2, tag_dict=tag_dict)


def attributes_to_dict(attributes):
    od = OrderedDict()
    for attribute in attributes[1: -1].split(","):
        dimension_value_pair = attribute.split(":")
        if dimension_value_pair[0] in list(od):
            od[dimension_value_pair[0]].append(dimension_value_pair[1])
        else:
            od[dimension_value_pair[0]] = [dimension_value_pair[1]]
    return od


def make_similarity_feature(attr1, attr2, tag_dict):
    feature = []
    for attr_name, attr_value_list in iteritems(tag_dict):
        similarity = 0.
        if attr_name == "材质成分":
            md1 = material_string_to_dict(attr1.get(attr_name))
            md2 = material_string_to_dict(attr2.get(attr_name))
            mdi = set(list(md1)) & set(list(md2))
            if len(mdi) > 0:
                material_similar_score = similarity
                for i in mdi:
                    material_similar_score += min(md1.get(i), md2.get(i))
                    feature.append(round(material_similar_score / 100, 4))
            else:
                feature.append(similarity)
            continue

        try:
            set(attr_value_list)
        except TypeError:
            feature.append(similarity)
            continue

        set1 = set(attr_value_list) & set(attr1.get(attr_name))
        set2 = set(attr_value_list) & set(attr2.get(attr_name))
        try:
            feature.append(round(len(set1 & set2) / len(set1 | set2), 4))
        except ZeroDivisionError:
            feature.append(similarity)

    return feature


def material_string_to_dict(material):
    if not material:
        return dict()

    material_dict = dict()
    for material_purity in material:
        purity = re.findall(r"\d+\.?\d*", material_purity)

        if purity:
            purity = purity[0]
        else:
            continue

        material = material_purity.replace(purity, "").replace("%", "")
        if "(" in material and ")" in material:
            material = material[material.find("(") + len("("): material.find(")")]
        if "（" in material and "）" in material:
            material = material[material.find("（") + len("（"): material.find("）")]
        if material not in ["其他", "其它"]:
            material_dict.update({material: purity})
    return material_dict


def tag_to_dict(df):
    df = df[["category_id", "attr_name", "attr_value"]]
    category_ids = df.category_id.unique().tolist()
    tag_dict = OrderedDict()
    for category_id in category_ids:
        attr_name_value_dict = OrderedDict()
        for row in df[df.category_id == category_id].values.tolist():
            attr_name_value_dict[row[1]] = row[2].split(",")
        tag_dict[category_id] = attr_name_value_dict
    return tag_dict


def do_essential_trick():
    return


def do_important_trick():
    return


def serialize():
    return


def generate_distance_df(attr, dummy, x, word_vectors, is_training_set=True):
    """
    生成distance metric

    Params:
        attr: attr_train/test_full/tagged, DataFrame
        dummy: attr_train/test_full/tagged_dummy, DataFrame
        data_set: train/test_categoryID, , DataFrame
        column_word_vector_dict: 儲存所有features的 word vector, dict
        is_training_set: if it is training data, append label, boolean
    Return:

    """
    # 引入word vector
    word_vec = pandas.DataFrame(dummy.ix[:, 1])  # initiate with itemID

    col_name = 'all_avg'
    word_vec[col_name] = dummy.apply(
        all_average_word_vector, column_word_vector_dict=word_vectors, full_column_list=dummy.columns[2:], axis=1
    ).ix[:, -1]
    # full_column_list 所有属性值

    word_vec = pandas.concat([word_vec, attr.apply(
        column_average_word_vector, column_word_vector_dict=word_vectors, full_column_list=attr.columns[2:], axis=1
    ).ix[:, 2:]], axis=1)

    # 去除 nan(超過99%) 太多的維度
    for col in word_vec:
        threshold = 0.01
        if sum(word_vec[col].notnull()) / float(len(word_vec)) < threshold:
            word_vec.drop(col, inplace=True, axis=1)

    # 計算距離
    distance = pandas.DataFrame()
    for col in word_vec.columns[1:]:
        distance = pandas.concat(
            [distance, x.apply(calculate_distance, item_word_vector=word_vec, column=col, axis=1)], axis=1)

    distance = pandas.concat([x.ix[:, 1:3], distance], axis=1)  # Insert ItemID pair

    # 去除全為 nan 或 0 的維度
    for col in distance.columns:
        # if sum(distance[col].notnull()) == 0 or sum(distance[col]) < 0.1:
        #     distance.drop(col, inplace=True, axis=1)

        try:
            if sum(distance[col].notnull()) == 0:
                distance.drop(col, inplace=True, axis=1)
        except:
            print("Null:", col)
        try:
            if sum(distance[col]) == 0:
                distance.drop(col, inplace=True, axis=1)
        except:
            print("Zeros:", col)

    # Training set 需要有label
    if is_training_set:
        distance = pandas.concat([distance, x.ix[:, -1]], axis=1)  # Append Label
    return distance


def get_column_word_vector(columns, word_vectors, wordID, word_vector_dict, size):
    """
    遍例欄名(即維度值),取出相應word vector,若無則賦值0向量,可保證之後index時不出error
    :param columns: list, 欄名
    :param word_vectors: list
    :param wordID: list
    :param word_vector_dict: dict
    :param size: int
    :return:
    """
    new_word_vector_dict = word_vector_dict.copy()
    for column in columns:
        if column not in list(word_vector_dict):
            try:
                new_word_vector_dict[column] = list(word_vectors[wordID.index(column)])
            except:
                new_word_vector_dict[column] = numpy.zeros(size)
    return new_word_vector_dict


def calculate_distance(row, item_word_vector, column):
    """
    計算一個指定維度的距離
    :param row: row of the DataFrame, 即一件商品的特徵向量
    :param item_word_vector:
    :param column: 要計算距離的維度
    :return:
    """
    # select the item & its word vector
    item1 = item_word_vector[item_word_vector["ItemID"].values == row[1]][:1]
    item2 = item_word_vector[item_word_vector["ItemID"].values == row[2]][:1]

    df = pandas.Series()

    try:
        vector1 = item1[column].values[0]
        vector2 = item2[column].values[0]

        df[column + "_cosine"] = cosine(vector1, vector2)
        df[column + "_cityblock"] = cityblock(vector1, vector2)
        df[column + "_euclidean"] = euclidean(vector1, vector2)
        df[column + "_chebyshev"] = chebyshev(vector1, vector2)
        df[column + "_canberra"] = canberra(vector1, vector2)
        df[column + "_braycurtis"] = braycurtis(vector1, vector2)
    except:
        df[column + "_cosine"] = numpy.nan
        df[column + "_cityblock"] = numpy.nan
        df[column + "_euclidean"] = numpy.nan
        df[column + "_chebyshev"] = numpy.nan
        df[column + "_canberra"] = numpy.nan
        df[column + "_braycurtis"] = numpy.nan
    return df


def column_average_word_vector(row, column_word_vector_dict, full_column_list, size):
    """
    分別取每格内所有单字的word vector平均
    :param row: row of the DataFrame (attr)
    :param column_word_vector_dict: dict, key為維度值 value為對應之詞向量
    :param full_column_list: list, 欄名(維度值)列表
    :param size: int, 詞向量長度
    :return: 包含新features的row
    """

    # Double Check whether we skipped the ItemID in column_list or not
    if len(full_column_list) == len(row):
        return "Use df.columns[1:] to skip ItemID"
    # filter the cols that equal NaN, exclude ItemID & itemDesc
    column_list = full_column_list[row[2:].notnull() == 1]

    for col in full_column_list:
        if col in column_list:
            temp = numpy.zeros(size)
            i = 0
            for value in row[col].split(",")[:-1]:  # -1: 去除空格
                try:
                    temp += column_word_vector_dict[value]
                    i += 1.0
                except:
                    pass
            if i == 0:
                i = 1
            row[col] = temp / i
    return row


def all_average_word_vector(row, column_word_vector_dict, full_column_list, size):
    """
    取所有欄位中所有单字的word vector平均

    :param row: row of the DataFrame (dummy)
    :param column_word_vector_dict: dict, key為分詞 value為對應之詞向量
    :param full_column_list: list, 欄名列表
    :param size: int, 詞向量長度
    :return: 新增一個欄位的row
    """
    # Double Check whether we skipped the ItemID in column_list or not
    if len(full_column_list) == len(row):
        return "Use df.columns[1:] to skip ItemID"

    # filter the cols that has 0 value, exclude ItemID
    full_column_list = full_column_list[row[2:] != 0]

    vector = numpy.zeros(size)
    # 將所有維度值的詞向量加起來
    for col in full_column_list:
        vector += column_word_vector_dict[col]

    # 平均
    vector /= len(full_column_list)
    row["word_vec_average_all"] = vector
    return row


def gen_print_var():
    return [
        "m",
        "n",
        "y_definition",
        "train_size",
        "predict_size",
        "framework_model",
        "train_elapse",
        "predict_elapse",
        "metric_accuracy",
        "run_time",
    ]


def gen_model_dict():
    model_dict = OrderedDict()
    model_dict["rf"] = RandomForestClassifier
    model_dict["gb"] = GradientBoostingClassifier
    model_dict["lr"] = LogisticRegression
    model_dict["kn"] = KNeighborsClassifier
    model_dict["ada"] = AdaBoostClassifier
    model_dict["nb"] = GaussianNB
    model_dict["svm"] = SVC
    return model_dict


if __name__ == "__main__":
    print(len(get_train_data(1623)))
