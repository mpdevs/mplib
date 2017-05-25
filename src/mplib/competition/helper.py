# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals, absolute_import, print_function, division
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.neighbors import KNeighborsClassifier
from sklearn.ensemble import AdaBoostClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.svm import SVC
from scipy.spatial.distance import cosine, cityblock, euclidean, chebyshev, canberra, braycurtis
from mplib.common import smart_encode
from collections import OrderedDict
from mplib import *
import pandas
import numpy
import re

try:
    from mplib.IO import PostgreSQL
except ImportError:
    pass
try:
    from mplib.IO import Hive  # 可能会有本地文件操作, 不一定要成功导入
except ImportError:
    pass


# region 维度法筛选
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
    return pandas.DataFrame(PostgreSQL().query(sql)).astype(unicode)


def do_dimension_trick(itd, etd, row):
    """
    利用重要维度法和必要维度法筛除一部分的预测数据, 减少计算量
    :param itd: 重要维度字典 OrderedDict
    :param etd: 必要维度字典 OrderedDict
    :param row: 行数据 row[0]:attr1, row[1]:attr2
    :return:
    """
    try:
        if not itd or not etd:
            return True

        a1 = attributes_to_dict(row[0])
        a2 = attributes_to_dict(row[1])

        # region 重要维度法
        a1_dim_intersection = set(list(a1)) & set(list(itd))
        a2_dim_intersection = set(list(a2)) & set(list(itd))

        if len(a1_dim_intersection) < 3 or len(a2_dim_intersection) < 3:
            return False
        # endregion
        # region 必要维度法
        public_intersection = a1_dim_intersection & a2_dim_intersection
        a1_only_dim_intersection = set([x for x in a1_dim_intersection if x not in public_intersection])
        a2_only_dim_intersection = set([x for x in a2_dim_intersection if x not in public_intersection])

        if not a1_only_dim_intersection and not a2_only_dim_intersection:
            return True

        for dim in public_intersection:
            a1_values = set(a1[dim]) & set(etd[dim])
            a2_values = set(a2[dim]) & set(etd[dim])

            if a1_values == a2_values:
                pass
            elif not a1_values and not a2_values:
                pass
            else:
                return False

        for dim in a1_dim_intersection:
            values = set(a1[dim]) & set(etd[dim])
            if values:
                return False

        for dim in a2_dim_intersection:
            values = set(a2[dim]) & set(etd[dim])
            if values:
                return False
        return True

    except:
        return False
    # endregion


def attributes_to_dict(attributes):
    od = OrderedDict()
    for attribute in attributes.split(","):
        dim, value = attribute.split(":")
        if dim in list(od):
            od[dim].append(value)
        else:
            od[dim] = [value]
    return od
# endregion


# region 维度字典相似度
def make_similarity_feature(tag_dict, row):
    attr1 = attributes_to_dict(row[0])
    attr2 = attributes_to_dict(row[1])
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
            s = set(attr_value_list)
            a1 = set(attr1[attr_name])
            a2 = set(attr2[attr_name])
        except:
            feature.append(similarity)
            continue

        set1 = s & a1
        set2 = s & a2

        try:
            feature.append(round(len(set1 & set2) / len(set1 | set2), 4))
        except ZeroDivisionError:
            feature.append(similarity)

    return row[2:4] + [",".join(smart_encode(feature, cast=True))] + row[4:]


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
# endregion


# region 距离特征
def get_word_vector():
    ret = PostgreSQL().query("SELECT value FROM json_value WHERE name = 'word_vector'")[0].get("value")
    return {k: numpy.array(v) for k, v in iteritems(ret)}


def get_dummy_head():
    ret = PostgreSQL().query("SELECT value FROM text_value WHERE name = 'attrname_attrvalue_columns'")[0].get("value")
    return list(map(lambda x: x.split("_")[1], ret.split(",")))


def get_attr_head():
    ret = PostgreSQL().query("SELECT value FROM text_value WHERE name = 'attrname_columns'")[0].get("value")
    return ret.split(",")


def get_distance(dummy_head, word_vector, size, data):
    customer_dummy = numpy.array(data[3].split(","))
    target_dummy = numpy.array(data[4].split(","))
    customer_attr = data[5].split(",")
    target_attr = data[6].split(",")
    customer_vector = build_base_feature(
        dummy_head, word_vector, customer_dummy, customer_attr, size)
    target_vector = build_base_feature(
        dummy_head, word_vector, target_dummy, target_attr, size)
    l = list(range(len(customer_vector)))
    feature = [numpy.nan_to_num(d(customer_vector[i], target_vector[i])) for i in l for d in gen_distance_method()]
    return data[:3] + [",".join(smart_encode(feature, cast=True))] + data[7:]


def build_base_feature(dummy_head, word_vector, dummy, attr, size):
    base_feature = []
    default_vector = numpy.zeros(size)

    # region 通过dummy内容产生对应的all_avg
    all_avg = numpy.zeros(size)
    for index, cell in enumerate(dummy):
        if cell:
            all_avg += word_vector.get(dummy_head[index], default_vector)
    all_avg /= len(dummy_head)
    base_feature.append(all_avg)
    # endregion

    # region 通过attr内容产生对应的all_avg
    for index, a in enumerate(attr):
        attr_avg = numpy.zeros(size)

        if a:
            scope = a.split(":")
            for s in scope:
                attr_avg += word_vector.get(s, default_vector)
            attr_avg /= len(scope)

        base_feature.append(attr_avg)
    # endregion
    return base_feature


def gen_header(mode="train"):
    columns = [
        "customer_item_id",
        "target_item_id",
        "similarity",
        "customer_dummy",
        "target_dummy",
        "customer_attr",
        "target_attr",
    ]
    columns = columns + ["score"] if mode == "train" else columns
    return columns


def gen_distance_method():
    return [
        cosine,
        cityblock,
        euclidean,
        chebyshev,
        canberra,
        braycurtis
    ]
# endregion


# region 算法相关
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
    return dict(
        rf=RandomForestClassifier(),
        gb=GradientBoostingClassifier(),
        lr=LogisticRegression(),
        kn=KNeighborsClassifier(),
        ada=AdaBoostClassifier(),
        nb=GaussianNB(),
        svm=SVC(),
    )


def split_x_y(d):
    x = []
    y = []
    for l in d:
        x.append([float(s) for s in l[3].split(",")])
        y.append(int(l[4]))
    return numpy.array(x), numpy.array(y)
# endregion


if __name__ == "__main__":
    # print(len(get_train_data(1623)))
    # print(list(get_essential_dict()))
    # print(get_attr_head())
    print(len(get_dummy_head()))
