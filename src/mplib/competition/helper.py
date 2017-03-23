# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
from scipy.spatial.distance import cosine, cityblock, euclidean, chebyshev, canberra, braycurtis
from mplib.common import smart_decode
from collections import OrderedDict
import matplotlib.pyplot as plt
import random
import pandas
import numpy
import re
import os


def transform_attr_value_dict(df):
    df = df[df.Flag == "A"]
    df = df[["CID", "DisplayName", "AttrValue"]]
    return df


def tag_to_dict(df):
    """
    用于竟品计算
    :param df: DataFrame
    :return: dict(key=CID, value=dict(key=AttrName, value=AttrValue list))
    """
    cid_list = df.CID.unique().tolist()
    tag_dict = OrderedDict()
    for cid in cid_list:
        attr_name_value_dict = OrderedDict()
        for row in df[df.CID == cid].values.tolist():
            attr_name_value_dict[row[1]] = row[2].split(b",")
        tag_dict[cid] = attr_name_value_dict
    return tag_dict


def make_similarity_feature(attr1, attr2, tag_dict, demo=False):
    """
    计算相似度的方法
    需要把两个已经转换好格式的属性以及需要用来计算竞品的标签
    竞品的标签必须是一个排序字典，用于后续的训练训练模型生成
    计算步骤：
        遍历每个维度
            特殊维度——材质成分
                将两个商品的材质维度做成字典，数据格式为 材质: 纯度
                    材质交集 mi = attr1.材质成分 & attr2.材质成分为空则，直接为0
                    非空，sigma(min(attr1.mi[i], attr2.mi[i]))
            查看两个商品之间和维度词典中是否有对应的维度
                如果有，则计算这些维度值的jaccard相似度: 交集数 / 并集数
                否则， 直接为0
    :param attr1: dict(key=unicode, value=list)
    :param attr2: dict(key=unicode, value=list)
    :param tag_dict: OrderedDict(key=unicode, value=list)
    :param demo: boolean
    :return: list
    """
    feature = []
    for dimension, value_list in tag_dict.iteritems():
        similarity = round(float(0), 4)
        if dimension in attr1.keys() and dimension in attr2.keys():
            pass
        else:
            if demo:
                pass
            else:
                feature.append(similarity)
                continue
        if dimension == "材质成分":
            try:
                m1 = material_string_to_dict(attr1[dimension])
                if len(m1) == 0:
                    similarity -= 1
            except KeyError:
                similarity -= 1
            try:
                m2 = material_string_to_dict(attr2[dimension])
                if len(m2) == 0:
                    similarity -= 1
            except KeyError:
                similarity -= 1
            if similarity >= 0:
                mi = set(m1.keys()) & set(m2.keys())
                if len(mi) > 0:
                    material_similar_score = similarity
                    for i in mi:
                        material_similar_score += min(float(m1[i]), float(m2[i]))
                    feature.append(round(float(material_similar_score) / 100, 4))
                elif len(mi) == 0:
                    feature.append(similarity)
            else:
                feature.append(similarity)
            continue
        else:
            pass
        try:
            set(value_list)
        except TypeError as e:
            feature.append(similarity)
            continue
        if demo:
            try:
                set1 = set(value_list) & set(attr1[dimension])
                if len(set1) == 0:
                    similarity -= 1
                else:
                    pass
            except KeyError:
                similarity -= 1
            try:
                set2 = set(value_list) & set(attr2[dimension])
                if len(set2) == 0:
                    similarity -= 1
                else:
                    pass
            except KeyError:
                similarity -= 1
            if similarity < 0:
                feature.append(similarity)
            else:
                try:
                    feature.append(round(float(len(set1 & set2)) / len(set1 | set2), 4))
                except ZeroDivisionError:
                    feature.append(similarity)
        else:
            set1 = set(value_list) & set(attr1[dimension])
            set2 = set(value_list) & set(attr2[dimension])
            try:
                feature.append(round(float(len(set1 & set2)) / len(set1 | set2), 4))
            except ZeroDivisionError:
                feature.append(similarity)
        continue
    return feature


def material_string_to_dict(material):
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


def attributes_to_dict(attributes):
    """
    attributes是数据库里商品已经打好的标签
    :param attributes: unicode
    :return: OrderedDict(unicode: list(unicode))
    """
    od = OrderedDict()
    for attribute in attributes[1: -1].split(b","):
        dimension_value_pair = attribute.split(b":")
        if dimension_value_pair[0] in od.keys():
            od[dimension_value_pair[0]].append(dimension_value_pair[1])
        else:
            od[dimension_value_pair[0]] = [dimension_value_pair[1]]
    return od


def construct_feature(attr1, attr2, tag_dict, demo=False):
    """
    根据品类-属性-值的字典返回两个商品之间的相似度向量
    每个维度就是属性
    :param attr1: unicode
    :param attr2: unicode
    :param tag_dict: OrderedDict
    :param demo: boolean
    :return: list
    """
    attr1, attr2 = attributes_to_dict(attr1), attributes_to_dict(attr2)
    return make_similarity_feature(attr1=attr1, attr2=attr2, tag_dict=tag_dict, demo=demo)


def construct_train_feature(raw_data, tag_dict, demo=False, type="Jaccard"):
    """
    构造训练数据特征, 维度为某个属性的相似度
    :param raw_data: DataFrame
    :param tag_dict: OrderedDict
    :return: numpy.array, numpy.array
    """
    x_set = []
    y_set = []

    # 崴 initialize empty list and dataframe for ID & raw features
    ID1 = []
    ID2 = []

    for row in raw_data:
        attr1 = row[0]
        attr2 = row[1]

        feature_vector = construct_feature(attr1=attr1, attr2=attr2, tag_dict=tag_dict, demo=demo)
        x_set.append(feature_vector)
        y_set.append(row[2])

        ID1.append(row[3])
        ID2.append(row[4])

    return numpy.array(x_set), numpy.array(y_set), numpy.array(ID1), numpy.array(ID2)


def construct_train_raw_feature(raw_data, tag_dict, labeling=True):
    """
    構造預測數據商品描述文字特徵，維度為此商品打標籤使用的特徵
    :param raw_data: DataFrame
    :param tag_dict: OrderedDict
    :param labeling: Boolean True:僅用有打標籤的維度 False: 使用所有維度
    :return: DataFrame
    """
    columns = tag_dict.keys() + ["ItemID"]
    test_attr_df = pandas.DataFrame(columns=columns)
    # key_list = pandas.Series(attr1.keys() + attr2.keys()).unique()
    # temp = pandas.DataFrame(columns=key_list)

    for index in range(2):

        for data in raw_data:
            attr = attributes_to_dict(data[index])
            temp = pandas.DataFrame()

            # 去重複ID
            try:
                if sum(data[index+3] == test_attr_df["ItemID"]) >= 1:
                    continue
            except:
                pass
            else:
                for key in attr.keys():
                    # don"t use the keys that are not in tag_dict
                    #  in order to ensure that two df has the same columns
                    if key not in tag_dict and labeling is True:
                        continue
                    else:
                        values = b""
                        for value in attr[key]:
                            values += value
                            values += b","
                        temp[key] = [values]

                if temp.shape[0] == 0:
                    # put numpy.nan in a random col so that it could be appended
                    temp[attr.keys()[0]] = [numpy.nan]
                temp["ItemID"] = long(data[index+3])
                test_attr_df = test_attr_df.append(temp)

    test_attr_df["ItemID"] = test_attr_df["ItemID"].astype(long)
    return test_attr_df


def construct_prediction_raw_feature(raw_data, tag_dict, labeling=True):
    """
    構造預測數據商品描述文字特徵，維度為此商品打標籤使用的特徵
    :param raw_data: DataFrame
    :param tag_dict: OrderedDict
    :param labeling: Boolean True:僅用有打標籤的維度 False: 使用所有維度
    :return: DataFrame
    """
    test_attr_df = pandas.DataFrame(columns=tag_dict)

    for data in raw_data:
        attr = attributes_to_dict(data[1])
        temp = pandas.DataFrame()

        for key in attr.keys():
            if key not in tag_dict and labeling is True:
                continue
            else:
                values = b""
                for value in attr[key]:
                    values += value
                    values += b","
                temp[key] = [values]

        if temp.shape[0] == 0:
            # put numpy.nan in a random col so that it could be appended
            temp[attr.keys()[0]] = [numpy.nan]

        temp["ItemID"] = data[0]
        test_attr_df = test_attr_df.append(temp)
    test_attr_df["ItemID"] = test_attr_df["ItemID"].astype(long)
    return test_attr_df


def construct_prediction_feature(customer_data, competitor_data, tag_dict, essential_tag_dict, important_tag_dict, demo=False):
    """
    构造预测数据特征, 维度为两个商品在所有相关维度上的相似度
    :param customer_data: DataFrame
    :param competitor_data: DataFrame
    :param tag_dict: OrderedDict
    :param essential_tag_dict: OrderedDict
    :param important_tag_dict: OrderedDict
    :return: numpy.array, tuple(long, long)
    """
    x_set = []
    item_pair = []
    item_dict = {}

    es_item_pair = []

    es_count = 0
    # ItemID, TaggedItemAttr, DiscountPrice, CategoryID
    for customer_item in customer_data:
        # ItemID, TaggedItemAttr, DiscountPrice, CategoryID
        for competitor_item in competitor_data:
            # 价格区间筛选
            lower_bound, upper_bound = 0.8 * customer_item[2], 1.2 * customer_item[2]
            if float(competitor_item[2]) < lower_bound or float(competitor_item[2]) > upper_bound:
                continue
            # 品类筛选 chunk 操作
            if competitor_item[3] != customer_item[3]:
                continue
            # 必要维度法
            attr1, attr2 = attributes_to_dict(competitor_item[1]), attributes_to_dict(customer_item[1])

            if not essential_dimension_trick(
                    attr1=attr1, attr2=attr2,
                    essential_tag_dict=essential_tag_dict, important_tag_dict=important_tag_dict):
                es_count += 1
                es_item_pair.append((customer_item[0], competitor_item[0]))
                # export to CSV
                continue

            # 崴 去除兩個相同 & 重復訓練 的情況
            if customer_item[0] == competitor_item[0]:
                continue
            try:
                if customer_item[0] in item_dict[competitor_item[0]]:
                    continue
            except:
                pass

            # 都没问题才纳入预测数据返回
            feature_vector = make_similarity_feature(attr1=attr1, attr2=attr2, tag_dict=tag_dict, demo=demo)
            x_set.append(feature_vector)
            item_pair.append((customer_item[0], competitor_item[0]))

            try:
                if item_dict[customer_item[0]] != 0:
                    item_dict[customer_item[0]].append(competitor_item[0])
            except:
                item_dict[customer_item[0]] = list()

    return numpy.asarray(x_set), item_pair, es_item_pair


def sample_balance(train_x, train_y):
    """
    将 y <= 0.5 和 y > 0.5 以 1: 1 的比例返回
    :param train_x: numpy.array
    :param train_y: numpy.array
    :return: numpy.array
    """
    t = numpy.array(xrange(len(train_y)))
    t = random.sample(t[train_y <= 0.5], sum(train_y > 0.5)) + t[train_y > 0.5].tolist()
    t = random.sample(t, len(t))
    train_x = train_x[t]
    train_y = train_y[t]
    return train_x, train_y


def parse_essential_dimension(df):
    od = OrderedDict()
    for row in df.values.tolist():
        if row[0] in od.keys():
            if row[1] in od[row[0]].keys():
                od[row[0]][row[1]].append(row[2])
            else:
                od[row[0]][row[1]] = [row[2]]
        else:
            dimension_value = OrderedDict()
            dimension_value[row[1]] = [row[2]]
            od[row[0]] = dimension_value
    return od


def essential_dimension_trick(attr1, attr2, essential_tag_dict, important_tag_dict):
    """
    必要维度法，用于竞品计算的时候过滤一些同属性，不同值的情况
    返回True的话就开始构造特征
    返回False的话就跳过 (異位)
    :param attr1: unicode
    :param attr2: unicode
    :param essential_tag_dict: OrderedDict
    :param important_tag_dict: OrderedDict
    :return: boolean
    """
    # if not essential_tag_dict:
    #     return True
    # elif not important_tag_dict:
    #     return True

    # 重要維度法
    a1_tag_intersection = set(attr1.keys()) & set(important_tag_dict.keys())
    a2_tag_intersection = set(attr2.keys()) & set(important_tag_dict.keys())

    threshold = 3
    if len(a1_tag_intersection) < threshold or len(a2_tag_intersection) < threshold:
        return False

    if not essential_tag_dict:
        return True

    # 必要維度法
    # a1_a2_intersection = set(attr1.keys()) & set(attr2.keys())
    a1_tag_intersection = set(attr1.keys()) & set(essential_tag_dict.keys())
    a2_tag_intersection = set(attr2.keys()) & set(essential_tag_dict.keys())
    public_intersection = a1_tag_intersection & a2_tag_intersection

    a1_tag_only_intersection = [x for x in a1_tag_intersection if x not in public_intersection]
    a2_tag_only_intersection = [x for x in a2_tag_intersection if x not in public_intersection]

    # 先檢查共同必要維度
    if public_intersection:
        for key in public_intersection:
            union_values_a1_tag = set(attr1[key]) & set(essential_tag_dict[key])
            union_values_a2_tag = set(attr2[key]) & set(essential_tag_dict[key])

            # 若兩者有相同必要維度值 且 包含所有必要維度值(長度和必要維度dict相同)  我不管
            if (union_values_a1_tag == union_values_a2_tag) and \
                    (len(union_values_a1_tag) == len(essential_tag_dict[key])):
                pass
            # 若兩者都沒有必要維度值 我不管
            elif not union_values_a1_tag and not union_values_a2_tag:
                pass
            # 除此之外
            else:
                return False

    # 沒有必要維度 我不管
    if not a1_tag_only_intersection and not a2_tag_only_intersection:
        return True

    # 各自有另一方沒有的必要維度
    # a1 的必要維度中
    if a1_tag_only_intersection:
        for key in a1_tag_only_intersection:
            union_values = set(attr1[key]) & set(essential_tag_dict[key])
            # 有其中幾個必要維度值 異位
            if union_values:
                return False
        # 沒有指定必要維度值 我不管
        pass

    # a2 的必要維度中
    if a2_tag_only_intersection:
        for key in a2_tag_only_intersection:
            union_values = set(attr2[key]) & set(essential_tag_dict[key])
            # 有其中幾個必要維度值 異位
            if union_values:
                return False
        # 沒有指定必要維度值 我不管
        return True


# 崴
def get_essential_tag(upper_limit=1000, lower_limit=10, max_es=50):
    """
    計算必要和重要維度值
    :param upper_limit:
    :param lower_limit:
    :param max_es:
    :return:
    """
    os.chdir("D:\workspace\preprocess\competition_finder\Essential Tag")
    es = pandas.read_csv("Es.txt", sep="\t")
    es = es.sort_values(["全异位比例"], ascending=False)
    mask_attr = [True if x not in ["品牌", "材质成分", "年份季节", "年份/季节", "适用年龄", "上市年份/季节", "上市年份季节"] else False for x in
                 es["维度"]]
    es = es[mask_attr]

    im = es[es["置信度"] > upper_limit]

    es = es[es["置信度"] > lower_limit]
    es = es[es["置信度"] < upper_limit]

    index_to_keep = list()
    for i in es.index:
        if es.loc[i, "全异位比例"] >= 0.7:  # and es.loc[i, "2-3人异位比例"] >= 0.75:
            index_to_keep.append(i)
    es = es.loc[index_to_keep, :]

    # 去除兩個維度值相同 但維度名不同的
    for i in list(es["﻿品类名"].unique()):
        value = es.loc[es["﻿品类名"] == i, "维度值"]
        duplicate = value[value.duplicated()]
        for i in duplicate.values:
            es = es[es["维度值"] != i]

    # 去除普遍都有 可有可無的維度
    es = es[es["维度值"] != "单件"]  # 162103
    es = es[es["维度值"] != "常规款"]  # 162116

    es_filter = pandas.DataFrame()
    for category in es.iloc[:, 0].unique():
        category_es = es[es.iloc[:, 0] == category]

        num_tags = len(category_es)
        if num_tags > max_es:
            num_tags = max_es

        es_filter = es_filter.append(category_es.iloc[:num_tags, :], ignore_index=True)

    es = es_filter.copy()
    es = es.iloc[:, 0:3]
    os.chdir("D:\\workspace\\preprocess\\competition_finder")
    es.to_csv("essential_tag_data.csv", encoding="utf8", index=False)
    im = im.iloc[:, :3]
    im.to_csv("important_tag_data.csv", encoding="utf8", index=False)
    return


def get_prediction_for_tagging(category, num_pos, num_neg):
    """
    cuo中儲存無法取得照片的商品ID 若出現在cuo中則不納入標註數據中

    從prediction_proba 中讀取模型預測結果，取中位數後，
    輸出分數最高的num_pos和分數最低的num_neg對做為標注數據
    :param category: int
    :param num_pos: int, 需要的正例數
    :param num_neg: int, 需要的負例數
    :return:
    """
    with open("cuo.txt", "r") as f:
        cuo = pandas.Series(f.readlines())
        cuo = [x[:-1] for x in cuo]

    # visualization
    temp_pred = pandas.read_csv("prediction_proba_" + str(category) + ".csv", encoding="utf8")
    temp_pred = temp_pred.drop("Unnamed: 0", axis=1)
    temp_pred = temp_pred.median(axis=1)
    plt.show()

    prediction_set = pandas.read_csv("prediction_" + str(category) + ".csv", encoding="utf8")
    txt = pandas.concat([prediction_set.iloc[:, 1:3], pandas.Series(temp_pred, name="median")], axis=1)
    txt.sort_values("median", inplace=True, ascending=False)

    # write to csv
    count_pos = 1
    count_neg = 1
    txt_name = str(category) + "_proba_median.txt"
    with open(txt_name, "w") as text_file:
        for row in range(10000):
            # pos
            if str(long(txt.iloc[row, 0])) in cuo or str(long(txt.iloc[row, 1])) in cuo or count_pos > num_pos:
                continue
            else:
                if str(long(txt.iloc[row, 0])) == str(long(txt.iloc[row, 1])):
                    print str(long(txt.iloc[row, 0])), str(long(txt.iloc[row, 1]))
                else:
                    text_file.write(
                        "{0}	{1}	{2}\n".format(str(long(txt.iloc[row, 0])), str(long(txt.iloc[row, 1])),
                                                     txt.iloc[row, 2]))
                    count_pos += 1
        for row in range(1, 10000):
            # neg
            if (str(long(txt.iloc[-row, 0])) in cuo or str(long(txt.iloc[-row, 1])) in cuo) or count_neg > num_neg:
                continue
                print count_neg
            else:
                text_file.write("{0}	{1}	{2}\n".format(str(long(txt.iloc[-row, 0])), str(long(txt.iloc[-row, 1])),
                                                             txt.iloc[-row, 2]))
                count_neg += 1
    return None


def get_photo_id(category):
    """
    生成能給 tupian.py 取照片的txt檔
    :param category:
    :return:
    """
    data = pandas.read_csv(str(category) + "_proba_median" + ".txt", sep="\t")
    items = data.iloc[:, 0].append(data.iloc[:, 1])
    items = items.reset_index(drop=True)
    items = items.drop_duplicates()
    items = items.reset_index(drop=True)

    d = "photo_" + str(category) + ".txt"

    for f, txt in zip([d], [items]):
        with open(f, "w") as text_file:
            for row in txt.values:
                text_file.write("{0}\n".format(str(long(row))))
    return


def get_sample_photo_id(category):
    """
    取範例的照片ID，存成tupian.py 取照片的txt檔
    :param category:
    :return:
    """
    photo = pandas.Series()
    data = pandas.read_csv("train_" + str(category) + ".csv", encoding="utf8")

    d = str(category) + ".txt"
    pos = str(category) + "_相似例子.txt"
    neg = str(category) + "_相异例子.txt"

    pos_data = data[data["Label"] > 0.5].iloc[:80, :]
    neg_data = data[data["Label"] == 0.0].iloc[:80, :]

    sort_data = data.sort_values(["ID_customer", "ID_competitor"])
    count = 0
    for i in range(len(sort_data) - 1):
        if sort_data.iloc[i, 1] == sort_data.iloc[i + 1, 1] and sort_data.iloc[i, 2] == sort_data.iloc[i + 1, 2]:
            count += 1

    for f, txt in zip([pos, neg], [pos_data, neg_data]):
        with open(f, "w") as text_file:
            for row in range(len(txt)):
                text_file.write("{0}	{1}	\n".format(str(int(txt.iloc[row, 1])), str(int(txt.iloc[row, 2]))))

    photo = photo.append(pos_data.iloc[:, 1]).append(pos_data.iloc[:, 2])
    photo = photo.append(neg_data.iloc[:, 1]).append(neg_data.iloc[:, 2])

    photo = photo.drop_duplicates().reset_index(drop=True)

    with open("sample" + str(category) + ".txt", "w") as f:
        for row in photo.values:
            pattern = str(long(row)) + "\n"
            f.write(pattern)


def split_tagging_set(category, num_of_files=2, len_per_file=200):
    txt_name = str(category) + "_proba_median.txt"

    with open(txt_name, "r") as txt_file:
        data = txt_file.readlines()
    numpy.random.shuffle(data)

    txt_name = str(category) + "_proba_median_shuffle.txt"
    with open(txt_name, "w") as txt_file:
        for i in data:
            txt_file.write(i)

    for i in range(1, num_of_files + 1):
        output = str(category) + ".a" + str(i) + ".txt"

        with open(output, "w") as txt_file:
            start = len_per_file * (i - 1)
            end = len_per_file * i
            for i in data[start:end]:
                txt_file.write(i)

    return None


def cold_start_tagging_data(category, k=10000):
    """
    取Jaccard相似度總和前k高的商品對作為相似商品對來標注,
    輸出商品ID來抓取照片
    :param category: int, category ID
    :param k:int
    :return:
    """
    with open("cuo.txt", "r") as f:
        cuo = pandas.Series(f.readlines())
        cuo = [x[:-1] for x in cuo]

    data = pandas.read_csv("prediction_" + str(category) + ".csv", encoding="utf8")
    data["sum"] = data.iloc[:, 3:].sum(axis=1)

    # 取前k高的
    output = data.sort_values("sum", ascending=False).iloc[:k, :]
    output.reset_index(drop=True, inplace=True)

    text_name = "cold_start_" + str(category) + ".txt"
    with open(text_name, "w") as f:
        for i in range(len(output)):
            if str(long(output.iloc[i, 1])) not in cuo and str(long(output.iloc[i, 2])) not in cuo:
                pattern = str(long(output.iloc[i, 1])) + "\t" + str(long(output.iloc[i, 2])) + "\t" + str(
                    long(output.iloc[i, -1])) + "\n"
                f.write(pattern)

    data_2 = pandas.read_csv(text_name, sep="\t")

    items = data_2.iloc[:, 0].append(data_2.iloc[:, 1])
    items = items.reset_index(drop=True)
    items = items.drop_duplicates()
    items = items.reset_index(drop=True)

    photo_txt = "photo_" + str(category) + ".txt"
    for f, txt in zip([photo_txt], [items]):
        with open(f, "w") as text_file:
            for row in txt.values:
                text_file.write("{0}\n".format(str(long(row))))
    return


def get_word_vector(SIZE=100):
    """
    讀取詞向量
    :param SIZE: word vector 样本量
    :return: wordID: list, 單字
    :return: word_vectors: list, list of vectors that correspond to each word in wordID
    """
    # Get word vectors
    # Word2Vec
    with open("wordid") as f:
        word_ids = [f.readline().replace(b"\r", b"").replace(b"\n", b"") for i in xrange(10000)]
        # word_ids = f.read().replace(b"\r", b"").split(b"\n")[0:100000]
        word_ids = pandas.Series(word_ids).str.split(b"\t")
        word_ids = list(word_ids.str[0])
        word_ids = smart_decode(word_ids)

    with open("WordVectors.txt") as f:
        word_vectors = [f.readline().replace(b"\r", b"").replace(b"\n", b"") for i in xrange(10000)]
        # word_vectors = f.read().replace(b"\r", "").split(b"\n")[:100000]
        word_vectors = [pandas.Series(vec.split(b"\t")).astype(float) for vec in word_vectors]

    return word_ids, word_vectors


def create_dummy_attr(file_name_prefix, category):
    """
    生成維度值的亞變量
    :param file_name_prefix: str, eg: attr_train_full
    :param category: int
    :return: DataFrame
    """
    # need encoding so that we can compare it with unicode(材质成分)
    attr = pandas.read_csv("{0}{1}.csv".format(file_name_prefix, category), encoding="utf-8-sig")
    # 用attr.drop會出現error
    attr = attr.ix[:, attr.columns != "材质成分"]
    attr = attr.ix[:, attr.columns != "品牌"]

    dummy = pandas.DataFrame()
    for col in attr.columns[2:]:
        if sum(attr[col].notnull()) / float(
                len(attr[col])) >= 0.01:  # skip if over 99% of the content in the column are NAN
            #  turn col into dummy variables and ignore NAN
            dummy = pandas.concat([dummy, attr[col].str.get_dummies(sep=",")], axis=1)
    dummy = pandas.concat([attr.iloc[:, 1], dummy], axis=1)

    return dummy


def generate_distance_df(attr, dummy, data_set, column_word_vector_dict, word_vectors, wordID, size, is_training_set=True):
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
    column_word_vector_dict = get_column_word_vector(dummy.columns[1:], word_vectors, wordID, column_word_vector_dict, size)
    word_vec = pandas.DataFrame(dummy.ix[:, 1])  # initiate with itemID

    col_name = 'all_avg'
    word_vec[col_name] = dummy.apply(
        all_average_word_vector, column_word_vector_dict=column_word_vector_dict, full_column_list=dummy.columns[2:],
        size=size, axis=1).ix[:, -1]

    word_vec = pandas.concat([word_vec, attr.apply(
        column_average_word_vector, column_word_vector_dict=column_word_vector_dict, full_column_list=attr.columns[2:],
        size=size, axis=1).ix[:, 2:]], axis=1)

    # 去除 nan(超過99%) 太多的維度
    for col in word_vec:
        threshold = 0.01
        if sum(word_vec[col].notnull()) / float(len(word_vec)) < threshold:
            word_vec.drop(col, inplace=True, axis=1)

    # 計算距離
    distance = pandas.DataFrame()
    for col in word_vec.columns[1:]:
        distance = pandas.concat(
            [distance, data_set.apply(calculate_distance, item_word_vector=word_vec, column=col, axis=1)], axis=1)

    distance = pandas.concat([data_set.ix[:, 1:3], distance], axis=1)  # Insert ItemID pair

    # 去除全為 nan 或 0 的維度
    for col in distance.columns:
        # if sum(distance[col].notnull()) == 0 or sum(distance[col]) < 0.1:
        #     distance.drop(col, inplace=True, axis=1)

        try:
            if sum(distance[col].notnull()) == 0:
                distance.drop(col, inplace=True, axis=1)
        except:
            print "Null:", col
        try:
            if sum(distance[col]) == 0:
                distance.drop(col, inplace=True, axis=1)
        except:
            print "Zeros:", col

    # Training set 需要有label
    if is_training_set:
        distance = pandas.concat([distance, data_set.ix[:, -1]], axis=1)  # Append Label
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
        if column not in word_vector_dict.keys():
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


def export_csv(df, filename, header=True):
    df.to_csv(filename, header=header, encoding="utf8")


def read_csv_data(is_training_set, category):
    """
    :param is_training_set: boolean
    :param category: int
    :returns:
        attr: 商品attributes
        dummy: attr的dummy variable版
        data:　商品對的Jaccard相似度
    """
    if is_training_set:
        attr_name = "attr_train_"
        data = pandas.read_csv("train_{0}.csv".format(category), encoding="utf8")
    else:
        attr_name = "attr_test_"
        data = pandas.read_csv("test_{0}.csv".format(category), encoding="utf8")
        data["Label"] = 0.0

    attr_name += "full_"
    attr = pandas.read_csv("{0}{1}.csv".format(attr_name, category), encoding="utf-8-sig")
    dummy = pandas.read_csv("{0}{1}_dummy.csv".format(attr_name, category), encoding="utf-8-sig")
    return attr, dummy, data


def separate_positive_negative(train, threshold=0.5):
    """
    拆解成正負例
    :param train: 訓練數據
    :param threshold: 閾值
    :return: 正負例
    """
    return train[train["Label"] > threshold]


def down_sample_testset(train_positive, test, ratio, is_random=False):
    """
    下採樣隨機商品對
    :param train_positive: 正例
    :param test: 全量負例
    :param ratio: 下採樣比例
    :param is_random: 是否設置random state
    :return: 下採樣之負例
    """
    if not is_random:
        numpy.random.seed(2017)
    random_index = numpy.random.randint(0, len(test), ratio * len(train_positive))
    sampled_test = test.iloc[random_index, :]
    sampled_test = sampled_test.reset_index(drop=True)

    return sampled_test


def generate_model_input(train_positive, sampled_test, train_distance_positive, test_distance):
    """
    生成最終模型使用的數據
    :param train_positive: 正例
    :param sampled_test: 下採樣之負例
    :param train_distance_positive: 正例之距離特徵
    :param test_distance: 負例之距離特徵
    :return:
        X: 正例+負例的特徵維度
        y: label
    """
    data = train_positive.iloc[:, 1:-1].append(sampled_test.iloc[:, 1:-1], ignore_index=True)  # skip labels first
    word_vec = train_distance_positive.append(test_distance, ignore_index=True).iloc[:, 2:]  # skip customer & competitor ID
    data = pandas.concat([data, word_vec], axis=1).fillna(0)
    X = data.drop(["Label", "ID_competitor", "ID_customer", "Unnamed: 0"], axis=1)  # skipped ID, label
    y = data["Label"]

    return X, y


if __name__ == "__main__":

    category = 1623
    # get_prediction_for_tagging(category, 6100, 3100)
    # get_photo_id(category)
    split_tagging_set(category, num_of_files=10, len_per_file=400)

    # from datetime import datetime
    # _attr_list = [u",风格:通勤,款式品名:小背心/小吊带,图案:纯色,领型:圆领,颜色分类:花色,颜色分类:黑色,颜色分类:白色,"
    #               u"颜色分类:深灰,颜色分类:浅灰,颜色分类:灰色,颜色分类:黄色,颜色分类:蓝色,颜色分类:绿色,颜色分类:西瓜红,"
    #               u"颜色分类:玫红,颜色分类:红色,上市年份季节:2015年秋季,组合形式:单件,厚薄:厚,厚薄:薄,厚薄:适中,"
    #               u"通勤:韩版,穿着方式:套头,衣长:常规,适用季节:春,适用季节:夏,适用季节:秋,适用季节:春夏,",
    #               u",款式品名:卫衣/绒衫,服装款式细节:口袋,领型:半开领,服饰工艺:立体裁剪,颜色分类:紫色,颜色分类:黄色,"
    #               u"颜色分类:酒红,颜色分类:红色,颜色分类:红黑,上市年份季节:2014年秋季,组合形式:两件套,厚薄:厚,厚薄:薄,"
    #               u"厚薄:适中,袖长:长袖,裤长:长裤,面料:棉,面料:聚酯,衣门襟:拉链,适用年龄:35-39周岁,袖型:常规,"
    #               u"服装版型:宽松,穿着方式:开衫,衣长:中长款,适用季节:春,适用季节:秋,"]
    #
    # print u"{0}开始测试tag_to_dict".format(datetime.now())
    # industry = u"mp_women_clothing"
    # _cid = 50000671
    # _attr_name = u"颜色分类"
    #
    # print u"{0}开始测试attributes_to_dict".format(datetime.now())
    # a1 = u",a:我,a:你,b:你,"
    # a2 = u",c:他,a:我,"
    # _attr1 = attributes_to_dict(a1)
    # _attr2 = attributes_to_dict(a2)
    # print str(_attr1)
    # print str(_attr2)
    # for k, v in _attr1.iteritems():
    #     print k, v
    # for k, v in _attr2.iteritems():
    #     print k, v
    #
    # print u"{0}开始测试make_similarity_feature".format(datetime.now())
    # _tag_dict = {u"a": [u"我", u"你"], u"b": [u"你"], u"c": [u"他"], u"d": [u"啥"]}
    #
    # from common.pickle_helper import pickle_load
    # from common.settings import *
    # _raw_tag_dict = pickle_load(TAG_DICT_PICKLE)
    #
    # _raw_attr1 = u",组合形式:单件,颜色分类:灰色,颜色分类:黄色,颜色分类:红色,服装版型:直筒,年份季节:2014年冬季," \
    #              u"材质成分:山羊绒(羊绒)100%,品牌:博依格,图案:纯色,风格:百搭,款式:背带,衣长:常规,适用年龄:25-29周岁,"
    # _raw_attr2 = u",组合形式:单件,服装版型:修身,年份季节:2015年秋季," \
    #              u"材质成分:聚对苯二甲酸乙二酯(涤纶)94% 聚氨酯弹性纤维(氨纶)6%,品牌:型色缤纷,图案:纯色,风格:通勤," \
    #              u"衣长:常规,"
    # _attr1 = attributes_to_dict(_raw_attr1)
    # _attr2 = attributes_to_dict(_raw_attr2)
    # _tag_dict = _raw_tag_dict[121412004]
    # fv = make_similarity_feature(_attr1, _attr2, _tag_dict, demo=False)
    # fv = make_similarity_feature(_attr1, _attr2, _tag_dict, demo=True)
    # print f
    #
    # print u"{0}parse_essential_dimension".format(datetime.now())
    # from db_apis import get_essential_dimensions
    # d = parse_essential_dimension(get_essential_dimensions(db=u"mp_women_clothing"))
    # print d.keys()

