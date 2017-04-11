# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from mplib.IO import PostgreSQL, Hive
import pandas
import numpy


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


def get_train_data(cid):
    sql = """
    SELECT DISTINCT
        c.SourceItemID AS ItemID1,
        c.TargetItemID AS ItemID2,
        a1.attr AS attr1,
        a2.attr AS attr2,
        c.score
    FROM elengjing.train_data_for_competitive_items AS c
    JOIN elengjing.women_clothing_item_attr_t AS a1
    ON c.SourceItemID = a1.itemid
    JOIN elengjing.women_clothing_item_attr_t AS a2
    ON c.TargetItemIDa = a2.itemid
    WHERE c.CategoryID = {cid}
    """.format(cid=cid)
    return


def get_predict_data():
    return


def do_word_to_vec():
    return


def get_distance():
    return


def construct_features():
    return


def construct_train_raw_feature(raw_data, tag_dict, labeling=True):
    """
    構造預測數據商品描述文字特徵，維度為此商品打標籤使用的特徵
    :param raw_data: DataFrame
    :param tag_dict: OrderedDict
    :param labeling: Boolean True:僅用有打標籤的維度 False: 使用所有維度
    :return: DataFrame
    """
    columns = list(tag_dict) + ["ItemID"]
    test_attr_df = pandas.DataFrame(columns=columns)

    for index in range(2):

        for data in raw_data:
            attr = attributes_to_dict(data[index])
            temp = pandas.DataFrame()

            # 去重複ID
            for key in list(attr):
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
                temp[list(attr)[0]] = [numpy.nan]
            temp["ItemID"] = long(data[index+3])
            test_attr_df = test_attr_df.append(temp)

    test_attr_df["ItemID"] = test_attr_df["ItemID"].astype(long)
    return test_attr_df


def do_essential_trick():
    return


def do_important_trick():
    return


def serialize():
    return


if __name__ == "__main__":
    get_attribute_meta()
