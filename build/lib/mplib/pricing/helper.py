# coding: utf8
# __author__: "John"
from __future__ import unicode_literals, absolute_import, print_function, division
from mplib.common.helper import get_print_var, vector_reshape_to_matrix as vrm
from mplib.common import smart_encode
from os.path import dirname, join, splitext
from os import listdir
from sklearn import model_selection
from mplib import *
import pandas
import numpy


def split_id_feature(data):
    item_list = []
    feature_list = []
    for line in data:
        item_list.append(line[0])
        feature_list.append(numpy.array(line[1].split(",")).astype(float))
    return item_list, numpy.array(feature_list)


def split_x_y(data):
    return data[:, 1:], data[:, 0]


def get_file_name(file_name):
    return splitext(file_name)[0]


def get_items(nrows=1000, cid=50008899, path=None):
    if train_test_splited(exists_source(cid, path)):
        return get_train_test(nrows, cid, path)
    else:
        return split_train_test(nrows, cid, path)


def exists_source(cid, path):
    path = path if path else __file__
    sources = list(filter(lambda x: "{0}_pricing.csv".format(cid), listdir(join(dirname(path), "data"))))
    if sources:
        return sources
    else:
        raise IOError("File {0}_pricing.csv does not exist".format(cid))


def train_test_splited(sources):
    train = list(filter(lambda x: "train" in x, sources))
    test = list(filter(lambda x: "test" in x, sources))
    if train and test:
        return True
    else:
        return False


def get_train_test(nrows, cid, path=None):
    path = path if path else __file__
    prefix = join(dirname(path), join("data", "{0}_pricing".format(cid)))
    x_train = pandas.read_table("{0}_x_train.csv".format(prefix), sep=",", nrows=nrows).values
    x_test = pandas.read_table("{0}_x_test.csv".format(prefix), sep=",", nrows=nrows).values
    y_train = pandas.read_table("{0}_y_train.csv".format(prefix), sep=",", nrows=nrows).values
    y_test = pandas.read_table("{0}_y_test.csv".format(prefix), sep=",", nrows=nrows).values

    train_size = int(nrows * 0.8)
    test_size = int(nrows * 0.2)

    return x_train[:train_size], x_test[:test_size], y_train[:train_size], y_test[:test_size]


def split_train_test(nrows, cid, path=None):
    path = path if path else __file__
    df = pandas.read_table(
        filepath_or_buffer=join(dirname(path), join("data", "{0}_pricing.csv".format(cid))),
        sep=",",
        nrows=nrows,
        header=None,
        error_bad_lines=False
    ).fillna(0)
    df = df[df[0] >= 100].values
    x_train, x_test, y_train, y_test = model_selection.train_test_split(
        df[:, 1:], df[:, 0], test_size=0.2, random_state=42)
    del df

    prefix = join(dirname(path), join("data", "{0}_pricing".format(cid)))
    numpy.savetxt("{0}_x_train.csv".format(prefix), x_train, delimiter=b",")
    numpy.savetxt("{0}_x_test.csv".format(prefix), x_test, delimiter=b",")
    numpy.savetxt("{0}_y_train.csv".format(prefix), y_train, delimiter=b",")
    numpy.savetxt("{0}_y_test.csv".format(prefix), y_test, delimiter=b",")
    return x_train, x_test, y_train, y_test


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
        "metric_mae",
        "metric_mse",
        "metric_r2",
        "metric_e5",
        "metric_e10",
        "metric_e15",
        "metric_e20",
        "run_time",
    ]


def logging_process(locals_var):
    var_dict = get_print_var(locals_var, gen_print_var())
    info = ",".join([str(v) for k, v in iteritems(var_dict)])
    with open(get_experiment_logs_path("experiment_logs.csv"), mode="a") as f:
        f.write(smart_encode(info + "\n"))


def get_experiment_logs_path(file_name):
    return join(dirname(__file__), join("logs", file_name))


def gen_experiment_logs_head():
    info = ",".join(gen_print_var())
    with open(get_experiment_logs_path("experiment_head.csv"), mode="w") as f:
        f.write(smart_encode(info + "\n"))


def gen_experiment_logs_table():
    with open(get_experiment_logs_path("experiment_head.csv")) as fh:
        head = fh.readline().replace("\n", "").replace("\r", "").split(",")
    with open(get_experiment_logs_path("experiment_logs.csv")) as fl:
        logs = [line.replace("\n", "").replace("\r", "").split(",") for line in fl]
    df = pandas.DataFrame(data=logs, columns=head)
    df.to_excel(get_experiment_logs_path("experiment_table.xls"), encoding="utf8", index=False)


def drop_exp_columns(array, feature_number=7, pow_number=5):
    for f in range(feature_number):
        for p in range(1, pow_number):
            array = numpy.delete(array, f + 1, 1)
    return array


def drop_zero_columns(array):
    return array[:, (array != 0).sum(axis=0) >= 1]


def y_approximation(vector):
    return 10 * numpy.ceil(vector / 10)


def get_hive_train_data(category_id, is_event=False):
    from mplib.IO import Hive
    sql = """
    SELECT data AS data
    FROM elengjing_price.train_{category_id}_{model_type}
    ORDER BY RAND(1000000)
    LIMIT 100000
    """.format(category_id=category_id, model_type="event" if is_event else "daily")
    lines = [line.get("data") for line in Hive("idc").query(sql)]
    train_x = []
    train_y = []
    for line in lines:
        line = [float(num) for num in line.split(",")]
        train_x.append(line[1:])
        train_y.append(line[0])
    return numpy.nan_to_num(numpy.array(train_x)), vrm(numpy.nan_to_num(numpy.array(train_y)))


def execute_hive_predict_data(category_id, is_event=False, show_sql=False, udf="udf_item_pricing13.py"):
    from mplib.IO import Hive
    sql = """
        USE elengjing_price;
        ADD FILE /home/script/normal_servers/serverudf/elengjing/{udf};

        DROP TABLE IF EXISTS tmp_{category_id};
        CREATE TABLE tmp_{category_id} (
            itemid STRING,
            data STRING
        )CLUSTERED BY (itemid) INTO 113 BUCKETS
        STORED AS ORC;

        INSERT INTO tmp_{category_id}
        SELECT
            i.itemid,
            CONCAT_WS(
                ',',
                NVL(CAST(i.avg_price AS STRING), '0'),
                NVL(CAST((UNIX_TIMESTAMP() - TO_UNIX_TIMESTAMP(i.listeddate)) / 86400.0 AS STRING), '0'),
                NVL(CAST(shop.level AS STRING), '0'),
                NVL(CAST(shop.favor AS STRING), '0'),
                NVL(CAST(s.all_spu AS STRING), '0'),
                NVL(CAST(s.all_sales_rank AS STRING), '0'),
                NVL(CAST(sc.category_spu AS STRING), '0'),
                NVL(CAST(sc.category_sales_rank AS STRING), '0'),
                NVL(CAST(s.shop_avg_price AS STRING), '0'),
                NVL(CAST(s.shop_avg_price_rank AS STRING), '0'),
                NVL(CAST(sc.category_avg_price AS STRING), '0'),
                NVL(CAST(sc.category_avg_price_rank AS STRING), '0'),
                v.data
            ) AS data
        FROM (
            SELECT
                shopid, itemid,
                MAX(listeddate) AS listeddate,
                CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS avg_price
            FROM elengjing.women_clothing_item
            WHERE categoryid = {category_id}
            GROUP BY
                shopid, itemid
        ) AS i
        JOIN (
            SELECT
                shopid,
                COUNT(DISTINCT itemid) AS all_spu,
                CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS shop_avg_price,
                ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS all_sales_rank,
                ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS shop_avg_price_rank
            FROM elengjing.women_clothing_item
            GROUP BY
                shopid
        ) AS s
        ON i.shopid = s.shopid
        JOIN (
            SELECT
                shopid,
                COUNT(DISTINCT itemid) AS category_spu,
                ROW_NUMBER() OVER(ORDER BY SUM(salesamt) DESC) AS category_sales_rank,
                ROW_NUMBER() OVER(ORDER BY CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END DESC) AS category_avg_price_rank,
                CASE WHEN SUM(salesqty) = 0 THEN 0 ELSE SUM(salesamt) / SUM(salesqty) END AS category_avg_price
            FROM elengjing.women_clothing_item
            WHERE categoryid = {category_id}
            GROUP BY
                shopid
            ) AS sc
        ON i.shopid = sc.shopid
        JOIN elengjing_base.shop AS shop
        ON i.shopid = shop.shopid
        JOIN elengjing_price.attr_itemid_value_matrix_{category_id} AS v
        ON i.itemid = v.itemid;

        DROP TABLE IF EXISTS predict_{category_id}_{model_type};
        CREATE TABLE predict_{category_id}_{model_type} (
            itemid STRING,
            price DECIMAL(20,2)
        ) STORED AS ORC;

        INSERT INTO predict_{category_id}_{model_type}
        SELECT TRANSFORM(itemid, data)
        USING 'python {udf} {category_id} {model_type}' AS (itemid, price)
        FROM tmp_{category_id};

        DROP TABLE IF EXISTS tmp_{category_id};
        """.format(category_id=category_id, model_type="event" if is_event else "daily", udf=udf)
    if show_sql:
        print(sql)
    else:
        Hive("idc").execute(sql)


def get_all_category(industry_id=16):
    from mplib.IO import PostgreSQL
    sql = "SELECT id FROM category WHERE industry_id = {0} AND is_leaf = 't'".format(industry_id)
    lines = PostgreSQL("v2_uat").query(sql)
    return [str(line.get("id")) for line in lines]


if __name__ == "__main__":
    print(__file__)
