# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
from mplib.common import time_elapse
from factory import DataDenoiser
import pandas
import json
import io


@time_elapse
def generic_denoise(
        file_path,
        use_keywords=False,
        use_series=False,
        use_tag=False,
        use_length=False,
        use_client=False,
        use_edit_distance=False,
        use_special_character=False):
    head, data = read_text_file(file_path)
    dd = DataDenoiser(data=data, head=head, content_index=2)
    dd.use_keywords = use_keywords
    dd.use_series = use_series
    dd.use_edit_distance = use_edit_distance
    dd.use_tag = use_tag
    dd.use_length = use_length
    dd.use_client = use_client
    dd.use_special_characters = use_special_character
    dd.run()
    write_text_file("processed_{0}".format(file_path), head, dd.data)


def domain_denoise():
    pass


def udf_generic_denoise():

    pass


@time_elapse
def read_text_file(file_path, limit=2000):
    with io.open(file_path, encoding="utf8") as f:
        head = f.readline().replace("\n", "").replace("\r", "").split("\t")
        data = [line.replace("\n", "").replace("\r", "").split("\t") for line in f]
        data = data[:limit] if limit else data
    return head, data


@time_elapse
def write_text_file(file_path, head, data):
    with io.open(file_path, mode="w", encoding="utf8") as f:
        f.write("\t".join(head) + "\n")
        for line in data:
            f.write("\t".join(line) + "\n")


def das_denoise(data, para_json):
    """
    Hive on Spark的分布式环境中的api
    数据由sys.stdin传入, 参数由mysql的json_value来决定
    :return: 已经配置好参数的降噪类创建的对象
    """
    para_dict = json.loads(para_json)
    dd = DataDenoiser(data=data, content_index=1, head=["id", "content"])
    dd.use_keywords = para_dict.get("use_keywords", False)
    dd.noise_keywords_list = para_dict.get("noise_keywords_list") if para_dict.get("noise_keywords_list") else dd.noise_keywords_list
    dd.use_series = para_dict.get("use_series", False)
    dd.use_tag = para_dict.get("use_tag", False)
    dd.use_length = para_dict.get("use_length", False)
    dd.noise_length_min = para_dict.get("noise_length_min") if para_dict.get("noise_length_min") else dd.noise_length_min
    dd.use_client = para_dict.get("use_client", False)
    dd.noise_client_label = para_dict.get("noise_client_label") if para_dict.get("noise_client_label") else dd.noise_client_label
    dd.noise_client_list = para_dict.get("noise_client_list") if para_dict.get("noise_client_list") else dd.noise_client_list
    dd.use_edit_distance = para_dict.get("use_edit_distance", False)
    dd.noise_edit_distance_threshold = para_dict.get("noise_edit_distance_threshold") if para_dict.get("noise_edit_distance_threshold") else dd.noise_edit_distance_threshold
    dd.use_special_character = para_dict.get("use_special_character", False)
    dd.udf_support = True
    dd.run()


if __name__ == "__main__":
    # generic_denoise(file_path="weibo1.txt", use_keywords=True, use_length=True, use_edit_distance=True)
    d = [
        dict(a="1", b="2", c="0"),
        dict(a="3", b="4", c="1"),
        dict(a="5", b="6", c="0"),
        dict(a="7", b="8", c="1"),
    ]
    f = pandas.DataFrame(d)
    l = DataDenoiser(f)
    l.run()
