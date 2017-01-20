# coding: utf-8
# __author__: u"John"
"""
This module contains common used trees in mp
"""
from __future__ import unicode_literals
from unicode_tools import smart_decode
from collections import defaultdict


def default_branch(text=""):
    return dict(text=text, children=[],)


def attach(tag_name, trunk, current_tag="", sep="."):
    """
    对传入的字典生成递归树形结构:
    {
        text = "",
        children = [
            {
                text = "",
                children = []
            }
        ]
    }
    :param tag_name: 正在处理的标签名
    :param trunk: 当前生成的树
    :param current_tag: 上一次填充在text字段的值
    :param sep: 标签名的指定分隔符
    :return:
    """
    def get_children_text(children):
        if children:
            return [child.get("text") for child in children]
        else:
            return []

    parts = tag_name.split(sep, 1)
    if len(parts) == 1:  # 到最后的节点了
        if current_tag:
            current_tag = "{0}.{1}".format(current_tag, parts[0])
        else:
            current_tag = parts[0]
        trunk["children"].append(default_branch(current_tag))
    else:
        node, others = parts
        if current_tag:
            node = "{0}.{1}".format(current_tag, node)

        # 如果这个节点之前没有出现过, 则append一个新的节点到children里, 对生成的树进行递归
        if node not in get_children_text(trunk.get("children")):
            trunk["children"].append(defaultdict(dict, default_branch(node)))
            children_index = trunk.get("children").index(default_branch(node))
        # 如果这个节点之前出现过, 则将出现过的那个节点作为新生成的树进行递归
        else:
            children_index = get_children_text(trunk.get("children")).index(node)
        attach(others, trunk.get("children")[children_index], node, sep)


def tag_dtype_check(tags):
    if isinstance(tags, str) or isinstance(tags, unicode):
        return [smart_decode(tags)]
    elif isinstance(tags, list):
        if not tags:
            return False
        for tag in tags:
            if isinstance(tag, str) or isinstance(tag, unicode):
                pass
            elif isinstance(tag, list) or isinstance(tag, dict) or isinstance(tag, tuple):
                raise TypeError("Only none-nested list could be processed, detect your element type is {0}".format(type(tag)))
            else:
                raise TypeError("Only basestring in list could be processed, detect your element type is {0}".format(type(tag)))
        return tags
    else:
        raise TypeError("Only basestring or list could be processed, detect your type is {0}".format(type(tags)))


def tag_to_tree(tags=list()):
    """
    生成标签数的封装
    :param tags: list::[tag1, tag2...]
    :return:
    """
    tags = tag_dtype_check(tags)
    if not tags:
        return default_branch()

    tree = defaultdict(dict, default_branch())
    for tag in tags:
        attach(tag, tree)
    return tree

if __name__ == "__main__":
    t = tag_to_tree(["品牌.奶粉.惠氏", "品牌.达能.奶粉", "属性.颜色.白色"])
    import json
    print json.dumps(t, indent=4, separators=(",", ":"))

