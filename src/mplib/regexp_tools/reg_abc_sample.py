共10种情况
1
regexp_para = dict(
    center=dict(
        value="中心|心中",
        distance="",
        logic="不包含",
    ),
    head=dict(
        value="",
        distance="",
        logic="",
    ),
    tail=dict(
        value="",
        distance="",
        logic="",
    ),
)
test_case = [
    (u"心中", False),
    (u"是是是中不不不", True),
    (u"是是是中心中不不不", False)
]

2
regexp_para = dict(
    center=dict(
        value="中心|心中",
        distance="",
        logic="包含",
    ),
    head=dict(
        value="",
        distance="",
        logic="",
    ),
    tail=dict(
        value="",
        distance="",
        logic="",
    ),
)
test_case = [
    (u"心中", True),
    (u"是是是中不不不心", False),
    (u"是是是中心不不不", True)
]

3
regexp_para = dict(
    center=dict(
        value="中心",
        distance="",
        logic="包含",
    ),
    head=dict(
        value="",
        distance="",
        logic="",
    ),
    tail=dict(
        value="后缀|后面",
        distance="2",
        logic="包含",
    ),
)
test_case = [
    (u"中心", False),
    (u"是是是中心的后缀", True),
    (u"是是是中心的后面", True),
    (u"是是是中心不不不后缀", False)
]

4
regexp_para = dict(
    center=dict(
        value="中心",
        distance="",
        logic="包含",
    ),
    head=dict(
        value="",
        distance="",
        logic="",
    ),
    tail=dict(
        value="后缀|后面",
        distance="2",
        logic="不包含",
    ),
)
test_case = [
    (u"中心", True),
    (u"是是是中心的后缀", False),
    (u"是是是中心的后面", False),
    (u"是是是中心不不不后缀", True)
]

5
regexp_para = dict(
    center=dict(
        value="中心",
        distance="",
        logic="包含",
    ),
    head=dict(
        value="前缀|前面",
        distance="3",
        logic="包含",
    ),
    tail=dict(
        value="",
        distance="",
        logic="",
    ),
)
test_case = [
    (u"中心", False),
    (u"前缀中心是是是", True),
    (u"前缀是是中心的", True),
    (u"前缀是是是中心不不不", False)
]

6
regexp_para = dict(
    center=dict(
        value="中心",
        distance="",
        logic="包含",
    ),
    head=dict(
        value="前缀|前面",
        distance="3",
        logic="不包含",
    ),
    tail=dict(
        value="",
        distance="",
        logic="",
    ),
)
test_case = [
    (u"中心", True),
    (u"前缀中心是是是", False),
    (u"前缀是是中心的", False),
    (u"前缀是是是中心不不不", True)
]

7
regexp_para = dict(
    center=dict(
        value="中心",
        distance="",
        logic="包含",
    ),
    head=dict(
        value="前缀|前面",
        distance="3",
        logic="包含",
    ),
    tail=dict(
        value="后缀|后面",
        distance="2",
        logic="包含",
    ),
)
test_case = [
    (u"中心", False),
    (u"前缀中心后面", True),
    (u"前面是是中心的后缀", True),
    (u"前缀是是中心不不不", False),
    (u"前缀是是是中心不后缀", False),
    (u"前缀是是是中心不不后缀", False)
]

8
regexp_para = dict(
    center=dict(
        value="中心",
        distance="",
        logic="包含",
    ),
    head=dict(
        value="前缀|前面",
        distance="3",
        logic="包含",
    ),
    tail=dict(
        value="后缀|后面",
        distance="2",
        logic="不包含",
    ),
)
test_case = [
    (u"中心", False),
    (u"前缀中心", True),
    (u"前面是是中心的后缀", False),
    (u"前缀是是中心不不不", True),
    (u"前缀是是是中心不不后缀", False),
    (u"前缀是是是中心不后缀", False)
]

9
regexp_para = dict(
    center=dict(
        value="中心",
        distance="",
        logic="包含",
    ),
    head=dict(
        value="前缀|前面",
        distance="3",
        logic="不包含",
    ),
    tail=dict(
        value="后缀|后面",
        distance="2",
        logic="包含",
    ),
)
test_case = [
    (u"中心", False),
    (u"中心后缀", True),
    (u"前面是是中心的后缀", False),
    (u"前缀是是是中心不后缀", True),
    (u"前缀是是是中心不不后缀", False),
    (u"前缀是是中心不不后缀", False)
]

10
regexp_para = dict(
    center=dict(
        value="中心",
        distance="",
        logic="包含",
    ),
    head=dict(
        value="前缀|前面",
        distance="3",
        logic="不包含",
    ),
    tail=dict(
        value="后缀|后面",
        distance="2",
        logic="不包含",
    ),
)
test_case = [
    (u"中心", True),
    (u"中心后缀", False),
    (u"前面是是中心的后缀", False),
    (u"前缀是是是中心不不后缀", True),
    (u"前缀是是是中心不后缀", False),
    (u"前缀是是中心不不后缀", False)
]