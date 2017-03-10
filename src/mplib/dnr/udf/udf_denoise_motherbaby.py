# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
from mplib.dnr.factory import DataDenoiser
from mplib.common import smart_decode
import traceback
import sys

reload(sys)
sys.setdefaultencoding("utf8")

keywords = [
    "此用户暂时被停用",
    "(\【|\】|\〖|\〗|\《|\》)",
    "\#\d*\W*\d*\W*\d*\#",
    "http\:\/\/t\.cn",
    "(①|②|③|1\.|2\.|1\、|2\、|图1|图2|1\）| 2\）|一\、|二\、|三\、)",
    "顶上去",
    "预订",
    "询价",
    "情感好文",
    "最好的选择",
    "看(的|得)到(的)*效果",
    "在线QQ",
    "代购",
    "澳洲销售",
    "大促销",
    "试用装",
    "包邮",
    "有意电联",
    "联系人",
    "赶紧行动",
    "选购",
    "赠品",
    "正品",
    "直邮",
    "热销",
    "特价",
    "限时",
    "微信关注",
    "全新未拆封",
    "微信号(:|：)*\d+",
    "(咨询|QQ|qq|微信|全文|链接|优惠|适合人群|电话|关注|微信号|群号|群)(\：|\:)",
    "官网查询",
    "购买(地址|链接)",
    "(本|此|这)(品|药|店|疗法|款)",
    "是\D{0,3}家喻户晓的",
    "由祖传秘方",
    "(来|断|现)货",
    "批发",
    "代理",
    "率先引进",
    "(V|v|微|wei)(信|商)",
    "详情(请)*点击",
    "发货",
    "http\:\/\/tieba\.baidu\.com\/",
    "无效退款",
    "友情价",
    "666666",
    "https\:\/\/item\.taobao\.com\/",
    "牛牛牛牛牛牛牛牛牛牛牛牛牛牛牛牛牛牛牛牛",
    "联系方式",
    "我是雷锋",
    "快试试吧，可以对自己使用挽尊卡咯",
    "网号",
    "加个q友",
    "转走",
    "适合你",
    "预定",
    "新品",
    "买手",
    "宜忌",
    "解决女人",
    "配制",
    "加卫星",
    "专属",
    "100%放心",
    "安全有效",
    "放心使用",
    "秘制",
    "添加剂",
    "专为孕妇",
    "温馨提示",
    "上架",
    "货到",
    "便宜转",
    "新款",
    "放心享用",
    "有货",
    "转载",
    "小说",
    "娱乐",
    "肌肤",
    "转发",
    "直邮",
    "药业",
    "最新发现",
    "研究指出",
    "数据证明",
    "研究发现",
]


try:
    data = [line for line in sys.stdin]
    data = map(lambda x: smart_decode(x).replace("\n", " ").replace("\r", " ").replace("\\N", "").split("\t"), data)
    dd = DataDenoiser(data=data, content_index=1, head=["id", "content"])
    dd.use_keywords = True
    dd.noise_keywords_list = keywords
    dd.use_length = True
    dd.noise_length_min = 0
    dd.noise_length_max = 500
    dd.udf_support = True
    dd.run()
except Exception as e:
    print "\t".join(["ERROR", traceback.format_exc().replace("\t", " ").replace("\n", " ")])
