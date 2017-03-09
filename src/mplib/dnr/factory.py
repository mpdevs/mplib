# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
from mplib.common import smart_decode, time_elapse, smart_encode
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.naive_bayes import MultinomialNB
import editdistance
import traceback
import pandas
import jieba
import json
import re


class DataDenoiser(object):
    """
    数据降噪
    传入数据有格式要求
    list(list()) 其中list可以被替换成tuple, 别的不行
    所有去水工作都是逐行处理
    """
    def __init__(self, data, content_index=0, head=None):
        self.head = self.get_head(head)
        self.data = self.process(data)
        self.line = None
        self.content_index = content_index
        self.content = ""
        self.noise_tag_column_name = "is_noise"
        self.is_noise_line = False
        self.noise_keywords_list = [
            "分享",
            "真的",
            "新歌",
            "红包",
            "小伙伴",
            "围观",
            "领取",
            "加入",
            "升级",
            "女神",
            "提供",
            "速度",
            "快来",
            "美女",
            "惊喜",
            "丰富",
            "激动",
            "表现",
            "少年",
        ]
        self.noise_tag_emoji = "\[.*?]"
        self.noise_tag_brackets = "【.*?】|★|◆"
        self.noise_tag_label = "#.*?#"
        self.noise_tag_count = 3
        self.noise_tag_char = 10
        self.noise_length_unicode = "[\u4e00-\u9fa5]"
        self.noise_length_min = 5
        self.noise_length_max = 500
        self.noise_series_list = ["[1-9][.|:|、][\u3007\u4E00-\u9FCB\uE815-\uE864]|[\u2460-\u2469]"]
        self.noise_series_threshold = 4
        self.noise_client_list = [
            "粉丝红包",
            "vivo智能手机",
            "微博等级",
            "IFTTT_Official",
            "应用广场",
            "明星势力榜",
            "IN",
            "openapi监控",
            "无线会员中心",
            "今日头条",
            "新浪长微博",
            "投票",
            "微问",
            "格瓦拉电影",
            "优酷土豆",
            "微博客户端",
            "微博位置",
            "2014刷刷没想到",
            "Q蒂_开心消消乐",
            "微游戏",
            "爱图购官网",
            "King_Queen游戏",
            "新浪新闻iPhone客户端",
            "喜马拉雅网",
            "新浪爱猜",
            "实惠",
            "红包锁屏app",
            "百词斩图背单词",
            "网易新闻客户端",
            "互粉大厅",
            "香港新浪微博主頁",
            "美团手机客户端",
            "网易云音乐",
            "平台测试",
            "爱问知识人",
            "微博之夜",
            "微卡券",
            "荔枝FM",
            "新浪新闻评论",
            "新浪新闻Android版",
            "喜马拉雅听我想听",
            "去动",
            "虾米音乐",
            "91duan短网址应用",
            "美分享",
            "豆瓣电影",
            "微博电影",
            "风水百科",
            "weipai微拍",
            "左岸格子",
            "搜狐新闻客户端",
            "新闻资讯App",
            "百词斩",
            "i看",
            "虾米音乐移动版",
            "保卫萝卜2",
            "小清新范",
            "勋章馆",
            "5pv土豪金",
            "足球大师",
            "58同城客户端",
            "GZ移动频道",
            "优酷网连接分享",
            "创意达人",
            "小影",
            "Tmall_Android客户端",
            "街机千炮捕鱼",
            "喜试网",
            "移动微店",
            "必应美图分享",
            "美文心语",
            "手机酷狗iPhone版",
            "保卫萝卜2安卓版",
            "不背单词",
            "小清新唯美风",
            "58_淘房淘二手找工作",
            "钱咖官网",
            "魅族官网",
            "微博电视指数",
            "我不愿将就",
            "搜狐视频",
            "美容达人",
            "酷狗音乐",
            "影视达人推荐",
            "Mtime时光网",
            "旅游达人推荐",
            "乐视网",
            "Nikepluschina",
            "凤凰视频一键转帖",
            "21CN新闻",
            "Princess公主部落",
            "天天动听高品质音乐",
            "我和小伙伴的2014",
            "秒拍安卓版",
            "微商铺",
            "唯美小调",
            "秒拍客户端",
            "美女达人",
            "知乎网",
            "忍将OL",
        ]
        self.noise_client_label = ">[^@]*<"
        self.valid_unicode = "[\u3007\u4E00-\u9FCB\uE815-\uE864]"
        self.noise_edit_distance_threshold = 5
        self.split_data_set = False
        self.use_keywords = False
        self.use_tag = False
        self.use_length = False
        self.use_series = False
        self.use_client = False
        self.use_special_characters = False
        self.use_edit_distance = False
        self.row_index = 0
        self.work_flow_list = []
        self.udf_support = False

    @staticmethod
    def process(data):
        if isinstance(data, list) or isinstance(data, tuple):
            return smart_decode(data, cast=True)
        elif isinstance(data, pandas.DataFrame):
            return smart_decode(data.values.tolist(), cast=True)

    def get_head(self, head):
        if isinstance(head, list) or isinstance(head, tuple):
            return smart_decode(head)
        elif head is None:
            return

    def get_content(self, line, content_index):
        try:
            content = line[content_index]
            if not content: self.skip_the_noise_line()
        except IndexError:
            self.error_info()
            content = ""
        return content

    def noise_list_loop(self, noise_list, match_count_threshold=1):
        for noise in noise_list:
            try:
                match_list = re.findall(re.compile(pattern=noise, flags=0), self.content)
                if len(match_list) > match_count_threshold - 1:
                    self.skip_the_noise_line()
                    break
                else:
                    continue
            except:
                self.error_info()
                continue

    def find_noise_keywords(self):
        """
        满足特定关键词条件的文本即为噪声数据
        :return:
        """
        self.noise_list_loop(self.noise_keywords_list)

    def find_noise_tag(self):
        try:
            noise_tag_count = 0
            noise_tag_char = 0
            hot_match_list = re.findall(re.compile(self.noise_tag_label), self.content)
            brackets_match_list = re.findall(re.compile(self.noise_tag_brackets), self.content)
            emoji_match_list = re.findall(re.compile(self.noise_tag_emoji), self.content)

            for match_list in [hot_match_list, emoji_match_list]:
                for match in match_list:
                    noise_tag_count += len(re.findall(re.compile(self.valid_unicode), match))

            noise_tag_char += len(hot_match_list) + len(brackets_match_list) * self.noise_tag_count

            char_list = re.findall(re.compile(self.valid_unicode), self.content)
            noise_tag_char = len(char_list) - noise_tag_char

            if noise_tag_char < self.noise_tag_char or noise_tag_count >= self.noise_tag_count:
                self.skip_the_noise_line()

        except:
            self.error_info()

    def find_noise_length(self):
        numbers = len(re.findall(re.compile(self.noise_length_unicode), self.content))
        if numbers < self.noise_length_min or numbers > self.noise_length_max:
            self.skip_the_noise_line()

    def find_noise_series(self):
        self.noise_list_loop(self.noise_series_list, match_count_threshold=self.noise_series_threshold)

    def find_noise_client(self):
        """
        由特定发出的客户端, 则是噪声数据(应用于微博)
        :return:
        """
        try:
            match_list = re.findall(re.compile(pattern=self.noise_client_label), self.content)
            match_list = match_list[0].strip(u"><") if match_list else ""
            if match_list in self.noise_client_list:
                self.skip_the_noise_line()
        except:
            self.error_info()

    def find_noise_special_characters(self):
        pass

    def find_noise_edit_distance(self):
        text_max_length = len(self.content)
        max_length_text_index = self.row_index
        for row_index in xrange(self.row_index, len(self.data)):
            if self.data[row_index][-1] == "True" or self.line[-1] == "True": continue  # 跳过已经标注为噪声的数据
            if row_index == self.row_index: continue  # 跳过自己
            content = self.get_content(line=self.data[row_index], content_index=self.content_index)
            if editdistance.eval(self.content, content) < self.noise_edit_distance_threshold:  # 编辑距离小于阈值的记为噪声数据
                if len(content) > text_max_length:  # 需要保留最长的文本
                    text_max_length = len(content)
                    max_length_text_index = row_index
                else:
                    self.data[row_index][-1] = "True"
        if max_length_text_index == self.row_index:
            self.line[-1] = "False"
        else:
            self.skip_the_noise_line()
            self.data[max_length_text_index][-1] = "False"

    # def bayes_classifier(self):
    #     def string_preprocess(string):
    #         raw_string = string
    #         http_info = re.compile("[a-zA-z]+://[^\s]*")
    #         string_without_http = http_info.sub(r"链接", raw_string)
    #         at_info = re.compile(r"@[^ @，,。.]*")
    #         string_without_http_and_at = at_info.sub(ur"@", string_without_http)
    #         number_eng_info = re.compile(r"[0-9|a-zA-Z]")
    #         clean_string = number_eng_info.sub("", string_without_http_and_at)
    #         return clean_string
    #     self.content = string_preprocess(self.content)
    #     v = TfidfVectorizer(tokenizer=lambda x: jieba.cut(x), analyzer="word", stop_words=stop_words)
    #     pass

    def skip_the_noise_line(self):
        self.line[-1] = "True"
        self.is_noise_line = True

    def generate_work_flow(self):
        if self.use_keywords: self.work_flow_list.append(self.find_noise_keywords)
        if self.use_length: self.work_flow_list.append(self.find_noise_length)
        if self.use_client: self.work_flow_list.append(self.find_noise_client)
        if self.use_series: self.work_flow_list.append(self.find_noise_series)
        if self.use_tag: self.work_flow_list.append(self.find_noise_tag)
        if self.use_edit_distance: self.work_flow_list.append(self.find_noise_edit_distance)

    def start_work_flow(self):
        self.is_noise_line = False
        for work_flow in self.work_flow_list:
            if not self.is_noise_line: work_flow()
        self.data[self.row_index] = self.line

    def error_info(self):
        if not self.udf_support:
            print "self.line = {}".format(self.line)
            print "self.content = {}".format(self.content)
            print traceback.print_exc()

    def run(self):
        """
        文本格式的处理
        :return:
        """
        self.data = [[] * len(self.head) + ["False"] if not self.line else self.line + ["False"] for self.line in self.data]
        self.generate_work_flow()
        for self.row_index, self.line in enumerate(self.data):
            self.content = self.get_content(line=self.line, content_index=self.content_index)
            self.start_work_flow()
            print "\t".join([self.line[0], self.line[-1]])
        # print "data size: {0}, noise size: {1}".format(len(self.data), len(filter(lambda x: x[-1] == "True", self.data)))


if __name__ == "__main__":
    from mplib.IO import MySQL
    d = DataDenoiser([])
    j = json.dumps(dict(noise_client_list=",".join(d.noise_client_list)))
    db = MySQL()
    db.execute("insert into das.process_parameters (name, platform, process_type, parameter_type, json_value, create_time, update_time) values ('微博客户端去水', 'weibo', 'common', 'client', '{0}', now(), now());".format(j))

