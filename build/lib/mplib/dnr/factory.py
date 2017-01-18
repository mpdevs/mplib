# coding: utf-8
# __author__: u"John"
from __future__ import division
from mplib.common import AttributeDict
from mplib.common import to_unicode
from glob import glob
from os import path
import pandas as pd
import re


# region 可复用基本类 可以得到文件名，文件拓展，还有一个加序号的方法
class BaseReducer(object):
    """
    变量初始化设定是可以复用的代码
    """
    def __init__(self):
        self.code_message = AttributeDict(code=0, message=u"")
        self.dict_abspath_list = []  # 预留词库类文件批量处理控制变量
        self.current_dict_abspath = None  # 当前正在处理词库的文件
        self.data_abspath_list = []  # 预留数据类文件批量处理控制变量
        self.current_data_abspath = None  # 当前正在处理数据的文件
        self.file_extension = None  # 当前文件的扩展名
        self.export_name = None  # 导出文件的命名
        self.export_head = None  # 导出文件的列名
        self.id_column_clean_index = None  # 指定需要处理的列索引号，起始号为0
        self.data_column_index = None  # 数据有效列标志位
        self.keywords_list = []  # 关键词词库
        self.current_keywords = None  # 正在使用的关键词或正则表达式
        self.result_list = []  # 匹配到关键词的水帖列表，用于关键词正确性检验
        self.current_result = None  # 当前正在使用的关键词
        self.error_list = []  # 用于异常处理的错误列表
        self.current_error = None  # 当前错误信息
        self.show_process = False  # 是否显示关键词匹配情况
        self.raw_list = []  # 处理前的数据
        self.current_string = None  # 正在处理的字符串
        self.trash_list = []  # 水军数据
        self.cleaned_list = []  # 去水之后的数据
        self.has_header = True  # 读入的数据是否有表头
        self.clean_index = []
        self.trash_index = []
        self.save_file_path = ''
        self.header = ''
        self.data_column_name = ''
        return

    @staticmethod
    def get_file_extension(file_path):
        return path.splitext(file_path)[1][1:]
        # splitext分离拓展名

    @staticmethod
    def get_file_name(file_path):
        return path.splitext(path.basename(file_path))[0]

    @staticmethod
    def one_column_clean_indexer(data_list):
        """
        将单列的数据变成带序号的数据
        :param data_list:  list(unicode)
        :return:  list(tuple(index, unicode))
        """
        ret = []
        for line in xrange(len(data_list)):
            ret.append((line + 1, data_list[line]))
        return ret

    def get_contents(self):
        """
        可以通过外部赋值设置值，也可以通过别的方法实现
        数据库的操作后将数据赋值给 self.raw_list
        :return:
        """
        if self.raw_list:
            self.data_column_index = self.data_column_name
            self.header = []
            return
        else:
            pass
        if self.current_data_abspath:
            self.file_extension = self.get_file_extension(self.current_data_abspath)
            self.export_name = self.get_file_name(self.current_data_abspath)
            if self.file_extension == u"txt":
                try:
                    f = open(self.current_data_abspath, u"rb")
                    # 2进制模式打开
                except IOError as e:
                    self.current_error = str(e)
                    self.code_message.code = 1
                    self.code_message.message = u"get_contents open(file) IOError"
                    self.error_list.append(self.current_error)
                else:
                    with f:
                        if self.has_header:
                            self.header = f.readline().decode(u"utf-8").replace(u"\r", u"").replace(u"\n", u"").split(u"\t")
                            self.data_column_index = self.header.index(self.data_column_name)
                        else:
                            self.header = []
                            self.data_column_index = self.data_column_name
                        for line in f:
                            line = line.decode(u"utf-8").replace(u"\r", u"").replace(u"\n", u"").split(u"\t")
                            self.raw_list.append(line)
                        self.code_message.message = u"data file import successfully"
        else:
            self.raw_list = [(1, u"关键%词1"), (2, u"1这是%一%个正2"), (3, u"这2%是一个藏得很深%的2水贴，你检测%不3出来"),
                             (4, u"1%则%4表达%2式6%呢")]
        return
# endregion


# region 关键词去水
class KeywordsReducer(BaseReducer):
    """
    利用关键词去水的工具，支持关键词是正则表达式的情况
    """
    def __init__(self):
        BaseReducer.__init__(self)
        self.one_hit_strategy = True
        self.count_list = []

    def get_keywords(self):
        """
        数据库的操作后将数据赋值给 self.keywords_list
        :return:
        """
        if self.keywords_list:
            return
        else:
            pass
        if self.current_dict_abspath:
            self.export_name = u"{0}-{1}".format(self.export_name, self.get_file_name(self.current_dict_abspath))
            try:
                f = open(self.current_dict_abspath, u"rb")
            except IOError as e:
                self.current_error = str(e)
                self.code_message.code = 1
                self.code_message.message = u"get_keywords open(file) IOError"
                self.error_list.append(self.current_error)
            else:
                with f:
                    # self.keywords_list += f.read().decode(u"gb2312").split(u"\r\n")
                    keyword = f.read().decode(u"utf-8").strip().split(u"\r\n")
                    if keyword:
                        self.keywords_list += keyword
                    self.code_message.message = u"keywords dictionary import successfully"
        else:
            self.keywords_list = [u"关键词", u"(正|则|表|达|式)"]

        return

    def keywords_finder(self):
        """
        python的正则表达式可以直接支持中文
        :return:
        """
        try:
            pattern = re.compile(pattern=self.current_keywords, flags=0)
        except Exception as e:
            self.current_error = str(e)
            self.code_message.code = 2
            self.code_message.message = u"keywords_finder re.compile error"
            self.error_list.append(self.current_error)
            self.current_result = []
        else:
            self.current_result = re.findall(pattern, self.current_string)
        return

    def main(self):
        """
        复杂应用才需要Override这部分
        :return:
        """
        self.get_contents()
        self.get_keywords()
        self.count_list = [0] * len(self.keywords_list)
        for index in range(len(self.raw_list)):
            string = self.raw_list[index]
            self.current_string = to_unicode(string[self.data_column_index])
            hit = False
            # for keywords in self.keywords_list:
            try:
                for keywords_index in range(0, len(self.keywords_list)):
                    keywords = self.keywords_list[keywords_index]
                    self.current_keywords = keywords
                    self.keywords_finder()
                    if self.current_result:
                        self.trash_list.append(string)
                        self.result_list.append(self.current_result)
                        hit = True
                        self.count_list[keywords_index] += 1
                        if self.one_hit_strategy:
                            break
                        else:
                            continue
                    else:
                        continue
                if hit:
                    self.trash_index.append(index)
                    continue
                else:
                    self.clean_index.append(index)
                    self.cleaned_list.append(string)
            except:
                pass
        if self.show_process:
            total_length = float(len(self.raw_list))
            keyword_count = 0
            for count in self.count_list:
                keyword_count += count
            print u'關鍵詞標記微博數量為 ' + str(keyword_count) + u' 占' + str(keyword_count / total_length * 100) + '%'
            print u"{0}以下是關鍵詞標記的水贴{0}".format(u"-" * 30)
            for count_index in range(len(self.count_list)):
                print u'关键词 "' + self.keywords_list[count_index] + u'" 匹配的微博数量为 ' \
                      + str(self.count_list[count_index]) + u'  占' + \
                      str(self.count_list[count_index] / total_length * 100) + '%'
        # 在多数据文件或者多词库文件进行批量处理的时候需要对这些数据进行重置
        # self.raw_list = None
        # self.keywords_list = None
        return
# endregion


# region 之前的打标签去水工具
class TaggingReducer(BaseReducer):
    """
    利用打标签去水的工具
    """
    def __init__(self):
        BaseReducer.__init__(self)

    def main(self):
        """
        复杂应用才需要Override这部分
        :return:
        """
        self.export_name = self.get_file_name(self.current_data_abspath)
        tags_dir_path = self.current_dict_abspath + u'\\*'
        tags = glob(tags_dir_path)

        # self.get_contents()
        # raw_data = DataFrame(self.raw_list, columns=self.header)

        # 读入数据
        raw_data = pd.read_csv(self.current_data_abspath, sep='\t', index_col=None, error_bad_lines=False,
                               low_memory=False)
        self.header = raw_data.columns
        # print ur'Data loaded!'
        # 处理缺失值
        raw_data = raw_data.ix[raw_data[self.data_column_name].dropna().index, :]
        # 处理重复值
        raw_data = raw_data.ix[raw_data[self.data_column_name].drop_duplicates().index, :]

        # 提取文本内容，打标签
        # data = Series([string.encode('utf-8') for string in raw_data[self.data_column_name]])
        data = raw_data[self.data_column_name]
        # att = tagging(data, tags)

        # is_trash = att.apply(sum, axis=1)
        # raw_data['is_trash'] = is_trash
        # mask_cleaned = is_trash == 0
        # cleaned = raw_data[mask_cleaned]
        # mask_trash = is_trash == 1
        # trash = raw_data[mask_trash]

        # 转换数据格式
        # cleaned_np_array = cleaned.values
        # trash_np_array = trash.values
        # self.cleaned_list = cleaned_np_array.tolist()
        # self.trash_list = trash_np_array.tolist()

        # cleaned_file_name = self.save_file_path + r'\clean_data.txt'
        # trash_file_name = self.save_file_path + r'\dictionary_data_trash.txt'
        #
        # cleaned.to_csv(cleaned_file_name, encoding="utf-8", sep="\t", index=None)
        # trash.to_csv(trash_file_name, encoding="utf-8", sep="\t", index=None)

        # 在多数据文件或者多词库文件进行批量处理的时候需要对这些数据进行重置
        # self.raw_list = None
        # self.keywords_list = None
        return
# endregion


# region 热门话题去水
class TagsReducer(BaseReducer):
    """
    利用#热门话题#个数，以及其余字数个数去水的工具
    """
    def __init__(self):
        BaseReducer.__init__(self)
        self.numbers = 10
        self.tags = 3

    def get_tags(self):
        if self.current_string:
            tags_char_num = 0
            # 匹配#热门话题#
            seg_list = re.findall(ur'#.*?#', self.current_string)
            # 匹配【】
            seg_list2 = re.findall(ur'【.*?】|★|◆', self.current_string)
            # 匹配[表情]
            seg_list3 = re.findall(ur'\[.*?]', self.current_string)
            # 标签数=#热门话题# + 【】*阈值 ，出现【】即判为垃圾
            tags = len(seg_list) + len(seg_list2)*self.tags
            for seg in seg_list:
                tags_char_num += len(re.findall(ur"[\u3007\u4E00-\u9FCB\uE815-\uE864]", seg))
            for seg3 in seg_list3:
                tags_char_num += len(re.findall(ur"[\u3007\u4E00-\u9FCB\uE815-\uE864]", seg3))

            char_list = re.findall(ur"[\u3007\u4E00-\u9FCB\uE815-\uE864]", self.current_string)
            char_num = len(char_list) - tags_char_num

            return tags, char_num

    def tags_finder(self):
        """
        python的正则表达式可以直接支持中文
        :return:
        """
        tags, char_num = self.get_tags()

        self.current_result = char_num >= self.numbers and tags < self.tags
        return

    def main(self):
        """
        复杂应用才需要Override这部分
        :return:
        """
        self.get_contents()
        for index in range(len(self.raw_list)):
            string = self.raw_list[index]
            self.current_string = to_unicode(string[self.data_column_index])
            try:
                self.tags_finder()
                if self.current_result:
                    self.clean_index.append(index)
                    self.cleaned_list.append(string)
                else:
                    self.trash_index.append(index)
                    self.trash_list.append(string)
            except:
                pass
        # 在多数据文件或者多词库文件进行批量处理的时候需要对这些数据进行重置
        # self.raw_list = None
        # self.keywords_list = None
        return
# endregion


# region 字数个数去水
class NumbersReducer(BaseReducer):
    """
    利用文字个数去水的工具
    """
    def __init__(self):
        BaseReducer.__init__(self)
        self.min_numbers = 5
        self.max_numbers = 500

    def number_finder(self):
        """
            python的正则表达式可以直接支持中文
            :return:
            """
        try:
            numbers = re.findall(ur"[\u4e00-\u9fa5]", self.current_string)
            number_counts = len(numbers)
        except Exception as e:
            self.current_error = str(e)
            self.code_message.code = 3
            self.code_message.message = u"number_finder re.findll error"
            self.error_list.append(self.current_error)
            self.current_result = []
        else:
            self.current_result = self.min_numbers < number_counts < self.max_numbers

        return

    def main(self):
        """
        复杂应用才需要Override这部分
        :return:
        """
        self.get_contents()
        for index in range(len(self.raw_list)):
            string = self.raw_list[index]
            self.current_string = to_unicode(string[self.data_column_index])
            self.number_finder()
            try:
                if self.current_result:
                    self.clean_index.append(index)
                    self.cleaned_list.append(string)
                else:
                    self.trash_index.append(index)
                    self.trash_list.append(string)
            except:
                pass
        # 在多数据文件或者多词库文件进行批量处理的时候需要对这些数据进行重置
        # self.raw_list = None
        # self.keywords_list = None
        return
# endregion


# region 非正常字符种数去水
class AbnormalReducer(BaseReducer):
    """
    利用异常字符种数去水的工具，数字暂定为5
    """
    def __init__(self):
        BaseReducer.__init__(self)
        self.abnormal = 5

    def abnormal_finder(self):
        """
            python的正则表达式可以直接支持中文
            :return:
            """
        try:
            char_list = re.findall(ur"[\u3007\u4E00-\u9FCB\uE815-\uE864]", self.current_string)
            match = re.findall(ur"[^—@~:：?!！/ ,，.\[\]()（）\dA-Za-z\u3007\u4E00-\u9FCB\uE815-\uE864]",
                               self.current_string)
        except Exception as e:
            self.current_error = str(e)
            self.code_message.code = 3
            self.code_message.message = u"abnormal_finder re.find error"
            self.error_list.append(self.current_error)
            self.current_result = []
        else:
            number_counts = ''
            for match_index in match:
                number_counts += match_index
            number_counts = set(number_counts)
            self.current_result = len(number_counts) < self.abnormal and len(match)*2 <= len(char_list)
        if self.show_process:
            if self.current_result:
                print u"含有的符号个数大于\n{0}".format(self.abnormal)
            else:
                # print u"没有匹配\n{0}".format(self.abnormal)
                pass
        else:
            pass
        return

    def main(self):
        """
        复杂应用才需要Override这部分
        :return:
        """
        self.get_contents()
        for index in range(len(self.raw_list)):
            string = self.raw_list[index]
            self.current_string = to_unicode(string[self.data_column_index])
            self.abnormal_finder()
            try:
                if self.current_result:
                    self.clean_index.append(index)
                    self.cleaned_list.append(string)
                else:
                    self.trash_index.append(index)
                    self.trash_list.append(string)
            except:
                pass
        # 在多数据文件或者多词库文件进行批量处理的时候需要对这些数据进行重置
        # self.raw_list = None
        # self.keywords_list = None
        return
# endregion


# region 序列去水
class SeriesReducer(BaseReducer):
    """
    利用序列去水的工具
    """
    def __init__(self):
        BaseReducer.__init__(self)

    def series_finder(self):
        """
            python的正则表达式可以直接支持中文
            :return:
            """
        try:
            pattern = re.compile(u'[1-9][.|:|、][\u3007\u4E00-\u9FCB\uE815-\uE864]|[\u2460-\u2469]')
            series_list = re.findall(pattern, self.current_string)

        except Exception as e:
            self.current_error = str(e)
            self.code_message.code = 3
            self.code_message.message = u"series_finder re.find error"
            self.error_list.append(self.current_error)
            self.current_result = []
        else:
            series_length = len(series_list)
            self.current_result = series_length <= 2
        if self.show_process:
            if self.current_result:
                print u"含有的序号个数小于\n{0}".format(2)
            else:
                # print u"没有匹配\n{0}".format(self.abnormal)
                pass
        else:
            pass
        return

    def main(self):
        """
        复杂应用才需要Override这部分
        :return:
        """
        self.get_contents()
        for index in range(len(self.raw_list)):
            string = self.raw_list[index]
            self.current_string = to_unicode(string[self.data_column_index])
            self.series_finder()
            try:
                if self.current_result:
                    self.clean_index.append(index)
                    self.cleaned_list.append(string)
                else:
                    self.trash_index.append(index)
                    self.trash_list.append(string)
            except:
                pass
        # 在多数据文件或者多词库文件进行批量处理的时候需要对这些数据进行重置
        # self.raw_list = None
        # self.keywords_list = None
        return
# endregion


# region 客户端去水
class SourcesReducer(BaseReducer):
    """
    利用客户端来源去水的工具
    """
    def __init__(self):
        BaseReducer.__init__(self)
        self.count_list = []

    def get_keywords(self):
        """
        数据库的操作后将数据赋值给 self.keywords_list
        :return:
        """
        if self.keywords_list:
            return
        else:
            pass
        if self.current_dict_abspath:
            self.export_name = u"{0}-{1}".format(self.export_name, self.get_file_name(self.current_dict_abspath))
            try:
                f = open(self.current_dict_abspath, u"rb")
            except IOError as e:
                self.current_error = str(e)
                self.code_message.code = 1
                self.code_message.message = u"get_keywords open(file) IOError"
                self.error_list.append(self.current_error)
            else:
                with f:
                    # self.keywords_list += f.read().decode(u"gb2312").split(u"\r\n")
                    keyword = f.read().decode(u"utf-8").strip().split(u"\r\n")
                    if keyword:
                        self.keywords_list += keyword
                    self.code_message.message = u"keywords dictionary import successfully"
        else:
            self.keywords_list = [u"扇贝单词", u"微博相机"]

        return

    def sources_finder(self):
        """
        python的正则表达式可以直接支持中文
        :return:
        """
        pattern = re.compile(pattern=ur'>[^@]*<', flags=0)
        self.current_string = re.findall(pattern, self.current_string)
        if self.current_string:
            self.current_string = self.current_string[0]
        else:
            self.current_string = ur''
        self.current_string = self.current_string.strip(u'><')
        self.current_result = self.current_string not in self.keywords_list
        if self.show_process:
            if self.current_result:
                print u"找到关键词\n{0}".format(self.current_keywords)
                for r in self.current_result:
                    print r
            else:
                # print u"没有找到关键词\n{0}".format(keywords)
                pass
        else:
            pass
        return

    def main(self):
        """
        复杂应用才需要Override这部分
        :return:
        """
        self.get_contents()
        self.get_keywords()
        self.count_list = [0] * len(self.keywords_list)
        for index in range(len(self.raw_list)):
            string = self.raw_list[index]
            self.current_string = to_unicode(string[self.data_column_index])
            self.sources_finder()
            try:
                if self.current_result:
                    self.clean_index.append(index)
                    self.cleaned_list.append(string)
                else:
                    self.trash_index.append(index)
                    self.trash_list.append(string)
            except:
                pass
        # 在多数据文件或者多词库文件进行批量处理的时候需要对这些数据进行重置
        # self.raw_list = None
        # self.keywords_list = None
        return
# endregion


if __name__ == u"__main__":
    # kr = AbnormalReducer()
    # kr = KeywordsReducer()
    # kr = SourcesReducer()
    # kr = TagsReducer()
    kr = NumbersReducer()
    # kr = SeriesReducer()
    kr.min_numbers = 5
    # kr.show_process = True
    kr.current_data_abspath = ur"D:\WorkSpace\Data\data_sample.txt"
    # kr.has_header = True
    # kr.data_column_name = ur"text"
    # kr.current_dict_abspath = ur"D:\workspace\Data\keywords.txt"
    kr.has_header = False
    kr.data_column_name = 2

    # kr.data_column_index = 3
    # kr.current_dict_abspath = ur"D:\WorkSpace\Data\trash_sources.txt"

    # kr.min_numbers = 5
    # kr.max_numbers = 500

    # kr = TaggingReducer()
    # kr.current_data_abspath = ur"D:\WorkSpace\Data\虎扑---帖1.txt"
    # kr.has_header = True
    # kr.data_column_name = 'Content'
    # kr.current_dict_abspath = ur"D:\workspace\Data\通用词库2"
    # kr.save_file_path = ur'D:\WorkSpace\Data'

    kr.main()

    print u"{0}统计信息{0}".format(u"-" * 30)
    print u'共有数据 ' + str(len(kr.raw_list))
    print u'水有 ' + str(len(kr.raw_list) - len(kr.cleaned_list)) + u'条'
    print u'重复水有 ' + str(len(kr.trash_list) - len(kr.raw_list) + len(kr.cleaned_list)) + u'条'
    print u'非水有 ' + str(len(kr.cleaned_list)) + u'条'
    print u'去水率 ' + str(float(len(kr.raw_list) - len(kr.cleaned_list)) / len(kr.raw_list) * 100) + u'%'

    # region 1000条人工标注数据data_sample的测试
    # clean_index = [3, 4, 26, 29, 33, 42, 55, 62, 70, 80, 83, 100, 109, 113, 119, 121, 171, 204, 261, 284, 290, 349,
    #                385, 397, 415, 421, 435, 551, 590, 615, 618, 771, 778, 781, 793, 843, 963, 965, 972]
    #
    # A = 0
    # for i in kr.trash_index:
    #     if (int(i) + 1) not in clean_index:
    #         A += 1
    #     else:
    #         print kr.raw_list[i][2]
    #         print kr.raw_list[i][3]
    #
    # C = 0
    # for i in kr.clean_index:
    #     if (int(i) + 1) not in clean_index:
    #         C += 1
    #
    # B = len(kr.raw_list) - len(kr.cleaned_list) - A
    # D = len(kr.cleaned_list) - C
    # if A + B != 0:
    #     precise = A / float(A + B) * 100
    # else:
    #     precise = 0
    # if A + C != 0:
    #     recall = A / float(A + C) * 100
    # else:
    #     recall = 0
    # if precise + recall != 0:
    #     F = precise * recall / (precise + recall) / 100
    # else:
    #     F = 0
    # print 'AB is:      ' + str(A) + ' ' + str(B)
    # print 'CD is:      ' + str(C) + ' ' + str(D)
    # print 'precise is: ' + str(precise) + '%'
    # print 'recall is:  ' + str(recall) + '%'
    # print 'F is        ' + str(F)
    # endregion
