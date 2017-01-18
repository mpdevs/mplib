# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
import pandas as pd
import csv


# 导出Excel文件
def export_to_excel(data_list, file_name, column_head):
    df = pd.DataFrame(data_list, columns=column_head)
    writer = pd.ExcelWriter(file_name)
    df.to_excel(writer, sheet_name="NDR_API_processed", encoding="utf8", engine="xlsxwriter")
    writer.save()
    writer.close()
    return


# 导出txt文件
def export_to_txt(data_list, file_name, column_head):
    if len(column_head) > 0:
        att_head = True
    else:
        att_head = False
        column_head = None
    df = pd.DataFrame(data_list, columns=column_head)
    df.to_csv(file_name, encoding="utf8", index=None, sep="\t".encode("utf8"), mode="w", quoting=csv.QUOTE_NONE,
              header=att_head)
    return


if __name__ == "__main__":
    export_to_txt(["a", "b", "c", "d"], "啊.txt", column_head=None)
