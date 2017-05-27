# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals, absolute_import, print_function, division
from mplib.common.logger import Logger


logger = Logger(level="error", name=__name__, path=__file__)


def log_import_error(imported_tuple, critical_level="error"):
    if len(imported_tuple) > 1:
        import_command = "from {0} import {1}".format(imported_tuple[0], imported_tuple[1])
    else:
        import_command = "import {0}".format(imported_tuple[0])

    exec_str = "\n".join([
        "try:",
        "\t{0}".format(import_command),
        "\tlogger.info('{0} success')".format(import_command),
        "except ImportError:",
        "\tlogger.{0}('{1} failed')".format(critical_level, import_command),
    ])
    exec (exec_str)


def start_test(case_list):
    for case in case_list:

        critical_level = case[-1]
        actual_case = case[:-1] if len(case) > 1 else case[0]

        if critical_level.lower() not in ["debug", "info", "warning", "error", "critical"]:
            critical_level = "error"

        log_import_error(actual_case, critical_level)


if __name__ == "__main__":
    immutable_case = [
        ("mplib.IO.db.hive.connector", "Hive"),
        ("mplib.IO.db.impala.connector", "Impala"),
        ("mplib.IO.db.mysql.connector", "MPMySQL"),
        ("mplib.IO.db.postgresql.connector", "MPPG"),
        ("mplib.IO.db.redis.connector", "MPRedis"),
        ("mplib.IO.file.binary.pickle_helper", "pickle_dump"),
        ("mplib.IO.file.binary.pickle_helper", "pickle_load"),
        ("mplib.Math.elementary_function", "npr"),
        ("mplib.Math.elementary_function", "ncr"),
        ("mplib.common.base_class", "AttributeDict"),
        ("mplib.common.base_class", "DateTime"),
        ("mplib.common.decorator", "time_elapse"),
        ("mplib.common.decorator", "clock_elapse"),
        ("chardet", "warning"),
        ("pyhs2", "warning"),
        ("impala", "warning"),
        ("pykylin", "warning"),
        ("MySQLdb", "warning"),
        ("psycopg2", "warning"),
        ("jieba", "warning"),
        ("redis", "warning"),
        ("sklearn", "error"),
        ("scipy", "error"),
        ("numpy", "error"),
        ("pandas", "error"),
        ("editdistance", "error"),
        ("matplotlib", "error"),
        ("pylab", "error"),
        ("treeinterpreter", "error"),
    ]

    start_test(immutable_case)
