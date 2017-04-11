# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
from __future__ import absolute_import
from __future__ import print_function
from __future__ import division


MYSQL_SETTINGS = dict(
    host="172.16.1.120",
    port=3306,
    user="test",
    passwd="Marcpoint_2016",
    db="mp_portal",
)


DAS_PRO_MYSQL_CONNECTION = dict(
    host="172.16.1.100",
    port=3306,
    user="das",
    passwd="123",
    db="das"
)


HIVE_CONNECTION = dict(
    database="das",
    host="172.16.1.12",
    user="big boss",
    password="it's secret",
    port=10000,
    authMechanism="PLAIN"
)


IDC_HIVE_CONNECTION = dict(
    database="elengjing",
    host="192.168.110.122",
    user="hive",
    password="hive1",
    port=10000,
    authMechanism="PLAIN"
)


REDIS_CONNECTION = dict(
    host="localhost",
    port=6379
)


PG_CONNECTION = dict(
    host="192.168.110.11",
    port=5432,
    user="elengjing",
    password="Marcpoint2016",
    dbname="mp_portal",
    minconn=1,
    maxconn=1000,
)


IMPALA_CONNECTION = dict(
    host="172.16.1.14",
    port=21050,
    database="das",
)


DEBUG = True
INFO = True
WARNING = True
ERROR = True
FATAL = True
