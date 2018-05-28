# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals, absolute_import, print_function, division


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


LJ_TEST_MYSQL_CONNECTION = dict(
    host="172.16.1.100",
    port=3306,
    db="elengjing",
    user="test",
    passwd="123",
)

LOCAL_HIVE_MYSQL_CONNECTION = dict(
    host="172.16.1.11",
    port=3306,
    db="hive",
    user="cdh",
    passwd="123",
)


HIVE_CONNECTION = dict(
    database="das",
    host="172.16.1.12",
    user="mplib",
    password="mplib",
    port=10000,
    authMechanism="PLAIN"
)


IDC_HIVE_CONNECTION = dict(
    database="elengjing",
    host="192.168.110.122",
    user="mplib",
    password="mplib",
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

PG_UAT_CONNECTION = dict(
    host="192.168.110.12",
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


KYLIN_CONNECTION = dict(
    username="ADMIN",
    password="KYLIN",
    endpoint="http://172.16.1.14:7070/kylin/api",
    project="elengjing",
)


IDC_KYLIN_CONNECTION = dict(
    username="ADMIN",
    password="KYLIN",
    endpoint="http://192.168.110.124:7070/kylin/api",
    project="elengjing",
)

PG_KOL_CONNECTION = dict(
    host="172.16.1.100",
    port=5432,
    user="kol",
    password="kol",
    dbname="kol",
    minconn=1,
    maxconn=1000,
)



DAS_API_MYSQL_CONNECTION = dict(
    host="172.16.1.100",
    port=3306,
    user="das",
    passwd="123",
    db="das_api",
)

DAS_API_DEV_MYSQL_CONNECTION = dict(
    host="172.16.1.100",
    port=3306,
    user="das",
    passwd="123",
    db="das_api_dev",
)


DEBUG = True
INFO = True
WARNING = True
ERROR = True
FATAL = True
