# coding: utf8
from __future__ import unicode_literals, print_function
from mplib.common.setting import HDFS_LOCAL_MASTER, HDFS_LOCAL_SLAVE
from hdfs.util import HdfsError
from hdfs.client import Client


def get_env_dict():
    return dict(
        local=(HDFS_LOCAL_MASTER, HDFS_LOCAL_SLAVE),
    )


def get_env(env):
    return get_env_dict().get(env, (HDFS_LOCAL_MASTER, HDFS_LOCAL_SLAVE))


def get_hdfs_client(env="local"):
    master, slave = get_env(env)
    try:
        client = Client(master)
        client.list("/")
    except HdfsError:
        client = Client(slave)
    return client


if __name__ == "__main__":
    hdfs_instance = get_hdfs_client()
    print(hdfs_instance.list("/"))
