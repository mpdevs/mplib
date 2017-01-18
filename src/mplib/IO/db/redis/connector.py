# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
from mplib.common.settings import REDIS_CONNECTION
from redis import Redis


class MPRedis(Redis):
    def __init__(self, db=0, connection_dict=REDIS_CONNECTION):
        Redis.__init__(self, db=db, decode_responses=True, **connection_dict)  # 自动连接redis
        self.expiration_time = 0  # 设置过期时间，单位秒

    def counter(self, key, step=1, ex=1):
        """
        自动计数器
        :param key: 变量名(basestring)
        :param step: 选择计数步长(int/long)
        :param ex: 设置过期时间(int/long)
        :return:
        """
        self.expiration_time = ex
        value = self.get(name=key)
        if value:  # 继续计数
            try:
                value = int(value) + step
                self.setex(name=key, value=value, time=self.ttl(key))
            except TypeError:  # 如果value不是数值类型，则重新开始计数
                value = 1
                self.setex(name=key, value=value, time=self.expiration_time)
        else:  # 开始新的计数
            value = 1
            self.setex(name=key, value=value, time=self.expiration_time)
        return value


if __name__ == "__main__":
    r = MPRedis()
    r.set("hello", "world")
    print [r.get("hello")]
