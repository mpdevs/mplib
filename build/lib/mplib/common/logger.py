# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals, absolute_import, print_function, division
from os.path import abspath, dirname, join
import logging


class Logger(object):
    formatter = logging.Formatter("%(asctime)s | %(name)s | %(levelname)s | %(message)s", '%Y-%m-%d %H:%M:%S')

    def __init__(self, level="debug", name=__name__, path=__file__):
        self.path = join(dirname(abspath(path)), "{0}.log".format(level))
        self.name = name
        self.level = getattr(logging, level.upper())
        self.logger = None
        self.log_file = None
        self.get_logger()
        print(self.path)

    def get_logger(self):
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(self.level)
        file_handler = logging.FileHandler(self.path)
        file_handler.setFormatter(Logger.formatter)
        file_handler.setLevel(self.level)
        self.logger.addHandler(file_handler)

    def debug(self, message):
        self.logger.debug(message)

    def info(self, message):
        self.logger.info(message)

    def warning(self, message):
        self.logger.warning(message)

    def error(self, message):
        self.logger.error(message)

    def critical(self, message):
        self.logger.critical(message)
