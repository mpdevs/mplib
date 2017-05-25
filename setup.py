# coding: utf-8
# __author__: u"John"
from __future__ import print_function
from __future__ import division
from setuptools import setup, find_packages


setup(
    name="mplib",
    version="0.5.9",
    packages=find_packages("src"),
    package_dir={"": "src"},
    include_package_data=True,
)
