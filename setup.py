# coding: utf-8
# __author__: u"John"
from setuptools import setup, find_packages


setup(
    name="mplib",
    version="0.5.0",
    packages=find_packages("src"),
    package_dir={"": "src"},
    include_package_data=True,
)
