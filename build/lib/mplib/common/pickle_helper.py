# coding: utf-8
# __author__: u"John"
from __future__ import unicode_literals
import pickle


def pickle_dump(file_name, dump_object):
    if ".pickle" in file_name:
        pickle_name = file_name
    else:
        pickle_name = file_name + ".pickle"

    f = open(pickle_name, "wb")
    pickle.dump(dump_object, f)
    f.close()
    return


def pickle_load(file_name):
    if ".pickle" in file_name:
        pickle_name = file_name
    else:
        pickle_name = file_name + ".pickle"

    f = open(pickle_name, "rb")
    load_object = pickle.load(f)
    f.close()
    return load_object
