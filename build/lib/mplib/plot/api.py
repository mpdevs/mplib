# coding: utf8
# __author__: "John"
from __future__ import unicode_literals, absolute_import, print_function, division
from os.path import join, dirname
import matplotlib.pyplot as plt
import pylab


def plot_scatter(p, y, file_name=None, plots_dir=None):
    plot_dir = plots_dir if plots_dir else __file__
    fig, ax = plt.subplots()
    ax.scatter(y, p)
    ax.plot([y.min(), y.max()], [y.min(), y.max()], "k--", lw=3)
    ax.set_xlabel("Measured")
    ax.set_ylabel("Predicted")
    if file_name:
        pylab.savefig(join(join(dirname(plot_dir), "plots"), file_name))
    else:
        plt.show()


def plot_line(p, y, file_name=None, plots_dir=None):
    plot_dir = plots_dir if plots_dir else __file__
    fig, ax = plt.subplots()
    ax.scatter(y, p)
    ax.plot([y.min(), y.max()], [y.min(), y.max()], "k--", lw=3)
    ax.set_xlabel("Measured")
    ax.set_ylabel("Predicted")
    if file_name:
        pylab.savefig(join(join(dirname(plot_dir), "plots"), file_name))
    else:
        plt.show()
