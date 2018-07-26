"""This module contains utilities functions used in others modules."""

__author__ = "Bartosz Walkowicz"
__copyright__ = "Copyright (C) 2018 ACK CYFRONET AGH"
__license__ = "This software is released under the Apache license cited in " \
              "LICENSE"

import sys

try:
    from termcolor import colored
except ImportError:
    colored = lambda text, _: text


VERBOSITY = 1

def info(text: str):
    if 3 >= VERBOSITY:
        print(colored('[INFO]', 'green'), text, file=sys.stderr)


def warning(text: str):
    if 2 >= VERBOSITY:
        print(colored('[WARNING]', 'yellow'), text, file=sys.stderr)


def error(text: str):
    if 1 >= VERBOSITY:
        print(colored('[ERROR]', 'red'), text, file=sys.stderr)
