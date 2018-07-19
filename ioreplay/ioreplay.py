"""This module contains ...nothing yet
"""

__author__ = "Bartosz Walkowicz"
__copyright__ = "Copyright (C) 2018 ACK CYFRONET AGH"
__license__ = "This software is released under the Apache license cited in " \
              "LICENSE"

import argparse
from time import sleep, time
from contextlib import suppress
from pprint import pprint

from parser import IOTraceParser


def replay(syscalls):
    start = time()
    duration = 0
    fds = {}
    for syscall in syscalls:
        # with suppress(Exception):
        duration += syscall.perform(fds)
    end = time()
    print(duration / (end - start))


def heh(mount_path: str, io_trace_path: str, create_env: bool):
    # with open(io_trace_path) as f:
    #     text = f.read().split('\n')[1:-1]
    #
    # # Drop first line as it is just a header
    # text.sort(key=lambda line: int(line.split(',', 1)[0]))
    #
    # with open('./qwe2', 'w') as f:
    #     f.write('\n'.join(text))

    parser = IOTraceParser(mount_path=mount_path, create_env=create_env)
    syscalls = parser.parse(io_trace_path)
    pprint(syscalls)
    replay(syscalls)


def main():
    parser = argparse.ArgumentParser(prog='ioreplay',
                                     description='Replay recorded activities '
                                                 'performed using Oneclient')

    parser.add_argument('mount_path',
                        help='Path to mounted Oneclient')
    parser.add_argument('-i', '--io_trace',
                        required=True,
                        help='Path to csv file containing recorded io')
    parser.add_argument('-e', '--create_env',
                        action='store_true',
                        help='If specified missing files and directories '
                             'will be created before start of replay')
    args = parser.parse_args()

    heh(args.mount_path, args.io_trace, args.create_env)


if __name__ == '__main__':
    heh('/home/cyfronet/Desktop/develop/test', './qwe2', False)
    # main()
