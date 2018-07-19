"""This module contains syscalls replayer and its command line interface"""

__author__ = "Bartosz Walkowicz"
__copyright__ = "Copyright (C) 2018 ACK CYFRONET AGH"
__license__ = "This software is released under the Apache license cited in " \
              "LICENSE"

import sys
import argparse
from time import sleep
from pprint import pprint
from itertools import tee, zip_longest

from parser import IOTraceParser


def pairwise(iterable):
    a, b = tee(iterable)
    next(b, None)
    return zip_longest(a, b)


def replay(syscalls):
    io_duration = 0
    cpu_duration = 0
    fds = {}
    for syscall, next_syscall in pairwise(syscalls):
        try:
           io_duration += syscall.perform(fds)
        except Exception as ex:
            print('Failed to execute {} due to {!r}'.format(syscall, ex),
                  file=sys.stderr)

        if next_syscall:
            delay = next_syscall.timestamp - (syscall.timestamp
                                              + syscall.duration)
            if delay < 0:
                delay = next_syscall.timestamp - syscall.timestamp

            cpu_duration += delay * 1000  # timestamp and duration are in us
            sleep(delay/10**6)

    print('Overhead: ', io_duration / (io_duration + cpu_duration))


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
    heh('/home/cyfronet/Desktop/develop/test2', './qwe', False)
    # main()
