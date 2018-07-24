#! /usr/bin/env python3.7
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
    next(b)
    return zip_longest(a, b)


def replay(mount_path: str, io_trace_path: str, create_env: bool):
    parser = IOTraceParser(mount_path=mount_path, create_env=create_env)
    syscalls, *prev_times = parser.parse(io_trace_path)
    pprint(syscalls)

    io_duration = 0
    cpu_duration = 0

    fds = {}
    for syscall, next_syscall in pairwise(syscalls):
        try:
           io_duration += syscall.perform(fds)
        except Exception as ex:
            print('Failed to execute {} due to {!r}'.format(syscall, ex),
                  file=sys.stderr)
            continue

        if next_syscall:
            delay = next_syscall.timestamp - (syscall.timestamp
                                              + syscall.duration)
            if delay < 0:
                delay = next_syscall.timestamp - syscall.timestamp

            cpu_duration += delay * 1000  # timestamp and duration are in us
            sleep(delay/10**6)

    prog_duration = io_duration + cpu_duration
    overhead = io_duration / prog_duration
    prev_overhead = prev_times[0] / prev_times[1]
    print('Statistics (original/replayed):',
          f'\n\tIO duration [ns]:      {prev_times[0]*1000}/{io_duration}',
          f'\n\tProgram duration [ns]: {prev_times[1]*1000}/{prog_duration}',
          f'\n\tOverhead:              {prev_overhead:0.5f}/{overhead:0.5f}')


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

    replay(args.mount_path, args.io_trace, args.create_env)


if __name__ == '__main__':
    replay('/home/cyfronet/Desktop/develop/test4', './qwe4.csv', True)
    # main()
