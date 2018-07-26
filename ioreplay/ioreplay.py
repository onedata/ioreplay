#! /usr/bin/env python3.7
"""This module contains syscalls replayer and its command line interface"""

__author__ = "Bartosz Walkowicz"
__copyright__ = "Copyright (C) 2018 ACK CYFRONET AGH"
__license__ = "This software is released under the Apache license cited in " \
              "LICENSE"

import os
import sys
import argparse
from uuid import uuid4
from time import sleep
from pprint import pprint
from itertools import tee, zip_longest

from utils import error
from parser import IOTraceParser
from operations import Context, Rename, Unlink, MkNod, MkDir, Create


def pairwise(iterable):
    a, b = tee(iterable)
    next(b)
    return zip_longest(a, b)


def replay(parser: IOTraceParser, mount_path: str) -> None:
    delays = 0
    io_duration = 0

    ctx = Context(mount_path, {}, {})
    for syscall, next_syscall in pairwise(parser.syscalls):
        try:
           io_duration += syscall.perform(ctx)
        except Exception as ex:
            print('Failed to execute {} due to {!r}'.format(syscall, ex),
                  file=sys.stderr)
            continue

        if next_syscall:
            delay = next_syscall.timestamp - (syscall.timestamp
                                              + syscall.duration)
            if delay < 0:
                delay = next_syscall.timestamp - syscall.timestamp

            delays += delay
            sleep(delay/10**9)

    prog_duration = io_duration + delays
    prev_prog_duration = parser.end_timestamp - parser.start_timestamp

    overhead = io_duration / prog_duration
    prev_overhead = parser.io_duration / prev_prog_duration

    print('Statistics (original/replayed):',
          f'\n\tIO duration [ns]:      {parser.io_duration}/{io_duration}',
          f'\n\tProgram duration [ns]: {prev_prog_duration}/{prog_duration}',
          f'\n\tOverhead:              {prev_overhead:0.5f}/{overhead:0.5f}')


def create_env(parser: IOTraceParser, mount_path: str) -> None:
    # first pass to create initial files and directories
    for file in parser.initial_files.values():
        path = os.path.join(mount_path, file.path)
        if not os.path.exists(path):
            try:
                if file.type == 'f':
                    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
                    os.truncate(fd, file.size)
                elif file.type == 'd':
                    os.mkdir(path)
            except Exception as ex:
                error('Failed to create {} due to {!r}'.format(path, ex))
                exit(1)

    # second pass to create dummy files in directories (for readdir)
    for file in parser.initial_files.values():
        if file.type == 'd':
            path = os.path.join(mount_path, file.path)
            try:
                dir_content = os.listdir(path)
                files_to_create = max(0, file.size[1] - len(dir_content))
                for _ in range(files_to_create):
                    file_name = str(uuid4())
                    while file_name in dir_content:
                        file_name = str(uuid4())

                    file_path = os.path.join(path, file_name)
                    os.close(os.open(file_path,
                                     os.O_WRONLY | os.O_CREAT | os.O_TRUNC))
            except Exception as ex:
                print('Failed to create dummy file in {} due to '
                      '{!r}'.format(path, ex), file=sys.stderr)
                exit(1)


def print_env_report(parser: IOTraceParser) -> None:
    width = 80
    table_line = '  ' + '-' * (width - 4)
    horizontal_line = '_' * width
    horizontal_bold_line = '=' * width

    print('\n\n', horizontal_bold_line,
          '\n{title:^{width}}\n'.format(title="ENVIRONMENT REPORT", width=width),
          horizontal_bold_line, sep='')

    print('\n', horizontal_line,
          '\n\n{title:^{width}}\n'.format(title='INITIAL FILES', width=width),
          '\n   File Type | File Size [B] | Path\n',
          table_line, sep='')
    for file in parser.initial_files.values():
        print(f'  {"dir" if file.type == "d" else "file":^11}| '
              f'{file.size if file.type == "f" else 0:13d} | '
              f'{file.path}')

    print('\n', horizontal_line,
          '\n\n{title:^{width}}\n'.format(title='CREATED FILES', width=width),
          '\n   File Type | Path\n',
          table_line, sep='')
    for syscall in parser.syscalls:
        if type(syscall) in (MkNod, Create):
            file_type = 'file'
        elif type(syscall) is MkDir:
            file_type = 'dir'
        else:
            continue

        print(f'  {file_type:^11}| {syscall.path}')

    print('\n', horizontal_line,
          '\n\n{title:^{width}}\n'.format(title='REMOVED FILES', width=width),
          sep='')
    for syscall in parser.syscalls:
        if type(syscall) == Unlink:
            print(f'  {syscall.path}')

    print('\n', horizontal_line,
          '\n\n{title:^{width}}\n'.format(title='RENAMED FILES', width=width),
          sep='')
    for syscall in parser.syscalls:
        if type(syscall) == Rename:
            print(f'  {syscall.src_path} -> {syscall.dst_path}')

    print('\n\n', horizontal_bold_line, sep='')


def main():
    parser = argparse.ArgumentParser(prog='ioreplay',
                                     description='Replay recorded activities '
                                                 'performed using Oneclient')
    parser.add_argument('io_trace_path',
                        help='Path to trace file containing recorder io')
    parser.add_argument('-m', '--mount-path',
                        help='Path to mounted Onelcient. If not specified '
                             'program will perform dry run meaning it will '
                             'parse trace file but not call any system calls')
    parser.add_argument('-s', '--syscalls',
                        action='store_true',
                        help='When specified detailed information about system '
                             'calls retrieved when parsing trace file will be '
                             'displayed')
    parser.add_argument('-e', '--env-report',
                        action='store_true',
                        help='When specified detailed report about environment '
                             '(file and directories) will be displayed')
    parser.add_argument('-c', '--create-env',
                        action='store_true',
                        help='If specified missing files and directories '
                             'will be created before start of replay')
    args = parser.parse_args()

    parser = IOTraceParser()
    parser.parse(args.io_trace_path)
    if args.syscalls:
        pprint(parser.syscalls)
    if args.env_report:
        print_env_report(parser)
    if args.create_env:
        create_env(parser, args.mount_path)
    if args.mount_path:
        replay(parser, args.mount_path)


if __name__ == '__main__':
    main()
