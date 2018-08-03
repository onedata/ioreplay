"""This module contains syscalls replayer and its command line interface"""

__author__ = "Bartosz Walkowicz"
__copyright__ = "Copyright (C) 2018 ACK CYFRONET AGH"
__license__ = "This software is released under the Apache license cited in " \
              "LICENSE"

import os
import sys
import heapq
import inspect
import argparse
import traceback
from time import sleep
from uuid import uuid4
from pprint import pprint
from functools import wraps
from contextlib import suppress
from tempfile import TemporaryDirectory
from itertools import tee, zip_longest, islice
from typing import Optional, Tuple, Dict, Callable
from collections import namedtuple, deque, OrderedDict, ChainMap

try:
    from time import time_ns
except ImportError:
    from time import time as _time

    def time_ns():
        return int(_time() * 10**9)


# Max delay, in us, between subsequent fuse calls of the same system call
# (every syscall translates up to several fuse calls) caused by context switch
# between kernel and userland.
CTX_SWITCH_DELAY = 250

# Number of lines read at ones as a chunk in external sort
DEFAULT_CHUNK_SIZE = 50000

FUSE_SET_ATTR_MODE = (1 << 0)
FUSE_SET_ATTR_SIZE = (1 << 3)
FUSE_SET_ATTR_ATIME = (1 << 4)
FUSE_SET_ATTR_MTIME = (1 << 5)
FUSE_SET_ATTR_ATIME_NOW = (1 << 7)
FUSE_SET_ATTR_MTIME_NOW = (1 << 8)


SYSCALLS = {}


Context = namedtuple('Context', ['mount_path', 'fds', 'scandirs'])


class Path(str):
    def __new__(cls, ctx: Context, rel_path: str) -> str:
        return os.path.join(ctx.mount_path, rel_path)


def syscall(fun: Callable[..., Optional[int]]) -> Callable[..., int]:
    signature = inspect.signature(fun)
    parameters = signature.parameters

    if signature.return_annotation is int:
        measure_time = False
    else:
        measure_time = True
        signature = signature.replace(return_annotation=int)

    @wraps(fun)
    def wrapper(ctx: Context, *args, **kwargs):
        bound_arguments = signature.bind(ctx, *args, **kwargs)
        arguments = bound_arguments.arguments
        for arg, val in arguments.items():
            annotation = parameters[arg].annotation
            if annotation is inspect.Parameter.empty:
                continue
            elif annotation is Path:
                arguments[arg] = Path(ctx, val)
            elif not isinstance(val, annotation):
                arguments[arg] = annotation(val)

        converted_args = bound_arguments.args
        converted_kwargs = bound_arguments.kwargs

        if measure_time:
            start = time_ns()
            fun(*converted_args, **converted_kwargs)
            return time_ns() - start
        else:
            return fun(*converted_args, **converted_kwargs)

    wrapper.__signature__ = signature
    SYSCALLS[fun.__name__] = wrapper
    return wrapper


@syscall
def posix_getattr(_: Context, path: Path) -> None:
    os.stat(path)


@syscall
def posix_open(ctx: Context, path: Path, flags: int, handle_id: int) -> None:
    ctx.fds[handle_id] = os.open(path, flags)


@syscall
def posix_release(ctx: Context, handle_id: int) -> None:
    os.close(ctx.fds.pop(handle_id))


@syscall
def posix_fsync(ctx: Context, handle_id: int) -> None:
    os.fsync(ctx.fds[handle_id])


@syscall
def posix_fdatasync(ctx: Context, handle_id: int) -> None:
    os.fdatasync(ctx.fds[handle_id])


@syscall
def posix_create(ctx: Context, path: Path, flags: int, mode: int,
                 handle_id: int) -> None:
    ctx.fds[handle_id] = os.open(path, flags, mode)


@syscall
def posix_mkdir(_: Context, path: Path, mode: int) -> None:
    os.mkdir(path, mode)


@syscall
def posix_mknod(_: Context, path: Path, mode: int) -> None:
    os.mknod(path, mode)


@syscall
def posix_unlink(_: Context, path: Path) -> None:
    os.unlink(path)


@syscall
def posix_rmdir(_: Context, path: Path) -> None:
    os.rmdir(path)


@syscall
def posix_getxattr(_: Context, path: Path, attr: str) -> None:
    os.getxattr(path, attr)


@syscall
def posix_setxattr(_: Context, path: Path, attr: str, val: str,
                   flags: int) -> None:
    os.setxattr(path, attr, bytes(val, 'utf8'), flags)


@syscall
def posix_removexattr(_: Context, path: Path, attr: str) -> None:
    os.removexattr(path, attr)


@syscall
def posix_listxattr(_: Context, path: Path) -> None:
    os.listxattr(path)


@syscall
def posix_read(ctx: Context, handle_id: int, size: int, offset: int) -> int:
    fd = ctx.fds[handle_id]
    os.lseek(fd, offset, os.SEEK_SET)

    start = time_ns()
    os.read(fd, size)
    return time_ns() - start


@syscall
def posix_write(ctx: Context, handle_id: int, size: int, offset: int) -> int:
    fd = ctx.fds[handle_id]
    os.lseek(fd, offset, os.SEEK_SET)
    content = bytes(size)

    start = time_ns()
    os.write(fd, content)
    return time_ns() - start


@syscall
def posix_rename(_: Context, src_path: Path, dst_path: Path) -> None:
    os.rename(src_path, dst_path)


@syscall
def posix_setattr(_: Context, path: Path, mask: int, mode: int, size: int,
                  atime: int, mtime: int) -> None:
    if mask & FUSE_SET_ATTR_MODE:
        os.chmod(path, mode)
    if mask & FUSE_SET_ATTR_SIZE:
        os.truncate(path, size)
    if mask & (FUSE_SET_ATTR_ATIME | FUSE_SET_ATTR_MTIME):
        os.utime(path, (atime, mtime))
    if mask & (FUSE_SET_ATTR_ATIME_NOW | FUSE_SET_ATTR_MTIME_NOW):
        os.utime(path)


@syscall
def posix_readdir(ctx: Context, path: Path, offset: int, size: int) -> int:
    if offset == 0:
        iterator = os.scandir(path)
    else:
        iterator = ctx.scandirs[path][offset].pop()

    new_offset = offset + (size if size < 128 else 128)
    start = time_ns()
    try:
        for _ in range(offset, new_offset):
            next(iterator)
    except StopIteration:
        return time_ns() - start
    else:
        end = time_ns()
        (ctx.scandirs
         .setdefault(path, {})
         .setdefault(new_offset, deque())
         .append(iterator))
        return end - start


IO_ENTRY_FIELDS = ['timestamp', 'op', 'duration', 'uuid',
                   'handle_id', 'retries', 'arg0', 'arg1',
                   'arg2', 'arg3', 'arg4', 'arg5', 'arg6']

IO_ENTRY_FIELDS_NUM = len(IO_ENTRY_FIELDS)


File = namedtuple('File', ['path', 'type', 'size'])


class IOEntry(namedtuple('IOEntry', IO_ENTRY_FIELDS)):
    __slots__ = ()

    @classmethod
    def from_str(cls, entry: str) -> 'IOEntry':
        fields = entry.split(',')
        fields_num = len(fields)

        # csv format do not require comma after last argument in line and
        # when there is no last argument (just ,\n) split returns one less
        # args than expected. In such case add empty string as default value
        if fields_num == IO_ENTRY_FIELDS_NUM - 1:
            fields.append('')
        elif fields_num == IO_ENTRY_FIELDS_NUM:
            pass
        else:
            raise ValueError('Expected {} number of arguments in entry instead '
                             'of specified {}'.format(IO_ENTRY_FIELDS_NUM,
                                                      fields_num))

        timestamp, op, duration, uuid, handle_id, *args = fields

        return cls(int(timestamp) * 1000, op, int(duration) * 1000, uuid,
                   int(handle_id), *args)


class IOTraceParser:

    def __init__(self, *, masked_files: Optional[Dict[str, str]] = None):
        self.syscalls = []
        self.io_duration = 0
        self.start_timestamp = 0
        self.end_timestamp = 0
        self.mount_dir_uuid = None
        self.masked_files = masked_files or {}

        self.root_dir = OrderedDict()
        self.initial_files = OrderedDict()
        self._current_files = OrderedDict()
        self._env = ChainMap(self._current_files, self.initial_files,
                             self.root_dir)
        self._open_fds = set()
        self._pending_lookups = {}

    def clean(self):
        self.__init__(masked_files=self.masked_files)

    def parse(self, io_trace_path: str):
        with open(io_trace_path) as trace_file:
            # ignore first line (headers)
            trace_file.readline()

            # second should contain mount entry
            try:
                mount_entry = IOEntry.from_str(trace_file.readline())
                assert mount_entry.op == 'mount', mount_entry.op
            except AssertionError as ex:
                print('Failed to parse trace file due to discovery of "{}" as '
                      'second entry instead of expected "mount" one'.format(ex))
                sys.exit(1)
            except Exception as ex:
                print('Failed to read "mount" entry at line 2, and as such '
                      'parse trace file, due to: {ex}'.format(ex=ex))
                sys.exit(1)
            else:
                self.mount_dir_uuid = mount_entry.uuid
                self.root_dir[mount_entry.uuid] = File('', 'd', [0, 0])

            for i, line in enumerate(trace_file, start=3):
                try:
                    entry = IOEntry.from_str(line)
                    operation = getattr(self, entry.op)
                    operation(entry)
                except Exception:
                    print('Parsing of line {} failed with:'.format(i),
                          file=sys.stderr)
                    traceback.print_exc(file=sys.stderr)
                else:
                    self.io_duration += entry.duration
                    self.end_timestamp = max(self.end_timestamp,
                                             entry.timestamp + entry.duration)

        self.syscalls.sort(key=lambda x: x[1])
        self.start_timestamp = self.syscalls[0][1]

    def lookup(self, entry: IOEntry):
        """[lookup] arg-0: child_name, arg-1: child_uuid, arg-2: child_type,
                    arg-3: child_size
        """
        file_type = entry.arg2
        file_size = int(entry.arg3) if file_type == 'f' else [0, 0]

        parent_dir = self._get_file(entry.uuid)
        uuid = entry.arg1
        path = self._join_path(parent_dir.path, entry.arg0)

        if uuid not in self._env:
            if entry.uuid == self.mount_dir_uuid:
                self.root_dir[uuid] = File(path, file_type, file_size)
            else:
                self.initial_files[uuid] = File(path, file_type, file_size)

        path_lookup = self._take_pending_lookup(parent_dir.path,
                                                entry.timestamp,
                                                entry.duration)
        self._pending_lookups.setdefault(path, []).insert(0, path_lookup)

    def getattr(self, entry: IOEntry):
        """[getattr] None"""
        path = self._get_file(entry.uuid).path
        timestamp, duration = self._take_pending_lookup(path, entry.timestamp,
                                                        entry.duration)
        self.syscalls.append((entry.op, timestamp, duration, path))

    def setattr(self, entry: IOEntry):
        """[setattr] arg-0: set_mask, arg-1: mode, arg-2: size, arg-3: atime,
                     arg-4: mtime
        """
        path = self._get_file(entry.uuid).path
        timestamp, duration = self._take_pending_lookup(path, entry.timestamp,
                                                        entry.duration)
        self.syscalls.append((entry.op, timestamp, duration, path, entry.arg0,
                              entry.arg1, entry.arg2, entry.arg3, entry.arg4))

    def readdir(self, entry: IOEntry):
        """[readdir] arg-0: max_entries, arg-1: offset, arg-2: entries_num"""
        offset = int(entry.arg1)
        entries_num = int(entry.arg2)

        if offset > 0 and entries_num == 0:
            return

        directory = self._get_file(entry.uuid)

        # '.' and '..' are also read
        dummy_files_to_create = offset + entries_num - 2 - directory.size[0]
        directory.size[1] = max(dummy_files_to_create, directory.size[1])

        timestamp, duration = self._take_pending_lookup(directory.path,
                                                        entry.timestamp,
                                                        entry.duration)
        self.syscalls.append((entry.op, timestamp, duration, directory.path,
                              offset, entries_num))

    def open(self, entry: IOEntry):
        """[open] arg-0: flags"""
        self._open_fds.add(entry.handle_id)
        path = self._get_file(entry.uuid).path
        timestamp, duration = self._take_pending_lookup(path, entry.timestamp,
                                                        entry.duration)
        self.syscalls.append((entry.op, timestamp, duration, path,
                              entry.arg0, entry.handle_id))

    def create(self, entry: IOEntry):
        """[create] arg-0: name, arg-1: new_uuid, arg-2: mode, arg-3: flags"""
        parent_dir = self._get_file(entry.uuid)
        parent_dir.size[0] += 1

        self._open_fds.add(entry.handle_id)
        path = self._join_path(parent_dir.path, entry.arg0)
        self._env[entry.arg1] = File(path, 'f', 0)
        timestamp, duration = self._take_pending_lookup(parent_dir.path,
                                                        entry.timestamp,
                                                        entry.duration)
        self.syscalls.append((entry.op, timestamp, duration, path, entry.arg3,
                              entry.arg2, entry.handle_id))

    def mkdir(self, entry: IOEntry):
        """[mkdir] arg-0: name, arg-1: new_dir_uuid, arg-2: mode"""
        parent_dir = self._get_file(entry.uuid)
        parent_dir.size[0] += 1

        path = self._join_path(parent_dir.path, entry.arg0)
        self._env[entry.arg1] = File(path, 'd', [0, 0])

        timestamp, duration = self._take_pending_lookup(parent_dir.path,
                                                        entry.timestamp,
                                                        entry.duration)
        self.syscalls.append((entry.op, timestamp, duration, path, entry.arg2))

    def mknod(self, entry: IOEntry):
        """[mknod] arg-0: name, arg-1: new_node_uuid, arg-2: mode"""
        parent_dir = self._get_file(entry.uuid)
        parent_dir.size[0] += 1

        path = self._join_path(parent_dir.path, entry.arg0)
        self._env[entry.arg1] = File(path, 'f', 0)

        timestamp, duration = self._take_pending_lookup(parent_dir.path,
                                                        entry.timestamp,
                                                        entry.duration)
        self.syscalls.append((entry.op, timestamp, duration, path, entry.arg2))

    def unlink(self, entry: IOEntry):
        """[unlink] arg-0: name, arg-1: uuid"""
        parent_dir = self._get_file(entry.uuid)
        parent_dir.size[0] -= 1

        file = self._get_file(entry.arg1)
        # prior to unlink lookup is made to check if entity already exists
        timestamp, duration = self._take_pending_lookup(file.path,
                                                        entry.timestamp,
                                                        entry.duration)
        if file.type == 'd':
            self.syscalls.append(('rmdir', timestamp, duration, file.path))
        else:
            self.syscalls.append(('unlink', timestamp, duration, file.path))

    def rename(self, entry: IOEntry):
        """[rename] arg-0: name, arg-1: old_uuid, arg-2: new_parent_uuid,
                    arg-3: new_name, arg-4: new_uuid
        """
        src_parent_dir = self._get_file(entry.uuid)
        src_parent_dir.size[0] -= 1

        src_file = self._get_file(entry.arg1)
        src_path = self._join_path(src_parent_dir.path, entry.arg0)

        dst_parent_dir = self._get_file(entry.arg2)
        dst_parent_dir.size[0] += 1

        dst_path = self._join_path(dst_parent_dir.path, entry.arg3)

        self._env[entry.arg4] = File(dst_path, src_file.type, src_file.size)

        timestamp, duration = self._take_pending_lookup(src_path,
                                                        entry.timestamp,
                                                        entry.duration)
        self.syscalls.append((entry.op, timestamp, duration,
                              src_path, dst_path))

    def getxattr(self, entry: IOEntry):
        """[getxattr] arg-0: name"""
        path = self._get_file(entry.uuid).path
        timestamp, duration = self._take_pending_lookup(path, entry.timestamp,
                                                        entry.duration)
        self.syscalls.append((entry.op, timestamp, duration, path, entry.arg0))

    def setxattr(self, entry: IOEntry):
        """[setxattr] arg-0: name, arg-1: val, arg-2: create, arg-3: replace"""
        if int(entry.arg2):
            flags = os.XATTR_CREATE
        elif int(entry.arg3):
            flags = os.XATTR_REPLACE
        else:
            flags = 0

        path = self._get_file(entry.uuid).path
        timestamp, duration = self._take_pending_lookup(path, entry.timestamp,
                                                        entry.duration)
        self.syscalls.append((entry.op, timestamp, duration, path, entry.arg0,
                              entry.arg1, flags))

    def removexattr(self, entry: IOEntry):
        """[removexattr] arg-0: name"""
        path = self._get_file(entry.uuid).path
        timestamp, duration = self._take_pending_lookup(path, entry.timestamp,
                                                        entry.duration)
        self.syscalls.append((entry.op, timestamp, duration, path, entry.arg0))

    def listxattr(self, entry: IOEntry):
        """[listxattr] None"""
        path = self._get_file(entry.uuid).path
        timestamp, duration = self._take_pending_lookup(path, entry.timestamp,
                                                        entry.duration)
        self.syscalls.append((entry.op, timestamp, duration, path))

    def read(self, entry: IOEntry):
        """[read] arg-0: offset, arg-1: size, arg-2: local_read,
                  arg-3: prefetch_size, arg-4: prefetch_type
        """
        self.syscalls.append((entry.op, entry.timestamp, entry.duration,
                              entry.handle_id, entry.arg1, entry.arg0))

    def write(self, entry: IOEntry):
        """[write] arg-0: offset, arg-1: size"""
        self.syscalls.append((entry.op, entry.timestamp, entry.duration,
                              entry.handle_id, entry.arg1, entry.arg0))

    def fsync(self, entry: IOEntry):
        """[fsync] arg-0: data_only"""
        # 'fsync' is called, alongside with 'release', on 'close' syscall
        # (but generally a bit later). So 'fsync' after 'release' should be
        # ignored as part of 'close' syscall.
        if entry.handle_id in self._open_fds:
            op = 'fdatasync' if int(entry.arg0) else 'fsync'
            self.syscalls.append((op, entry.timestamp, entry.duration,
                                  entry.handle_id))

    def release(self, entry: IOEntry):
        """[release] None"""
        self._open_fds.remove(entry.handle_id)
        self.syscalls.append((entry.op, entry.timestamp, entry.duration,
                              entry.handle_id))

    def flush(self, _: IOEntry):
        """[flush] None"""
        # This is called on 'close', alongside with 'release', so ignore it as
        # 'close' is/was scheduled on 'release' already.
        pass

    def _take_pending_lookup(self, path: str, timestamp: int,
                             duration: int) -> Tuple[int, int]:
        pending_lookups_for_path = self._pending_lookups.get(path, [])
        for pl in pending_lookups_for_path:
            pl_timestamp, pl_duration = pl
            if 0 <= timestamp - pl_timestamp - pl_duration <= CTX_SWITCH_DELAY:
                pending_lookups_for_path.remove(pl)
                new_lookup = (pl_timestamp, timestamp + duration - pl_timestamp)
                break
        else:
            new_lookup = (timestamp, duration)

        return new_lookup

    def _get_file(self, uuid: str) -> File:
        file = self._env.get(uuid)
        if file:
            return file
        else:
            raise ValueError('unknown file with uuid {}'.format(uuid))

    def _join_path(self, parent_path: str, name: str) -> str:
        path = os.path.join(parent_path, name)
        return self.masked_files.get(path, path)


def pairwise(iterable):
    a, b = tee(iterable)
    next(b)
    return zip_longest(a, b)


def replay(parser: IOTraceParser, mount_path: str) -> None:
    delays = 0
    io_duration = 0

    ctx = Context(mount_path, {}, {})
    for sc, next_sc in pairwise(parser.syscalls):
        try:
            io_duration += SYSCALLS['posix_{}'.format(sc[0])](ctx, *sc[3:])
        except Exception as ex:
            print('Failed to execute {} due to {!r}'.format(sc[0], ex),
                  file=sys.stderr)
            continue

        if next_sc:
            delay = next_sc[1] - (sc[1] + sc[2])
            if delay < 0:
                delay = next_sc[1] - sc[1]

            delays += delay
            sleep(delay/10**9)

    prog_duration = io_duration + delays
    prev_prog_duration = parser.end_timestamp - parser.start_timestamp

    overhead = io_duration / prog_duration
    prev_overhead = parser.io_duration / prev_prog_duration

    print('Statistics (original/replayed):',
          '\n\tIO duration [ns]:      {}/{}'.format(parser.io_duration,
                                                    io_duration),
          '\n\tProgram duration [ns]: {}/{}'.format(prev_prog_duration,
                                                    prog_duration),
          '\n\tOverhead:              {:0.5f}/{:0.5f}'.format(prev_overhead,
                                                              overhead))


def create_env(initial_files: Dict[str, File], mount_path: str) -> None:
    # first pass to create initial files and directories
    for file in initial_files.values():
        path = os.path.join(mount_path, file.path)
        if not os.path.exists(path):
            try:
                if file.type == 'f':
                    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
                    os.truncate(fd, file.size)
                elif file.type == 'd':
                    os.mkdir(path)
            except Exception as ex:
                print('Failed to create {} due to {!r}'.format(path, ex))
                sys.exit(1)

    # second pass to create dummy files in directories (for readdir)
    for file in initial_files.values():
        if file.type == 'd':
            dir_path = os.path.join(mount_path, file.path)
            try:
                dir_content = os.listdir(dir_path)
                files_to_create = max(0, file.size[1] - len(dir_content))
                for _ in range(files_to_create):
                    file_name = str(uuid4())
                    while file_name in dir_content:
                        file_name = str(uuid4())

                    file_path = os.path.join(dir_path, file_name)
                    os.close(os.open(file_path,
                                     os.O_WRONLY | os.O_CREAT | os.O_TRUNC))
            except Exception as ex:
                print('Failed to create dummy file in {} due to '
                      '{!r}'.format(dir_path, ex), file=sys.stderr)
                sys.exit(1)


def print_env_report(syscalls, initial_files: Dict[str, File]) -> None:
    width = 80
    table_line = '  ' + '-' * (width - 4)
    horizontal_line = '_' * width
    horizontal_bold_line = '=' * width

    print('\n\n', horizontal_bold_line,
          '\n{title:^{width}}\n'.format(title="ENVIRONMENT REPORT",
                                        width=width),
          horizontal_bold_line, sep='')

    print('\n', horizontal_line,
          '\n\n{title:^{width}}\n'.format(title='INITIAL FILES', width=width),
          '\n   File Type | File Size [B] | Path\n',
          table_line, sep='')
    for file in initial_files.values():
        print('  {type:^11}| {size:13d} | {path}'
              ''.format(type='dir' if file.type == 'd' else 'file',
                        size=file.size if file.type == 'f' else 0,
                        path=file.path))

    print('\n', horizontal_line,
          '\n\n{title:^{width}}\n'.format(title='CREATED FILES', width=width),
          '\n   File Type | Path\n',
          table_line, sep='')
    for sc in syscalls:
        if sc[0] in ('mknod', 'create'):
            file_type = 'file'
        elif sc[0] == 'mkdir':
            file_type = 'dir'
        else:
            continue

        print('  {:^11}| {}'.format(file_type, sc[3]))

    print('\n', horizontal_line,
          '\n\n{title:^{width}}\n'.format(title='REMOVED FILES', width=width),
          sep='')
    for sc in syscalls:
        if sc[0] in ('unlink', 'rmdir)'):
            print('  {}'.format(sc[3]))

    print('\n', horizontal_line,
          '\n\n{title:^{width}}\n'.format(title='RENAMED FILES', width=width),
          sep='')
    for sc in syscalls:
        if sc[0] == 'rename':
            print('  {} -> {}'.format(sc[3], sc[4]))

    print('\n\n', horizontal_bold_line, sep='')


def sort_trace_file(path: str, chunk_size: int = DEFAULT_CHUNK_SIZE) -> None:
    key_fun = lambda line: int(line.split(',', maxsplit=1)[0])

    with TemporaryDirectory() as tmpdir, open(path, 'r+') as trace_file:
        chunks = []

        headers = trace_file.readline()
        mount_entry = trace_file.readline()
        input_iterator = iter(trace_file)

        while True:
            current_chunk = list(islice(input_iterator, chunk_size))
            if not current_chunk:
                break

            current_chunk.sort(key=key_fun)

            output_chunk_path = os.path.join(tmpdir,
                                             'chunk{}'.format(len(chunks)))
            output_chunk = open(output_chunk_path, 'w+')
            output_chunk.writelines(current_chunk)
            output_chunk.flush()
            output_chunk.seek(0)
            chunks.append(output_chunk)

        trace_file.seek(0)
        trace_file.write(headers)
        trace_file.write(mount_entry)
        trace_file.writelines(heapq.merge(*chunks, key=key_fun))

        for output_chunk in chunks:
            with suppress(Exception):
                output_chunk.close()


def main():
    parser = argparse.ArgumentParser(prog='ioreplay',
                                     description='Replay recorded activities '
                                                 'performed using Oneclient')
    parser.add_argument('io_trace_path',
                        help='Path to trace file containing recorder io')
    parser.add_argument('-s', '--sort-trace',
                        action='store_true',
                        help='If specified trace file will be sorted. '
                             'Otherwise it is assumed that trace file is '
                             'already sorted')
    parser.add_argument('--chunk-size',
                        type=int,
                        metavar='SIZE',
                        default=DEFAULT_CHUNK_SIZE,
                        help='Number of lines read at once and sorted during '
                             'external sort of trace file')
    parser.add_argument('-m', '--mount-path',
                        help='Path to mounted Oneclient. If not specified '
                             'program will perform dry run meaning it will '
                             'parse trace file but not call any system calls')
    parser.add_argument('-g', '--syscalls',
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
    parser.add_argument('-l', '--replace',
                        action='append', default=[],
                        help='Allows to mask files by specifying alternate '
                             'path in form: <original_path>:<alternate_path> '
                             '(e.q krk-c/one/data:krk-c/one/data2). Only last '
                             'component of path should differ')
    parser.add_argument('-r', '--run',
                        action='store_true',
                        help='If specified, alongside `-m`, recorded system '
                             'calls will be replayed')
    args = parser.parse_args()

    if args.sort_trace:
        sort_trace_file(args.io_trace_path, args.chunk_size)

    masked_files = dict(paths.split(':') for paths in args.replace)
    parser = IOTraceParser(masked_files=masked_files)
    parser.parse(args.io_trace_path)

    if args.syscalls:
        pprint(parser.syscalls, width=100)
    if args.env_report:
        print_env_report(parser.syscalls, parser.initial_files)
    if args.mount_path:
        if args.create_env:
            create_env(parser.initial_files, args.mount_path)
        if args.run:
            replay(parser, args.mount_path)


if __name__ == '__main__':
    main()
