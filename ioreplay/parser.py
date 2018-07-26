"""This module contains parser capable of parsing io trace file into
system calls.
"""

__author__ = "Bartosz Walkowicz"
__copyright__ = "Copyright (C) 2018 ACK CYFRONET AGH"
__license__ = "This software is released under the Apache license cited in " \
              "LICENSE"

import os
import traceback
from typing import Optional, Tuple, Dict
from collections import namedtuple, OrderedDict, ChainMap

from operations import (GetAttr, Open, Release, Fsync, Create, MkDir,
                        MkNod, Unlink, RmDir, GetXAttr, SetXAttr, RemoveXAttr,
                        ListXAttr, Read, Write, Rename, SetAttr, ReadDir)
from utils import info, error, warning


# Max delay, in us, between subsequent fuse calls of the same system call
# (every syscall translates up to several fuse calls) caused by context switch
# between kernel and userland.
CTX_SWITCH_DELAY = 250

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
            raise ValueError(f'Expected {IO_ENTRY_FIELDS_NUM} number of '
                             f'arguments in entry instead of '
                             f'specified {fields_num:>5}')

        timestamp, op, duration, uuid, handle_id, retries, *args = fields

        return cls(int(timestamp) * 1000, op, int(duration) * 1000, uuid,
                   int(handle_id), int(retries), *args)


# noinspection PyBroadException
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
        info(f'Beginning parsing of {io_trace_path} trace file')
        with open(io_trace_path) as f:
            # ignore first line (headers)
            f.readline()

            # second should contain mount entry
            try:
                mount_entry = IOEntry.from_str(f.readline())
                assert mount_entry.op == 'mount', mount_entry.op
            except AssertionError as ex:
                error(f'Failed to parse trace file due to discovery of "{ex}" '
                      f'as second entry instead of expected "mount" one')
                exit(1)
            except Exception as ex:
                error(f'Failed to read "mount" entry at line 2, and as such '
                      f'parse trace file, due to: {ex}')
                exit(1)
            else:
                self.mount_dir_uuid = mount_entry.uuid
                self.root_dir[mount_entry.uuid] = File('', 'd', [0, 0])

            lines = f.readlines()

        lines.sort(key=lambda x: int(x.split(',', maxsplit=1)[0]))

        err_count = 0
        for i, line in enumerate(lines, start=3):
            try:
                entry = IOEntry.from_str(line)
                operation = getattr(self, entry.op)
                operation(entry)
            except Exception:
                err_count += 0
                warning(f'Parsing of line {i} failed with:')
                traceback.print_exc()
            else:
                self.io_duration += entry.duration
                self.end_timestamp = max(self.end_timestamp,
                                         entry.timestamp + entry.duration)

        info(f'Finished parsing of {io_trace_path} trace file with {err_count} '
             f'problems encountered')
        self.syscalls.sort(key=lambda syscall: syscall.timestamp)
        self.start_timestamp = self.syscalls[0].timestamp

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
            parent_dir.size[0] += 1
            parent_dir.size[1] -= 1
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
        self.syscalls.append(GetAttr(timestamp, duration, path))

    def setattr(self, entry: IOEntry):
        """[setattr] arg-0: set_mask, arg-1: mode, arg-2: size, arg-3: atime,
                     arg-4: mtime
        """
        set_mask = int(entry.arg0)
        mode = int(entry.arg1)
        size = int(entry.arg2)
        atime = int(entry.arg3)
        mtime = int(entry.arg4)

        path = self._get_file(entry.uuid).path
        timestamp, duration = self._take_pending_lookup(path, entry.timestamp,
                                                        entry.duration)
        self.syscalls.append(SetAttr(timestamp, duration, path, set_mask,
                                     mode, size, atime, mtime))

    def readdir(self, entry: IOEntry):
        """[readdir] arg-0: max_entries, arg-1: offset, arg-2: entries_num"""
        offset = int(entry.arg1)
        entries_num = int(entry.arg2)

        if offset > 0 and entries_num == 0:
            return

        directory = self._get_file(entry.uuid)

        if entry.uuid != self.mount_dir_uuid:
            # if offset == 0 '.' and '..' will also be read
            size = offset + entries_num - 2 - directory.size[0]
            directory.size[1] = max(size, directory.size[1])

        timestamp, duration = self._take_pending_lookup(directory.path,
                                                        entry.timestamp,
                                                        entry.duration)
        self.syscalls.append(ReadDir(timestamp, duration, directory.path,
                                     offset, entries_num))

    def open(self, entry: IOEntry):
        """[open] arg-0: flags"""
        flags = int(entry.arg0)

        path = self._get_file(entry.uuid).path
        timestamp, duration = self._take_pending_lookup(path, entry.timestamp,
                                                        entry.duration)
        self._open_fds.add(entry.handle_id)
        self.syscalls.append(Open(timestamp, duration, path, flags,
                                  entry.handle_id))

    def flush(self, entry: IOEntry):
        """[flush] None"""
        # This is called on 'close', alongside with 'release', so ignore it as
        # 'close' is/was scheduled on 'release' already.
        pass

    def create(self, entry: IOEntry):
        """[create] arg-0: name, arg-1: new_uuid, arg-2: mode, arg-3: flags"""
        mode = int(entry.arg2)
        flags = int(entry.arg3)

        parent_dir = self._get_file(entry.uuid)
        parent_dir.size[0] += 1

        path = self._join_path(parent_dir.path, entry.arg0)
        self._env[entry.arg1] = File(path, 'f', 0)
        timestamp, duration = self._take_pending_lookup(parent_dir.path,
                                                        entry.timestamp,
                                                        entry.duration)
        self._open_fds.add(entry.handle_id)
        self.syscalls.append(Create(timestamp, duration, path, flags, mode,
                                    entry.handle_id))

    def mkdir(self, entry: IOEntry):
        """[mkdir] arg-0: name, arg-1: new_dir_uuid, arg-2: mode"""
        mode = int(entry.arg2)

        parent_dir = self._get_file(entry.uuid)
        parent_dir.size[0] += 1

        path = self._join_path(parent_dir.path, entry.arg0)
        self._env[entry.arg1] = File(path, 'd', [0, 0])

        timestamp, duration = self._take_pending_lookup(parent_dir.path,
                                                        entry.timestamp,
                                                        entry.duration)
        self.syscalls.append(MkDir(timestamp, duration, path, mode))

    def mknod(self, entry: IOEntry):
        """[mknod] arg-0: name, arg-1: new_node_uuid, arg-2: mode"""
        mode = int(entry.arg2)

        parent_dir = self._get_file(entry.uuid)
        parent_dir.size[0] += 1

        path = self._join_path(parent_dir.path, entry.arg0)
        self._env[entry.arg1] = File(path, 'f', 0)

        timestamp, duration = self._take_pending_lookup(parent_dir.path,
                                                        entry.timestamp,
                                                        entry.duration)
        self.syscalls.append(MkNod(timestamp, duration, path, mode))

    def unlink(self, entry: IOEntry):
        """[unlink] arg-0: name, arg-1: uuid"""
        # prior to unlink lookup is made to check if entity already exists
        parent_dir = self._get_file(entry.uuid)
        parent_dir.size[0] -= 1

        file = self._get_file(entry.arg1)
        timestamp, duration = self._take_pending_lookup(file.path,
                                                        entry.timestamp,
                                                        entry.duration)
        if file.type == 'd':
            self.syscalls.append(RmDir(timestamp, duration, file.path))
        else:
            self.syscalls.append(Unlink(timestamp, duration, file.path))

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
        self.syscalls.append(Rename(timestamp, duration, src_path, dst_path))

    def getxattr(self, entry: IOEntry):
        """[getxattr] arg-0: name"""
        path = self._get_file(entry.uuid).path
        timestamp, duration = self._take_pending_lookup(path, entry.timestamp,
                                                        entry.duration)
        self.syscalls.append(GetXAttr(timestamp, duration, path, entry.arg0))

    def setxattr(self, entry: IOEntry):
        """[setxattr] arg-0: name, arg-1: val, arg-2: create, arg-3: replace"""
        attr = entry.arg0
        val = bytes(entry.arg1, 'utf8')

        if int(entry.arg2):
            flags = os.XATTR_CREATE
        elif int(entry.arg3):
            flags = os.XATTR_REPLACE
        else:
            flags = 0

        path = self._get_file(entry.uuid).path
        timestamp, duration = self._take_pending_lookup(path, entry.timestamp,
                                                        entry.duration)
        self.syscalls.append(SetXAttr(timestamp, duration, path,
                                      attr, val, flags))

    def removexattr(self, entry: IOEntry):
        """[removexattr] arg-0: name"""
        path = self._get_file(entry.uuid).path
        timestamp, duration = self._take_pending_lookup(path, entry.timestamp,
                                                        entry.duration)
        self.syscalls.append(RemoveXAttr(timestamp, duration, path, entry.arg0))

    def listxattr(self, entry: IOEntry):
        """[listxattr] None"""
        path = self._get_file(entry.uuid).path
        timestamp, duration = self._take_pending_lookup(path, entry.timestamp,
                                                        entry.duration)
        self.syscalls.append(ListXAttr(timestamp, duration, path))

    def read(self, entry: IOEntry):
        """[read] arg-0: offset, arg-1: size, arg-2: local_read,
                  arg-3: prefetch_size, arg-4: prefetch_type
        """
        offset = int(entry.arg0)
        size = int(entry.arg1)
        self.syscalls.append(Read(entry.timestamp, entry.duration,
                                   entry.handle_id, size, offset))

    def write(self, entry: IOEntry):
        """[write] arg-0: offset, arg-1: size"""
        offset = int(entry.arg0)
        size = int(entry.arg1)
        self.syscalls.append(Write(entry.timestamp, entry.duration,
                                   entry.handle_id, size, offset))

    def fsync(self, entry: IOEntry):
        """[fsync] arg-0: data_only"""
        # 'fsync' is called, alongside with 'release', on 'close' syscall
        # (but generally a bit later). So 'fsync' after 'release' should be
        # ignored as part of 'close' syscall.
        if entry.handle_id in self._open_fds:
            data_only = bool(int(entry.arg0))
            self.syscalls.append(Fsync(entry.timestamp, entry.duration,
                                       entry.handle_id, data_only))

    def release(self, entry: IOEntry):
        """[release] None"""
        self._open_fds.remove(entry.handle_id)
        self.syscalls.append(Release(entry.timestamp, entry.duration,
                                     entry.handle_id))

    def _take_pending_lookup(self, path: str, timestamp: int,
                             duration: int) -> Tuple[int, int]:
        pending_lookups_for_path = self._pending_lookups.get(path, [])
        for pl in pending_lookups_for_path:
            pl_timestamp, pl_duration = pl
            if 0 <= timestamp - (pl_timestamp + pl_duration) <= CTX_SWITCH_DELAY:
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
            raise ValueError(f'unknown file with uuid {uuid}')

    def _join_path(self, parent_path: str, name: str) -> str:
        path = os.path.join(parent_path, name)
        return self.masked_files.get(path, path)
