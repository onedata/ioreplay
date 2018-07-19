"""This module contains parser that takes as input path to file containing
io trace and returns estimated system calls.
"""

__author__ = "Bartosz Walkowicz"
__copyright__ = "Copyright (C) 2018 ACK CYFRONET AGH"
__license__ = "This software is released under the Apache license cited in " \
              "LICENSE"

import os
import sys
from collections import namedtuple

from operations import (GetAttr, Open, Release, Fsync, Create, MkDir,
                        MkNod, Unlink, GetXAttr, SetXAttr, RemoveXAttr,
                        ListXAttr, Read, Write, Rename, SetAttr)


IO_ENTRY_FIELDS = ['timestamp', 'op', 'duration', 'uuid',
                   'handle_id', 'retries', 'arg0', 'arg1',
                   'arg2', 'arg3', 'arg4', 'arg5', 'arg6']

IO_ENTRY_FIELDS_NUM = len(IO_ENTRY_FIELDS)


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
            raise ValueError('Required {} arguments in entry, '
                             'instead given {}'.format(IO_ENTRY_FIELDS_NUM,
                                                       fields_num))

        timestamp, op, duration, uuid, handle_id, retries, *args = fields

        return cls(int(timestamp), op, int(duration), uuid,
                   int(handle_id), int(retries), *args)


class IOTraceParser:

    def __init__(self, *, mount_path: str, create_env: bool = False):
        self.mount_path = mount_path
        self.create_env = create_env

        self.mount_dir_uuid = ''

        self.env = {}
        self.syscalls = []
        self.pending_lookups = {}

    def parse(self, io_trace_path: str):
        with open(io_trace_path) as f:
            # ignore first line as it contains headers only
            f.readline()

            # second line contains mount_dir_uuid (this is getattr on mount_dir)
            mount_dir_getattr_entry = IOEntry.from_str(f.readline())
            mount_dir_uuid = mount_dir_getattr_entry.uuid
            self.mount_dir_uuid = mount_dir_uuid
            self.env[mount_dir_uuid] = self.mount_path

            for i, line in enumerate(f):
                try:
                    entry = IOEntry.from_str(line)
                    op = getattr(self, entry.op)
                except ValueError as ex:
                    print('Failed to parse line {} due to {}'.format(i, ex),
                          file=sys.stderr)
                except AttributeError:
                    print('Failed to parse line {} due to unrecognized '
                          'operation'.format(i), file=sys.stderr)
                else:
                    op(entry)

        return sorted(self.syscalls, key=lambda syscall: syscall.timestamp)

    def lookup(self, entry: IOEntry):
        """[lookup] arg-0: child_name, arg-1: child_uuid, arg-2: child_type,
                    arg-3: child_size
        """
        parent_dir = self.env[entry.uuid]
        uuid = entry.arg1
        path = os.path.join(parent_dir, entry.arg0)
        if self.create_env and uuid not in self.env and not os.path.exists(path):
            file_type = entry.arg2
            try:
                if file_type == 'd':
                    os.mkdir(path)
                if file_type == 'f':
                    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
                    os.truncate(fd, int(entry.arg3))
            except Exception as ex:
                print('Failed to create {} due to {!r}'.format(path, ex),
                      file=sys.stderr)

        self.env[uuid] = path

        path_lookup = self._take_pending_lookup(parent_dir, entry.timestamp,
                                                entry.duration)

        self.pending_lookups.setdefault(path, []).insert(0, path_lookup)

    def getattr(self, entry: IOEntry):
        """[getattr] None"""
        path = self.env[entry.uuid]
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

        path = self.env[entry.uuid]
        timestamp, duration = self._take_pending_lookup(path, entry.timestamp,
                                                        entry.duration)
        self.syscalls.append(SetAttr(timestamp, duration, path, set_mask,
                                     mode, size, atime, mtime))

    def readdir(self, entry: IOEntry):
        """[readdir] arg-0: max_entries, arg-1: offset"""
        pass

    def open(self, entry: IOEntry):
        """[open] arg-0: flags"""
        flags = int(entry.arg0)
        path = self.env[entry.uuid]
        timestamp, duration = self._take_pending_lookup(path, entry.timestamp,
                                                        entry.duration)
        self.syscalls.append(Open(timestamp, duration, path, flags,
                                  entry.handle_id))

    def release(self, entry: IOEntry):
        """[release] None"""
        self.syscalls.append(Release(entry.timestamp, entry.duration,
                                     entry.handle_id))

    def fsync(self, entry: IOEntry):
        """[fsync] arg-0: data_only"""
        data_only = bool(int(entry.arg0))
        self.syscalls.append(Fsync(entry.timestamp, entry.duration,
                                   entry.handle_id, data_only))

    def flush(self, entry: IOEntry):
        """[flush] None"""
        pass

    def create(self, entry: IOEntry):
        """[create] arg-0: name, arg-1: new_uuid, arg-2: mode, arg-3: flags"""
        mode = int(entry.arg2)
        flags = int(entry.arg3)

        parent_dir = self.env[entry.uuid]
        path = os.path.join(parent_dir, entry.arg0)
        self.env[entry.arg1] = path
        timestamp, duration = self._take_pending_lookup(parent_dir,
                                                        entry.timestamp,
                                                        entry.duration)
        self.syscalls.append(Create(timestamp, duration, path, flags, mode,
                                    entry.handle_id))

    def mkdir(self, entry: IOEntry):
        """[mkdir] arg-0: name, arg-1: new_dir_uuid, arg-2: mode"""
        self._mk(entry, MkDir)

    def mknod(self, entry: IOEntry):
        """[mknod] arg-0: name, arg-1: new_node_uuid, arg-2: mode"""
        self._mk(entry, MkNod)

    def unlink(self, entry: IOEntry):
        """[unlink] arg-0: name"""
        # prior to unlink lookup is made to check if entity
        # already exists; remove this lookup
        path = os.path.join(self.env[entry.uuid], entry.arg0)
        timestamp, duration = self._take_pending_lookup(path, entry.timestamp,
                                                        entry.duration)
        self.syscalls.append(Unlink(timestamp, duration, path))

    def rename(self, entry: IOEntry):
        """[rename] arg-0: name, arg-1: new_parent_uuid, arg-2: new_name,
                    arg-3: new_uuid
        """
        src_parent_dir = self.env[entry.uuid]
        src_path = os.path.join(src_parent_dir, entry.arg0)

        dst_parent_dir = self.env[entry.arg1]
        dst_path = os.path.join(dst_parent_dir, entry.arg2)

        self.env[entry.arg3] = dst_path

        timestamp, duration = self._take_pending_lookup(src_path,
                                                        entry.timestamp,
                                                        entry.duration)
        self.syscalls.append(Rename(timestamp, duration, src_path, dst_path))

    def getxattr(self, entry: IOEntry):
        """[getxattr] arg-0: name"""
        path = self.env[entry.uuid]
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

        path = self.env[entry.uuid]
        timestamp, duration = self._take_pending_lookup(path, entry.timestamp,
                                                        entry.duration)
        self.syscalls.append(SetXAttr(timestamp, duration, path,
                                      attr, val, flags))

    def removexattr(self, entry: IOEntry):
        """[removexattr] arg-0: name"""
        path = self.env[entry.uuid]
        timestamp, duration = self._take_pending_lookup(path, entry.timestamp,
                                                        entry.duration)
        self.syscalls.append(RemoveXAttr(timestamp, duration, path, entry.arg0))

    def listxattr(self, entry: IOEntry):
        """[listxattr] None"""
        path = self.env[entry.uuid]
        timestamp, duration = self._take_pending_lookup(path, entry.timestamp,
                                                        entry.duration)
        self.syscalls.append(ListXAttr(timestamp, duration, path))

    def read(self, entry: IOEntry):
        """[read] arg-0: offset, arg-1: size, arg-2: local_read,
                  arg-3: prefetch_size, arg-4: prefetch_type
        """
        self._rw(entry, Read)

    def write(self, entry: IOEntry):
        """[write] arg-0: offset, arg-1: size"""
        self._rw(entry, Write)

    def _take_pending_lookup(self, path: str, timestamp: int, duration: int):
        pending_lookups_for_path = self.pending_lookups.get(path, [])
        for pl in pending_lookups_for_path:
            pl_timestamp, pl_duration = pl
            if pl_timestamp + pl_duration <= timestamp:
                pending_lookups_for_path.remove(pl)
                new_lookup = (pl_timestamp, timestamp + duration - pl_timestamp)
                break
        else:
            new_lookup = (timestamp, duration)

        return new_lookup

    def _rw(self, entry: IOEntry, syscall):
        offset = int(entry.arg0)
        size = int(entry.arg1)
        handle_id = entry.handle_id
        self.syscalls.append(syscall(entry.timestamp, entry.duration,
                                     handle_id, size, offset))

    def _mk(self, entry: IOEntry, syscall):
        mode = int(entry.arg2)

        parent_dir = self.env[entry.uuid]
        path = os.path.join(parent_dir, entry.arg0)
        self.env[entry.arg1] = path

        timestamp, duration = self._take_pending_lookup(parent_dir,
                                                        entry.timestamp,
                                                        entry.duration)
        self.syscalls.append(syscall(timestamp, duration, path, mode))
