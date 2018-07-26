"""This module contains system calls wrappers"""

__author__ = "Bartosz Walkowicz"
__copyright__ = "Copyright (C) 2018 ACK CYFRONET AGH"
__license__ = "This software is released under the Apache license cited in " \
              "LICENSE"

import os
from time import time_ns
from collections import namedtuple, deque


FUSE_SET_ATTR_MODE = (1 << 0)
FUSE_SET_ATTR_SIZE = (1 << 3)
FUSE_SET_ATTR_ATIME = (1 << 4)
FUSE_SET_ATTR_MTIME = (1 << 5)
FUSE_SET_ATTR_ATIME_NOW = (1 << 7)
FUSE_SET_ATTR_MTIME_NOW = (1 << 8)

BASIC_FIELDS = ['timestamp', 'duration']


Context = namedtuple('Context', ['mount_path', 'fds', 'scandirs'])


class GetAttr(namedtuple('GetAttr', BASIC_FIELDS + ['path'])):
    __slots__ = ()

    def perform(self, ctx: Context) -> int:
        path = os.path.join(ctx.mount_path, self.path)
        s = time_ns()
        os.stat(path)
        return time_ns() - s


class Open(namedtuple('Open', BASIC_FIELDS + ['path', 'flags', 'handle_id'])):
    __slots__ = ()

    def perform(self, ctx: Context) -> int:
        path = os.path.join(ctx.mount_path, self.path)
        s = time_ns()
        fd = os.open(path, self.flags)
        e = time_ns()
        ctx.fds[self.handle_id] = fd
        return e - s


class Release(namedtuple('Release', BASIC_FIELDS + ['handle_id'])):
    __slots__ = ()

    def perform(self, ctx: Context) -> int:
        fd = ctx.fds.pop(self.handle_id)
        s = time_ns()
        os.close(fd)
        return time_ns() - s


class Fsync(namedtuple('Fsync', BASIC_FIELDS + ['handle_id', 'data_only'])):
    __slots__ = ()

    def perform(self, ctx: Context) -> int:
        fd = ctx.fds[self.handle_id]
        sync = os.fdatasync if self.data_only else os.fsync
        s = time_ns()
        sync(fd)
        return time_ns() - s


class Create(namedtuple('Create', BASIC_FIELDS + ['path', 'flags', 'mode',
                                                  'handle_id'])):
    __slots__ = ()

    def perform(self, ctx: Context) -> int:
        path = os.path.join(ctx.mount_path, self.path)
        s = time_ns()
        fd = os.open(path, self.flags, self.mode)
        e = time_ns()
        ctx.fds[self.handle_id] = fd
        return e - s


class MkDir(namedtuple('MkDir', BASIC_FIELDS + ['path', 'mode'])):
    __slots__ = ()

    def perform(self, ctx: Context) -> int:
        path = os.path.join(ctx.mount_path, self.path)
        s = time_ns()
        os.mkdir(path, self.mode)
        return time_ns() - s


class MkNod(namedtuple('MkNod', BASIC_FIELDS + ['path', 'mode'])):
    __slots__ = ()

    def perform(self, ctx: Context) -> int:
        path = os.path.join(ctx.mount_path, self.path)
        s = time_ns()
        os.mknod(path, self.mode)
        return time_ns() - s


class Unlink(namedtuple('Unlink', BASIC_FIELDS + ['path'])):
    __slots__ = ()

    def perform(self, ctx: Context) -> int:
        path = os.path.join(ctx.mount_path, self.path)
        s = time_ns()
        os.unlink(path)
        return time_ns() - s


class RmDir(namedtuple('RmDir', BASIC_FIELDS + ['path'])):
    __slots__ = ()

    def perform(self, ctx: Context) -> int:
        path = os.path.join(ctx.mount_path, self.path)
        s = time_ns()
        os.rmdir(path)
        return time_ns() - s


class GetXAttr(namedtuple('GetXAttr', BASIC_FIELDS + ['path', 'attr'])):
    __slots__ = ()

    def perform(self, ctx: Context) -> int:
        path = os.path.join(ctx.mount_path, self.path)
        s = time_ns()
        os.getxattr(path, self.attr)
        return time_ns() - s


class SetXAttr(namedtuple('SetXAttr', BASIC_FIELDS + ['path', 'attr', 'val',
                                                      'flags'])):
    __slots__ = ()

    def perform(self, ctx: Context) -> int:
        path = os.path.join(ctx.mount_path, self.path)
        s = time_ns()
        os.setxattr(path, self.attr, self.val, self.flags)
        return time_ns() - s


class RemoveXAttr(namedtuple('RemoveXAttr', BASIC_FIELDS + ['path', 'attr'])):
    __slots__ = ()

    def perform(self, ctx: Context) -> int:
        path = os.path.join(ctx.mount_path, self.path)
        s = time_ns()
        os.removexattr(path, self.attr)
        return time_ns() - s


class ListXAttr(namedtuple('ListXAttr', BASIC_FIELDS + ['path'])):
    __slots__ = ()

    def perform(self, ctx: Context) -> int:
        path = os.path.join(ctx.mount_path, self.path)
        s = time_ns()
        os.listxattr(path)
        return time_ns() - s


class Read(namedtuple('Read', BASIC_FIELDS + ['handle_id', 'size', 'offset'])):
    __slots__ = ()

    def perform(self, ctx: Context) -> int:
        fd = ctx.fds[self.handle_id]
        os.lseek(fd, self.offset, os.SEEK_SET)
        s = time_ns()
        os.read(fd, self.size)
        return time_ns() - s


class Write(namedtuple('Write', BASIC_FIELDS + ['handle_id', 'size',
                                                'offset'])):
    __slots__ = ()

    def perform(self, ctx: Context) -> int:
        fd = ctx.fds[self.handle_id]
        random_junk = os.read(os.open('/dev/zero', os.O_RDONLY), self.size)
        os.lseek(fd, self.offset, os.SEEK_SET)
        s = time_ns()
        os.write(fd, random_junk)
        return time_ns() - s


class Rename(namedtuple('Rename', BASIC_FIELDS + ['src_path', 'dst_path'])):
    __slots__ = ()

    def perform(self, ctx: Context) -> int:
        src_path = os.path.join(ctx.mount_path, self.src_path)
        dst_path = os.path.join(ctx.mount_path, self.dst_path)
        s = time_ns()
        os.rename(src_path, dst_path)
        return time_ns() - s


class SetAttr(namedtuple('Rename', BASIC_FIELDS + ['path', 'mask', 'mode',
                                                   'size', 'atime', 'mtime'])):
    __slots__ = ()

    def perform(self, ctx: Context) -> int:
        path = os.path.join(ctx.mount_path, self.path)

        s = time_ns()
        if self.mask & FUSE_SET_ATTR_MODE:
            os.chmod(path, self.mode)
        if self.mask & FUSE_SET_ATTR_SIZE:
            os.truncate(path, self.size)
        if self.mask & (FUSE_SET_ATTR_ATIME | FUSE_SET_ATTR_MTIME):
            os.utime(path, (self.atime, self.mtime))
        if self.mask & (FUSE_SET_ATTR_ATIME_NOW | FUSE_SET_ATTR_MTIME_NOW):
            os.utime(path)

        return time_ns() - s


class ReadDir(namedtuple('ReadDir', BASIC_FIELDS + ['path', 'offset',
                                                    'entries_num'])):
    __slots__ = ()

    def perform(self, ctx: Context) -> int:
        path = os.path.join(ctx.mount_path, self.path)
        if self.offset == 0:
            iterator = os.scandir(path)
        else:
            iterator = ctx.scandirs[path][self.offset].pop()

        iter_range = self.entries_num + 1 if self.entries_num < 128 else 128
        s = time_ns()
        try:
            for _ in range(iter_range):
                next(iterator)
        except StopIteration:
            return time_ns() - s
        else:
            e = time_ns()
            new_offset = self.offset + iter_range
            (ctx.scandirs
             .setdefault(path, {})
             .setdefault(new_offset, deque())
             .append(iterator))
            return e - s
