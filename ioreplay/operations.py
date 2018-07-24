"""This module contains system calls wrappers"""

__author__ = "Bartosz Walkowicz"
__copyright__ = "Copyright (C) 2018 ACK CYFRONET AGH"
__license__ = "This software is released under the Apache license cited in " \
              "LICENSE"

import os
from typing import Dict
from time import time_ns
from collections import namedtuple


FUSE_SET_ATTR_MODE = (1 << 0)
FUSE_SET_ATTR_SIZE = (1 << 3)
FUSE_SET_ATTR_ATIME = (1 << 4)
FUSE_SET_ATTR_MTIME = (1 << 5)
FUSE_SET_ATTR_ATIME_NOW = (1 << 7)
FUSE_SET_ATTR_MTIME_NOW = (1 << 8)

BASIC_FIELDS = ['timestamp', 'duration']


class GetAttr(namedtuple('GetAttr', BASIC_FIELDS + ['path'])):
    __slots__ = ()

    def perform(self, _: Dict[int, int]) -> int:
        s = time_ns()
        os.stat(self.path)
        return time_ns() - s


class Open(namedtuple('Open', BASIC_FIELDS + ['path', 'flags', 'handle_id'])):
    __slots__ = ()

    def perform(self, fds: Dict[int, int]) -> int:
        s = time_ns()
        fd = os.open(self.path, self.flags)
        e = time_ns()
        fds[self.handle_id] = fd
        return e - s


class Release(namedtuple('Release', BASIC_FIELDS + ['handle_id'])):
    __slots__ = ()

    def perform(self, fds: Dict[int, int]) -> int:
        fd = fds.pop(self.handle_id)
        s = time_ns()
        os.close(fd)
        return time_ns() - s


class Fsync(namedtuple('Fsync', BASIC_FIELDS + ['handle_id', 'data_only'])):
    __slots__ = ()

    def perform(self, fds: Dict[int, int]) -> int:
        fd = fds[self.handle_id]
        sync = os.fdatasync if self.data_only else os.fsync
        s = time_ns()
        sync(fd)
        return time_ns() - s


class Create(namedtuple('Create', BASIC_FIELDS + ['path', 'flags', 'mode',
                                                  'handle_id'])):
    __slots__ = ()

    def perform(self, fds: Dict[int, int]) -> int:
        s = time_ns()
        fd = os.open(self.path, self.flags, self.mode)
        e = time_ns()
        fds[self.handle_id] = fd
        return e - s


class MkDir(namedtuple('MkDir', BASIC_FIELDS + ['path', 'mode'])):
    __slots__ = ()

    def perform(self, _: Dict[int, int]) -> int:
        s = time_ns()
        os.mkdir(self.path, self.mode)
        return time_ns() - s


class MkNod(namedtuple('MkNod', BASIC_FIELDS + ['path', 'mode'])):
    __slots__ = ()

    def perform(self, _: Dict[int, int]) -> int:
        s = time_ns()
        os.mknod(self.path, self.mode)
        return time_ns() - s


class Unlink(namedtuple('Unlink', BASIC_FIELDS + ['path'])):
    __slots__ = ()

    def perform(self, _: Dict[int, int]) -> int:
        s = time_ns()
        os.unlink(self.path)
        return time_ns() - s


class GetXAttr(namedtuple('GetXAttr', BASIC_FIELDS + ['path', 'attr'])):
    __slots__ = ()

    def perform(self, _: Dict[int, int]) -> int:
        s = time_ns()
        os.getxattr(self.path, self.attr)
        return time_ns() - s


class SetXAttr(namedtuple('SetXAttr', BASIC_FIELDS + ['path', 'attr', 'val',
                                                      'flags'])):
    __slots__ = ()

    def perform(self, _: Dict[int, int]) -> int:
        s = time_ns()
        os.setxattr(self.path, self.attr, self.val, self.flags)
        return time_ns() - s


class RemoveXAttr(namedtuple('RemoveXAttr', BASIC_FIELDS + ['path', 'attr'])):
    __slots__ = ()

    def perform(self, _: Dict[int, int]) -> int:
        s = time_ns()
        os.removexattr(self.path, self.attr)
        return time_ns() - s


class ListXAttr(namedtuple('ListXAttr', BASIC_FIELDS + ['path'])):
    __slots__ = ()

    def perform(self, _: Dict[int, int]) -> int:
        s = time_ns()
        os.listxattr(self.path)
        return time_ns() - s


class Read(namedtuple('Read', BASIC_FIELDS + ['handle_id', 'size', 'offset'])):
    __slots__ = ()

    def perform(self, fds: Dict[int, int]) -> int:
        fd = fds[self.handle_id]
        os.lseek(fd, self.offset, os.SEEK_SET)
        s = time_ns()
        os.read(fd, self.size)
        return time_ns() - s


class Write(namedtuple('Write', BASIC_FIELDS + ['handle_id', 'size',
                                                'offset'])):
    __slots__ = ()

    def perform(self, fds: Dict[int, int]) -> int:
        fd = fds[self.handle_id]
        random_junk = os.read(os.open('/dev/zero', os.O_RDONLY), 1000000)
        os.lseek(fd, self.offset, os.SEEK_SET)
        s = time_ns()
        os.write(fd, random_junk)
        return time_ns() - s


class Rename(namedtuple('Rename', BASIC_FIELDS + ['src_path', 'dst_path'])):
    __slots__ = ()

    def perform(self, _: Dict[int, int]) -> int:
        s = time_ns()
        os.rename(self.src_path, self.dst_path)
        return time_ns() - s


class SetAttr(namedtuple('Rename', BASIC_FIELDS + ['path', 'mask', 'mode',
                                                   'size', 'atime', 'mtime'])):
    __slots__ = ()

    def perform(self, _: Dict[int, int]) -> int:
        functions = []
        if self.mask & FUSE_SET_ATTR_MODE:
            functions.append(lambda: os.chmod(self.path, self.mode))
        if self.mask & FUSE_SET_ATTR_SIZE:
            functions.append(lambda: os.truncate(self.path, self.size))
        if self.mask & (FUSE_SET_ATTR_ATIME | FUSE_SET_ATTR_MTIME):
            functions.append(lambda: os.utime(self.path,
                                              (self.atime, self.mtime)))
        if self.mask & (FUSE_SET_ATTR_ATIME_NOW | FUSE_SET_ATTR_MTIME_NOW):
            functions.append(lambda: os.utime(self.path))

        s = time_ns()
        for fun in functions:
            fun()
        return time_ns() - s
