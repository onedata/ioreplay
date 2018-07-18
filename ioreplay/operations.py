"""This module contains system calls wrappers
"""

__author__ = "Bartosz Walkowicz"
__copyright__ = "Copyright (C) 2018 ACK CYFRONET AGH"
__license__ = "This software is released under the Apache license cited in " \
              "LICENSE"

import os
from time import time
from typing import Callable, Any, Tuple, Dict
from collections import namedtuple


FUSE_SET_ATTR_MODE = (1 << 0)
FUSE_SET_ATTR_SIZE = (1 << 3)
FUSE_SET_ATTR_ATIME = (1 << 4)
FUSE_SET_ATTR_MTIME = (1 << 5)
FUSE_SET_ATTR_ATIME_NOW = (1 << 7)
FUSE_SET_ATTR_MTIME_NOW = (1 << 8)

BASIC_FIELDS = ['timestamp', 'duration']


def timeit(func: Callable[[], Any]) -> Tuple[Any, float]:
    start = time()
    result = func()
    return result, time() - start


class GetAttr(namedtuple('GetAttr', BASIC_FIELDS + ['path'])):
    __slots__ = ()

    def perform(self, _: Dict[int, int]) -> float:
        _, duration = timeit(lambda: os.stat(self.path))
        return duration


class Open(namedtuple('Open', BASIC_FIELDS + ['path', 'flags', 'handle_id'])):
    __slots__ = ()

    def perform(self, fds: Dict[int, int]) -> float:
        fd, duration = timeit(lambda: os.open(self.path, self.flags))
        fds[self.handle_id] = fd
        return duration


class Release(namedtuple('Release', BASIC_FIELDS + ['handle_id'])):
    __slots__ = ()

    def perform(self, fds: Dict[int, int]) -> float:
        fd = fds.pop(self.handle_id)
        _, duration = timeit(lambda: os.close(fd))
        return duration


class Fsync(namedtuple('Fsync', BASIC_FIELDS + ['handle_id', 'data_only'])):
    __slots__ = ()

    def perform(self, fds: Dict[int, int]) -> float:
        fd = fds[self.handle_id]
        sync = os.fdatasync if self.data_only else os.fsync
        _, duration = timeit(lambda: sync(fd))
        return duration


class Create(namedtuple('Create', BASIC_FIELDS + ['path', 'flags', 'mode',
                                                  'handle_id'])):
    __slots__ = ()

    def perform(self, fds: Dict[int, int]) -> float:
        fd, duration = timeit(lambda: os.open(self.path, self.flags, self.mode))
        fds[self.handle_id] = fd
        return duration


class MkDir(namedtuple('MkDir', BASIC_FIELDS + ['path', 'mode'])):
    __slots__ = ()

    def perform(self, _: Dict[int, int]) -> float:
        _, duration = timeit(lambda: os.mkdir(self.path, self.mode))
        return duration


class MkNod(namedtuple('MkNod', BASIC_FIELDS + ['path', 'mode'])):
    __slots__ = ()

    def perform(self, _: Dict[int, int]) -> float:
        _, duration = timeit(lambda: os.mknod(self.path, self.mode))
        return duration


class Unlink(namedtuple('Unlink', BASIC_FIELDS + ['path'])):
    __slots__ = ()

    def perform(self, _: Dict[int, int]) -> float:
        _, duration = timeit(lambda: os.unlink(self.path))
        return duration


class GetXAttr(namedtuple('GetXAttr', BASIC_FIELDS + ['path', 'attr'])):
    __slots__ = ()

    def perform(self, _: Dict[int, int]) -> float:
        _, duration = timeit(lambda: os.getxattr(self.path, self.attr))
        return duration


class SetXAttr(namedtuple('SetXAttr', BASIC_FIELDS + ['path', 'attr', 'val',
                                                      'flags'])):
    __slots__ = ()

    def perform(self, _: Dict[int, int]) -> float:
        _, duration = timeit(lambda: os.setxattr(self.path, self.attr,
                                                 self.val, self.flags))
        return duration


class RemoveXAttr(namedtuple('RemoveXAttr', BASIC_FIELDS + ['path', 'attr'])):
    __slots__ = ()

    def perform(self, _: Dict[int, int]) -> float:
        _, duration = timeit(lambda: os.removexattr(self.path, self.attr))
        return duration


class ListXAttr(namedtuple('ListXAttr', BASIC_FIELDS + ['path'])):
    __slots__ = ()

    def perform(self, _: Dict[int, int]) -> float:
        _, duration = timeit(lambda: os.listxattr(self.path))
        return duration


class Read(namedtuple('Read', BASIC_FIELDS + ['handle_id', 'size', 'offset'])):
    __slots__ = ()

    def perform(self, fds: Dict[int, int]) -> float:
        fd = fds[self.handle_id]
        os.lseek(fd, self.offset, os.SEEK_SET)
        _, duration = timeit(lambda: os.read(fd, self.size))
        return duration


class Write(namedtuple('Write', BASIC_FIELDS + ['handle_id', 'size',
                                                'offset'])):
    __slots__ = ()

    def perform(self, fds: Dict[int, int]) -> float:
        fd = fds[self.handle_id]
        random_junk = os.urandom(self.size)
        os.lseek(fd, self.offset, os.SEEK_SET)
        _, duration = timeit(lambda: os.write(fd, random_junk))
        return duration


class Rename(namedtuple('Rename', BASIC_FIELDS + ['src_path', 'dst_path'])):
    __slots__ = ()

    def perform(self, _: Dict[int, int]) -> float:
        _, duration = timeit(lambda: os.rename(self.src_path, self.dst_path))
        return duration


class SetAttr(namedtuple('Rename', BASIC_FIELDS + ['path', 'mask', 'mode',
                                                   'size', 'atime', 'mtime'])):
    __slots__ = ()

    def perform(self, _: Dict[int, int]) -> float:
        functions = []
        if self.mask & FUSE_SET_ATTR_MODE:
            functions.append(lambda: os.truncate(self.path, self.mode))
        if self.mask & FUSE_SET_ATTR_SIZE:
            functions.append(lambda: os.truncate(self.path, self.size))
        if self.mask & (FUSE_SET_ATTR_ATIME | FUSE_SET_ATTR_MTIME):
            functions.append(lambda: os.utime(self.path,
                                              (self.atime, self.mtime)))
        if self.mask & (FUSE_SET_ATTR_ATIME_NOW | FUSE_SET_ATTR_MTIME_NOW):
            functions.append(lambda: os.utime(self.path))

        _, duration = timeit(lambda: [fun() for fun in functions])
        return duration
