"""This module contains system calls wrappers
"""

__author__ = "Bartosz Walkowicz"
__copyright__ = "Copyright (C) 2018 ACK CYFRONET AGH"
__license__ = "This software is released under the Apache license cited in " \
              "LICENSE"

import os
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

    def perform(self, _):
        os.stat(self.path)


class Open(namedtuple('Open', BASIC_FIELDS + ['path', 'flags', 'handle_id'])):
    __slots__ = ()

    def perform(self, fds):
        fd = os.open(self.path, self.flags)
        fds[self.handle_id] = fd


class Release(namedtuple('Release', BASIC_FIELDS + ['handle_id'])):
    __slots__ = ()

    def perform(self, fds):
        fd = fds.pop(self.handle_id)
        os.close(fd)


class Fsync(namedtuple('Fsync', BASIC_FIELDS + ['handle_id', 'data_only'])):
    __slots__ = ()

    def perform(self, fds):
        fd = fds[self.handle_id]
        sync = os.fdatasync if self.data_only else os.fsync
        sync(fd)


class Create(namedtuple('Create', BASIC_FIELDS + ['path', 'flags', 'mode',
                                                  'handle_id'])):
    __slots__ = ()

    def perform(self, fds):
        fd = os.open(self.path, self.flags, self.mode)
        fds[self.handle_id] = fd


class MkDir(namedtuple('MkDir', BASIC_FIELDS + ['path', 'mode'])):
    __slots__ = ()

    def perform(self, _):
        os.mkdir(self.path, self.mode)


class MkNod(namedtuple('MkNod', BASIC_FIELDS + ['path', 'mode'])):
    __slots__ = ()

    def perform(self, _):
        os.mknod(self.path, self.mode)


class Unlink(namedtuple('Unlink', BASIC_FIELDS + ['path'])):
    __slots__ = ()

    def perform(self, _):
        os.unlink(self.path)


class GetXAttr(namedtuple('GetXAttr', BASIC_FIELDS + ['path', 'attr'])):
    __slots__ = ()

    def perform(self, _):
        os.getxattr(self.path, self.attr)


class SetXAttr(namedtuple('SetXAttr', BASIC_FIELDS + ['path', 'attr', 'val',
                                                      'flags'])):
    __slots__ = ()

    def perform(self, _):
        os.setxattr(self.path, self.attr, self.val, self.flags)


class RemoveXAttr(namedtuple('RemoveXAttr', BASIC_FIELDS + ['path', 'attr'])):
    __slots__ = ()

    def perform(self, _):
        os.removexattr(self.path, self.attr)


class ListXAttr(namedtuple('ListXAttr', BASIC_FIELDS + ['path'])):
    __slots__ = ()

    def perform(self, _):
        os.listxattr(self.path)


class Lseek(namedtuple('Fseek', BASIC_FIELDS + ['handle_id', 'offset'])):
    __slots__ = ()

    def perform(self, fds):
        fd = fds[self.handle_id]
        os.lseek(fd, self.offset, os.SEEK_SET)


class Read(namedtuple('Read', BASIC_FIELDS + ['handle_id', 'size', 'offset'])):
    __slots__ = ()

    def perform(self, fds):
        fd = fds[self.handle_id]
        os.lseek(fd, self.offset, os.SEEK_SET)
        os.read(fd, self.size)


class Write(namedtuple('Write', BASIC_FIELDS + ['handle_id', 'size',
                                                'offset'])):
    __slots__ = ()

    def perform(self, fds):
        fd = fds[self.handle_id]
        random_junk = os.urandom(self.size)
        os.lseek(fd, self.offset, os.SEEK_SET)
        os.write(fd, random_junk)


class Rename(namedtuple('Rename', BASIC_FIELDS + ['src_path', 'dst_path'])):
    __slots__ = ()

    def perform(self, _):
        os.rename(self.src_path, self.dst_path)


class SetAttr(namedtuple('Rename', BASIC_FIELDS + ['path', 'mask', 'mode',
                                                   'size', 'atime', 'mtime'])):
    __slots__ = ()

    def perform(self, _):
        if self.mask & FUSE_SET_ATTR_MODE:
            os.truncate(self.path, self.mode)
        if self.mask & FUSE_SET_ATTR_SIZE:
            os.truncate(self.path, self.size)
        if self.mask & (FUSE_SET_ATTR_ATIME | FUSE_SET_ATTR_MTIME):
            os.utime(self.path, (self.atime, self.mtime))
        if self.mask & (FUSE_SET_ATTR_ATIME_NOW | FUSE_SET_ATTR_MTIME_NOW):
            os.utime(self.path)
