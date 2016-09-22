#!/usr/bin/env python
"""
Print storage information for every HDF5 dataset in a file.

Dataset storage information is printed on a single line. For chunked layout,
storage information on one line applies to one chunk.

Format:

Dataset: {name}[, chunk #{int}, logical address ({int}, ...)], at byte {int} of size {int} bytes

Usage:

    dset_stinfo.py <HDF5 file>
"""

from __future__ import print_function
import sys
import h5py


def dset_stinfo(name, obj):
    """Print storage information for each dataset in the file."""
    if isinstance(obj, h5py.Dataset):
        try:
            stinfo = obj.storage
        except Exception:
            return

        if isinstance(stinfo, h5py.h5d.ContiguousStorageInfo):
            print ('Dataset: {}, at byte {} of size {} bytes'
                   .format(obj.name, stinfo.file_addr, stinfo.size))
        else:
            for si in stinfo:
                print ('Dataset: {}, chunk #{}, logical address {}, '
                       'at byte {} of size {} bytes'
                       .format(obj.name, si.order, si.logical_addr,
                               si.file_addr, si.size))


f = h5py.File(sys.argv[1], 'r')
f.visititems(dset_stinfo)
