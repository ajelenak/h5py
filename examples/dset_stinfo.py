#!/usr/bin/env python
"""
Print storage information for every HDF5 dataset in a file.

Dataset storage information for each byte stream is printed on a single line.

Format:

Dataset: {name}, byte stream #{int}, logical address ({int}, ...), at byte {int} of size {int} bytes

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
            print('Caught exception for {}'.format(obj.name))
            return

        for si in stinfo:
            print ('Dataset: {}, byte stream #{}, logical address {}, '
                   'at byte {} of size {} bytes'
                   .format(obj.name, si.order, si.logical_addr,
                           si.file_addr, si.size))


f = h5py.File(sys.argv[1], 'r')
f.visititems(dset_stinfo)
