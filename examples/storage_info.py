#!/usr/bin/env python
"""
Example program to demonstrate retrieving HDF5 dataset storage information.

A special version of the HDF5 v1.8.17 is required for this functionality and it
is not publicly available (yet).
"""

from __future__ import print_function
import os
from tempfile import gettempdir
import numpy as np
import h5py

# Create an HDF5 file with two datasets: one with contiguous and the other with
# chunked storage.
filename = os.path.join(gettempdir(), 'storage-demo.h5')
with h5py.File(filename, 'w') as f:
    f.create_dataset('cont', data=np.random.rand(10, 20))
    f.create_dataset('chunk', data=np.random.rand(43, 37), chunks=(9, 12))
    f.create_dataset('empty', shape=(5, 10), dtype='i4')
    f.create_dataset('scalar', shape=(), data=1000, dtype='u2')

# Open the same file and display the storage information for the three
# datasets. That information is obtained via the storage property of the
# h5py.Dataset class.
dset_paths = ['/cont', '/chunk', '/empty', '/scalar']
with h5py.File(filename, 'r') as f:
    for dp in dset_paths:
        dset = f[dp]
        stinfo = dset.storage
        print('Storage information for {}'.format(dset.name))
        if stinfo:
            for s in stinfo:
                print('Byte stream #{}: at byte {}, size {} bytes, '
                      'logical address {}'.format(s.order, s.file_addr, s.size,
                                                  s.logical_addr))
        else:
            print('Empty dataset')
        print('\n')

# Remove the file
try:
    os.remove(filename)
except Exception:
    pass
