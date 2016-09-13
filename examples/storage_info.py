#!/usr/bin/env python
"""
Example program to demonstrate retrieving HDF5 dataset storage information.
Currently supported are contiguous and chunked dataset storage.

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

# Open the same file and display the storage information for the two datasets.
# That information is obtained via the storage property of the dataset.
f = h5py.File(filename, 'r')

# Contiguous storage dataset
cont = f['/cont']
stinfo = cont.storage
print('Storage information for {} is {}'.format(cont.name, type(stinfo)))
print('Dataset {}: at byte {}, size {} bytes'
      .format(cont.name, stinfo.file_addr, stinfo.size))

print('\n\n')

# Chunked storage dataset
chunk = f['/chunk']
stinfo = chunk.storage
print('Storage information for {} are {} in a {}'
      .format(chunk.name, type(stinfo[0]), type(stinfo)))
for si in stinfo:
    print('Dataset {}: chunk #{} at byte {}, size {} bytes, '
          'logical address {}'.format(chunk.name, si.order, si.file_addr,
                                      si.size, si.logical_addr))

f.close()

# Remove the file
try:
    os.remove(filename)
except Exception:
    pass
