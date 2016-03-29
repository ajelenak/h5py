# Example program for showcasing the query API of the HDF5 library's Fast
# Forward version.
import os
import sys

# Figure out h5py's build directory to import from
import distutils.util
platform = distutils.util.get_platform()
libdir = 'lib.{platform}-{version[0]}.{version[1]}'.format(
            platform=platform, version=sys.version_info)
curr_dir = os.path.abspath(os.path.dirname(__file__))
h5py_dir = os.path.abspath(os.path.join(curr_dir, os.path.pardir, 'build',
                                        libdir))
sys.path.insert(1, h5py_dir)

import numpy as np
from mpi4py import MPI
import h5py
from h5py.eff_control import eff_init, eff_finalize

print """\
***********************************************************************

%s program started

***********************************************************************
""" % __file__

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
if rank != 0:
    raise RuntimeError('This process is not MPI rank 0')

eff_init(comm, MPI.INFO_NULL)

# Base for HDF5 file names
fname_base = os.environ['USER'] + "_demo_view_"

# Version number for read contexts and transactions
version = 1

# Data to be stored as datasets in HDF5 files
print 'Data to be stored in all HDF5 files:'
p = np.arange(20, 30)
print '"pressure" =\n', p
t = np.arange(20, 30, .1)
print '"temperature" =\n', t

# Create files and store data in them
f = dict()
for i in range(1, 6):
    fname = fname_base + str(i) + '.h5'
    print 'Creating file "%s"' % fname
    f[fname] = h5py.File(fname, 'w', driver='iod', comm=comm,
                         info=MPI.INFO_NULL)

    # Acquire a read context and start a transaction
    f[fname].acquire_context(version)
    f[fname].create_transaction(version+1)
    f[fname].tr.start()

    # Create two datasets
    print 'Creating dataset "pressure"'
    x = f[fname].create_dataset("pressure", data=p, dtype='i4')
    print 'Creating dataset "temperature"'
    y = f[fname].create_dataset("temperature", data=t, dtype='f8')

    # Add an attribute to the "pressure" dataset
    print 'Creating attribute "SensorID" for the "pressure" dataset'
    x.attrs.create('SensorID', '1234-567-89', shape=(1,))

    # Finish the transaction and release the read context
    f[fname].tr.finish()
    f[fname].rc.release()

    # Close HDF5 resources
    print 'Finished storing data in file "%s"' % fname
    x.close()
    y.close()
    f[fname].close()

# Increase version number for next read context operation
version += 1


# Comment out a query to apply

# print 'Query is: link_name = "pressure"'
# q = h5py.AQuery('link_name') == 'pressure'

print 'Query is: 21.7 < data_elem < 26.9'
q = (h5py.AQuery('data_elem') > 21.7) & (h5py.AQuery('data_elem') < 26.9)

# print 'Query is: attr_name = "SensorID"'
# q = h5py.AQuery('attr_name') == 'SensorID'

# Open the HDF5 files and acquire new read contexts
fids = list()
rcids = list()
version += 1
for fn in f.keys():
    f[fn] = h5py.File(fn, 'r', driver='iod', comm=comm, info=MPI.INFO_NULL)
    f[fn].acquire_context(version)
    fids.append(f[fn].id)
    rcids.append(f[fn].rc.id)

# Apply the query and retrieve results. myview_grp is HDF5 group with view
# results. view_res is a bit mask indicating what types of view results are
# available in myview_grp.
print 'Applying query'
myview_grp, view_res = q.id.apply_multi_ff(fids, rcids)

if view_res & h5py.h5q.REF_OBJ:
    print ('View has object references, stored in "%s" dataset'
           % h5py.h5q.VIEW_REF_OBJ_NAME)
if view_res & h5py.h5q.REF_REG:
    print ('View has region references, stored in "%s" dataset'
           % h5py.h5q.VIEW_REF_REG_NAME)
if view_res & h5py.h5q.REF_ATTR:
    print ('View has attribute references, stored in "%s" dataset'
           % h5py.h5q.VIEW_REF_ATTR_NAME)

# Object references
if view_res & h5py.h5q.REF_OBJ:
    # Open dataset holding view results
    obj_dset = h5py.h5d.open(myview_grp, h5py.h5q.VIEW_REF_OBJ_NAME)

    # Read object references into a NumPy array
    ref_dtype = h5py.special_dtype(ref=h5py.Reference)
    sid = h5py.h5s.create_simple(obj_dset.shape)
    ref_array = np.empty(obj_dset.shape, dtype=ref_dtype)
    tid = h5py.h5t.py_create(ref_dtype)
    obj_dset.read(h5py.h5s.ALL, sid, ref_array, tid)

    # Print info about references and their objects
    print 'Found %d object references' % ref_array.size
    for i in range(ref_array.size):
        print ('obj name: "{0}"; ref type "{1}"; obj file: "{2}"'
               .format(h5py.h5r.get_name(ref_array[i], fids[0]),
                       ref_array[i],
                       ref_array[i].get_file_name())
               )

# Attribute references
if view_res & h5py.h5q.REF_ATTR:
    # Open dataset holding view results
    attr_dset = h5py.h5d.open(myview_grp, h5py.h5q.VIEW_REF_ATTR_NAME)

    # Read attribute references into a NumPy array
    ref_dtype = h5py.special_dtype(ref=h5py.AttributeReference)
    sid = h5py.h5s.create_simple(attr_dset.shape)
    ref_array = np.empty(attr_dset.shape, dtype=ref_dtype)
    tid = h5py.h5t.py_create(ref_dtype)
    attr_dset.read(h5py.h5s.ALL, sid, ref_array, tid)

    # Print info about references and their attributes
    print 'Found %d attribute references' % ref_array.size
    for i in range(ref_array.size):
        print ('attr name: "{0}"; ref type "{1}"; attr file: "{2}"'
               .format(h5py.h5r.get_name(ref_array[i], fids[0]),
                       ref_array[i],
                       ref_array[i].get_file_name())
               )

# Region references
if view_res & h5py.h5q.REF_REG:
    # Open dataset holding view results
    reg_dset = h5py.h5d.open(myview_grp, h5py.h5q.VIEW_REF_REG_NAME)

    # Read region references into a NumPy array
    ref_dtype = h5py.special_dtype(ref=h5py.RegionReference)
    sid = h5py.h5s.create_simple(reg_dset.shape)
    ref_array = np.empty(reg_dset.shape, dtype=ref_dtype)
    tid = h5py.h5t.py_create(ref_dtype)
    reg_dset.read(h5py.h5s.ALL, sid, ref_array, tid)
    print 'Found %d region references' % ref_array.size

    # Print info about references and their objects
    for i in range(ref_array.size):
        print '\n'
        obj_name = h5py.h5r.get_name(ref_array[i], fids[0])
        obj_file = ref_array[i].get_file_name()
        print ('obj name: "{0}"; ref type "{1}"; obj file: "{2}"'
               .format(obj_name, ref_array[i], obj_file))

        # Print values that satisfied the query
        obj = h5py.h5r.get_object_ff(ref_array[i], f[obj_file].id,
                                     f[obj_file].rc.id)
        sid = h5py.h5r.get_region(ref_array[i], f[obj_file].id)
        nelem = sid.get_select_npoints()
        tid = h5py.h5t.py_create(obj.dtype)
        obj_data = np.empty((nelem,), dtype=obj.dtype)
        mem_sid = h5py.h5s.create_simple((nelem,))
        obj.read_ff(mem_sid, sid, obj_data, f[obj_file].rc.id, tid)
        print 'Values of "%s" that satisfy the query: %d' % (obj_name,
                                                             obj_data.size)
        print obj_data
        obj._close_ff()

print """\
***********************************************************************

Demo finished

***********************************************************************
"""
for fobj in f.values():
    fobj.rc.release()
    fobj.close()
eff_finalize()
