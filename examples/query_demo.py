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

# HDF5 file name
fname = os.environ['USER'] + "_demo_view.h5"

# Version number for read contexts and transactions
version = 1

# Create simple test file
print 'Creating file "%s"' % fname
f = h5py.File(fname, 'w', driver='iod', comm=comm, info=MPI.INFO_NULL)

# Acquire a read context and start a transaction
f.acquire_context(version)
version += 1
f.create_transaction(version)
f.tr.start()

# Create two datasets
p = np.arange(20, 30)
print 'Creating dataset "pressure" with values:\n', p
x = f.create_dataset("pressure", data=p, dtype='i4')
t = np.arange(20, 30, .1)
print 'Creating dataset "temperature" with values:\n', t
y = f.create_dataset("temperature", data=t, dtype='f8')

# Add an attribute to the x dataset
print 'Creating attribute "SensorID" for the "pressure" dataset'
x.attrs.create('SensorID', '1234-567-89', shape=(1,))

# Finish the transaction and release the read context
f.tr.finish()
f.rc.release()

# Close HDF5 resources
print 'Finished creating data'
x.close()
y.close()
f.close()


# Comment out a query to run

# print 'Query is: link_name = "pressure"'
# q = h5py.AQuery('link_name') == 'pressure'

# print 'Query is: data_elem == "50"'
# q = h5py.AQuery('data_elem') == 50

# print 'Query is: 21.7 < data_elem < 26.9'
# q = (h5py.AQuery('data_elem') > 21.7) & (h5py.AQuery('data_elem') < 26.9)

print 'Query is: ((21.7 < data_elem < 26.9) AND (data_elem != 23)) OR (data_elem == 29)'
q1 = h5py.h5q.create(h5py.h5q.TYPE_DATA_ELEM, h5py.h5q.MATCH_GREATER_THAN,
                     h5py.h5t.NATIVE_DOUBLE, np.asarray(21.7))
q2 = h5py.h5q.create(h5py.h5q.TYPE_DATA_ELEM, h5py.h5q.MATCH_LESS_THAN,
                     h5py.h5t.NATIVE_DOUBLE, np.asarray(26.9))
q3 = q1.combine(h5py.h5q.COMBINE_AND, q2)
q4 = h5py.h5q.create(h5py.h5q.TYPE_DATA_ELEM, h5py.h5q.MATCH_NOT_EQUAL,
                     h5py.h5t.NATIVE_INT32, np.asarray(23))
q5 = q3.combine(h5py.h5q.COMBINE_AND, q4)
q6 = h5py.h5q.create(h5py.h5q.TYPE_DATA_ELEM, h5py.h5q.MATCH_EQUAL,
                     h5py.h5t.NATIVE_INT32, np.asarray(29))
q = q5.combine(h5py.h5q.COMBINE_OR, q6)

# q = (
#         (
#             (h5py.AQuery('data_elem') > 21.7) &
#             (h5py.AQuery('data_elem') < 26.9)
#         ) &
#         (h5py.AQuery('data_elem') != 23)
#     ) |
#     (h5py.AQuery('data_elem') == 29)


# print 'Query is: attr_name = "SensorID"'
# q = h5py.AQuery('attr_name') == 'SensorID'

# Open the file and acquire new read context
f = h5py.File(fname, 'r', driver='iod', comm=comm, info=MPI.INFO_NULL)
version += 1
f.acquire_context(version)

# Apply the query and retrieve results. myview_grp is HDF5 group with view
# results. view_res is a bit mask indicating what types of view results are
# available in myview_grp.
print 'Applying query'
if isinstance(q, h5py.h5q.QueryID):
    myview_grp, view_res = q.apply_ff(f.id, f.rc.id, es=f.es.id)
else:
    myview_grp, view_res = f.apply_query(q)

if view_res & h5py.h5q.REF_OBJ:
    print ('View has object references, stored in "%s" dataset'
           % h5py.h5q.VIEW_REF_OBJ_NAME)
if view_res & h5py.h5q.REF_REG:
    print ('View has region references, stored in "%s" dataset'
           % h5py.h5q.VIEW_REF_REG_NAME)
if view_res & h5py.h5q.REF_ATTR:
    print ('View has attribute references, stored in "%s" dataset'
           % h5py.h5q.VIEW_REF_ATTR_NAME)

if view_res & h5py.h5q.REF_OBJ:
    # The dataset holding view results as object references
    obj_dset = h5py.h5d.open(myview_grp, h5py.h5q.VIEW_REF_OBJ_NAME)
    ref_dtype = h5py.special_dtype(ref=h5py.Reference)
    sid = h5py.h5s.create_simple(obj_dset.shape)
    ref_array = np.empty(obj_dset.shape, dtype=ref_dtype)
    tid = h5py.h5t.py_create(ref_dtype)
    obj_dset.read(h5py.h5s.ALL, sid, ref_array, tid)
    print 'Found %d object references' % ref_array.size
    for i in range(ref_array.size):
        print ('ref name: "{0}"; ref type "{1}"; ref file: "{2}"'
               .format(h5py.h5r.get_name(ref_array[i], f.id),
                       ref_array[i],
                       ref_array[i].get_file_name())
               )

if view_res & h5py.h5q.REF_ATTR:
    # The dataset holding view results as attribute references
    attr_dset = h5py.h5d.open(myview_grp, h5py.h5q.VIEW_REF_ATTR_NAME)
    ref_dtype = h5py.special_dtype(ref=h5py.AttributeReference)
    sid = h5py.h5s.create_simple(attr_dset.shape)
    ref_array = np.empty(attr_dset.shape, dtype=ref_dtype)
    tid = h5py.h5t.py_create(ref_dtype)
    attr_dset.read(h5py.h5s.ALL, sid, ref_array, tid)
    print 'Found %d attribute references' % ref_array.size
    for i in range(ref_array.size):
        print ('ref name: "{0}"; ref type "{1}"; ref file: "{2}"'
               .format(h5py.h5r.get_name(ref_array[i], f.id),
                       ref_array[i],
                       ref_array[i].get_file_name())
               )

if view_res & h5py.h5q.REF_REG:
    # The dataset holding view results as region references
    reg_dset = h5py.h5d.open(myview_grp, h5py.h5q.VIEW_REF_REG_NAME)
    ref_dtype = h5py.special_dtype(ref=h5py.RegionReference)
    sid = h5py.h5s.create_simple(reg_dset.shape)
    ref_array = np.empty(reg_dset.shape, dtype=ref_dtype)
    tid = h5py.h5t.py_create(ref_dtype)
    reg_dset.read(h5py.h5s.ALL, sid, ref_array, tid)
    print 'Found %d region references' % ref_array.size
    for i in range(ref_array.size):
        print '\n'
        obj_name = h5py.h5r.get_name(ref_array[i], f.id)
        print ('obj name: "{0}"; ref type "{1}"; ref file: "{2}"'
               .format(obj_name,
                       ref_array[i],
                       ref_array[i].get_file_name())
               )
        obj = h5py.h5r.get_object_ff(ref_array[i], f.id, f.rc.id)

        # Print found values
        sid = h5py.h5r.get_region(ref_array[i], f.id)
        nelem = sid.get_select_npoints()
        tid = h5py.h5t.py_create(obj.dtype)
        obj_data = np.empty((nelem,), dtype=obj.dtype)
        mem_sid = h5py.h5s.create_simple((nelem,))
        obj.read_ff(mem_sid, sid, obj_data, f.rc.id, tid)
        print 'Values of "%s" that satisfy the query: %d' % (obj_name,
                                                             obj_data.size)
        print obj_data
        obj._close_ff()

print """\
***********************************************************************

Demo finished

***********************************************************************
"""
f.rc.release()
f.close()
eff_finalize()
