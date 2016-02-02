#!/usr/bin/env python

# Make sure the correct h5py module will be imported...
import sys
sys.path.insert(1, sys.argv[1])

import os
from h5py.eff_control import eff_init, eff_finalize
from h5py.highlevel import EventStack, File, Map
from mpi4py import MPI
import numpy

comm = MPI.COMM_WORLD
eff_init(comm, MPI.INFO_NULL)
my_rank = comm.Get_rank()
es = EventStack()
fname = "%s_%s" % (os.environ["USER"], "ff_file_map.h5")
f = File(fname, 'w', driver='iod', comm=comm, info=MPI.INFO_NULL)
my_version = 1
version = f.acquire_context(my_version)
assert my_version == version, "Read context version: %d, requested %d" \
    % (version, my_version)

comm.Barrier()

if my_rank == 0:
    f.create_transaction(2)
    f.tr.start()

    m = f.create_map('test_map', f.tr, key_dtype='S7', val_dtype='int64')
    m.set('a', 1, f.tr)
    m.set('b', 2, f.tr)
    m.set(1, 3, f.tr) # will convert key to string '1'
    m.set('12345678', 4, f.tr) # will clip key to 7 chars

    f.tr.finish()
    f.tr._close()

f.rc.release()
comm.Barrier()
f.rc._close()

my_version = 2
version = f.acquire_context(2)        
assert my_version == version, "Read context version: %d, requested %d" \
        % (version, my_version)

comm.Barrier()

if my_rank == 0:
    kv_exists = m.exists('a', f.rc)
    assert kv_exists

    kv_exists = m.exists('b', f.rc)
    assert kv_exists

    kv_exists = m.exists('c', f.rc)
    assert not kv_exists

    kv_exists = m.exists('1', f.rc)
    assert kv_exists

    kv_exists = m.exists(1, f.rc)
    assert kv_exists

    kv_exists = m.exists('1234567', f.rc)
    assert kv_exists

    cnt = m.count(f.rc)
    assert cnt == 4

    # Close the map object so it can be opened for a get()
    m.close()
    m = f.open_map('test_map', f.rc)

    val = m.get('a', f.rc)
    assert val == 1

    val = m.get('b', f.rc)
    assert val == 2

    val = m.get('1', f.rc)
    assert val == 3

    val = m.get(1, f.rc)
    assert val == 3

    val = m.get('1234567', f.rc)
    assert val == 4

    # Delete some key-value pairs...
    f.create_transaction(3)
    f.tr.start()
    m.delete('b', f.tr)
    m.delete('1234567', f.tr)
    f.tr.finish()
    f.tr._close()

f.rc.release()
comm.Barrier()
f.rc._close()

my_version = 3
version = f.acquire_context(3)
assert my_version == version, "Read context version: %d, requested %d" \
        % (version, my_version)

comm.Barrier()

if my_rank == 0:
    kv_exists = m.exists('a', f.rc)
    assert kv_exists

    kv_exists = m.exists('b', f.rc)
    assert not kv_exists

    kv_exists = m.exists('1', f.rc)
    assert kv_exists

    kv_exists = m.exists('1234567', f.rc)
    assert not kv_exists

    cnt = m.count(f.rc)
    assert cnt == 2


m.close()

f.rc.release()
comm.Barrier()
f.rc._close()

f.close()
eff_finalize()
