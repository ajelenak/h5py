#!/usr/bin/env python

# Make sure correct h5py module is imported...
import os
import sys
curr_dir = os.path.abspath(os.path.dirname(__file__))
h5py_dir = os.path.abspath(os.path.join(curr_dir, os.path.pardir,
                                        "build", "lib.linux-x86_64-2.6"))
if not os.path.isdir(h5py_dir):
    raise RuntimeError('%s: Not a directory' % h5py_dir)
sys.path.insert(1, h5py_dir)
#print 'sys.path = ', sys.path

from mpi4py import rc
rc.initialize = False

import os
from mpi4py import MPI
from h5py import h5, h5p, File, EventStack
from h5py.eff_control import eff_init as EFF_init, eff_finalize as EFF_finalize

print "mpi = ", h5.get_config().mpi
print "HDF5 version = ", h5.get_libversion()

provided = MPI.Init_thread(required=MPI.THREAD_MULTIPLE)
print 'MPI.THREAD_MULTIPLE = %d' % MPI.THREAD_MULTIPLE
print 'provided = %d' % provided

comm = MPI.COMM_WORLD
EFF_init(comm, MPI.INFO_NULL)
my_rank = comm.Get_rank()
my_size = comm.Get_size()
print 'size = %d; rank = %d' % (my_size, my_rank)
f = File(os.environ["USER"]+"_ff_file_ex1.h5", mode='w', driver='iod',
         comm=comm, info=MPI.INFO_NULL)
my_version = 1
version = f.acquire_context(my_version)
print "Requested read context version = %d" % my_version
print "Acquired read context version = %d" % version

comm.Barrier()

if my_rank == 0:
    f.create_transaction(2)
    f.tr.start()

    print ">>>>>>>"
    print "f.ctn =", f.ctn
    print "f.tr =", f.tr
    print "f.rc =", f.rc
    print "f.es =", f.es
    print "<<<<<<"

    grp1 = f.create_group("G1")
    print ">>>>>>>"
    print "grp1.ctn =", grp1.ctn
    print "grp1.tr =", grp1.tr
    print "grp1.rc =", grp1.rc
    print "grp1.es =", grp1.es
    print "<<<<<<"

    grp2 = grp1.create_group("G2")
    print ">>>>>>>"
    print "grp2.ctn =", grp2.ctn
    print "grp2.tr =", grp2.tr
    print "grp2.rc =", grp2.rc
    print "grp2.es =", grp2.es
    print "<<<<<<"

    f.tr.finish()
#    f.tr.close()


f.rc.release()

comm.Barrier()

if my_rank == 0:
    grp1.close()
    grp2.close()

#f.rc.close()
f.close()
#es.close()
#comm.Barrier()
EFF_finalize()

print 'The End'
