#!/usr/bin/env python

import os
import sys
# Figure out the h5py's import directory...
curr_dir = os.path.abspath(os.path.dirname(__file__))
h5py_dir = os.path.abspath(os.path.join(curr_dir, os.path.pardir,
                                        os.path.pardir, os.path.pardir))
sys.path.insert(1, sys.argv[1])

import numpy as np
from .common_ff import ut, TestCaseFF
from h5py.highlevel import File, Group, Dataset
from h5py.eff_control import eff_init, eff_finalize
from h5py import h5t, h5
import h5py

if not h5.get_config().eff:
    raise RuntimeError('The h5py module was not built for Exascale FastForward')



class TestCases(TestCaseFF):

    def setUp(self):
        self.ff_cleanup()
        self.start_h5ff_server()
        self.fname = self.filename("ff_file_dataset.h5")


    def tearDown(self):
        pass


    def test_reshape(self):
        """ Create from existing data, and make it fit a new shape """
        from mpi4py import MPI
        comm = MPI.COMM_WORLD
        eff_init(comm, MPI.INFO_NULL)
        rank = comm.Get_rank()
        f = File(self.fname, 'w', driver='iod', comm=comm, info=MPI.INFO_NULL)
        f.acquire_context(1)
        comm.Barrier()
        if rank == 0:
            f.create_transaction(2)
            f.tr.start()

            data = np.arange(30, dtype='f')
            dset = f.create_dataset('foo', shape=(10, 3), data=data)
            self.assertEqual(dset.shape, (10, 3))
            self.assertArrayEqual(dset[...], data.reshape((10, 3)))

            f.tr.finish()
        f.rc.release()
        comm.Barrier()
        if rank == 0:
            dset.close()
        f.close()
        eff_finalize()


if __name__ == '__main__':
    ut.main()
