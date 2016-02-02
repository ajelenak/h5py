# Test suite for Exascale FastForward "Example1".

import os
from .common_ff import ut, TestCaseFF
from h5py import h5, h5es
from h5py.eff_control import eff_init, eff_finalize
from h5py.highlevel import File, EventStack

if not h5.get_config().eff:
    raise RuntimeError('The h5py module was not built for Exascale FastForward') 


class BaseTest(TestCaseFF):
    
    def setUp(self):
        self.ff_cleanup()
        self.start_h5ff_server()


    def tearDown(self):
        pass


class TestExample1(BaseTest):

    def test_example1(self):
        """File create/close"""
        from mpi4py import MPI
        comm = MPI.COMM_WORLD
        eff_init(comm, MPI.INFO_NULL)
        fname = self.filename("ff_file_ex1.h5")
        f = File(fname, 'w', driver='iod', comm=comm, info=MPI.INFO_NULL)
        f.close()
        eff_finalize()


    def test_example2(self):
        """Group create in a file"""
        from mpi4py import MPI

        comm = MPI.COMM_WORLD
        eff_init(comm, MPI.INFO_NULL)
        my_rank = comm.Get_rank()

        fname = self.filename("ff_file_ex1.h5")
        f = File(fname, 'w', driver='iod', comm=comm, info=MPI.INFO_NULL)
        
        my_version = 1
        version = f.acquire_context(my_version)
        self.assertEqual(my_version, version)

        comm.Barrier()

        if my_rank == 0:
            f.create_transaction(2)
            f.tr.start()

            grp1 = f.create_group("G1")
            grp2 = grp1.create_group("G2")

            self.assertIsNone(f.ctn)
            self.assertEqual(str(grp1.ctn), str(f))
            self.assertEqual(str(grp1.ctn), str(grp2.ctn))

            self.assertEqual(str(grp1.tr), str(f.tr))
            self.assertEqual(str(grp1.tr), str(grp2.tr))

            self.assertEqual(str(grp1.rc), str(f.rc))
            self.assertEqual(str(grp1.rc), str(grp2.rc))

            self.assertEqual(str(grp1.es), str(f.es))
            self.assertEqual(str(grp1.es), str(grp2.es))

            f.tr.finish()

        f.rc.release()

        comm.Barrier()

        if my_rank == 0:
            grp1.close()
            grp2.close()
        f.close()        
        eff_finalize()
