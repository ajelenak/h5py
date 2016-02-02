# Basic suite of tests for Exascale FastForward HDF5 library.

import os
from .common_ff import TestCaseFF
from h5py import h5, h5es, h5i
from h5py.highlevel import EventStack
from h5py._hl.event_stack import es_null

if not h5.get_config().eff:
    raise RuntimeError('The h5py module was not built for Exascale FastForward')



class TestWorkEnv(TestCaseFF):

    def setUp(self):
        pass


    def tearDown(self):
        pass


    def test_write_dir(self):
        """Work directory writeable"""
        cwd = os.getcwd()
        self.assertTrue(os.access(cwd, os.W_OK))


    def test_env_vars(self):
        """Verify important env. variables"""
        self.assertIn("EFF_MPI_IONS", os.environ)
        self.assertNotEqual(len(os.environ["EFF_MPI_IONS"]), 0)

        self.assertIn("EFF_MPI_CNS", os.environ)
        self.assertNotEqual(len(os.environ["EFF_MPI_CNS"]), 0)

        self.assertIn("H5FF_SERVER", os.environ)
        self.assertNotEqual(len(os.environ["H5FF_SERVER"]), 0)
        self.assertTrue(os.path.isfile(os.environ["H5FF_SERVER"]))
        self.assertTrue(os.access(os.environ["H5FF_SERVER"], os.X_OK))


    def test_mpi_thread_multi(self):
        """ MPI_THREAD_MULTIPLE support """
        from mpi4py import MPI
        provided = MPI.Query_thread()
        self.assertEqual(provided, MPI.THREAD_MULTIPLE)


    def test_mpi_auto_init(self):
        """ MPI auto initialization """
        from mpi4py import MPI
        self.assertTrue(MPI.Is_initialized())


    def test_comm_class(self):
        """ Check MPI.COMM_WORLD class """
        from  mpi4py import MPI
        comm = MPI.COMM_WORLD
        self.assertIsInstance(comm, MPI.Intracomm)



class TestEventStack(TestCaseFF):
    """Event stack basic operations"""

    def setUp(self):
        pass


    def tearDown(self):
        pass


    def test_event_stack(self):
        """Event stack create ops"""
        es = EventStack()
        self.assertIsInstance(es, EventStack)
        self.assertIsNone(es.id)
        es.create()
        self.assertIsInstance(es.id, h5es.EventStackID)
        self.assertTrue(es.id.valid)
        es.close()
        self.assertFalse(es.id.valid)


    def test_es_null(self):
        """es_null (H5_STACK_NULL) object"""
        self.assertIsNone(es_null.id)
        with self.assertRaises(AttributeError):
            es_null.create()


    def test_h5ies(self):
        """H5Iget_type() value for event stack object"""
        es = EventStack()
        es.create()
        self.assertEqual(h5i.get_type(es.id), h5i.ES)
