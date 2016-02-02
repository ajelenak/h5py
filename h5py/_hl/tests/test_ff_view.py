# Testing HDF5 H5V Exascale FastForward

import numpy as np
from .common_ff import ut, TestCaseFF
from h5py.highlevel import File, View, AQuery, CQuery
from h5py.eff_control import eff_init, eff_finalize



class BaseTest(TestCaseFF):

    def setUp(self):
        self.ff_cleanup()
        self.start_h5ff_server(quiet=True)
        self.fname = self.filename("ff_file_view.h5")


    def tearDown(self):
        self.shut_h5ff_server()



class TestView(BaseTest):

    """
        Feature: View (H5V) operations
    """

    def test_view_001(self):
        """ View creation on file, group, dataset, attribute"""
        from mpi4py import MPI
        comm = MPI.COMM_WORLD
        eff_init(comm, MPI.INFO_NULL)
        rank = comm.Get_rank()
        if rank == 0:
            f = File(self.fname, 'w', driver='iod', comm=comm,
                     info=MPI.INFO_NULL)
            f.acquire_context(1)
            f.create_transaction(2)
            f.tr.start()

            f.attrs['a'] = 4.0
            g = f.create_group('G')
            d = g.create_dataset('D', (10,))

            f.tr.finish()
            f.rc.release()
            f.acquire_context(2)

            q = AQuery('link_name') == 'foo'
            v1 = View(f, q)
            self.assertIsInstance(v1, View)
            v2 = View(g, q)
            self.assertIsInstance(v2, View)
            v3 = View(d, q)
            self.assertIsInstance(v3, View)
            with self.assertRaises(TypeError):
                v4 = View(f.attrs['a'], q)

            f.rc.release()

            d.close()
            g.close()
            f.close()
        eff_finalize()


    def test_view_002(self):
        """ View location object"""
        from h5py.highlevel import Group, Dataset
        from mpi4py import MPI
        comm = MPI.COMM_WORLD
        eff_init(comm, MPI.INFO_NULL)
        rank = comm.Get_rank()
        if rank == 0:
            f = File(self.fname, 'w', driver='iod', comm=comm,
                     info=MPI.INFO_NULL)
            f.acquire_context(1)
            f.create_transaction(2)
            f.tr.start()

            g = f.create_group('G')
            d = g.create_dataset('D', (10,))

            f.tr.finish()
            f.rc.release()
            f.acquire_context(2)

            q = AQuery('link_name') == 'foo'
            v1 = View(f, q)
            v2 = View(g, q)
            v3 = View(d, q)

            # This is an issue with how to create a new file instance from
            # FileID object.
            # self.assertIsInstance(v1.location, File)
            obj = v2.location
            self.assertIsInstance(obj, Group)
            obj.close()
            obj = v3.location
            self.assertIsInstance(obj, Dataset)
            obj.close()

            f.rc.release()

            d.close()
            g.close()
            f.close()
        eff_finalize()


    def test_view_003(self):
        """Finding attributes and objects by name"""
        from mpi4py import MPI
        comm = MPI.COMM_WORLD
        eff_init(comm, MPI.INFO_NULL)
        rank = comm.Get_rank()
        if rank == 0:
            f = File(self.fname, 'w', driver='iod', comm=comm,
                     info=MPI.INFO_NULL)
            f.acquire_context(1)
            f.create_transaction(2)
            f.tr.start()

            f.attrs['foo'] = 1967
            f.attrs['bar'] = 47.
            g1 = f.create_group('G1')
            d1 = g1.create_dataset('foo', (10,))
            g2 = g1.create_group('G2')
            d2 = g2.create_dataset('foo', (20,))
            d3 = g2.create_dataset('baz', (500,1000))

            f.tr.finish()
            f.rc.release()
            f.acquire_context(2)

            q = AQuery('link_name') == 'foo'
            v = View(f, q)
            self.assertEqual(v.attr_count, 0)
            self.assertEqual(v.obj_count, 2)
            self.assertEqual(v.reg_count, 0)

            q = AQuery('link_name') == 'baz'
            v = View(g2, q)
            self.assertEqual(v.attr_count, 0)
            self.assertEqual(v.obj_count, 1)
            self.assertEqual(v.reg_count, 0)

            q = AQuery('link_name') != 'foo'
            v = View(f, q)
            self.assertEqual(v.attr_count, 0)
            self.assertEqual(v.obj_count, 4)
            self.assertEqual(v.reg_count, 0)

            q = AQuery('link_name') == 'G1'
            v = View(f, q)
            self.assertEqual(v.attr_count, 0)
            self.assertEqual(v.obj_count, 1)
            self.assertEqual(v.reg_count, 0)

            q = AQuery('attr_name') == 'foo'
            v = View(f, q)
            self.assertEqual(v.attr_count, 0)
            self.assertEqual(v.obj_count, 1)
            self.assertEqual(v.reg_count, 0)

            q = AQuery('attr_name') != 'foo'
            v = View(f, q)
            self.assertEqual(v.attr_count, 0)
            self.assertEqual(v.obj_count, 1)
            self.assertEqual(v.reg_count, 0)

            f.rc.release()

            d1.close()
            d2.close()
            d3.close()
            g1.close()
            g2.close()
            f.close()
        eff_finalize()


    def test_view_004(self):
        """Finding datasets by value"""
        from mpi4py import MPI
        comm = MPI.COMM_WORLD
        eff_init(comm, MPI.INFO_NULL)
        rank = comm.Get_rank()
        if rank == 0:
            data = np.arange(50)
            data.resize((5,10))
            f = File(self.fname, 'w', driver='iod', comm=comm,
                     info=MPI.INFO_NULL)
            f.acquire_context(1)
            f.create_transaction(2)
            f.tr.start()

            d1 = f.create_dataset('foo', data=data)
            g1 = f.create_group('G1')
            d2 = g1.create_dataset('foo', data=1.0*data)
            g2 = g1.create_group('G2')
            d3 = g2.create_dataset('foo', data=data)
            d4 = g2.create_dataset('baz', data=np.arange(50))

            f.tr.finish()
            f.rc.release()
            f.acquire_context(2)

            self.assertEqual(d1.dtype, np.int64)
            self.assertEqual(d2.dtype, np.float64)
            self.assertEqual(d3.dtype, np.int64)
            self.assertEqual(d4.dtype, np.int64)
            self.assertEqual(d4.shape, (50,))

            q = AQuery('data_elem') > 24
            v = View(f, q)
            self.assertEqual(v.attr_count, 0)
            self.assertEqual(v.obj_count, 0)
            self.assertEqual(v.reg_count, 4)

            f.rc.release()

            d1.close()
            d2.close()
            d3.close()
            d4.close()
            g1.close()
            g2.close()
            f.close()
        eff_finalize()


    def test_view_005(self):
        """Finding attributes by value"""
        from mpi4py import MPI
        comm = MPI.COMM_WORLD
        eff_init(comm, MPI.INFO_NULL)
        rank = comm.Get_rank()
        if rank == 0:
            data = np.arange(50)
            data.resize((5,10))
            f = File(self.fname, 'w', driver='iod', comm=comm,
                     info=MPI.INFO_NULL)
            f.acquire_context(1)
            f.create_transaction(2)
            f.tr.start()

            f.attrs['foo'] = data
            f.attrs['bar'] = 1.0*data
            f.attrs['baz'] = np.arange(50)
            f.attrs['blah'] = 1.

            f.tr.finish() 
            f.rc.release()
            f.acquire_context(2)

            q = AQuery('attr_value') > 24
            v = View(f, q)
            self.assertEqual(v.attr_count, 3)
            self.assertEqual(v.obj_count, 0)
            self.assertEqual(v.reg_count, 0)

            f.rc.release()

            f.close()
        eff_finalize()


    def test_view_006(self):
        """Finding attributes with compound query"""
        from mpi4py import MPI
        comm = MPI.COMM_WORLD
        eff_init(comm, MPI.INFO_NULL)
        rank = comm.Get_rank()
        if rank == 0:
            data = np.arange(50)
            data.resize((5,10))
            f = File(self.fname, 'w', driver='iod', comm=comm,
                     info=MPI.INFO_NULL)
            f.acquire_context(1)
            f.create_transaction(2)
            f.tr.start()

            f.attrs['foo'] = data
            f.attrs['bar'] = 1.0*data
            f.attrs['baz'] = np.arange(50)
            f.attrs['blah'] = 1.

            f.tr.finish() 
            f.rc.release()
            f.acquire_context(2)

            q = (AQuery('attr_value') > 24) & (AQuery('attr_name') == 'foo')
            self.assertTrue(q.is_compound)
            v = View(f, q)
            self.assertEqual(v.attr_count, 1)
            self.assertEqual(v.obj_count, 0)
            self.assertEqual(v.reg_count, 0)

            q = (AQuery('attr_value') > 24) & (AQuery('attr_name') != 'foo')
            self.assertTrue(q.is_compound)
            v = View(f, q)
            self.assertEqual(v.attr_count, 2)
            self.assertEqual(v.obj_count, 0)
            self.assertEqual(v.reg_count, 0)

            f.rc.release()

            f.close()
        eff_finalize()


    def test_view_007(self):
        """Finding datasets with compound query"""
        from mpi4py import MPI
        comm = MPI.COMM_WORLD
        eff_init(comm, MPI.INFO_NULL)
        rank = comm.Get_rank()
        if rank == 0:
            data = np.arange(50)
            data.resize((5,10))
            f = File(self.fname, 'w', driver='iod', comm=comm,
                     info=MPI.INFO_NULL)
            f.acquire_context(1)
            f.create_transaction(2)
            f.tr.start()

            d1 = f.create_dataset('foo', data=data)
            g1 = f.create_group('G1')
            d2 = g1.create_dataset('foo', data=1.0*data)
            g2 = g1.create_group('G2')
            d3 = g2.create_dataset('foo', data=data)
            d4 = g2.create_dataset('baz', data=np.arange(50))

            f.tr.finish()
            f.rc.release()
            f.acquire_context(2)

            self.assertEqual(d1.dtype, np.int64)
            self.assertEqual(d2.dtype, np.float64)
            self.assertEqual(d3.dtype, np.int64)
            self.assertEqual(d4.dtype, np.int64)
            self.assertEqual(d4.shape, (50,))

            q = (AQuery('data_elem') > 24) & (AQuery('link_name') == 'foo')
            v = View(f, q)
            self.assertEqual(v.attr_count, 0)
            self.assertEqual(v.obj_count, 0)
            self.assertEqual(v.reg_count, 3)

            q = (AQuery('data_elem') > 24) & (AQuery('link_name') != 'foo')
            v = View(f, q)
            self.assertEqual(v.attr_count, 0)
            self.assertEqual(v.obj_count, 0)
            self.assertEqual(v.reg_count, 1)

            f.rc.release()

            d1.close()
            d2.close()
            d3.close()
            d4.close()
            g1.close()
            g2.close()
            f.close()
        eff_finalize()


    def test_view_008(self):
        """Finding objects with compound query"""
        from mpi4py import MPI
        comm = MPI.COMM_WORLD
        eff_init(comm, MPI.INFO_NULL)
        rank = comm.Get_rank()
        if rank == 0:
            data = np.arange(50)
            data.resize((5,10))
            f = File(self.fname, 'w', driver='iod', comm=comm,
                     info=MPI.INFO_NULL)
            f.acquire_context(1)
            f.create_transaction(2)
            f.tr.start()

            d1 = f.create_dataset('foo', data=data)
            g1 = f.create_group('G1')
            d2 = g1.create_dataset('foo', data=1.0*data)
            g2 = g1.create_group('G2')
            d3 = g2.create_dataset('foo', data=data)
            d4 = g2.create_dataset('baz', data=np.arange(50))

            f.tr.finish()
            f.rc.release()
            f.acquire_context(2)

            self.assertEqual(d1.dtype, np.int64)
            self.assertEqual(d2.dtype, np.float64)
            self.assertEqual(d3.dtype, np.int64)
            self.assertEqual(d4.dtype, np.int64)
            self.assertEqual(d4.shape, (50,))

            q = (AQuery('data_elem') > 24) | (AQuery('link_name') == 'G1') \
                | (AQuery('link_name') == 'G2')
            v = View(f, q)
            self.assertEqual(v.attr_count, 0)
            self.assertEqual(v.obj_count, 2)
            self.assertEqual(v.reg_count, 4)

            f.rc.release()

            d1.close()
            d2.close()
            d3.close()
            d4.close()
            g1.close()
            g2.close()
            f.close()
        eff_finalize()


    def test_view_009(self):
        """Retrieving found objects and regions"""
        from h5py.highlevel import Group
        from h5py import h5d, h5s
        from mpi4py import MPI
        comm = MPI.COMM_WORLD
        eff_init(comm, MPI.INFO_NULL)
        rank = comm.Get_rank()
        if rank == 0:
            data = np.arange(50)
            data.resize((5,10))
            f = File(self.fname, 'w', driver='iod', comm=comm,
                     info=MPI.INFO_NULL)
            f.acquire_context(1)
            f.create_transaction(2)
            f.tr.start()

            d1 = f.create_dataset('foo', data=data)
            g1 = f.create_group('G1')
            d2 = g1.create_dataset('foo', data=1.0*data)
            g2 = g1.create_group('G2')
            d3 = g2.create_dataset('foo', data=data)
            d4 = g2.create_dataset('baz', data=np.arange(50))

            f.tr.finish()
            f.rc.release()
            f.acquire_context(2)

            q = (AQuery('data_elem') > 24) | (AQuery('link_name') == 'G1') \
                | (AQuery('link_name') == 'G2')
            v = View(f, q)
            self.assertEqual(v.attr_count, 0)
            self.assertEqual(v.obj_count, 2)
            self.assertEqual(v.reg_count, 4)

            objs = v.objs(count=v.obj_count)
            counter = 0
            for o in objs:
                self.assertIsInstance(o, Group)
                o.close()
                counter += 1
            self.assertEqual(counter, v.obj_count)

            f.rc.release()

            d1.close()
            d2.close()
            d3.close()
            d4.close()
            g1.close()
            g2.close()
            f.close()
        eff_finalize()


    def test_view_010(self):
        """Retrieving found attributes"""
        from h5py import h5a
        from mpi4py import MPI
        comm = MPI.COMM_WORLD
        eff_init(comm, MPI.INFO_NULL)
        rank = comm.Get_rank()
        if rank == 0:
            data = np.arange(50)
            data.resize((5,10))
            f = File(self.fname, 'w', driver='iod', comm=comm,
                     info=MPI.INFO_NULL)
            f.acquire_context(1)
            f.create_transaction(2)
            f.tr.start()

            f.attrs['foo'] = data
            f.attrs['bar'] = 1.0*data
            f.attrs['baz'] = np.arange(50)
            f.attrs['blah'] = 1.

            f.tr.finish()
            f.rc.release()
            f.acquire_context(2)

            q = (AQuery('attr_value') > 24) & (AQuery('attr_name') == 'foo')
            v = View(f, q)

            attrs = v.attrs(count=v.attr_count)
            counter = 0
            for a in attrs:
                self.assertIsInstance(a.id, h5a.AttrID)
                a.close()
                counter += 1
            self.assertEqual(counter, v.attr_count)

            f.rc.release()

            f.close()
        eff_finalize()
