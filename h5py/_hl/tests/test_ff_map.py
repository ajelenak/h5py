# Test suite for Exascale FastForward H5M API.

import os
import numpy
from .common_ff import ut, TestCaseFF
from h5py import h5
from h5py.eff_control import eff_init, eff_finalize

from h5py.highlevel import File, Map

# Check if this HDF5 is built with MPI and for EFF...
eff = h5.get_config().eff
if not eff:
    raise RuntimeError('The h5py module was not built for Exascale FastForward') 
mpi = h5.get_config().mpi
if not mpi:
    raise RuntimeError('This HDF5 does not appear to be built with MPI support')


class TestMap(TestCaseFF):

    def setUp(self):
        self.ff_cleanup()
        self.start_h5ff_server()


    def tearDown(self):
        pass


    def test_create_map_root(self):
        """ Create an empty map in the root group """
        from mpi4py import MPI
        from h5py import h5m

        comm = MPI.COMM_WORLD
        eff_init(comm, MPI.INFO_NULL)
        my_rank = comm.Get_rank()
        fname = self.filename("ff_file_map.h5")
        f = File(fname, 'w', driver='iod', comm=comm, info=MPI.INFO_NULL)
        my_version = 1
        version = f.acquire_context(my_version)
        self.assertEqual(my_version, version)
        
        comm.Barrier()
        
        if my_rank == 0:
            f.create_transaction(2)
            f.tr.start()

            m = f.create_map('empty_map')
            self.assertIsInstance(m, Map)
            self.assertIsInstance(m.id, h5m.MapID)

            m.close()

            f.tr.finish()
        
        f.rc.release()
        
        comm.Barrier()
        
        f.close()
        eff_finalize()


    def test_default_kv_types(self):
        """ Default key/value datatypes """
        from mpi4py import MPI
        from h5py.highlevel import Datatype

        comm = MPI.COMM_WORLD
        eff_init(comm, MPI.INFO_NULL)
        my_rank = comm.Get_rank()
        fname = self.filename("ff_file_map.h5")
        f = File(fname, 'w', driver='iod', comm=comm, info=MPI.INFO_NULL)
        my_version = 1
        version = f.acquire_context(my_version)
        self.assertEqual(my_version, version)
        
        comm.Barrier()
        
        if my_rank == 0:
            f.create_transaction(2)
            f.tr.start()

            m = f.create_map('empty_map')

            self.assertIsInstance(m.key_dtype, Datatype)
            self.assertIsInstance(m.val_dtype, Datatype)
            self.assertEqual(m.key_dtype.dtype, numpy.dtype('=f4'))
            self.assertEqual(m.val_dtype.dtype, numpy.dtype('=f4'))

            m.close()

            f.tr.finish()
        
        f.rc.release()
        
        comm.Barrier()
        
        f.close()
        eff_finalize()


    def test_create_map_group(self):
        """ Create an empty map in a group """
        from mpi4py import MPI
        from h5py import h5m

        comm = MPI.COMM_WORLD
        eff_init(comm, MPI.INFO_NULL)
        my_rank = comm.Get_rank()
        fname = self.filename("ff_file_map.h5")
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

            m = grp2.create_map('empty_map')
            self.assertIsInstance(m, Map)
            self.assertIsInstance(m.id, h5m.MapID)

            self.assertIsNone(f.ctn)
            self.assertEqual(str(grp1.ctn), str(f))
            self.assertEqual(str(grp2.ctn), str(grp1.ctn))
            self.assertEqual(str(m.ctn), str(grp2.ctn))

            self.assertEqual(str(grp1.tr), str(f.tr))
            self.assertEqual(str(grp2.tr), str(grp1.tr))
            self.assertEqual(str(m.tr), str(grp2.tr))

            self.assertEqual(str(grp1.rc), str(f.rc))
            self.assertEqual(str(grp2.rc), str(grp1.rc))
            self.assertEqual(str(m.rc), str(grp2.rc))

            self.assertEqual(str(grp1.es), str(f.es))
            self.assertEqual(str(grp2.es), str(grp1.es))
            self.assertEqual(str(m.es), str(grp2.es))

            m.close()

            f.tr.finish()
        
        f.rc.release()
        
        comm.Barrier()
        
        if my_rank == 0:
            grp1.close()
            grp2.close()
        f.close()
        eff_finalize()


    def test_create_open_group_map(self):
        """ Create an empty map in a group then open it """
        from mpi4py import MPI
        from h5py import h5m

        comm = MPI.COMM_WORLD
        eff_init(comm, MPI.INFO_NULL)
        my_rank = comm.Get_rank()
        fname = self.filename("ff_file_map.h5")
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

            m = grp2.create_map("empty_map")
            self.assertIsInstance(m, Map)
            self.assertIsInstance(m.id, h5m.MapID)
            m.close()

            grp2.close()
            grp1.close()

            f.tr.finish()
            f.tr.close()
        
        f.rc.release()
        
        comm.Barrier()

        f.rc.close()

        my_version = 2
        version = f.acquire_context(2)
        self.assertEqual(my_version, version)

        comm.Barrier()

        if my_rank == 0:
            m = f.open_map('G1/G2/empty_map')
            self.assertIsInstance(m, Map)
            self.assertIsInstance(m.id, h5m.MapID)
            m.close()

        f.rc.release()
        comm.Barrier()
        f.rc.close()

        f.close()
        eff_finalize()


    def test_get_kv_types(self):
        """ h5m.get_types_ff reported datatypes """
        from mpi4py import MPI
        from h5py.highlevel import Datatype

        comm = MPI.COMM_WORLD
        eff_init(comm, MPI.INFO_NULL)
        my_rank = comm.Get_rank()
        fname = self.filename("ff_file_map.h5")
        f = File(fname, 'w', driver='iod', comm=comm, info=MPI.INFO_NULL)
        my_version = 1
        version = f.acquire_context(my_version)
        self.assertEqual(my_version, version)
        
        comm.Barrier()
        
        if my_rank == 0:
            f.create_transaction(2)
            f.tr.start()

            m = f.create_map('empty_map')
            key_dt = m.key_type()
            val_dt = m.value_type()

            self.assertIsInstance(key_dt, Datatype)
            self.assertIsInstance(val_dt, Datatype)
            self.assertEqual(key_dt.dtype, numpy.dtype('=f4'))
            self.assertEqual(val_dt.dtype, numpy.dtype('=f4'))

            m.close()

            f.tr.finish()
        
        f.rc.release()
        
        comm.Barrier()
        
        f.close()
        eff_finalize()


    def test_user_kv_types(self):
        """ User supplied datatypes """
        from mpi4py import MPI
        from h5py import h5p
        from h5py.highlevel import Datatype

        comm = MPI.COMM_WORLD
        eff_init(comm, MPI.INFO_NULL)
        my_rank = comm.Get_rank()
        fname = self.filename("ff_file_map.h5")
        f = File(fname, 'w', driver='iod', comm=comm, info=MPI.INFO_NULL)
        my_version = 1
        version = f.acquire_context(my_version)
        self.assertEqual(my_version, version)
        
        comm.Barrier()
        
        if my_rank == 0:
            f.create_transaction(2)
            f.tr.start()

            m = f.create_map('empty_map', key_dtype='S7', val_dtype='int64')
            key_dt = m.key_type()
            val_dt = m.value_type()

            self.assertIsInstance(key_dt, Datatype)
            self.assertIsInstance(val_dt, Datatype)
            self.assertEqual(key_dt.dtype, numpy.dtype('S7'))
            self.assertEqual(val_dt.dtype, numpy.dtype('int64'))

            m.close()

            f.tr.finish()
        
        f.rc.release()
        
        comm.Barrier()
        
        f.close()
        eff_finalize()


    def test_get_empty_map(self):
        """Getting a key/value pair from empty map raises exception"""
        from mpi4py import MPI

        comm = MPI.COMM_WORLD
        eff_init(comm, MPI.INFO_NULL)
        my_rank = comm.Get_rank()
        fname = self.filename("ff_file_map.h5")
        f = File(fname, 'w', driver='iod', comm=comm, info=MPI.INFO_NULL)
        my_version = 1
        version = f.acquire_context(my_version)
        self.assertEqual(my_version, version)
        
        comm.Barrier()
        
        if my_rank == 0:
            f.create_transaction(2)
            f.tr.start()

            m = f.create_map('empty_map')

            f.tr.finish()
            f.tr.close()
        
        f.rc.release()
        
        comm.Barrier()
        
        f.rc.close()

        my_version = 2
        version = f.acquire_context(2)        
        self.assertEqual(my_version, version)

        comm.Barrier()

        if my_rank == 0:
            kv_pairs = m.count()
            self.assertEqual(kv_pairs, 0)
            with self.assertRaises(KeyError):
                val = m.get(1)

        m.close()

        f.rc.release()

        comm.Barrier()

        f.rc.close()

        f.close()
        eff_finalize()


    def test_map_kv_ops_user_types(self):
        """ Map get/set/delete/exists key/value ops with user supplied datatypes
        """
        from mpi4py import MPI

        comm = MPI.COMM_WORLD
        eff_init(comm, MPI.INFO_NULL)
        my_rank = comm.Get_rank()
        fname = self.filename("ff_file_map.h5")
        f = File(fname, 'w', driver='iod', comm=comm, info=MPI.INFO_NULL)
        my_version = 1
        version = f.acquire_context(my_version)
        self.assertEqual(my_version, version)

        comm.Barrier()

        if my_rank == 0:
            f.create_transaction(2)
            f.tr.start()

            m = f.create_map('test_map', key_dtype='S7', val_dtype='int64')
            m.set('a', 1)
            m.set('b', 2)
            m.set(1, 3) # will convert key to string '1'
            m.set('12345678', 4) # will clip key to 7 chars

            f.tr.finish()
            f.tr.close()

        f.rc.release()
        comm.Barrier()
        f.rc.close()

        my_version = 2
        version = f.acquire_context(2)        
        self.assertEqual(my_version, version)

        comm.Barrier()

        if my_rank == 0:
            kv_exists = m.exists('a')
            self.assertTrue(kv_exists)

            kv_exists = m.exists('b')
            self.assertTrue(kv_exists)

            kv_exists = m.exists('c')
            self.assertFalse(kv_exists)

            kv_exists = m.exists('1')
            self.assertTrue(kv_exists)

            kv_exists = m.exists(1)
            self.assertTrue(kv_exists)

            kv_exists = m.exists('1234567')
            self.assertTrue(kv_exists)

            cnt = m.count()
            self.assertEqual(cnt, 4)

            # Close the map object so it can be opened for a get()
            m.close()
            m = f.open_map('test_map')

            val = m.get('a')
            self.assertEqual(val, 1)

            val = m.get('b')
            self.assertEqual(val, 2)

            val = m.get('1')
            self.assertEqual(val, 3)

            val = m.get(1)
            self.assertEqual(val, 3)

            val = m.get('1234567')
            self.assertEqual(val, 4)

            # Delete some key-value pairs...
            f.create_transaction(3)
            f.tr.start()
            m.delete('b')
            m.delete('1234567')
            f.tr.finish()
            f.tr.close()

        f.rc.release()
        comm.Barrier()
        f.rc.close()

        my_version = 3
        version = f.acquire_context(3)
        self.assertEqual(my_version, version)

        comm.Barrier()

        if my_rank == 0:
            kv_exists = m.exists('a')
            self.assertTrue(kv_exists)

            kv_exists = m.exists('b')
            self.assertFalse(kv_exists)

            kv_exists = m.exists('1')
            self.assertTrue(kv_exists)

            kv_exists = m.exists('1234567')
            self.assertFalse(kv_exists)

            cnt = m.count()
            self.assertEqual(cnt, 2)


        m.close()

        f.rc.release()
        comm.Barrier()
        f.rc.close()

        f.close()
        eff_finalize()


    def test_map_kv_ops_w_special(self):
        """ Map get/set/delete/exists key/value ops using special methods
        """
        from mpi4py import MPI

        comm = MPI.COMM_WORLD
        eff_init(comm, MPI.INFO_NULL)
        my_rank = comm.Get_rank()
        fname = self.filename("ff_file_map.h5")
        f = File(fname, 'w', driver='iod', comm=comm, info=MPI.INFO_NULL)
        my_version = 1
        version = f.acquire_context(my_version)
        self.assertEqual(my_version, version)

        comm.Barrier()

        if my_rank == 0:
            f.create_transaction(2)
            f.tr.start()

            m = f.create_map('test_map', key_dtype='S7', val_dtype='int64')
            m['a'] = 1
            m['b'] = 2
            m[1] = 3 # will convert key to string '1'
            m['12345678'] = 4 # will clip key to 7 chars

            f.tr.finish()
            f.tr.close()

        f.rc.release()
        comm.Barrier()
        f.rc.close()

        my_version = 2
        version = f.acquire_context(2)        
        self.assertEqual(my_version, version)

        comm.Barrier()

        if my_rank == 0:
            self.assertTrue('a' in m)
            self.assertTrue('b' in m)
            self.assertFalse('c' in m)
            self.assertTrue('1' in m)
            self.assertTrue(1 in m)
            self.assertTrue('1234567' in m)
            self.assertEqual(len(m), 4)

            # Close the map object so it can be opened get() operations...
            m.close()
            m = f.open_map('test_map')

            self.assertEqual(m['a'], 1)
            self.assertEqual(m['b'], 2)
            self.assertEqual(m['1'], 3)
            self.assertEqual(m[1], 3)
            self.assertEqual(m['1234567'], 4)

            # Delete some key-value pairs...
            f.create_transaction(3)
            f.tr.start()
            del m['b']
            del m['1234567']
            f.tr.finish()
            f.tr.close()

        f.rc.release()
        comm.Barrier()
        f.rc.close()

        my_version = 3
        version = f.acquire_context(3)
        self.assertEqual(my_version, version)

        comm.Barrier()

        if my_rank == 0:
            self.assertTrue('a' in m)
            self.assertFalse('b' in m)
            self.assertTrue('1' in m)
            self.assertFalse('1234567' in m)
            self.assertEqual(len(m), 2)

        m.close()

        f.rc.release()
        comm.Barrier()
        f.rc.close()

        f.close()
        eff_finalize()
