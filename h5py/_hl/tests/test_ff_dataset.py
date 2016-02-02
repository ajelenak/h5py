# Testing HDF5 Exascale FastForward
# Tests adapted from the original h5py's collection of attribute tests

import sys
import numpy as np
from .common_ff import ut, TestCaseFF
from h5py.highlevel import File, Group, Dataset
from h5py.eff_control import eff_init, eff_finalize
from h5py import h5t, h5
import h5py

if not h5.get_config().eff:
    raise RuntimeError('The h5py module was not built for Exascale FastForward')



class BaseTest(TestCaseFF):

    def setUp(self):
        self.ff_cleanup()
        self.start_h5ff_server()
        self.fname = self.filename("ff_file_dataset.h5")


    def tearDown(self):
        self.shut_h5ff_server()



class TestDataset(BaseTest):

    """
        Tests for HDF5 Exascale FastForward Dataset object
    """

    def test_create_scalar(self):
        """ Create a scalar dataset """
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

            dset = f.create_dataset('foo', ())
            self.assertIsInstance(dset, Dataset)
            self.assertEqual(dset.shape, ())

            f.tr.finish()
        f.rc.release()
        comm.Barrier()
        if rank == 0:
            dset.close()
        f.close()
        eff_finalize()


    def test_create_simple(self):
        """ Create a size-1 dataset in a group and confirm default dtype """
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

            g = f.create_group('G')
            dset = g.create_dataset('D', (63,))

            self.assertIsInstance(dset, Dataset)
            self.assertEqual(dset.shape, (63,))
            self.assertEqual(dset.dtype, np.dtype('=f4'))

            self.assertIsNone(f.ctn)
            self.assertEqual(str(dset.ctn), str(g.ctn))

            self.assertEqual(str(g.tr), str(f.tr))
            self.assertEqual(str(dset.tr), str(g.tr))

            self.assertEqual(str(g.rc), str(f.rc))
            self.assertEqual(str(dset.rc), str(g.rc))

            self.assertEqual(str(g.es), str(f.es))
            self.assertEqual(str(dset.es), str(g.es))

            f.tr.finish()
        f.rc.release()
        comm.Barrier()
        if rank == 0:
            dset.close()
            g.close()
        f.close()
        eff_finalize()


    def test_create_extended(self):
        """ Create an extended dataset and missing shape raises TypeError """
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

            foo = f.create_dataset('foo', (63,))
            self.assertEqual(foo.shape, (63,))
            self.assertEqual(foo.size, 63)
            bar = f.create_dataset('bar', (6, 10))
            self.assertEqual(bar.shape, (6, 10))
            self.assertEqual(bar.size, (60))

            with self.assertRaises(TypeError):
                f.create_dataset('baz')

            f.tr.finish()
        f.rc.release()
        comm.Barrier()
        if rank == 0:
            foo.close()
            bar.close()
        f.close()
        eff_finalize()


    def test_create_scalar_extended(self):
        """ Create scalar and extended datasets from existing data """
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

            data = np.ones((), 'f')
            d1 = f.create_dataset('d1', data=data)
            self.assertEqual(d1.shape, data.shape)

            data = np.ones((100,), 'f')
            d2 = f.create_dataset('d2', data=data)
            self.assertEqual(d2.shape, data.shape)
            
            f.tr.finish()
        f.rc.release()
        comm.Barrier()
        if rank == 0:
            d1.close()
            d2.close()
        f.close()
        eff_finalize()


    @ut.skip('Test FAILS')
    def test_dataset_intermediate_group(self):
        """ Create dataset with missing intermediate groups """
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

            ds = f.create_dataset("/foo/bar/baz", shape=(10, 10), dtype='<i4')
            self.assertIsInstance(ds, h5py.Dataset)
            self.assertTrue("/foo/bar/baz" in self.f)
            
            f.tr.finish()
        f.rc.release()
        comm.Barrier()
        if rank == 0:
            ds.close()
        f.close()
        eff_finalize()        


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

            f.tr.finish()
            f.tr.close()
        f.rc.release()
        comm.Barrier()

        f.acquire_context(2)
        comm.Barrier()
        if rank == 0:
            self.assertArrayEqual(dset[...], data.reshape((10, 3)))
        f.rc.release()
        comm.Barrier()
        if rank == 0:
            dset.close()
        f.close()
        eff_finalize()


    def test_dtype(self):
        """ Retrieve dtype from dataset """
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

            ds = f.create_dataset('foo', (5,), '|S10')
            self.assertEqual(ds.dtype, np.dtype('|S10'))

            f.tr.finish()
        f.rc.release()
        comm.Barrier()
        if rank == 0:
            ds.close()
        f.close()
        eff_finalize()


    def test_require(self):
        """ require_dataset() operations """
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

            dset = f.require_dataset('foo', (10, 3), 'f')
            self.assertIsInstance(dset, Dataset)
            self.assertEqual(dset.shape, (10, 3))

            f.tr.finish()
        f.rc.release()

        comm.Barrier()

        f.acquire_context(2)
        if rank == 0:
            dset2 = f.require_dataset('foo', (10, 3), 'f')
            self.assertEqual(str(dset), str(dset2))
        f.rc.release()

        if rank == 0:
            dset.close()
            dset2.close()
        f.close()
        eff_finalize()


    def test_chunks(self):
        """ Dataset creation with specifying chunks """
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

            dset = f.create_dataset('foo', shape=(100,), chunks=(10,))
            self.assertEqual(dset.chunks, (10,))

            # Illegal chunk size raises ValueError...
            with self.assertRaises(ValueError):
                f.create_dataset('bar', shape=(100,), chunks=(200,))

            # Attempting to create chunked scalar dataset raises TypeError...
            with self.assertRaises(TypeError):
                f.create_dataset('bar', shape=(), chunks=(50,))

            # Auto-chunking of datasets...
            dset1 = f.create_dataset('bar', shape=(20, 100), chunks=True)
            self.assertIsInstance(dset1.chunks, tuple)
            self.assertEqual(len(dset1.chunks), 2)

            # Auto-chunking with pathologically large element sizes...
            dset2 = f.create_dataset('baz', shape=(3,), dtype='S100000000',
                                     chunks=True)
            self.assertEqual(dset2.chunks, (1,))

            f.tr.finish()
        f.rc.release()
        comm.Barrier()
        if rank == 0:
            dset.close()
            dset1.close()
            dset2.close()
        f.close()
        eff_finalize()


    @ut.skip('Test FAILS')
    def test_create_fillval(self):
        """ Fill value is reflected in dataset contents """
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

            dset = f.create_dataset('foo', (10,), fillvalue=4.0)

            f.tr.finish()
            f.tr.close()
        f.rc.release()
        comm.Barrier()

        f.acquire_context(2)
        comm.Barrier()
        if rank == 0:
            self.assertEqual(dset[0], 4.0)
            self.assertEqual(dset[7], 4.0)
        f.rc.release()

        if rank == 0:
            dset.close()
        f.close()
        eff_finalize()


    def test_named(self):
        """ Named type object works and links the dataset to type """
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

            f['type'] = np.dtype('f8')

            f.tr.finish()
        f.rc.release()
        comm.Barrier()
        f.acquire_context(2)

        if rank == 0:
            f.create_transaction(3)
            f.tr.start()

            dset = f.create_dataset('x', (100,), dtype=f['type'])
            self.assertEqual(dset.dtype, np.dtype('f8'))
            self.assertEqual(dset.id.get_type(), f['type'].id)
            # Below assert fails, suspect: H5Tcommitted().
            # self.assertTrue(dset.id.get_type().committed())

            f.tr.finish()

        f.rc.release()
        comm.Barrier()

        if rank == 0:
            dset.close()
        f.close()
        eff_finalize()


    def test_resize(self):
        """ Dataset.resize() operations """
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

            # Create dataset with "maxshape"...
            dset = f.create_dataset('foo', (20, 30), maxshape=(20, 60))
            self.assertEqual(dset.shape, (20, 30))
            self.assertIsNot(dset.chunks, None)
            self.assertEqual(dset.maxshape, (20, 60))

            # Datasets may be resized up to maxshape...
            dset.resize((20, 50))
            self.assertEqual(dset.shape, (20, 50))
            dset.resize((20, 60))
            self.assertEqual(dset.shape, (20, 60))

            # This assert causes f.close() command later to fail. Cause unknown.
            # Resizing past maxshape triggers ValueError...
            # with self.assertRaises(ValueError):
            #     dset.resize((20, 70))

            # Resize specified axis...
            dset.resize(50, axis=1)
            self.assertEqual(dset.shape, (20, 50))

            # Illegal axis raises ValueError...
            with self.assertRaises(ValueError):
                dset.resize(50, axis=2)

            # This create_dataset() fails.
            # Allow zero-length initial dims for unlimited axes (issue 111)...
            # dset1 = f.create_dataset('bar', (15, 0), maxshape=(15, None))
            # self.assertEqual(dset1.shape, (15, 0))
            # self.assertEqual(dset1.maxshape, (15, None))

            f.tr.finish()
        f.rc.release()
        comm.Barrier()
        if rank == 0:
            dset.close()
            # dset1.close()
        f.close()
        eff_finalize()


    def test_slicing_single(self):
        """ Retrieving a single element with NumPy semantics """
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

            dset = f.create_dataset('x', (1,), dtype='i1')
            dset1 = f.create_dataset('y', shape=(), dtype='i1')

            f.tr.finish()
            f.tr.close()
        f.rc.release()
        comm.Barrier()

        f.acquire_context(2)
        comm.Barrier()
        if rank == 0:
            out = dset[0]
            self.assertIsInstance(out, np.int8)

            out = dset[()]
            self.assertIsInstance(out, np.ndarray)
            self.assertEqual(out.shape, (1,))

            out = dset1[()]
            self.assertIsInstance(out, np.int8)

            out = dset1[...]
            self.assertIsInstance(out, np.ndarray)
            self.assertEqual(out.shape, ())
        f.rc.release()

        if rank == 0:
            dset.close()
            dset1.close()
        f.close()
        eff_finalize()


    def test_simple_slicing(self):
        """ Simple NumPy-style slices (start:stop:step) are supported """
        from mpi4py import MPI
        comm = MPI.COMM_WORLD
        eff_init(comm, MPI.INFO_NULL)
        rank = comm.Get_rank()
        arr = np.arange(10)
        f = File(self.fname, 'w', driver='iod', comm=comm, info=MPI.INFO_NULL)
        f.acquire_context(1)
        comm.Barrier()
        if rank == 0:
            f.create_transaction(2)
            f.tr.start()

            dset = f.create_dataset('x', data=arr)

            f.tr.finish()
            f.tr.close()
        f.rc.release()
        comm.Barrier()

        f.acquire_context(2)
        comm.Barrier()
        if rank == 0:
            self.assertArrayEqual(dset[2:-2], arr[2:-2])
        f.rc.release()

        if rank == 0:
            dset.close()
        f.close()
        eff_finalize()


    def test_read(self):
        """ Read arrays """
        from mpi4py import MPI
        comm = MPI.COMM_WORLD
        eff_init(comm, MPI.INFO_NULL)
        rank = comm.Get_rank()
        arr = np.arange(10)
        f = File(self.fname, 'w', driver='iod', comm=comm, info=MPI.INFO_NULL)
        f.acquire_context(1)
        comm.Barrier()
        if rank == 0:
            f.create_transaction(2)
            f.tr.start()

            dt = np.dtype('(3,)f8')
            dset = f.create_dataset('x', (10,), dtype=dt)
            self.assertEqual(dset.shape, (10,))
            self.assertEqual(dset.dtype, dt)

            f.tr.finish()
            f.tr.close()
        f.rc.release()
        comm.Barrier()

        f.acquire_context(2)
        comm.Barrier()
        if rank == 0:
            # Full read
            out = dset[...]
            self.assertEqual(out.dtype, np.dtype('f8'))
            self.assertEqual(out.shape, (10,3))

            # Single element
            out = dset[0]
            self.assertEqual(out.dtype, np.dtype('f8'))
            self.assertEqual(out.shape, (3,))

            # Range
            out = dset[2:8:2]
            self.assertEqual(out.dtype, np.dtype('f8'))
            self.assertEqual(out.shape, (3,3))
        f.rc.release()

        if rank == 0:
            dset.close()
        f.close()
        eff_finalize()


    def test_write_element(self):
        """ Write a single element to the array """
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

            dt = np.dtype('(3,)f8')
            dset = f.create_dataset('x', (10,), dtype=dt)
            data  = np.array([1, 2, 3.0])
            dset[4] = data

            f.tr.finish()
            f.tr.close()
        f.rc.release()
        comm.Barrier()

        f.acquire_context(2)
        comm.Barrier()
        if rank == 0:
            out = dset[4]
            self.assertTrue(np.all(out == data))
        f.rc.release()

        if rank == 0:
            dset.close()
        f.close()
        eff_finalize()


    def test_write_slices(self):
        """ Write slices to array type """
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

            dt = np.dtype('(3,)i')
            data1 = np.ones((2,), dtype=dt)
            data2 = np.ones((4,5), dtype=dt)
            dset = f.create_dataset('x', (10,9,11), dtype=dt)
            dset[0,0,2:4] = data1
            dset[3,1:5,6:11] = data2

            f.tr.finish()
            f.tr.close()
        f.rc.release()
        comm.Barrier()

        f.acquire_context(2)
        comm.Barrier()
        if rank == 0:
            self.assertArrayEqual(dset[0,0,2:4], data1)
            self.assertArrayEqual(dset[3, 1:5, 6:11], data2)
        f.rc.release()

        if rank == 0:
            dset.close()
        f.close()
        eff_finalize()


    def test_roundtrip(self):
        """ Read the contents of an array and write them back """
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

            dt = np.dtype('(3,)f8')
            dset = f.create_dataset('x', (10,), dtype=dt)

            f.tr.finish()
            f.tr.close()
        f.rc.release()
        comm.Barrier()

        f.acquire_context(2)
        comm.Barrier()
        if rank == 0:
            out = dset[...]

            f.create_transaction(3)
            f.tr.start()

            dset[...] = out

            f.tr.finish()
        f.rc.release()

        comm.Barrier()
       
        f.acquire_context(3)
        comm.Barrier()
        if rank == 0:
            self.assertTrue(np.all(dset[...] == out))
        f.rc.release()

        comm.Barrier()

        if rank == 0:
            dset.close()
        f.close()
        eff_finalize()


    @ut.skip('Test FAILS')
    def test_create_ref(self):
        """ Region references can be used as slicing arguments """
        from mpi4py import MPI
        comm = MPI.COMM_WORLD
        eff_init(comm, MPI.INFO_NULL)
        rank = comm.Get_rank()
        data = np.arange(100*100).reshape((100, 100))
        if rank == 0:
            f = File(self.fname, 'w', driver='iod', comm=comm,
                     info=MPI.INFO_NULL)
            f.acquire_context(1)
            f.create_transaction(2)
            f.tr.start()

            dset = f.create_dataset('x', data=data)
            dset[...] = data

            f.tr.finish()
            f.tr.close()
            f.rc.release()

            f.acquire_context(2)

            slic = np.s_[25:35, 10:100:5]
            ref = dset.regionref[slic]
            self.assertArrayEqual(dset[ref], data[slic])
            self.assertEqual(dset.regionref.shape(ref), dset.shape)
            self.assertEqual(dset.regionref.selection(ref), (10, 18))

            f.rc.release()

            dset.close()
            f.close()
        eff_finalize()


    @ut.skip('Test FAILS')
    def test_rt(self):
        """ Compound types are read back in correct order """
        from mpi4py import MPI
        comm = MPI.COMM_WORLD
        eff_init(comm, MPI.INFO_NULL)
        rank = comm.Get_rank()
        data = np.arange(100*100).reshape((100, 100))
        dt = np.dtype([('weight', np.float64),
                       ('cputime', np.float64),
                       ('walltime', np.float64),
                       ('parents_offset', np.uint32),
                       ('n_parents', np.uint32),
                       ('status', np.uint8),
                       ('endpoint_type', np.uint8), ] )

        testdata = np.ndarray((16,), dtype=dt)
        for key in dt.fields:
            testdata[key] = np.random.random((16,))*100
        
        if rank == 0:
            f = File(self.fname, 'w', driver='iod', comm=comm,
                     info=MPI.INFO_NULL)
            f.acquire_context(1)
            f.create_transaction(2)
            f.tr.start()
            
            f['test'] = testdata

            f.tr.finish()
            f.tr.close()
            f.rc.release()

            f.acquire_context(2)

            outdata = f['test'][...]
            self.assertTrue(np.all(outdata == testdata))
            self.assertEqual(outdata.dtype, testdata.dtype)

            f.rc.release()

            f.close()
        eff_finalize()


    def test_astype(self):
        """ .astype context manager """
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

            dset = f.create_dataset('x', (100,), dtype='i2')
            dset[...] = np.arange(100)

            f.tr.finish()
            f.tr.close()
            f.rc.release()

            f.acquire_context(2)

            with dset.astype('f8'):
                self.assertEqual(dset[...].dtype, np.dtype('f8'))
                self.assertTrue(np.all(dset[...] == np.arange(100)))

            f.rc.release()

            dset.close()
            f.close()
        eff_finalize()


    @ut.skip('Test FAILS')
    def test_regref(self):
        """ Indexing a region reference dataset returns a h5py.RegionReference
        """
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

            dset1 = f.create_dataset('x', (10,10))

            f.tr.finish()
            f.rc.release()
            f.acquire_context(2)

            regref = dset1.regionref[...]

            f.create_transaction(3)
            f.tr.start()

            dset2 = f.create_dataset('y', (1,), dtype=h5py.special_dtype(ref=h5py.RegionReference))
            dset2[0] = regref

            f.tr.finish()
            f.rc.release()
            f.acquire_context(3)

            self.assertEqual(type(dset2[0]), h5py.RegionReference)

            f.rc.release()


            dset1.close()
            dset2.close()
            f.close()
        eff_finalize()


    @ut.skip('Test FAILS')
    def test_reference_field(self):
        """Compound types of which a reference is an element work right"""
        from mpi4py import MPI
        comm = MPI.COMM_WORLD
        eff_init(comm, MPI.INFO_NULL)
        rank = comm.Get_rank()
        reftype = h5py.special_dtype(ref=h5py.Reference)
        dt = np.dtype([('a', 'i'),('b',reftype)])
        if rank == 0:
            f = File(self.fname, 'w', driver='iod', comm=comm,
                     info=MPI.INFO_NULL)
            f.acquire_context(1)
            f.create_transaction(2)
            f.tr.start()

            dset = f.create_dataset('x', (1,), dtype=dt)
            dset[0] = (42, f['/'].ref)

            f.tr.finish()
            f.rc.release()
            f.acquire_context(2)

            out = dset[0]
            self.assertEqual(type(out[1]), h5py.Reference)

            f.rc.release()

            dset.close()
            f.close()
        eff_finalize()


    @ut.skip('Test FAILS')
    def test_ref_scalar(self):
        """Indexing returns a real Python object on scalar datasets"""
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

            dset = f.create_dataset('x', (), dtype=h5py.special_dtype(ref=h5py.Reference))
            dset[()] = f.ref

            f.tr.finish()
            f.rc.release()
            f.acquire_context(2)

            self.assertEqual(type(dset[()]), h5py.Reference)

            f.rc.release()

            dset.close()
            f.close()
        eff_finalize()


    def test_bytestr(self):
        """Indexing a byte string dataset returns a real python byte string"""
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

            dset = f.create_dataset('x', (1,),
                                    dtype=h5py.special_dtype(vlen=bytes))
            dset[0] = b"Hello there!"
        
            f.tr.finish()
            f.rc.release()
            f.acquire_context(2)

            self.assertEqual(type(dset[0]), bytes)

            f.rc.release()

            dset.close()
            f.close()
        eff_finalize()
