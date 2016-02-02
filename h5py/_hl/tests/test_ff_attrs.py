# Testing HDF5 Exascale FastForward
# Tests adapted from the original h5py's collection of attribute tests

import numpy as np
from .common_ff import ut, TestCaseFF
from h5py.highlevel import File, AttributeManager
from h5py.eff_control import eff_init, eff_finalize
from h5py import h5t, h5a, get_config, special_dtype

if not get_config().eff:
    raise RuntimeError('The h5py module was not built for Exascale FastForward')



class BaseTest(TestCaseFF):

    def setUp(self):
        self.ff_cleanup()
        self.start_h5ff_server(quiet=False)
        self.fname = self.filename("ff_file_attrs.h5")


    def tearDown(self):
        self.shut_h5ff_server()
        # pass



class TestAccess(BaseTest):

    """
        Feature: Attribute creation/retrieval/deletion via special methods
    """

    def test_create_scalar(self):
        """ Attribute creation by direct asignment """
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

            f.tr.finish()
            f.rc.release()
            f.acquire_context(2)

            #File "h5a.pyx", line 432, in h5py.h5a.iterate (h5py/h5a.c:5695)
            # H5Aiterate2(loc.id, <H5_index_t>index_type, <H5_iter_order_t>order,
            # RuntimeError: link iteration failed (Symbol table: Iteration failed)
            # self.assertEqual(f.attrs.keys(), ['a'])
            self.assertEqual(len(f.attrs), 1)
            self.assertEqual(f.attrs['a'], 4.0)

            with self.assertRaises(KeyError):
                f.attrs['b']

            f.rc.release()

            f.close()
        eff_finalize()


    def test_overwrite(self):
        """ Attributes are silently overwritten """
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
            f.attrs['a'] = 5.0

            f.tr.finish()
            f.rc.release()
            f.acquire_context(2)

            self.assertEqual(f.attrs['a'], 5.0)

            f.rc.release()

            f.close()
        eff_finalize()


    def test_rank(self):
        """ Attribute rank is preserved """
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

            f.attrs['a'] = (4.0, 5.0)
            f.attrs['b'] = np.ones((1,))

            f.tr.finish()
            f.rc.release()
            f.acquire_context(2)

            self.assertEqual(f.attrs['a'].shape, (2,))
            self.assertArrayEqual(f.attrs['a'], np.array((4.0,5.0)))

            out = f.attrs['b']
            self.assertEqual(out.shape, (1,))
            self.assertEqual(out[()], 1)

            f.rc.release()

            f.close()
        eff_finalize()


    def test_delete(self):
        """ Deletion of attributes using __delitem__ """
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

            f.tr.finish()
            f.rc.release()

            f.acquire_context(2)

            self.assertIn('a', f.attrs)

            f.create_transaction(3)
            f.tr.start()

            del f.attrs['a']

            f.tr.finish()
            f.rc.release()

            f.acquire_context(3)

            self.assertNotIn('a', f.attrs)

            f.rc.release()

            f.close()
        eff_finalize()


    def test_unicode(self):
        """ Attributes can be accessed via Unicode or byte strings  """
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

            f.attrs[b'ascii'] = 47
            name1 = b'non-ascii\xfe'
            f.attrs[name1] = 47
            name2 = u'Omega \u03A9'
            f.attrs[name2] = 47

            f.tr.finish()
            f.rc.release()

            f.acquire_context(2)

            self.assertEqual(f.attrs[b'ascii'], 47)
            self.assertEqual(f.attrs[name1], 47)
            self.assertEqual(f.attrs[name2], 47)

            f.rc.release()

            f.close()
        eff_finalize()


    def test_named(self):
        """ Attributes created from named types link to the source type object
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

            f['type'] = np.dtype('u8')

            f.tr.finish()
            f.rc.release()

            f.acquire_context(2)
            f.create_transaction(3)
            f.tr.start()

            f.attrs.create('x', 42, dtype=f['type'])

            f.tr.finish()
            f.rc.release()

            f.acquire_context(3)

            self.assertEqual(f.attrs['x'], 42)
            aid = h5a.open_ff(f.id, f.rc.id, b'x')
            htype = aid.get_type()
            htype2 = f['type'].id
            self.assertEqual(htype, htype2)
            # self.assertTrue(htype.committed())

            f.rc.release()

            htype._close()
            htype2._close()
            aid._close_ff()
            f.close()
        eff_finalize()


    @ut.skip('Test FAILS')
    def test_vlen(self):
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

            a = np.array([np.arange(3), np.arange(4)],
                         dtype=h5t.special_dtype(vlen=int))
            f.attrs['a'] = a

            f.tr.finish()
            f.rc.release()

            f.acquire_context(2)

            self.assertArrayEqual(f.attrs['a'][0], a[0])

            f.rc.release()

            f.close()
        eff_finalize()


class TestScalar(BaseTest):

    def test_int(self):
        """ Integers are read as correct NumPy type """
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

            f.attrs['x'] = np.array(1, dtype=np.int8)

            f.tr.finish()
            f.rc.release()

            f.acquire_context(2)

            out = f.attrs['x']
            self.assertIsInstance(out, np.int8)

            f.rc.release()

            f.close()
        eff_finalize()


    def test_compound(self):
        """ Compound scalars are read as numpy.void """
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

            dt = np.dtype([('a','i'),('b','f')])
            data = np.array((1,4.2), dtype=dt)
            f.attrs['x'] = data

            f.tr.finish()
            f.rc.release()

            f.acquire_context(2)

            out = f.attrs['x']
            self.assertIsInstance(out, np.void)
            self.assertEqual(out, data)
            self.assertEqual(out['b'], data['b'])

            f.rc.release()

            f.close()
        eff_finalize()


class TestArray(BaseTest):


    def test_single(self):
        """ Single-element arrays are correctly recovered """
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

            data = np.ndarray((1,), dtype='f')
            f.attrs['x'] = data

            f.tr.finish()
            f.rc.release()

            f.acquire_context(2)

            out = f.attrs['x']
            self.assertIsInstance(out, np.ndarray)
            self.assertEqual(out.shape, (1,))

            f.rc.release()

            f.close()
        eff_finalize()


    def test_multi(self):
        """ Rank-1 arrays are correctly recovered """
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

            data = np.ndarray((47,), dtype='f')
            data[:] = 47.0
            data[10:35] = -47.0
            f.attrs['x'] = data

            f.tr.finish()
            f.rc.release()

            f.acquire_context(2)

            out = f.attrs['x']
            self.assertIsInstance(out, np.ndarray)
            self.assertEqual(out.shape, (47,))
            self.assertArrayEqual(out, data)

            f.rc.release()

            f.close()
        eff_finalize()


class TestTypes(BaseTest):
    """ Feature: All supported types can be stored in attributes """


    def test_int(self):
        """ Storage of integer types  """
        from mpi4py import MPI
        comm = MPI.COMM_WORLD
        eff_init(comm, MPI.INFO_NULL)
        rank = comm.Get_rank()
        if rank == 0:
            f = File(self.fname, 'w', driver='iod', comm=comm,
                     info=MPI.INFO_NULL)
            rc_ver = 1
            tr_ver = 2
            dtypes = (np.int8, np.int16, np.int32, np.int64, np.uint8,
                      np.uint16, np.uint32, np.uint64)
            for dt in dtypes:
                f.acquire_context(rc_ver)
                f.create_transaction(tr_ver)
                f.tr.start()

                data = np.ndarray((1,), dtype=dt)
                data[...] = 47
                f.attrs['x'] = data

                f.tr.finish()
                f.rc.release()

                rc_ver += 1
                f.acquire_context(rc_ver)

                out = f.attrs['x']
                self.assertEqual(out.dtype, dt)
                self.assertArrayEqual(out, data)

                f.rc.release()
                tr_ver += 1

            f.close()
        eff_finalize()


    def test_float(self):
        """ Storage of floating point types """
        from mpi4py import MPI
        comm = MPI.COMM_WORLD
        eff_init(comm, MPI.INFO_NULL)
        rank = comm.Get_rank()
        if rank == 0:
            f = File(self.fname, 'w', driver='iod', comm=comm,
                     info=MPI.INFO_NULL)
            rc_ver = 1
            tr_ver = 2
            dtypes = tuple(np.dtype(x) for x in ('<f4','>f4','<f8','>f8'))
            for dt in dtypes:
                f.acquire_context(rc_ver)
                f.create_transaction(tr_ver)
                f.tr.start()

                data = np.ndarray((1,), dtype=dt)
                data[...] = 47.1967
                f.attrs['x'] = data

                f.tr.finish()
                f.rc.release()

                rc_ver += 1
                f.acquire_context(rc_ver)

                out = f.attrs['x']
                self.assertEqual(out.dtype, dt)
                self.assertArrayEqual(out, data)

                f.rc.release()
                tr_ver += 1

            f.close()
        eff_finalize()


    def test_complex(self):
        """ Storage of complex types """
        from mpi4py import MPI
        comm = MPI.COMM_WORLD
        eff_init(comm, MPI.INFO_NULL)
        rank = comm.Get_rank()
        if rank == 0:
            f = File(self.fname, 'w', driver='iod', comm=comm,
                     info=MPI.INFO_NULL)
            rc_ver = 1
            tr_ver = 2
            dtypes = tuple(np.dtype(x) for x in ('<c8','>c8','<c16','>c16'))
            for dt in dtypes:
                f.acquire_context(rc_ver)
                f.create_transaction(tr_ver)
                f.tr.start()

                data = np.ndarray((1,), dtype=dt)
                data[...] = 47.-1967j
                f.attrs['x'] = data

                f.tr.finish()
                f.rc.release()

                rc_ver += 1
                f.acquire_context(rc_ver)

                out = f.attrs['x']
                self.assertEqual(out.dtype, dt)
                self.assertArrayEqual(out, data)

                f.rc.release()
                tr_ver += 1

            f.close()
        eff_finalize()


    def test_string(self):
        """ Storage of fixed-length strings """
        from mpi4py import MPI
        comm = MPI.COMM_WORLD
        eff_init(comm, MPI.INFO_NULL)
        rank = comm.Get_rank()
        if rank == 0:
            f = File(self.fname, 'w', driver='iod', comm=comm,
                     info=MPI.INFO_NULL)
            rc_ver = 1
            tr_ver = 2
            dtypes = tuple(np.dtype(x) for x in ('|S1', '|S10'))
            for dt in dtypes:
                f.acquire_context(rc_ver)
                f.create_transaction(tr_ver)
                f.tr.start()

                data = np.ndarray((1,), dtype=dt)
                data[...] = 'n'
                f.attrs['x'] = data

                f.tr.finish()
                f.rc.release()

                rc_ver += 1
                f.acquire_context(rc_ver)

                out = f.attrs['x']
                self.assertEqual(out.dtype, dt)
                self.assertEqual(out[0], data[0])

                f.rc.release()
                tr_ver += 1

            f.close()
        eff_finalize()


    def test_bool(self):
        """ Storage of NumPy booleans """
        from mpi4py import MPI
        comm = MPI.COMM_WORLD
        eff_init(comm, MPI.INFO_NULL)
        rank = comm.Get_rank()
        if rank == 0:
            f = File(self.fname, 'w', driver='iod', comm=comm,
                     info=MPI.INFO_NULL)
            data = np.ndarray((2,), dtype=np.bool_)
            data[...] = True, False
            f.acquire_context(1)
            f.create_transaction(2)
            f.tr.start()

            f.attrs['x'] = data

            f.tr.finish()
            f.rc.release()

            f.acquire_context(2)

            out = f.attrs['x']
            self.assertEqual(out.dtype, data.dtype)
            self.assertEqual(out[0], data[0])
            self.assertEqual(out[1], data[1])

            f.rc.release()

            f.close()
        eff_finalize()


    @ut.skip('Test FAILS')
    def test_vlen_string_array(self):
        """ Storage of vlen byte string arrays """
        from mpi4py import MPI
        comm = MPI.COMM_WORLD
        eff_init(comm, MPI.INFO_NULL)
        rank = comm.Get_rank()
        if rank == 0:
            f = File(self.fname, 'w', driver='iod', comm=comm,
                     info=MPI.INFO_NULL)
            dt = special_dtype(vlen=bytes)
            data = np.ndarray((2,), dtype=dt)
            data[...] = b"Hello", b"HDF5 FastForward is awesome!"
            f.acquire_context(1)
            f.create_transaction(2)
            f.tr.start()

            f.attrs['x'] = data

            f.tr.finish()
            f.rc.release()

            f.acquire_context(2)

            out = f.attrs['x']
            self.assertEqual(out.dtype, data.dtype)
            self.assertEqual(out[0], data[0])
            self.assertEqual(out[1], data[1])

            f.rc.release()

            f.close()
        eff_finalize()


    @ut.skip('Test FAILS')
    def test_string_scalar(self):
        """ Storage of variable-length byte string scalars (auto-creation) """
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

            f.attrs['x'] = b'HDF5 FastForward is awesome!'

            f.tr.finish()
            f.rc.release()

            f.acquire_context(2)

            out = f.attrs['x']
            self.assertEqual(out, b'HDF5 FastForward is awesome!')
            self.assertEqual(type(out), data[0])
            aid = h5a.open_ff(f.id, f.rc.id, b"x")
            tid = aid.get_type()
            self.assertEqual(type(tid), h5t.TypeStringID)
            self.assertEqual(tid.get_cset(), h5t.CSET_ASCII)
            self.assertTrue(tid.is_variable_str())
            aid._close_ff()
            tid._close_ff()

            f.rc.release()

            f.close()
        eff_finalize()


    @ut.skip('Test FAILS')
    def test_unicode_scalar(self):
        """ Storage of variable-length Unicode string scalars (auto-creation)
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

            f.attrs['x'] = u'HDF5 FastForward is awesome\u2340!'

            f.tr.finish()
            f.rc.release()

            f.acquire_context(2)

            out = f.attrs['x']
            self.assertEqual(out, b'HDF5 FastForward is awesome\u2340!')
            self.assertEqual(type(out), data[0])
            aid = h5a.open_ff(f.id, f.rc.id, b"x")
            tid = aid.get_type()
            self.assertEqual(type(tid), h5t.TypeStringID)
            self.assertEqual(tid.get_cset(), h5t.CSET_UTF8)
            self.assertTrue(tid.is_variable_str())
            aid._close_ff()
            tid._close_ff()

            f.rc.release()

            f.close()
        eff_finalize()
