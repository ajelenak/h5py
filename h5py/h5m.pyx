"""
    H5M Map API low-level operations.

    For Exascale FastForward.
"""

include "config.pxi"

from h5p cimport pdefault, PropID
from h5g cimport GroupID
from h5t cimport typewrap, TypeID, py_create
from numpy cimport import_array, ndarray, PyArray_DATA
from utils cimport check_numpy_read, check_numpy_write
from _proxy cimport map_del_ff, map_check_ff, map_gs_ff

from h5es cimport esid_default, EventStackID
from h5rc cimport RCntxtID
from h5tr cimport TransactionID

from h5py import _objects

# Initialize NumPy
import_array()


def create_ff(GroupID loc not None, char* name, TypeID key_type not None,
              TypeID val_type not None, TransactionID tr not None,
              PropID lcpl=None, EventStackID es=None):
    """(GroupID loc, STRING name, TypeID key_type, TypeID val_type,
    TransactionID tr, PropID lcpl=None, EventStackID es=None) => MapID

    Create a new map object and link it into a file or a group, possibly
    asynchronously.
    """
    cdef hid_t mid
    cdef MapID mapid
    mid = H5Mcreate_ff(loc.id, name, key_type.id, val_type.id, pdefault(lcpl),
                       H5P_DEFAULT, H5P_DEFAULT, tr.id, esid_default(es))
    mapid = MapID.open(mid)
    mapid.key_typeid = key_type
    mapid.val_typeid = val_type
    return mapid


def open_ff(GroupID loc not None, char* name, RCntxtID rc not None, EventStackID es=None):
    """(GroupID loc, STRING name, RCntxtID rc, EventStackID es=None) => MapID

    Open an existing map object possibly asynchronously.
    """
    cdef hid_t mid
    cdef MapID m
    cdef tuple kv_types
    mid = H5Mopen_ff(loc.id, name, H5P_DEFAULT, rc.id, esid_default(es))
    m = MapID.open(mid)

    # Set up the key/value TypeIDs...
    kv_types = m.get_types_ff(rc, es=es)
    m.key_typeid = kv_types[0]
    m.val_typeid = kv_types[1]

    return m


cdef class MapID(ObjectID):
    """ Represents HDF5 map object identifier """

    def __cinit__(self):
        self._ktid = None
        self._vtid = None

    property key_typeid:
        """ Stores key TypeID """

        def __get__(self):
            return self._ktid

        def __set__(self, tid):
            self._ktid = tid

    property val_typeid:
        """ Stores value TypeID """

        def __get__(self):
            return self._vtid

        def __set__(self, tid):
            self._vtid = tid

    def _close_ff(self, EventStackID es=None):
        """(EventStackID es=None)

        Close the specified map object, possibly asynchronously.

        Terminate access through this identifier. You shouldn't have to call
        this manually; event stack identifiers are automatically released when
        their Python wrappers are freed.
        """
        with _objects.registry.lock:
            H5Mclose_ff(self.id, esid_default(es))
            if not self.valid:
                del _objects.registry[self.id]


    def get_count_ff(self, RCntxtID rc not None, EventStackID es=None):
        """(RCntxtID rc, EventStackID es=None) => INT count

        Retrieve the number of key/value pairs in the map object, possibly
        asynchronously.
        """
        cdef hsize_t count
        H5Mget_count_ff(self.id, &count, rc.id, esid_default(es))
        return count


    def get_types_ff(self, RCntxtID rc not None, EventStackID es=None):
        """(RCntxtID rc, EventStackID es=None) => TUPLE (TypeID key_type, TypeID val_type)

        Retrieve the datatypes for the keys and values of the map object,
        possibly asynchronously.
        """
        cdef hid_t key_type_id, val_type_id
        H5Mget_types_ff(self.id, &key_type_id, &val_type_id, rc.id, esid_default(es))
        return typewrap(key_type_id), typewrap(val_type_id)


    def delete_ff(self, ndarray key not None, TransactionID tr not None,
                  EventStackID es=None):
        """(NDARRAY key, TransactionID tr, EventStackID es=None)

        Delete a key/value pair from the map object.
        """
        cdef TypeID key_type

        try:
            check_numpy_read(key)
            key_type = self.key_typeid
            map_del_ff(self.id, key_type.id, PyArray_DATA(key), tr.id, 
                       esid_default(es))
        finally:
            pass


    def exists_ff(self, ndarray key not None, RCntxtID rc not None,
                  EventStackID es=None):
        """(NDARRAY key, RCntxtID rc, EventStackID es=None) => BOOL

        Determine whether a key exists in a map object, possibly
        asynchronously.
        """
        cdef hbool_t exists
        cdef TypeID key_type

        try:
            check_numpy_read(key)
            key_type = self.key_typeid
            exists = map_check_ff(self.id, key_type.id, PyArray_DATA(key),
                                  rc.id, esid_default(es))
        finally:
            pass
        
        return <bint>exists


    def get_ff(self, ndarray key not None, ndarray val not None, RCntxtID rc not None,
               PropID dxpl=None, EventStackID es=None):
        """(NDARRAY key, NDARRAY val, RCntxtID rc, PropID dxpl=None,
        EventStackID es=None)

        Retrieve the value for a given key from the map object, possibly
        asynchronously.
        """
        cdef TypeID key_type, val_type

        try:
            check_numpy_read(key)
            check_numpy_write(val)
            key_type = self.key_typeid
            val_type = self.val_typeid
            map_gs_ff(self.id, key_type.id, PyArray_DATA(key), val_type.id,
                      PyArray_DATA(val), pdefault(dxpl), rc.id,
                      esid_default(es), 1)
        finally:
            pass


    def set_ff(self, ndarray key not None, ndarray val not None,
               TransactionID tr not None, PropID dxpl=None,
               EventStackID es=None):
        """(NDARRAY key, NDARRAY val, TransactionID tr, PropID dxpl=None,
        EventStackID es=None)

        Set the value for a given key in the map object, possibly
        asynchronously.
        """
        cdef TypeID key_type, val_type

        try:
            check_numpy_read(key)
            check_numpy_read(val)
            key_type = self.key_typeid
            val_type = self.val_typeid

            map_gs_ff(self.id, key_type.id, PyArray_DATA(key), val_type.id,
                      PyArray_DATA(val), pdefault(dxpl), tr.id,
                      esid_default(es), 0)
        finally:
            pass


    def evict_ff(self, ctn_ver, PropID dxpl=None, EventStackID es=None):
        """UINT ctn_ver, PropID dxpl=None, EventStackID es=None)

        Evict map from the burst buffer, possibly asynchronously.

        For Exascale FastForward.
        """
        H5Mevict_ff(self.id, <uint64_t>ctn_ver, pdefault(dxpl),
                    esid_default(es))


    def prefetch_ff(self, RCntxtID rc not None, hrpl_t replica_id,
                    PropID dxpl=None, EventStackID es=None):
        """(RCntxtID rc, UINT replica_id, PropID dxpl=None, EventStackID es=None)

        For Exascale FastForward.
        """
        H5Mprefetch_ff(self.id, rc.id, &replica_id, pdefault(dxpl),
                       esid_default(es))

