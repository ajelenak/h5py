# High-level Python interface for Map Exascale FastForward object.

import numpy
import h5py
from h5py import h5s, h5t, h5m
from .base import HLObject
from .dataset import readtime_dtype
from . import datatype


def make_new_map(parent, name, tr, es, kdt=None, vdt=None):
    """ Create a new map object and return its low-level identifier object

    For Exascale FastForward.
    """

    def type_sel(dt):
        """ Select the data type for key or value """
        if isinstance(dt, h5py.Datatype):
            # This is named datatype, use it...
            return dt.id
        else:
            # More work required to figure out datatype...
            if dt is None:
                dtype = numpy.dtype("=f4")
            else:
                dtype = numpy.dtype(dt)
            return h5t.py_create(dtype, logical=1)

    k_tid = type_sel(kdt)
    v_tid = type_sel(vdt)
    mapid = h5m.create_ff(parent.id, name, k_tid, v_tid, tr.id, es=es.id)
    return mapid


class Map(HLObject):
    """ Represent an Exascale FastForward Map object """

    @property
    def key_dtype(self):
        """Key datatype

        For Exascale FastForward.
        """
        return datatype.Datatype(self.id.key_typeid)

    @property
    def val_dtype(self):
        """Value datatype

        For Exascale FastForward.
        """
        return datatype.Datatype(self.id.val_typeid)

    @property
    def key_shape(self):
        """ Numpy ndarray shape tuple for map's keys """
        return self._key_shape

    @property
    def val_shape(self):
        """ Numpy ndarray shape tuple for map's values """
        return self._val_shape


    def __init__(self, mapid, container=None):
        """ Initialize a new EFF map object from the MapID identifier object
        """
        if not isinstance(mapid, h5m.MapID):
            raise TypeError("%s is not a MapID object" % mapid)
        HLObject.__init__(self, mapid)
        self._ctn = container

        # Shapes for storing key and value data...
        self._key_shape = None
        self._val_shape = None


    def close(self):
        """Close the map.

        For Exascale FastForward.
        """
        self.id._close_ff(es=self.es.id)


    def count(self):
        """Count of key/value pairs.

        For Exascale FastForward.
        """
        return self.id.get_count_ff(self.rc.id, es=self.es.id)


    def __len__(self):
        """Count of key/value pairs.

        For Exascale FastForward.
        """
        return self.count()


    def get(self, key):
        """Read the value for the given key.

        For Exascale FastForward.
        """
        if not self.exists(key):
            raise KeyError(str(key))
        vdt = readtime_dtype(self.val_dtype.dtype, [])
        val = numpy.ndarray(self.val_shape, dtype=vdt, order='C')
        key = numpy.asarray(key, order='C', dtype=self.key_dtype.dtype)
        self.id.get_ff(key, val, self.rc.id, es=self.es.id)
        if len(val.shape) == 0:
            return val[()]
        return val


    def __getitem__(self, key):
        """Get the value for the given map's key.

        For Exascale FastForward.
        """
        return self.get(key)


    def set(self, key, value):
        """Set the value for the given key of the map object.

        For Exascale FastForward.
        """
        key = numpy.asarray(key, order='C', dtype=self.key_dtype.dtype)
        value = numpy.asarray(value, order='C', dtype=self.val_dtype.dtype)

        if self.key_shape is None:
            # Remember the key's shape the first time
            self._key_shape = key.shape
        elif self._key_shape != key.shape:
            raise ValueError("Key shape mismatch; got %s, expected %s" %
                             (key.shape, self._key_shape))

        if self.val_shape is None:
            # Remember the value's shape the first time
            self._val_shape = value.shape
        elif self._val_shape != value.shape:
            raise ValueError("Value shape mismatch; got %s, expected %s" %
                             (value.shape, self._val_shape))

        self.id.set_ff(key, value, self.tr.id, es=self.es.id)


    def __setitem__(self, key, value):
        """Set the value of the map's key"""
        self.set(key, value)


    def key_type(self):
        """Return map's key datatype

        For Exascale FastForward.
        """
        t = self.id.get_types_ff(self.rc.id, es=self.es.id)
        return datatype.Datatype(t[0])


    def value_type(self):
        """Return map's value datatype

        For Exascale FastForward.
        """
        t = self.id.get_types_ff(self.rc.id, es=self.es.id)
        return datatype.Datatype(t[1])


    def delete(self, key):
        """Delete a key/value pair from the map object.

        For Exascale FastForward.
        """
        key = numpy.asarray(key, order='C', dtype=self.key_dtype.dtype)
        self.id.delete_ff(key, self.tr.id, es=self.es.id)


    def __delitem__(self, key):
        """ Delete the map's key """
        self.delete(key)


    def exists(self, key):
        """Determine whether a key exists in the map object.

        For Exascale FastForward.
        """
        key = numpy.asarray(key, order='C', dtype=self.key_dtype.dtype)
        return self.id.exists_ff(key, self.rc.id, es=self.es.id)


    def __contains__(self, key):
        """Test if key is in the map"""
        return self.exists(key)
