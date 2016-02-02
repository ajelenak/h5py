from defs cimport *

from _objects cimport ObjectID

cdef class MapID(ObjectID):
    cdef object _ktid, _vtid
