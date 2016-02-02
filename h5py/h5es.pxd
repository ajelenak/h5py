from defs cimport *

from _objects cimport ObjectID

cdef class EventStackID(ObjectID):
   pass

cdef hid_t esid_default(EventStackID es)
