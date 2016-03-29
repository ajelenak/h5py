# H5Q API Low-Level Bindings
include "config.pxi"

cdef extern from "hdf5.h":
    hid_t H5Qcreate(H5Q_type_t query_type, H5Q_match_op_t match_op, ...) except *

include "_locks.pxi"

from _errors cimport set_exception
from h5t cimport typewrap, TypeID
from h5p cimport pdefault, PropVCID
from h5rc cimport RCntxtID
from h5es cimport EventStackID, esid_default
cimport h5i
from numpy cimport import_array, ndarray, PyArray_DATA
from utils cimport check_numpy_read, emalloc, efree

from h5py import _objects

# Initialize NumPy
import_array()

# API Constants

# Query type
TYPE_DATA_ELEM = H5Q_TYPE_DATA_ELEM
TYPE_ATTR_VALUE = H5Q_TYPE_ATTR_VALUE
TYPE_ATTR_NAME = H5Q_TYPE_ATTR_NAME
TYPE_LINK_NAME = H5Q_TYPE_LINK_NAME
TYPE_MISC = H5Q_TYPE_MISC

# Match operator
MATCH_EQUAL = H5Q_MATCH_EQUAL
MATCH_NOT_EQUAL = H5Q_MATCH_NOT_EQUAL
MATCH_LESS_THAN = H5Q_MATCH_LESS_THAN
MATCH_GREATER_THAN = H5Q_MATCH_GREATER_THAN

# Combine operator
COMBINE_AND = H5Q_COMBINE_AND
COMBINE_OR = H5Q_COMBINE_OR
SINGLETON = H5Q_SINGLETON

# Query result bitmasks
REF_REG = H5Q_REF_REG
REF_OBJ = H5Q_REF_OBJ
REF_ATTR = H5Q_REF_ATTR

# Names of view datasets
VIEW_REF_REG_NAME = H5Q_VIEW_REF_REG_NAME
VIEW_REF_OBJ_NAME = H5Q_VIEW_REF_OBJ_NAME
VIEW_REF_ATTR_NAME = H5Q_VIEW_REF_ATTR_NAME

#
# API Bindings
#

def create(int query_type, int match_op, *args):
    """(INT query_type, INT match_op, *args) => QueryID

    Create a new atomic query object with match_op condition.
    """
    cdef hid_t qid, dtid
    cdef char* name

    rlock = FastRLock()
    if query_type == H5Q_TYPE_DATA_ELEM or query_type == H5Q_TYPE_ATTR_VALUE:
        dt = args[0]
        if not isinstance(dt, TypeID):
            raise ValueError("Third argument must be TypeID")
        dtid = dt.id

        value = args[1]
        if not isinstance(value, ndarray):
            raise ValueError("Fourth argument must be ndarray")
        check_numpy_read(value)

        with rlock:
            qid = H5Qcreate(<H5Q_type_t>query_type, <H5Q_match_op_t>match_op,
                            dtid, PyArray_DATA(value))
            if qid < 0:
                set_exception()
                PyErr_Occurred()

    elif query_type == H5Q_TYPE_ATTR_NAME or query_type == H5Q_TYPE_LINK_NAME:
        obj_name = args[0]
        if not isinstance(obj_name, str):
            raise ValueError("Third argument must be string")
        name = obj_name

        with rlock:
            qid = H5Qcreate(<H5Q_type_t>query_type, <H5Q_match_op_t>match_op,
                            name)
            if qid < 0:
                set_exception()
                PyErr_Occurred()

    else:
        raise ValueError("%d: Unsupported query type" % query_type)

    return QueryID.open(qid)


def decode(buf):
    """(BYTES buf) => QueryID

    Deserialize a buffer containing a serialized query and return a new
    query handle. Python pickles can also be used instead of this method.
    """
    cdef char *buf_ = buf
    return QueryID.open(H5Qdecode(buf_))


cdef class QueryID(ObjectID):
    """ HDF5 query object identifier class """

    def _close(self):
        """()

        Close the query object.
        """
        with _objects.registry.lock:
            H5Qclose(self.id)
            if not self.valid:
                del _objects.registry[self.id]


    def combine(self, int combine_op, QueryID other not None):
        """(INT combine_op, QueryID other) => QueryID

        Create a new compound query by combining two query objects using the
        operator combine_op.
        """
        cdef hid_t qid
        qid = H5Qcombine(self.id, <H5Q_combine_op_t>combine_op, other.id)
        return QueryID.open(qid)


    def get_type(self):
        """() => INT type

        Get the query type of the atomic query object.
        """
        cdef H5Q_type_t query_type
        H5Qget_type(self.id, &query_type)
        return <int>query_type


    def get_match_op(self):
        """() => INT op

        Get the match operator of the atomic query object.
        """
        cdef H5Q_match_op_t match_op
        H5Qget_match_op(self.id, &match_op)
        return <int>match_op


    def get_components(self):
        """() => TUPLE (QueryID subquery1, QueryID subquery2)

        Get component queries from the compound query.
        """
        cdef hid_t subq1, subq2
        H5Qget_components(self.id, &subq1, &subq2)
        return QueryID.open(subq1), QueryID.open(subq2)


    def get_combine_op(self):
        """() => INT combine_op

        Get the combine operator type of the compound query object.
        """
        cdef H5Q_combine_op_t combine_op
        H5Qget_combine_op(self.id, &combine_op)
        return <int>combine_op


    def encode(self):
        """() => BYTES

        Serialize the query. The same can be done with the native Python
        pickling. The query is unaffected by this operation.
        """
        cdef void *buf = NULL
        cdef size_t nalloc = 0

        H5Qencode(self.id, NULL, &nalloc)
        buf = emalloc(nalloc)
        try:
            H5Qencode(self.id, buf, &nalloc)
            pystr = <char*>buf
        finally:
            efree(buf)

        return pystr


    def apply_ff(self, ObjectID loc not None, RCntxtID rc not None,
                 PropVCID vcpl=None, EventStackID es=None):
        """(ObjectID loc not None, RCntxtID rc not None, PropVCID vcpl=None, EventStackID es=None) => TUPLE(GroupID, INT)

        Create a new view from a query on an HDF5 object (group, dataset, map,
        or container). The returned group identifier is only valid in the
        native HDF5 environment as the returned view is similarly stored using
        the core VFD present in the native HDF5 VOL plugin.

        The presence of each type of results is indicated in the second tuple
        element.
        """
        cdef unsigned int result
        cdef hid_t oid

        oid = H5Qapply_ff(loc.id, self.id, &result, pdefault(vcpl), rc.id,
                          esid_default(es))
        return (h5i.wrap_identifier(oid), result)


    def apply_multi_ff(self, list locs not None, list rcs not None,
                       PropVCID vcpl=None, EventStackID es=None):
        """(LIST locs not None, LIST rcs not None, PropVCID vcpl=None, EventStackID es=None) => TUPLE(GroupID, INT)

        Create a new view from the query on multiple containers.

        Its resulting behavior is similar to the apply_ff() methid. Note that
        the query is for now only serially processed on the set of containers.

        The presence of each type of results is indicated in the second tuple
        element.
        """
        cdef unsigned int result
        cdef hid_t oid
        cdef hid_t* loc_ids = NULL
        cdef hid_t* rc_ids = NULL
        cdef int count, i

        count = len(locs)
        if count == 0:
            raise ValueError('Multi query requires at least one container')
        try:
            loc_ids = <hid_t*>emalloc(sizeof(hid_t)*count)
            rc_ids = <hid_t*>emalloc(sizeof(hid_t)*count)
            for i in range(count):
                loc_ids[i] = locs[i].id
                rc_ids[i] = rcs[i].id
            oid = H5Qapply_multi_ff(count, loc_ids, self.id, &result,
                                    pdefault(vcpl), rc_ids, esid_default(es))
        finally:
            efree(loc_ids)
            efree(rc_ids)

        return (h5i.wrap_identifier(oid), result)
