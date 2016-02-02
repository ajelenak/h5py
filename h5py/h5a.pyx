# This file is part of h5py, a Python interface to the HDF5 library.
#
# http://www.h5py.org
#
# Copyright 2008-2013 Andrew Collette and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

"""
    Provides access to the low-level HDF5 "H5A" attribute interface.
    With additions for the Exascale FastForward project.
"""

# Compile-time imports
from _objects cimport pdefault
from h5t cimport TypeID, typewrap, py_create
from h5s cimport SpaceID
from h5p cimport PropID
from numpy cimport import_array, ndarray, PyArray_DATA
from utils cimport check_numpy_read, check_numpy_write, emalloc, efree
from _proxy cimport attr_rw, attr_rw_ff

from h5py import _objects

# For Exascale FastForward
from h5es cimport esid_default, EventStackID
from h5tr cimport TransactionID
from h5rc cimport RCntxtID

# Initialization
import_array()

# === General attribute operations ============================================

# --- create, create_by_name ---

def create(ObjectID loc not None, char* name, TypeID tid not None,
    SpaceID space not None, *, char* obj_name='.', PropID lapl=None):
    """(ObjectID loc, STRING name, TypeID tid, SpaceID space, **kwds) => AttrID

    Create a new attribute, attached to an existing object.

    STRING obj_name (".")
        Attach attribute to this group member instead

    PropID lapl
        Link access property list for obj_name
    """

    return AttrID.open(H5Acreate_by_name(loc.id, obj_name, name, tid.id,
            space.id, H5P_DEFAULT, H5P_DEFAULT, pdefault(lapl)))


# --- create_ff, create_by_name_ff ---

def create_ff(ObjectID loc not None, char* name, TypeID tid not None,
              SpaceID space not None, TransactionID tr not None, EventStackID es=None,
              *, char* obj_name='.', PropID lapl=None):
    """(ObjectID loc, STRING name, TypeID tid, SpaceID space, TransactionID tr, EventStackID es=None, **kwds) => AttrID

    For Exascale FastForward.

    Create a new attribute, possibly asynchronously, attached to an existing
    object. Keywords:

    STRING obj_name (".")
        Attach attribute to this group member instead

    PropID lapl
        Link access property list for obj_name
    """

    return AttrID.open(H5Acreate_by_name_ff(loc.id, obj_name, name, tid.id,
                                            space.id, H5P_DEFAULT, H5P_DEFAULT,
                                            H5P_DEFAULT, tr.id, esid_default(es)))

# --- open, open_by_name, open_by_idx ---

def open(ObjectID loc not None, char* name=NULL, int index=-1, *,
    char* obj_name='.', int index_type=H5_INDEX_NAME, int order=H5_ITER_NATIVE,
    PropID lapl=None):
    """(ObjectID loc, STRING name=, INT index=, **kwds) => AttrID

    Open an attribute attached to an existing object.  You must specify
    exactly one of either name or idx.  Keywords are:

    STRING obj_name (".")
        Attribute is attached to this group member

    PropID lapl (None)
        Link access property list for obj_name

    INT index_type (h5.INDEX_NAME)

    INT order (h5.ITER_NATIVE)

    """
    if (name == NULL and index < 0) or (name != NULL and index >= 0):
        raise TypeError("Exactly one of name or idx must be specified")

    if name != NULL:
        return AttrID.open(H5Aopen_by_name(loc.id, obj_name, name,
                        H5P_DEFAULT, pdefault(lapl)))
    else:
        return AttrID.open(H5Aopen_by_idx(loc.id, obj_name,
            <H5_index_t>index_type, <H5_iter_order_t>order, index,
            H5P_DEFAULT, pdefault(lapl)))


# --- open_ff, open_by_name_ff, open_by_idx ---

def open_ff(ObjectID loc not None, RCntxtID rc not None, char* name=NULL, int index=-1,
            *, char* obj_name='.', int index_type=H5_INDEX_NAME, int order=H5_ITER_NATIVE,
            PropID lapl=None, EventStackID es=None):
    """(ObjectID loc, RCntxtID rc, STRING name=, INT index=, **kwds) => AttrID

    For Exascale FastForward. H5Aopen_by_name_ff() is used here only.

    Open an attribute attached to an existing objecti, possibly
    asynchronously. You must specify exactly one of either name or idx.
    Keywords are:

    STRING obj_name (".")
        Attribute is attached to this group member

    PropID lapl (None)
        Link access property list for obj_name

    EventStackID es (None)
        Event stack identifier object

    INT index_type (h5.INDEX_NAME)

    INT order (h5.ITER_NATIVE)

    """
    if (name == NULL and index < 0) or (name != NULL and index >= 0):
        raise TypeError("Exactly one of name or idx must be specified")

    if name != NULL:
        return AttrID.open(H5Aopen_by_name_ff(loc.id, obj_name, name,
                                              H5P_DEFAULT, pdefault(lapl),
                                              rc.id, esid_default(es)))
    else:
        # The H5A function below is not part of the Exascale FastForward
        # project so am nout sure if it will work but for this part stays.
        return AttrID.open(H5Aopen_by_idx(loc.id, obj_name,
            <H5_index_t>index_type, <H5_iter_order_t>order, index,
            H5P_DEFAULT, pdefault(lapl)))


# --- exists, exists_by_name ---

def exists(ObjectID loc not None, char* name, *,
            char* obj_name=".", PropID lapl=None):
    """(ObjectID loc, STRING name, **kwds) => BOOL

    Determine if an attribute is attached to this object.  Keywords:

    STRING obj_name (".")
        Look for attributes attached to this group member

    PropID lapl (None):
        Link access property list for obj_name
    """
    return <bint>H5Aexists_by_name(loc.id, obj_name, name, pdefault(lapl))


# --- exists_ff, exists_by_name_ff ---

def exists_ff(ObjectID loc not None, char* name, RCntxtID rc not None, *,
              char* obj_name=".", PropID lapl=None, EventStackID es=None):
    """(ObjectID loc, STRING name, RCntxtID rc, **kwds) => BOOL

    For Exascale FastForward.

    Determine if an attribute is attached to this object, possibly
    asynchronously. Keywords:

    STRING obj_name (".")
        Look for attributes attached to this group member

    PropID lapl (None):
        Link access property list for obj_name

    EventStackID es (None)
        Event stack identifier
    """
    cdef hbool_t exists
    H5Aexists_ff(loc.id, name, &exists, rc.id, esid_default(es))
    # H5Aexists_by_name_ff(loc.id, obj_name, name, pdefault(lapl), &exists, rc.id,
    #                      esid_default(es))
    return <bint>exists


# --- rename, rename_by_name ---

def rename(ObjectID loc not None, char* name, char* new_name, *,
    char* obj_name='.', PropID lapl=None):
    """(ObjectID loc, STRING name, STRING new_name, **kwds)

    Rename an attribute.  Keywords:

    STRING obj_name (".")
        Attribute is attached to this group member

    PropID lapl (None)
        Link access property list for obj_name
    """
    H5Arename_by_name(loc.id, obj_name, name, new_name, pdefault(lapl))


# --- rename_ff, rename_by_name_ff ---

def rename_ff(ObjectID loc not None, char* name, char* new_name, TransactionID tr not None,
              *, char* obj_name='.', PropID lapl=None, EventStackID es=None):
    """(ObjectID loc, STRING name, STRING new_name, TransactionID tr, **kwds)

    For Exascale FastForward.

    Rename an attribute, possibly asynchronously. Keywords:

    STRING obj_name (".")
        Attribute is attached to this group member

    PropID lapl (None)
        Link access property list for obj_name

    EventStackID es (None)
        Event stack identifier.
    """
    H5Arename_by_name_ff(loc.id, obj_name, name, new_name, pdefault(lapl),
                         tr.id, esid_default(es))


def delete(ObjectID loc not None, char* name=NULL, int index=-1, *,
    char* obj_name='.', int index_type=H5_INDEX_NAME, int order=H5_ITER_NATIVE,
    PropID lapl=None):
    """(ObjectID loc, STRING name=, INT index=, **kwds)

    Remove an attribute from an object.  Specify exactly one of "name"
    or "index". Keyword-only arguments:

    STRING obj_name (".")
        Attribute is attached to this group member

    PropID lapl (None)
        Link access property list for obj_name

    INT index_type (h5.INDEX_NAME)

    INT order (h5.ITER_NATIVE)
    """
    if name != NULL and index < 0:
        H5Adelete_by_name(loc.id, obj_name, name, pdefault(lapl))
    elif name == NULL and index >= 0:
        H5Adelete_by_idx(loc.id, obj_name, <H5_index_t>index_type,
            <H5_iter_order_t>order, index, pdefault(lapl))
    else:
        raise TypeError("Exactly one of index or name must be specified.")


# For Exascale FastForward
def delete_ff(ObjectID loc not None, TransactionID tr not None, char* name=NULL,
              int index=-1, *, char* obj_name='.', int index_type=H5_INDEX_NAME,
              int order=H5_ITER_NATIVE, PropID lapl=None, EventStackID es=None):
    """(ObjectID loc, TransactionID tr, STRING name=, INT index=, **kwds)

    For Exascale FastForward.

    Remove an attribute from an object, possibly asynchronously. Specify
    exactly one of "name" or "index". Keyword-only arguments:

    STRING obj_name (".")
        Attribute is attached to this group member

    PropID lapl (None)
        Link access property list for obj_name

    EventStackID es (None)
        Event stack identifier.

    INT index_type (h5.INDEX_NAME)

    INT order (h5.ITER_NATIVE)
    """
    if name != NULL and index < 0:
        H5Adelete_by_name_ff(loc.id, obj_name, name, pdefault(lapl), tr.id, esid_default(es))
    elif name == NULL and index >= 0:
        # The function below has no FastForward version so am not sure how it
        # will work but let's keep it for now.
        H5Adelete_by_idx(loc.id, obj_name, <H5_index_t>index_type,
            <H5_iter_order_t>order, index, pdefault(lapl))
    else:
        raise TypeError("Exactly one of index or name must be specified.")


def get_num_attrs(ObjectID loc not None):
    """(ObjectID loc) => INT

    Determine the number of attributes attached to an HDF5 object.
    """
    return H5Aget_num_attrs(loc.id)


cdef class AttrInfo:

    cdef H5A_info_t info

    property corder_valid:
        """Indicates if the creation order is valid"""
        def __get__(self):
            return <bint>self.info.corder_valid
    property corder:
        """Creation order"""
        def __get__(self):
            return <int>self.info.corder
    property cset:
        """Character set of attribute name (integer typecode from h5t)"""
        def __get__(self):
            return <int>self.info.cset
    property data_size:
        """Size of raw data"""
        def __get__(self):
            return self.info.data_size

    def _hash(self):
        return hash((self.corder_valid, self.corder, self.cset, self.data_size))


def get_info(ObjectID loc not None, char* name=NULL, int index=-1, *,
            char* obj_name='.', PropID lapl=None,
            int index_type=H5_INDEX_NAME, int order=H5_ITER_NATIVE):
    """(ObjectID loc, STRING name=, INT index=, **kwds) => AttrInfo

    Get information about an attribute, in one of two ways:

    1. If you have the attribute identifier, just pass it in
    2. If you have the parent object, supply it and exactly one of
       either name or index.

    STRING obj_name (".")
        Use this group member instead

    PropID lapl (None)
        Link access property list for obj_name

    INT index_type (h5.INDEX_NAME)
        Which index to use

    INT order (h5.ITER_NATIVE)
        What order the index is in
    """
    cdef AttrInfo info = AttrInfo()

    if name == NULL and index < 0:
        H5Aget_info(loc.id, &info.info)
    elif name != NULL and index >= 0:
        raise TypeError("At most one of name and index may be specified")
    elif name != NULL:
        H5Aget_info_by_name(loc.id, obj_name, name, &info.info, pdefault(lapl))
    elif index >= 0:
        H5Aget_info_by_idx(loc.id, obj_name, <H5_index_t>index_type,
            <H5_iter_order_t>order, index, &info.info, pdefault(lapl))

    return info

# === Iteration routines ======================================================

cdef class _AttrVisitor:
    cdef object func
    cdef object retval
    def __init__(self, func):
        self.func = func
        self.retval = None

cdef herr_t cb_attr_iter(hid_t loc_id, char* attr_name, H5A_info_t *ainfo, void* vis_in) except 2:
    cdef _AttrVisitor vis = <_AttrVisitor>vis_in
    cdef AttrInfo info = AttrInfo()
    info.info = ainfo[0]
    vis.retval = vis.func(attr_name, info)
    if vis.retval is not None:
        return 1
    return 0

cdef herr_t cb_attr_simple(hid_t loc_id, char* attr_name, H5A_info_t *ainfo, void* vis_in) except 2:
    cdef _AttrVisitor vis = <_AttrVisitor>vis_in
    vis.retval = vis.func(attr_name)
    if vis.retval is not None:
        return 1
    return 0


def iterate(ObjectID loc not None, object func, int index=0, *,
    int index_type=H5_INDEX_NAME, int order=H5_ITER_NATIVE, bint info=0):
    """(ObjectID loc, CALLABLE func, INT index=0, **kwds) => <Return value from func>

    Iterate a callable (function, method or callable object) over the
    attributes attached to this object.  You callable should have the
    signature::

        func(STRING name) => Result

    or if the keyword argument "info" is True::

        func(STRING name, AttrInfo info) => Result

    Returning None continues iteration; returning anything else aborts
    iteration and returns that value.  Keywords:

    BOOL info (False)
        Callback is func(STRING name, AttrInfo info), not func(STRING name)

    INT index_type (h5.INDEX_NAME)
        Which index to use

    INT order (h5.ITER_NATIVE)
        Index order to use
    """
    if index < 0:
        raise ValueError("Starting index must be a non-negative integer.")

    cdef hsize_t i = index
    cdef _AttrVisitor vis = _AttrVisitor(func)
    cdef H5A_operator2_t cfunc

    if info:
        cfunc = cb_attr_iter
    else:
        cfunc = cb_attr_simple

    H5Aiterate2(loc.id, <H5_index_t>index_type, <H5_iter_order_t>order,
        &i, cfunc, <void*>vis)

    return vis.retval



# === Attribute class & methods ===============================================

cdef class AttrID(ObjectID):

    """
        Logical representation of an HDF5 attribute identifier.

        Objects of this class can be used in any HDF5 function call
        which expects an attribute identifier.  Additionally, all ``H5A*``
        functions which always take an attribute instance as the first
        argument are presented as methods of this class.

        * Hashable: No
        * Equality: Identifier comparison
    """

    property name:
        """The attribute's name"""
        def __get__(self):
            return self.get_name()

    property shape:
        """A Numpy-style shape tuple representing the attribute's dataspace"""
        def __get__(self):

            cdef SpaceID space
            space = self.get_space()
            return space.get_simple_extent_dims()

    property dtype:
        """A Numpy-stype dtype object representing the attribute's datatype"""
        def __get__(self):

            cdef TypeID tid
            tid = self.get_type()
            return tid.py_dtype()


    def _close(self):
        """()

        Close this attribute and release resources.  You don't need to
        call this manually; attributes are automatically destroyed when
        their Python wrappers are freed.
        """
        with _objects.registry.lock:
            H5Aclose(self.id)
            if not self.valid:
                del _objects.registry[self.id]


    def _close_ff(self, EventStackID es=None):
        """(EventStackID es=None)

        For Exactly FastForward.

        Close this attribute and release resources, possibly asynchronously.
        You don't need to call this manually; attributes are automatically
        destroyed when their Python wrappers are freed.
        """
        with _objects.registry.lock:
            H5Aclose_ff(self.id, esid_default(es))
            if not self.valid:
                del _objects.registry[self.id]


    def read(self, ndarray arr not None):
        """(NDARRAY arr)

        Read the attribute data into the given Numpy array.  Note that the
        Numpy array must have the same shape as the HDF5 attribute, and a
        conversion-compatible datatype.

        The Numpy array must be writable and C-contiguous.  If this is not
        the case, the read will fail with an exception.
        """
        cdef TypeID mtype
        cdef hid_t space_id
        space_id = 0

        try:
            space_id = H5Aget_space(self.id)
            check_numpy_write(arr, space_id)

            mtype = py_create(arr.dtype)

            attr_rw(self.id, mtype.id, PyArray_DATA(arr), 1)

        finally:
            if space_id:
                H5Sclose(space_id)


    def read_ff(self, ndarray arr not None, RCntxtID rc not None,
                EventStackID es=None):
        """(NDARRAY arr, RCntxtID rc, EventStackID es=None)

        For Exascale FastForward.

        Read the attribute data into the given Numpy array, possibly
        asynchronously. Note that the Numpy array must have the same shape as
        the HDF5 attribute, and a conversion-compatible datatype.

        The Numpy array must be writable and C-contiguous.  If this is not
        the case, the read will fail with an exception.
        """
        cdef TypeID mtype
        cdef hid_t space_id
        space_id = 0

        try:
            space_id = H5Aget_space(self.id)
            check_numpy_write(arr, space_id)

            mtype = py_create(arr.dtype)

            attr_rw_ff(self.id, mtype.id, PyArray_DATA(arr), 1, rc.id, esid_default(es))

        finally:
            if space_id:
                H5Sclose(space_id)


    def write(self, ndarray arr not None):
        """(NDARRAY arr)

        Write the contents of a Numpy array too the attribute.  Note that
        the Numpy array must have the same shape as the HDF5 attribute, and
        a conversion-compatible datatype.

        The Numpy array must be C-contiguous.  If this is not the case,
        the write will fail with an exception.
        """
        cdef TypeID mtype
        cdef hid_t space_id
        space_id = 0

        try:
            space_id = H5Aget_space(self.id)
            check_numpy_read(arr, space_id)
            mtype = py_create(arr.dtype)

            attr_rw(self.id, mtype.id, PyArray_DATA(arr), 0)

        finally:
            if space_id:
                H5Sclose(space_id)


    # For Exascale FastForward
    def write_ff(self, ndarray arr not None, TransactionID tr not None,
                 EventStackID es=None):
        """(NDARRAY arr, TransactionID tr, EventStackID es=None)

        For Exascale FastForward.

        Write the contents of a Numpy array too the attribute, possibly
        asynchronously. Note that the Numpy array must have the same shape as
        the HDF5 attribute, and a conversion-compatible datatype.

        The Numpy array must be C-contiguous.  If this is not the case, the
        write will fail with an exception.
        """
        cdef TypeID mtype
        cdef hid_t space_id
        space_id = 0

        try:
            space_id = H5Aget_space(self.id)
            check_numpy_read(arr, space_id)
            mtype = py_create(arr.dtype)

            attr_rw_ff(self.id, mtype.id, PyArray_DATA(arr), 0, tr.id, esid_default(es))

        finally:
            if space_id:
                H5Sclose(space_id)


    def get_name(self):
        """() => STRING name

        Determine the name of this attribute.
        """
        cdef int blen
        cdef char* buf
        buf = NULL

        try:
            blen = H5Aget_name(self.id, 0, NULL)
            assert blen >= 0
            buf = <char*>emalloc(sizeof(char)*blen+1)
            blen = H5Aget_name(self.id, blen+1, buf)
            strout = <bytes>buf
        finally:
            efree(buf)

        return strout


    def get_space(self):
        """() => SpaceID

        Create and return a copy of the attribute's dataspace.
        """
        return SpaceID.open(H5Aget_space(self.id))


    def get_type(self):
        """() => TypeID

        Create and return a copy of the attribute's datatype.
        """
        return typewrap(H5Aget_type(self.id))


    def get_storage_size(self):
        """() => INT

        Get the amount of storage required for this attribute.
        """
        return H5Aget_storage_size(self.id)


    def evict_ff(self, ctn_ver, PropID dxpl=None, EventStackID es=None):
        """UINT ctn_ver, PropID dxpl=None, EventStackID es=None)

        Evict attribute from the burst buffer, possibly asynchronously.

        For Exascale FastForward.
        """
        H5Aevict_ff(self.id, <uint64_t>ctn_ver, pdefault(dxpl),
                    esid_default(es))


    def prefetch_ff(self, RCntxtID rc not None, hrpl_t replica_id,
                    PropID dxpl=None, EventStackID es=None):
        """(RCntxtID rc, UINT replica_id, PropID dxpl=None, EventStackID es=None)

        For Exascale FastForward.
        """
        H5Aprefetch_ff(self.id, rc.id, &replica_id, pdefault(dxpl),
                       esid_default(es))
