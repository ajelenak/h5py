# This file is part of h5py, a Python interface to the HDF5 library.
#
# http://www.h5py.org
#
# Copyright 2008-2013 Andrew Collette and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

"""
    Module for HDF5 "H5O" functions.
    With Exascale FastForward additions.
"""

include "config.pxi"

# Pyrex compile-time imports
from _objects cimport ObjectID, pdefault
from h5g cimport GroupID
from h5i cimport wrap_identifier
from h5p cimport PropID
from utils cimport emalloc, efree

from h5py import _objects

# For Exascale FastForward
from h5rc cimport RCntxtID
from h5tr cimport TransactionID
from h5es cimport esid_default, EventStackID

# === Public constants ========================================================

TYPE_GROUP = H5O_TYPE_GROUP
TYPE_DATASET = H5O_TYPE_DATASET
TYPE_NAMED_DATATYPE = H5O_TYPE_NAMED_DATATYPE
IF EFF:
    TYPE_MAP = H5O_TYPE_MAP

COPY_SHALLOW_HIERARCHY_FLAG = H5O_COPY_SHALLOW_HIERARCHY_FLAG
COPY_EXPAND_SOFT_LINK_FLAG  = H5O_COPY_EXPAND_SOFT_LINK_FLAG
COPY_EXPAND_EXT_LINK_FLAG   = H5O_COPY_EXPAND_EXT_LINK_FLAG
COPY_EXPAND_REFERENCE_FLAG  = H5O_COPY_EXPAND_REFERENCE_FLAG
COPY_WITHOUT_ATTR_FLAG      = H5O_COPY_WITHOUT_ATTR_FLAG
COPY_PRESERVE_NULL_FLAG     = H5O_COPY_PRESERVE_NULL_FLAG

# === Giant H5O_info_t structure ==============================================

cdef class _ObjInfoBase:

    cdef H5O_info_t *istr

cdef class _OHdrMesg(_ObjInfoBase):

    property present:
        def __get__(self):
            return self.istr[0].hdr.mesg.present
    property shared:
        def __get__(self):
            return self.istr[0].hdr.mesg.shared

    def _hash(self):
        return hash((self.present, self.shared))

cdef class _OHdrSpace(_ObjInfoBase):

    property total:
        def __get__(self):
            return self.istr[0].hdr.space.total
    property meta:
        def __get__(self):
            return self.istr[0].hdr.space.meta
    property mesg:
        def __get__(self):
            return self.istr[0].hdr.space.mesg
    property free:
        def __get__(self):
            return self.istr[0].hdr.space.free

    def _hash(self):
        return hash((self.total, self.meta, self.mesg, self.free))

cdef class _OHdr(_ObjInfoBase):

    cdef public _OHdrSpace space
    cdef public _OHdrMesg mesg

    property version:
        def __get__(self):
            return self.istr[0].hdr.version
    property nmesgs:
        def __get__(self):
            return self.istr[0].hdr.nmesgs

    def __init__(self):
        self.space = _OHdrSpace()
        self.mesg = _OHdrMesg()

    def _hash(self):
        return hash((self.version, self.nmesgs, self.space, self.mesg))

cdef class _ObjInfo(_ObjInfoBase):

    property fileno:
        def __get__(self):
            return self.istr[0].fileno
    property addr:
        def __get__(self):
            return self.istr[0].addr
    property type:
        def __get__(self):
            return <int>self.istr[0].type
    property rc:
        def __get__(self):
            return self.istr[0].rc

    def _hash(self):
        return hash((self.fileno, self.addr, self.type, self.rc))

cdef class ObjInfo(_ObjInfo):

    """
        Represents the H5O_info_t structure
    """

    cdef H5O_info_t infostruct
    cdef public _OHdr hdr

    def __init__(self):
        self.hdr = _OHdr()

        self.istr = &self.infostruct
        self.hdr.istr = &self.infostruct
        self.hdr.space.istr = &self.infostruct
        self.hdr.mesg.istr = &self.infostruct

    def __copy__(self):
        cdef ObjInfo newcopy
        newcopy = ObjInfo()
        newcopy.infostruct = self.infostruct
        return newcopy


def get_info(ObjectID loc not None, char* name=NULL, int index=-1, *,
        char* obj_name='.', int index_type=H5_INDEX_NAME, int order=H5_ITER_NATIVE,
        PropID lapl=None):
    """(ObjectID loc, STRING name=, INT index=, **kwds) => ObjInfo

    Get information describing an object in an HDF5 file.  Provide the object
    itself, or the containing group and exactly one of "name" or "index".

    STRING obj_name (".")
        When "index" is specified, look in this subgroup instead.
        Otherwise ignored.

    PropID lapl (None)
        Link access property list

    INT index_type (h5.INDEX_NAME)

    INT order (h5.ITER_NATIVE)
    """
    cdef ObjInfo info
    info = ObjInfo()

    if name != NULL and index >= 0:
        raise TypeError("At most one of name or index may be specified")
    elif name != NULL and index < 0:
        H5Oget_info_by_name(loc.id, name, &info.infostruct, pdefault(lapl))
    elif name == NULL and index >= 0:
        H5Oget_info_by_idx(loc.id, obj_name, <H5_index_t>index_type,
            <H5_iter_order_t>order, index, &info.infostruct, pdefault(lapl))
    else:
        H5Oget_info(loc.id, &info.infostruct)

    return info


# For Exascale FastForward
IF EFF:
    cdef class _ObjInfo_ff:

        cdef H5O_ff_info_t *istr

        property addr:
            def __get__(self):
                return self.istr[0].addr
        property type:
            def __get__(self):
                return <int>self.istr[0].type
        property rc:
            def __get__(self):
                return self.istr[0].rc
        property num_attrs:
            def __get__(self):
                return <int>self.istr[0].num_attrs

        def _hash(self):
            return hash((self.addr, self.type, self.rc, self.num_attrs))


    cdef class ObjInfo_ff(_ObjInfo_ff):

        """
            Represents the H5O_ff_info_t structure.
            For Exascale FastForward.
        """

        cdef H5O_ff_info_t infostruct

        def __init__(self):
            self.istr = &self.infostruct

        def __copy__(self):
            cdef ObjInfo_ff newcopy
            newcopy = ObjInfo_ff()
            newcopy.infostruct = self.infostruct
            return newcopy


    def get_info_ff(ObjectID loc not None, RCntxtID rc not None,
                    char* name=NULL, PropID lapl=None, EventStackID es=None):
        """(ObjectID loc, RCntxtID rc, STRING name=NULL, PropID lapl=None, EventStackID es=None) => ObjInfo_ff

        For Exascale FastForward.

        Get information describing an object in an HDF5 file, possibly
        asynchronously. Keywords:

        PropID lapl (None)
            Link access property list

        EventStackID es (None)
            Event stack identifier
        """
        cdef ObjInfo_ff info
        info = ObjInfo_ff()

        if name == NULL:
            H5Oget_info_ff(loc.id, &info.infostruct, rc.id, esid_default(es))
        else:
            H5Oget_info_by_name_ff(loc.id, name, &info.infostruct,
                                   pdefault(lapl), rc.id, esid_default(es))
        return info


    def get_info_by_name_ff(ObjectID loc not None, char* name,
                            RCntxtID rc not None, PropID lapl=None,
                            EventStackID es=None):
        """(ObjectID loc, STRING name, RCntxtID rc, PropID lapl=None, EventStackID es=None) => ObjInfo_ff

        Retrieve the metadata for an object specified by a location and a
        pathname, possibly asynchronously. For Exascale FastForward.
        """
        cdef ObjInfo_ff info
        info = ObjInfo_ff()

        H5Oget_info_by_name_ff(loc.id, name, &info.infostruct, pdefault(lapl),
                               rc.id, esid_default(es))
        return info

# === General object operations ===============================================


def open(ObjectID loc not None, char* name, PropID lapl=None):
    """(ObjectID loc, STRING name, PropID lapl=None) => ObjectID

    Open a group, dataset, or named datatype attached to an existing group.
    """
    return wrap_identifier(H5Oopen(loc.id, name, pdefault(lapl)))


IF EFF:
    def open_ff(ObjectID loc not None, char* name, RCntxtID rc not None, PropID lapl=None):
        """(ObjectID loc, STRING name, RCntxtID rc, PropID lapl=None) => ObjectID

        For Exascale FastForward. (TODO: Check h5i.wrap_identifier() if is still
        useable.)

        Open a group, dataset, or named datatype attached to an existing group.
        """
        cdef hid_t objid
        objid = H5Oopen_ff(loc.id, name, pdefault(lapl), rc.id)
        return wrap_identifier(objid)

    def _close_ff(ObjectID obj not None, EventStackID es=None):
        """(EventStackID es=None)

        For Exascale FastForward.

        Close an object in an HDF5 file, possibly asynchronously.
        """
        with _objects.registry.lock:
            H5Oclose_ff(obj.id, esid_default(es))
            if not obj.valid:
                del _objects.registry[obj.id]


def link(ObjectID obj not None, GroupID loc not None, char* name,
    PropID lcpl=None, PropID lapl=None):
    """(ObjectID obj, GroupID loc, STRING name, PropID lcpl=None,
    PropID lapl=None)

    Create a new hard link to an object.  Useful for objects created with
    h5g.create_anon() or h5d.create_anon().
    """
    H5Olink(obj.id, loc.id, name, pdefault(lcpl), pdefault(lapl))


IF EFF:
    def link_ff(ObjectID obj not None, GroupID loc not None, char* name,
                TransactionID tr not None, PropID lcpl=None, PropID lapl=None,
                EventStackID es=None):
        """(ObjectID obj, GroupID loc, STRING name, TransactionID tr, PropID lcpl=None,
        PropID lapl=None, EventStackID es=None)

        For Exascale FastForward.

        Create a new hard link to an object, possibly asynchronously.  Useful for
        objects created with h5g.create_anon() or h5d.create_anon().
        """
        H5Olink_ff(obj.id, loc.id, name, pdefault(lcpl), pdefault(lapl), tr.id,
                   esid_default(es))

    def exists_by_name_ff(GroupID loc not None, char* name, RCntxtID rc not None,
                          PropID lapl=None, EventStackID es=None):
        """(GroupID loc, STRING name, RCntxtID rc, PropID lapl=None,
        EventStackID es=None) => BOOL

        For Exascale FastForward.

        Determine whether a link resolves to an actual object, possibly
        asynchronously.
        """
        cdef hbool_t exists
        H5Oexists_by_name_ff(loc.id, name, &exists, pdefault(lapl), rc.id,
                             esid_default(es))
        return <bint>exists


def copy(ObjectID src_loc not None, char* src_name, GroupID dst_loc not None,
    char* dst_name, PropID copypl=None, PropID lcpl=None):
    """(ObjectID src_loc, STRING src_name, GroupID dst_loc, STRING dst_name,
    PropID copypl=None, PropID lcpl=None)

    Copy a group, dataset or named datatype from one location to another.  The
    source and destination need not be in the same file.

    The default behavior is a recursive copy of the object and all objects
    below it.  This behavior is modified via the "copypl" property list.
    """
    H5Ocopy(src_loc.id, src_name, dst_loc.id, dst_name, pdefault(copypl),
        pdefault(lcpl))


def set_comment(ObjectID loc not None, char* comment, *, char* obj_name=".",
    PropID lapl=None):
    """(ObjectID loc, STRING comment, **kwds)

    Set the comment for any-file resident object.  Keywords:

    STRING obj_name (".")
        Set comment on this group member instead

    PropID lapl (None)
        Link access property list
    """
    H5Oset_comment_by_name(loc.id, obj_name, comment, pdefault(lapl))


IF EFF:
    def set_comment_ff(ObjectID loc not None, char* comment, TransactionID tr,
                       *, char* obj_name=".", PropID lapl=None, EventStackID es=None):
        """(ObjectID loc, STRING comment, TransactionID tr, **kwds)

        For Exascale FastForward.

        Set the comment for any-file resident object, possibly asynchronously.
        Keywords:

        STRING obj_name (".")
            Set comment on this group member instead

        PropID lapl (None)
            Link access property list

        EventStackID es (None)
            Event stack identifier
        """
        H5Oset_comment_by_name_ff(loc.id, obj_name, comment, pdefault(lapl),
                                  tr.id, esid_default(es))


def get_comment(ObjectID loc not None, char* comment, *, char* obj_name=".",
    PropID lapl=None):
    """(ObjectID loc, STRING comment, **kwds)

    Get the comment for any-file resident object.  Keywords:

    STRING obj_name (".")
        Set comment on this group member instead

    PropID lapl (None)
        Link access property list
    """
    cdef ssize_t size
    cdef char* buf

    size = H5Oget_comment_by_name(loc.id, obj_name, NULL, 0, pdefault(lapl))
    buf = <char*>emalloc(size+1)
    try:
        H5Oget_comment_by_name(loc.id, obj_name, buf, size+1, pdefault(lapl))
        pstring = buf
    finally:
        efree(buf)

    return pstring


IF EFF:
    def get_comment_ff(ObjectID loc not None, char* comment, RCntxtID rc not None,
                       *, char* obj_name=".", PropID lapl=None, EventStackID es=None):
        """(ObjectID loc, STRING comment RCntxtID rc, **kwds)

        For Exascale FastForward.

        Get the comment for any-file resident object.  Keywords:

        STRING obj_name (".")
            Set comment on this group member instead

        PropID lapl (None)
            Link access property list

        EventStackID es (None)
            Event stack identifier
        """
        cdef ssize_t size
        cdef char* buf

        H5Oget_comment_by_name_ff(loc.id, obj_name, NULL, 0, &size, pdefault(lapl),
                                  rc.id, esid_default(es))
        buf = <char*>emalloc(size+1)
        try:
            H5Oget_comment_by_name_ff(loc.id, obj_name, buf, size+1, &size,
                                      pdefault(lapl), rc.id, esid_default(es))
            pstring = buf
        finally:
            efree(buf)

        return pstring


    def get_token(ObjectID obj not None):
        """(ObjectID obj) => bytes

        Retrieve the object token containing all the object metadata needed to
        open the object from any rank in the application, even in the same
        transaction that the object was created in.
        """
        cdef size_t token_size
        cdef uint8_t *token = NULL
        cdef bytes py_bytes

        try:
            # Get token buffer size...
            H5Oget_token(obj.id, NULL, &token_size)
            assert token_size > 0

            # Get token buffer...
            token = <uint8_t*>emalloc(sizeof(uint8_t)*token_size)
            H5Oget_token(obj.id, token, &token_size)

            # Slice token to token_size number of bytes to include any null
            # bytes...
            py_bytes = token[:token_size]

            return py_bytes

        finally:
            efree(token)


    def open_by_token(bytes py_token not None, TransactionID tr not None,
                      EventStackID es=None):
        """Open the existing HDF5 object described by the token buffer token.
        """
        cdef uint8_t *token = NULL
        token = py_token
        return wrap_identifier(H5Oopen_by_token(token, tr.id, esid_default(es)))


# === Visit routines ==========================================================

cdef class _ObjectVisitor:

    cdef object func
    cdef object retval
    cdef ObjInfo objinfo

    def __init__(self, func):
        self.func = func
        self.retval = None
        self.objinfo = ObjInfo()

cdef herr_t cb_obj_iterate(hid_t obj, char* name, H5O_info_t *info, void* data) except 2:

    cdef _ObjectVisitor visit

    # HDF5 doesn't respect callback return for ".", so skip it
    if strcmp(name, ".") == 0:
        return 0

    visit = <_ObjectVisitor>data
    visit.objinfo.infostruct = info[0]
    visit.retval = visit.func(name, visit.objinfo)

    if visit.retval is not None:
        return 1
    return 0

cdef herr_t cb_obj_simple(hid_t obj, char* name, H5O_info_t *info, void* data) except 2:

    cdef _ObjectVisitor visit

    # Not all versions of HDF5 respect callback value for ".", so skip it
    if strcmp(name, ".") == 0:
        return 0

    visit = <_ObjectVisitor>data
    visit.retval = visit.func(name)

    if visit.retval is not None:
        return 1
    return 0


def visit(ObjectID loc not None, object func, *,
          int idx_type=H5_INDEX_NAME, int order=H5_ITER_NATIVE,
          char* obj_name=".", PropID lapl=None, bint info=0):
    """(ObjectID loc, CALLABLE func, **kwds) => <Return value from func>

    Iterate a function or callable object over all objects below the
    specified one.  Your callable should conform to the signature::

        func(STRING name) => Result

    or if the keyword argument "info" is True::

        func(STRING name, ObjInfo info) => Result

    Returning None continues iteration; returning anything else aborts
    iteration and returns that value.  Keywords:

    BOOL info (False)
        Callback is func(STRING, Objinfo)

    STRING obj_name (".")
        Visit a subgroup of "loc" instead

    PropLAID lapl (None)
        Control how "obj_name" is interpreted

    INT idx_type (h5.INDEX_NAME)
        What indexing strategy to use

    INT order (h5.ITER_NATIVE)
        Order in which iteration occurs

    Compatibility note:  No callback is executed for the starting path ("."),
    as some versions of HDF5 don't correctly handle a return value for this
    case.  This differs from the behavior of the native H5Ovisit, which
    provides a literal "." as the first value.
    """
    cdef _ObjectVisitor visit = _ObjectVisitor(func)
    cdef H5O_iterate_t cfunc

    if info:
        cfunc = cb_obj_iterate
    else:
        cfunc = cb_obj_simple

    H5Ovisit_by_name(loc.id, obj_name, <H5_index_t>idx_type,
        <H5_iter_order_t>order, cfunc, <void*>visit, pdefault(lapl))

    return visit.retval
