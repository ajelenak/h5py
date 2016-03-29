# This file is part of h5py, a Python interface to the HDF5 library.
#
# http://www.h5py.org
#
# Copyright 2008-2013 Andrew Collette and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

"""
    H5R API for object and region references.
"""

include "config.pxi"

# cdef extern from "hdf5.h":
#     herr_t H5Rcreate(void *ref, H5R_type_t ref_type, ...) except *

from cpython.object cimport Py_EQ
from _objects cimport ObjectID, pdefault
from h5p cimport PropID
from utils cimport emalloc
from h5rc cimport RCntxtID
from h5es cimport EventStackID, esid_default

# === Public constants and data structures ====================================

OBJECT = H5R_OBJECT                   # Object reference
DATASET_REGION = H5R_DATASET_REGION   # Dummy for backward compatibility
REGION = H5R_REGION                   # Region reference
ATTR = H5R_ATTR                       # Attribute reference
EXT_OBJECT = H5R_EXT_OBJECT           # External Object reference
EXT_REGION = H5R_EXT_REGION           # External Region Reference
EXT_ATTR = H5R_EXT_ATTR               # External Attribute reference

# === Reference API ===========================================================


# def create(int ref_type, *args):
#     """(INT ref_type, *args) => ReferenceObject ref
#     Create a new reference. The value of ref_type detemines the kind
#     of reference created:
#     OBJECT
#         Reference to an object in an HDF5 file.  Parameters "loc"
#         and "name" identify the object.
#     DATASET_REGION
#         Reference to a dataset region.  Parameters "loc" and
#         "name" identify the dataset; the selection on "space"
#         identifies the region.
#     """
#     cdef hid_t loc_id, space_id
#     cdef char* name
#     cdef Reference ref

#     loc = args[0]
#     if not isinstance(loc, ObjectID):
#         raise ValueError("Second argument must be ObjectID")
#     loc_id = loc.id

#     obj_name = args[1]
#     if not isinstance(obj_name, str):
#         raise ValueError("Third argument must be string")
#     name = obj_name

#     if ref_type == H5R_OBJECT:
#         ref = Reference()
#         H5Rcreate(&ref.ref, <H5R_type_t>ref_type, loc_id, name)
#     elif ref_type == H5R_DATASET_REGION:
#         space = args[2]
#         if not isinstance(space, ObjectID): # work around segfault in HDF5
#             raise ValueError("Dataspace required for region reference")
#         space_id = space.id
#         ref = DsetRegionReference()
#         H5Rcreate(&ref.ref, <H5R_type_t>ref_type, loc_id, name, space_id)
#     elif ref_type == H5R_REGION:
#         ref = RegionReference()
#     elif ref_type == H5R_ATTR:
#         ref = AttributeReference()
#     else:
#         raise ValueError("Unknown reference typecode")

#     return ref

# def dereference(Reference ref not None, ObjectID id not None, PropID oapl=None):
#     """(Reference ref, ObjectID id, PropID oapl=None) => ObjectID or None
#     Open the object pointed to by the reference and return its
#     identifier.  The file identifier (or the identifier for any object
#     in the file) must also be provided.  Returns None if the reference
#     is zero-filled.
#     The reference may be either Reference or DsetRegionReference.
#     """
#     import h5i
#     cdef hid_t objid
#     if not ref:
#         return None
#     objid = H5Rdereference2(id.id, pdefault(oapl), <H5R_type_t>ref.typecode,
#                             &ref.ref)
#     return h5i.wrap_identifier(objid)

def get_region(Reference ref not None, ObjectID id not None):
    """(Reference ref, ObjectID id) => SpaceID or None

    Retrieve the dataspace selection pointed to by the reference. Returns a
    copy of the dataset's dataspace, with the appropriate elements selected.
    The file identifier or the identifier of any object in the file (including
    the dataset itself) must also be provided.

    The reference object must be a [Ext]RegionReference.  If it is zero-filled,
    returns None.
    """
    import h5s
    if ref.typecode not in (H5R_DATASET_REGION, H5R_REGION, H5R_EXT_REGION) or\
            not ref:
        return None
    return h5s.SpaceID(H5Rget_region2(id.id, ref.ref))

# def get_obj_type(Reference ref not None, ObjectID id not None):
#     """(Reference ref, ObjectID id) => INT obj_code or None
#     Determine what type of object the reference points to.  The
#     reference may be a Reference or DsetRegionReference.  The file
#     identifier or the identifier of any object in the file must also
#     be provided.
#     The return value is one of:
#     - h5o.H5O_TYPE_GROUP
#     - h5o.H5O_TYPE_DATASET
#     If the reference is zero-filled, returns None.
#     """
#     cdef H5O_type_t type_
#     if not ref:
#         return None
#     H5Rget_obj_type2(id.id, <H5R_type_t>ref.typecode, &ref.ref, &type_)
#     return <int>type_

def get_type(Reference ref not None):
    """(Reference ref) => H5R_type_t code

    Get reference's type as H5R_type_t value.
    """
    return <H5R_type_t>H5Rget_type(ref)


def get_name(Reference ref not None, ObjectID loc not None):
    """(Reference ref, ObjectID loc) => STRING name or None

    Determine the name of the object pointed to by this reference. Reference
    types supported: Reference, AttributeReference, ExtReference,
    ExtAttributeReference, ExtRegionReference.
    """
    cdef ssize_t nchar = 0
    cdef char* namebuf = NULL

    if ref.typecode in (H5R_OBJECT, H5R_EXT_OBJECT, H5R_REGION,
                        H5R_EXT_REGION):
        nchar = H5Rget_obj_name(loc.id, ref.ref, NULL, <size_t>0)
    elif ref.typecode in (H5R_ATTR, H5R_EXT_ATTR):
        nchar = H5Rget_attr_name(loc.id, ref.ref, NULL, <size_t>0)
    else:
        raise TypeError('Not object or attribute reference')

    if nchar > 0:
        namebuf = <char*>emalloc(nchar+1)
        try:
            if ref.typecode in (H5R_OBJECT, H5R_EXT_OBJECT, H5R_REGION,
                                H5R_EXT_REGION):
                nchar = H5Rget_obj_name(loc.id, ref.ref, namebuf,
                                        <size_t>nchar)
            elif ref.typecode in (H5R_ATTR, H5R_EXT_ATTR):
                nchar = H5Rget_attr_name(loc.id, ref.ref, namebuf,
                                         <size_t>nchar)
            return namebuf
        finally:
            free(namebuf)
    else:
        return None


def get_object(Reference ref not None, ObjectID loc not None, PropID oapl=None):
    """(Reference ref, ObjectID loc, PropID oapl=None) => ObjectID or None

    Get object referenced to by ref.
    """
    import h5i
    cdef hid_t objid

    if not ref:
        return None
    objid = H5Rget_object(loc.id, pdefault(oapl), ref.ref)
    return h5i.wrap_identifier(objid)


IF EFF:

    def get_object_ff(Reference ref not None, ObjectID loc not None,
                      RCntxtID rc not None, PropID oapl=None,
                      EventStackID es=None):
        """
        (Reference ref, ObjectID loc, RCntxtID rc, PropID oapl=None, EventStackID es=None) => ObjectID or None

        Get object referenced to by ref.

        For Exascale Fast Forward.
        """
        import h5i
        cdef hid_t objid

        if not ref:
            return None
        objid = H5Rget_object_ff(loc.id, pdefault(oapl), ref.ref, rc.id,
                                 esid_default(es))
        return h5i.wrap_identifier(objid)


cdef class Reference:

    """
        Opaque representation of an HDF5 reference.
        Objects of this class are created exclusively by the library and
        cannot be modified.  The read-only attribute "typecode" determines
        whether the reference is to an object in an HDF5 file (OBJECT)
        or a dataset region (DATASET_REGION).
        The object's truth value indicates whether it contains a nonzero
        reference.  This does not guarantee that is valid, but is useful
        for rejecting "background" elements in a dataset.
    """

    def __cinit__(self, *args, **kwds):
        self.typecode = H5R_OBJECT
        self.typesize = sizeof(href_t)

    def __nonzero__(self):
        cdef int i
        for i from 0<=i<self.typesize:
            if (<unsigned char*>&self.ref)[i] != 0: return True
        return False

    def __repr__(self):
        return "<HDF5 object reference%s>" % ("" if self else " (null)")

    def __richcmp__(self, Reference other not None, int op):
        """Compare two Reference objects.

        Only equality operation is supported. Others will raise NotImplemented
        exception.
        """
        if op != Py_EQ:
            raise NotImplemented('Comparison operation not supported')
        else:
            return <bint>H5Requal(self.ref, other.ref)


cdef class RegionReference(Reference):

    """
        Opaque representation of an HDF5 region reference.
        This is a subclass of Reference which exists mainly for programming
        convenience.
    """

    def __cinit__(self, *args, **kwds):
        self.typecode = H5R_REGION
        self.typesize = sizeof(href_t)

    def __repr__(self):
        return "<HDF5 region reference%s>" % ("" if self else " (null)")


cdef class AttributeReference(Reference):

    """
        Opaque representation of an HDF5 attribute reference.
        This is a subclass of Reference which exists mainly for programming
        convenience.
    """

    def __cinit__(self, *args, **kwds):
        self.typecode = H5R_ATTR
        self.typesize = sizeof(href_t)

    def __repr__(self):
        return "<HDF5 attribute reference%s>" % ("" if self else " (null)")


cdef class ExtReference(Reference):

    """
        Opaque representation of an HDF5 external reference.
        This is a subclass of Reference which exists mainly for programming
        convenience.
    """

    def __cinit__(self, *args, **kwds):
        self.typecode = H5R_EXT_OBJECT
        self.typesize = sizeof(href_t)

    def __repr__(self):
        return "<HDF5 external reference%s>" % ("" if self else " (null)")

    def get_file_name(self):
        """() => STRING name

        Get the name of the file where this external object reference is
        located.
        """
        cdef ssize_t nchar = 0
        cdef char* namebuf = NULL

        nchar = H5Rget_file_name(self.ref, NULL, <size_t>0)
        if nchar > 0:
            namebuf = <char*>emalloc(nchar+1)
            try:
                nchar = H5Rget_file_name(self.ref, namebuf, <size_t>nchar)
                return namebuf
            finally:
                free(namebuf)


cdef class ExtRegionReference(Reference):

    """
        Opaque representation of an HDF5 external region reference.
        This is a subclass of Reference which exists mainly for programming
        convenience.
    """

    def __cinit__(self, *args, **kwds):
        self.typecode = H5R_EXT_REGION
        self.typesize = sizeof(href_t)

    def __repr__(self):
        return "<HDF5 external region reference%s>" % ("" if self else " (null)")

    def get_file_name(self):
        """() => STRING name

        Get the name of the file where this external object reference is
        located.
        """
        cdef ssize_t nchar = 0
        cdef char* namebuf = NULL

        nchar = H5Rget_file_name(self.ref, NULL, <size_t>0)
        if nchar > 0:
            namebuf = <char*>emalloc(nchar+1)
            try:
                nchar = H5Rget_file_name(self.ref, namebuf, <size_t>nchar)
                return namebuf
            finally:
                free(namebuf)


cdef class ExtAttributeReference(Reference):

    """
        Opaque representation of an HDF5 external attribute reference.
        This is a subclass of Reference which exists mainly for programming
        convenience.
    """

    def __cinit__(self, *args, **kwds):
        self.typecode = H5R_EXT_ATTR
        self.typesize = sizeof(href_t)

    def __repr__(self):
        return "<HDF5 external attribute reference%s>" % ("" if self else " (null)")

    def get_file_name(self):
        """() => STRING name

        Get the name of the file where this external attribute reference is
        located.
        """
        cdef ssize_t nchar = 0
        cdef char* namebuf = NULL

        nchar = H5Rget_file_name(self.ref, NULL, <size_t>0)
        if nchar > 0:
            namebuf = <char*>emalloc(nchar+1)
            try:
                nchar = H5Rget_file_name(self.ref, namebuf, <size_t>nchar)
                return namebuf
            finally:
                free(namebuf)
