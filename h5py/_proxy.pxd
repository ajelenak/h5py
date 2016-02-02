# This file is part of h5py, a Python interface to the HDF5 library.
#
# http://www.h5py.org
#
# Copyright 2008-2013 Andrew Collette and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

from defs cimport *

cdef herr_t attr_rw(hid_t attr, hid_t mtype, void *progbuf, int read) except -1

cdef herr_t dset_rw(hid_t dset, hid_t mtype, hid_t mspace, hid_t fspace,
                    hid_t dxpl, void* progbuf, int read) except -1

cdef herr_t dset_rw_ff(hid_t dset, hid_t mtype, hid_t mspace, hid_t fspace,
                       hid_t dxpl, void* progbuf, int read, hid_t objid,
                       hid_t esid) except -1

cdef herr_t attr_rw_ff(hid_t attr, hid_t mtype, void *progbuf, int read,
                       hid_t objid, hid_t esid) except -1

cdef herr_t map_del_ff(hid_t map, hid_t mtype, void *progbuf, hid_t trid,
                       hid_t esid) except -1

cdef hbool_t map_check_ff(hid_t map, hid_t mtype, void *progbuf, hid_t rcid,
                          hid_t esid) except -1

cdef herr_t map_gs_ff(hid_t map, hid_t key_mtype, void* key_buf,
                      hid_t val_mtype, void* val_buf, hid_t dxpl,
                      hid_t objid, hid_t esid, int get) except -1
