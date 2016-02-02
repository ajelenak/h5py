"""
Exascale FastForward HDF5 Index API Low-Level Binding
"""

include "config.pxi"

from _objects cimport ObjectID
from h5f cimport FileID
from h5p cimport pdefault, PropCreateID
from h5tr cimport TransactionID
from h5rc cimport RCntxtID
from h5es cimport esid_default, EventStackID

# Public constants
PLUGIN_ERROR = H5X_PLUGIN_ERROR
PLUGIN_NONE = H5X_PLUGIN_NONE
PLUGIN_DUMMY = H5X_PLUGIN_DUMMY
PLUGIN_FASTBIT = H5X_PLUGIN_FASTBIT
PLUGIN_ALACRITY = H5X_PLUGIN_ALACRITY
PLUGIN_RESERVED = H5X_PLUGIN_RESERVED
PLUGIN_MAX = H5X_PLUGIN_MAX
MAX_NPLUGINS = H5X_MAX_NPLUGINS
TYPE_LINK_NAME = H5X_TYPE_LINK_NAME
TYPE_ATTR_NAME = H5X_TYPE_ATTR_NAME
TYPE_DATA_ELEM = H5X_TYPE_DATA_ELEM
TYPE_MAP_VALUE = H5X_TYPE_MAP_VALUE

# H5X API

def create_ff(FileID f not None, unsigned plugin, ObjectID scope not None,
              TransactionID tr not None, PropCreateID xcpl=None,
              EventStackID es=None):
    """(FileID f, INT plugin, ObjectID scope, TransactionID tr, PropCreateID xcpl=None, EventStackID es=None)

    Create a new index of type plugin in the container, given by f, over a set
    of its objects, given by scope, posibly asynchronously.

    For Exascale FastForward.
    """
    H5Xcreate_ff(f.id, plugin, scope.id, pdefault(xcpl), tr.id,
                 esid_default(es))


def remove_ff(FileID f not None, unsigned plugin, ObjectID scope not None,
              TransactionID tr not None, EventStackID es=None):
    """(FileID f, INT plugin, ObjectID scope, TransactionID tr, EventStackID es=None)

    Remove index of type plugin from the container, given by f, over a set of
    its objects, given by scope, posibly asynchronously.

    For Exascale FastForward.
    """
    H5Xremove_ff(f.id, plugin, scope.id, tr.id, esid_default(es))


def get_count_ff(ObjectID scope not None, RCntxtID rc not None,
                 EventStackID es=None):
    """(ObjectID scope, RCntxtID rc, EventStackID es=None) => INT count

    Number of indexes on an object given by scope, posibly asynchronously.

    For Exascale FastForward.
    """
    cdef hsize_t idx_count
    H5Xget_count_ff(scope.id, &idx_count, rc.id, esid_default(es))
    return <int>idx_count
