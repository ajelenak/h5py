"""
    H5RC API low-level Cython bindings
"""

include "config.pxi"

from h5p cimport pdefault, PropRCAID
from h5es cimport EventStackID, esid_default
from h5py import _objects

# API constants
EXACT = H5RC_EXACT
PREV = H5RC_PREV
NEXT = H5RC_NEXT
LAST = H5RC_LAST

# Read Context operations

def create(ObjectID fid not None, uint64_t container_version):
    """(ObjectID fid, int container_version) => RCntxtID

    Create a read context associated with a container and version.
    """
    return RCntxtID.open(H5RCcreate(fid.id, container_version))


def acquire(ObjectID fid not None, uint64_t container_version, PropRCAID rcapl=None, EventStackID es=None):
    """(ObjectID fid, UINT container_version, PropRCAID rcapl=None, EventStack es=None) => TUPLE (RCntxtID, UINT container_version)

    Acquire a read handle for a container at a given version and create a
    read context associated with the container and version.
    """
    cdef uint64_t cv
    cdef hid_t rcid
    cv = container_version
    rcid = H5RCacquire(fid.id, &cv, pdefault(rcapl), esid_default(es))
    container_version = cv
    return (RCntxtID.open(rcid), container_version)


# Read Context ID implementation for Exascale FastForward
cdef class RCntxtID(ObjectID):
    """
    Represents an HDF5 read context identifier
    """

    # Let's first trust Python to clean up correctly.
    def _close(self):
        """()

        Terminate access through this identifier. You shouldn't have to
        call this manually; event stack identifiers are automatically released
        when their Python wrappers are freed.
        """
        with _objects.registry.lock:
            H5RCclose(self.id)
            if not self.valid:
                del _objects.registry[self.id]


    def get_version(self):
        """() => UINT container_version

        Retrieve the container version associated with this read context
        identifier.
        """
        cdef uint64_t container_version
        H5RCget_version(self.id, &container_version)
        return container_version


    def persist(self, EventStackID es=None):
        """(EventStack es=None)

        Copy data for a container from IOD to DAOS, bringing DAOS up to
        specified container version.
        """
        H5RCpersist(self.id, esid_default(es))


    def release(self, EventStackID es=None):
        """(EventStackID es=None)

        Close the read context and release a read handle for the
        associated container version.
        """
        H5RCrelease (self.id, esid_default(es))


    def snapshot(self, char* snapshot_name, EventStackID es=None):
        """(STRING snapshot_name, EventStackID es=None)

        Make a snapshot of a container on DAOS and give it the indicated
        snapshot name.
        """
        H5RCsnapshot(self.id, snapshot_name, esid_default(es))
