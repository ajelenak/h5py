"""
    H5TR API low-level Cython bindings.
"""

include "config.pxi"

from h5es cimport esid_default, EventStackID
from h5p cimport tsdefault, PropTSID, PropID
from h5py import _objects


# Transaction operations


def create(ObjectID fid not None, RCntxtID rcid not None, uint64_t trans_num):
    """(ObjectID fid, RCntxtID rcid, UINT trans_num) => TransactionID

    Create a transaction associated with a specified container, read context,
    and number.
    """
    return TransactionID.open(H5TRcreate(fid.id, rcid.id, trans_num))


def skip(ObjectID fid not None, uint64_t start_trans_num, uint64_t count=1,
         EventStackID esid=None):
    """(ObjectID fid, UINT start_trans_num, UINT count, EventStackID esid=None)

    Explicitly skip one or more transaction numbers for a container.
    """
    H5TRskip(fid.id, start_trans_num, count, esid_default(esid))


# TransactionID implementation


cdef class TransactionID(ObjectID):
    """
    Represents an HDF5 transaction identifier
    """

    def start(self, PropTSID tspl=None, EventStackID esid=None):
        """(PropTSID tspl, EventStackID esid=None)
        
        Start a transaction.
        """
        H5TRstart(self.id, tsdefault(tspl), esid_default(esid))


    def finish(self, PropID tfpl=None, bint rcid_flag=False, EventStackID esid=None):
        """(PropID tfpl=None, BOOL rcid_flag=False, EventStackID esid=None) => RCntxtID

        Finish the transaction. If rcid_flag is set to True, new read context
        identifier will be acquired and returned. If rcid_flag is False
        (default) this method does not return anything.
        """
        cdef hid_t rcid
        if rcid_flag:
            H5TRfinish(self.id, tsdefault(tfpl), &rcid, esid_default(esid))
            return RCntxtID.open(rcid)
        H5TRfinish(self.id, tsdefault(tfpl), NULL, esid_default(esid))


    def set_dependency(self, uint64_t trans_num, EventStackID esid=None):
        """(UINT trans_num, EventStackID esid=None)

        Register the dependency of the transaction on a lower-numbered
        transaction.
        """
        H5TRset_dependency(self.id, trans_num, esid_default(esid))


    def abort(self, EventStackID esid=None):
        """(EventStackID esid=None)

        Abort the transaction.
        """
        H5TRabort(self.id, esid_default(esid))


    # Let's first trust Python to clean up correctly.
    def _close(self):
        """()

        Close the specified transaction.
        """
        with _objects.registry.lock:
            H5TRclose(self.id)
            if not self.valid:
                del _objects.registry[self.id]


    def get_trans_num(self):
        """() => UINT

        Retrieve the transaction number associated with the transaction.
        """
        cdef uint64_t trans_num
        H5TRget_trans_num(self.id, &trans_num)
        return trans_num


    def get_version(self):
        """() => UINT

        Retrieve the container version associated with this transaction.
        """
        cdef uint64_t container_version
        H5TRget_version(self.id, &container_version)
        return container_version
