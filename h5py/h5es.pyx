"""
   H5ES API low-level Cython bindings.
"""

include "config.pxi"

from h5py import _objects

# Helper function for the correct value of the event stack id.
cdef hid_t esid_default(EventStackID es):
    if es is None:
        return <hid_t>H5_EVENT_STACK_NULL
    return <hid_t>es.id

# API constants
STATUS_IN_PROGRESS = H5ES_STATUS_IN_PROGRESS
STATUS_SUCCEED = H5ES_STATUS_SUCCEED
STATUS_FAIL = H5ES_STATUS_FAIL
STATUS_CANCEL = H5ES_STATUS_CANCEL

# Event stack operations

def create():
   """() => EventStackID

   Create an event stack.
   """

   return EventStackID.open(H5EScreate())


# Not implemented in the HDF5_FF library
# def cancel(EventStackID es not None, int event_idx not None):
#    """(EventStackID es, INT event_idx) => INT status
# 
#    Cancels the in-progress event specified by event_idx in the event stack
#    specified by es and returns the event’s status.  
#    """
# 
#    cdef H5ES_status_t status
# 
#    H5EScancel(es.id, event_idx, &status)
#    return status
#
# 
# def cancel_all(EventStackID es not None):
#    """(EventStackID es)
# 
#    Cancels all in-progress events in the event stack specified by es and
#    indicates their statuses.
#    """
# 
#    cdef H5ES_status_t *status
#    H5EScancel_all(es.id, &status)
#    return status


# EventStackID implementation
cdef class EventStackID(ObjectID):
    """
    Represents an HDF5 event stack identifier
    """
   
    def _close(self):
        """()

        Terminate access through this identifier. You shouldn't have to
        call this manually; event stack identifiers are automatically released
        when their Python wrappers are freed.
        """
        with _objects.registry.lock:
            H5ESclose(self.id)
            if not self.valid:
                del _objects.registry[self.id]


    def get_count(self):
       """() => INT count

       Returns the number of events in the event stack.
       """

       cdef size_t count
       H5ESget_count(self.id, &count)
       return count


    def clear(self):
       """()

       Clear all events from the event stack after first confirming that no
       in-progress events remain in the stack. If the stack does contain one
       or more in-progress events, no events are cleared and the call fails.
       """

       H5ESclear(self.id)


    def test(self, size_t event_idx=0):
       """(INT event_idx=0) => INT status

       Reports the completion status of the event specified by event_idx in the
       event stack.
       """

       cdef H5ES_status_t status
       H5EStest(self.id, event_idx, &status)
       return status


    def test_all(self):
       """() => INT status

       Reports an overall completion status for all events in the event stack.
       """

       cdef H5ES_status_t status
       H5EStest_all(self.id, &status)
       return status


    def wait(self, size_t event_idx=0):
       """(INT event_idx=0) => INT status

       Waits for the completion or cancellation of the event specified by
       event_idx in the event stack and reports the event’s completion status.
       The call does not return until the event being waited on completes or
       is cancelled.
       """

       cdef H5ES_status_t status
       H5ESwait(self.id, event_idx, &status)
       return status


    def wait_all(self):
       """() => INT status

       Waits for the completion or cancellation of all events in the event
       stack and reports an overall completion status. The call does not
       return until all events in the event stack complete or are cancelled.
       """

       cdef H5ES_status_t status
       H5ESwait_all(self.id, &status)
       return status
