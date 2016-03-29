# This file implements the high-level Python interface to the HDF5 Event Stack
# API

from h5py import h5, h5es

# Check if this module can be used
eff = h5.get_config().eff
if not eff:
    raise RuntimeError("This module requires h5py to be built with HDF5 "
                       "FastForward library")

_status = {h5es.STATUS_IN_PROGRESS: "in_progress",
           h5es.STATUS_SUCCEED: "succeed",
           h5es.STATUS_FAIL: "fail",
           h5es.STATUS_CANCEL: "cancel"}


class _EventStackNull(object):
    """Represents H5_EVENT_STACK_NULL"""

    @property
    def id(self):
        return None

    def __init__(self):
        pass

    def __repr__(self):
        return "<HDF5 H5_EVENT_STACK_NULL (%s)>" % hex(id(self))


es_null = _EventStackNull()


class EventStack(object):
    """
    Represents an HDF5 event stack.

    Requires HDF5 FastForward (prereq.: MPI and Parallel HDF5).
    """

    @property
    def id(self):
        """Hold h5es.EventStackID object"""
        return self._id

    def __init__(self):
        # For start, event stack is not created...
        self._id = None

    def __repr__(self):
        if not self._id:
            return "<HDF5 closed event stack>"
        else:
            return "<HDF5 event stack (%s)>" % hex(id(self._id))

    def create(self):
        """ Create event stack """
        self._id = h5es.create()
        return self

    def close(self):
        """Close an event stack.

        At this point the event stack identifier becomes invalid.
        """
        self._id._close()

    @property
    def count(self):
        """ Number of events in the event stack """
        return self.id.get_count()

    def clear(self):
        """Clear all events from the event stack after first confirming that no
        in-progress events remain in the stack. If the stack does contain one
        or more in-progress events, no events are cleared and the call fails.
        """
        self.id.clear()

    def test(self, event_idx=0):
        """Report the completion status of the event in the event stack.
        Possible status values: 'in_progress', 'succeed', 'fail', 'cancel'."""
        s = self.id.test(event_idx=event_idx)
        return _status[s]

    def test_all(self):
        """Report overall completion status for all events in the event stack.
        Possible status values: 'in_progress', 'succeed', 'fail', 'cancel'."""
        s = self.id.test_all()
        return _status[s]

    def wait(self, event_idx=0):
        """Wait for the completion or cancellation of the event specified by
        event_idx in the event stack and reports the event's completion status.
        The call does not return until the event being waited on completes or
        is cancelled. Possible status values: 'in_progress', 'succeed', 'fail',
        'cancel'.
        """
        s = self.id.wait(event_idx=event_idx)
        return _status[s]

    def wait_all(self):
        """Wait for the completion or cancellation of all events in the event
        stack and reports an overall completion status. The call does not
        return until all events in the event stack complete or are cancelled.
        Possible status values: 'in_progress', 'succeed', 'fail', 'cancel'.
        """
        s = self.id.wait_all()
        return _status[s]

    def null(self):
        """Return an object representing the H5_EVENT_STACK_NULL value."""
        return es_null
