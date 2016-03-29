"""
Python interface to the Exascale FastForward HDF5 H5Q API
"""

from h5py import h5q, h5t
import numpy as np

##############################################################################
_query_type = {'data_elem': h5q.TYPE_DATA_ELEM,
               'attr_value': h5q.TYPE_ATTR_VALUE,
               'attr_name': h5q.TYPE_ATTR_NAME,
               'link_name': h5q.TYPE_LINK_NAME,
               'misc': h5q.TYPE_MISC}

_match_op = {'==': h5q.MATCH_EQUAL,
             '!=': h5q.MATCH_NOT_EQUAL,
             '<': h5q.MATCH_LESS_THAN,
             '>': h5q.MATCH_GREATER_THAN}


##############################################################################
def make_query(qid):
    """ Create an instance of correct query class: AQuery or CQuery.
    """

    if not isinstance(qid, h5q.QueryID):
        raise TypeError("%s not h5q.QueryID" % qid)

    if qid.get_combine_op() == h5q.SINGLETON:
        # Query is atomic...
        return AQuery(qid)
    else:
        # Query is compound...
        return CQuery(qid)


##############################################################################
class Query(object):
    """ Mixin class for other query classes """

    @property
    def id(self):
        """ Holds h5q.QueryID object """
        return self._id

    @property
    def is_atomic(self):
        cop = self.id.get_combine_op()
        return cop == h5q.SINGLETON

    @property
    def is_compound(self):
        cop = self.id.get_combine_op()
        return cop != h5q.SINGLETON

    def __repr__(self):
        if not self.id:
            return "<Closed HDF5 query object>"
        else:
            if self.is_atomic:
                qt = "atomic"
            else:
                qt = "compound"
        return "<HDF5 %s query object (id: %s)>" % (qt, hex(id(self)))

    def close(self):
        """ Close the query object """
        self.id._close()


##############################################################################
def _val_helper(obj):
    """ Returns a tuple with ndarray and TypeID representation of obj. """

    val = np.asarray(obj, order='C')
    dt = h5t.py_create(val.dtype, logical=1)

    return val, dt


##############################################################################
class AQuery(Query):
    """ Atomic query class """

    @property
    def type(self):
        """ Return string representation of the query's type. """

        qt = self.id.get_type()
        return _query_type.keys()[_query_type.values().index(qt)]

    @property
    def op(self):
        """ Return string representation of the query's match operator. """

        op = self.id.get_match_op()
        return _match_op.keys()[_match_op.values().index(op)]

    def __init__(self, qobj):
        """Initialize atomic query. Arguments:

        qobj
            Either a h5q.QueryID object or a query type string. Supported
            string values are: "data_elem", "attr_value", "attr_name", and
            "link_name".
        """
        self._id = None

        if isinstance(qobj, h5q.QueryID):
            if qobj.get_combine_op() != h5q.SINGLETON:
                raise TypeError("Init argument is not atomic query")
            self._id = qobj
        else:
            if qobj not in _query_type:
                raise ValueError("%s: Invalid query type" % qobj)
            self._qt = qobj

    def __gt__(self, other):
        if self.id is not None:
            raise RuntimeError("Atomic query already created")
        if self._qt not in ('data_elem', 'attr_value'):
            raise NotImplementedError("Invalid operation for %s query type"
                                      % self._qt)
        qt = _query_type[self._qt]
        val, dt = _val_helper(other)
        qid = h5q.create(qt, h5q.MATCH_GREATER_THAN, dt, val)
        self._id = qid

        return self

    def __lt__(self, other):
        if self.id is not None:
            raise RuntimeError("Atomic query already created")
        if self._qt not in ('data_elem', 'attr_value'):
            raise NotImplementedError("Invalid operation for %s query type"
                                      % self._qt)
        qt = _query_type[self._qt]
        val, dt = _val_helper(other)
        qid = h5q.create(qt, h5q.MATCH_LESS_THAN, dt, val)
        self._id = qid

        return self

    def __eq__(self, other):
        if self.id is not None:
            raise RuntimeError("Atomic query already created")
        if self._qt in ('data_elem', 'attr_value'):
            qt = _query_type[self._qt]
            val, dt = _val_helper(other)
            qid = h5q.create(qt, h5q.MATCH_EQUAL, dt, val)
            self._id = qid
        elif self._qt in ('attr_name', 'link_name'):
            name = str(other)
            qt = _query_type[self._qt]
            qid = h5q.create(qt, h5q.MATCH_EQUAL, name)
            self._id = qid

        return self

    def __ne__(self, other):
        if self.id is not None:
            raise RuntimeError("Atomic query already created")
        if self._qt in ('data_elem', 'attr_value'):
            qt = _query_type[self._qt]
            val, dt = _val_helper(other)
            qid = h5q.create(qt, h5q.MATCH_NOT_EQUAL, dt, val)
            self._id = qid
        elif self._qt in ('attr_name', 'link_name'):
            name = str(other)
            qt = _query_type[self._qt]
            qid = h5q.create(qt, h5q.MATCH_NOT_EQUAL, name)
            self._id = qid

        return self

    def __and__(self, other_query):
        qid = self.id.combine(h5q.COMBINE_AND, other_query.id)
        return CQuery(qid)

    def __or__(self, other_query):
        qid = self.id.combine(h5q.COMBINE_OR, other_query.id)
        return CQuery(qid)


##############################################################################
class CQuery(Query):
    """ Compound query class """

    @property
    def op(self):
        """Return string representation of the compound query's combine
        operator."""

        op = self.id.get_combine_op()
        if op == h5q.COMBINE_AND:
            return '&'
        elif op == h5q.COMBINE_OR:
            return '|'
        else:
            raise ValueError("%s: Invalid combine operator for compound query"
                             % op)

    def __init__(self, qid):
        """Initialize compound query. Arguments:

        qid
            An h5q.QueryID object representing a compound query.
        """
        if not isinstance(qid, h5q.QueryID):
            raise TypeError("%s is not QueryID" % qid)
        if qid.get_combine_op() == h5q.SINGLETON:
            raise TypeError("Init argument is not a compound query")
        self._id = qid

    def __and__(self, other_query):
        qid = self.id.combine(h5q.COMBINE_AND, other_query.id)
        return CQuery(qid)

    def __or__(self, other_query):
        qid = self.id.combine(h5q.COMBINE_OR, other_query.id)
        return CQuery(qid)

    def components(self):
        """Tuple of the subqueries of the compound query."""

        qtpl = self.id.get_components()
        qlist = []
        for qid in qtpl:
            qlist.append(make_query(qid))

        return tuple(qlist)
