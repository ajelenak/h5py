"""
Python interface to HDF5 Exascale FastForward H5V API
"""

from h5py import h5v, h5s, h5p, h5a, h5f, h5g, h5d, h5q, h5r, h5t, h5m
from .group import Group
from .files import File
from .dataset import readtime_dtype, Dataset
from .datatype import Datatype
from .maps import Map
from .query import make_query
import numpy


###########################################################################


class Attribute(object):
    """Class representing an HDF5 attribute.

    This is a minimal implementation, just enough to support returning attribute
    objects from views.
    
    For Exascale FastForward.
    """

    @property
    def id(self):
        """ h5a.AttrID instance """
        return self._id


    @property
    def ctn(self):
        """Container (File) object this attribute belongs to."""
        return self._ctn


    def __init__(self, bind, container=None):
        if not isinstance(bind, h5a.AttrID):
            raise ValueError("%s is not h5a.AttrID" % bind)
        self._id = bind
        self._ctn = container


    def close(self):
        """ Close the attribute object """
        self.id._close_ff(es=self.ctn.es.id)


    def value(self):
        """ Read attribute's value """

        if self.id.get_space().get_simple_extent_type() == h5s.NULL:
            raise IOError("Empty attributes cannot be read")

        tid = self.id.get_type()
        dt = readtime_dtype(self.id.dtype, [])
        val = numpy.ndarray(self.id.shape, dtype=dt, order='C')
        self.id.read_ff(val, self.ctn.rc.id, es=self.ctn.es.id)

        if len(val.shape) == 0:
            return val[()]
        return val


###########################################################################


class View(object):
    """ Represents HDF5 Exascale FastForward H5V API """

    @property
    def id(self):
        """ h5v.ViewID instance """
        return self._id


    @property
    def ctn(self):
        """Container (File) object this view belongs to."""
        return self._ctn


    @property
    def location(self):
        """ Object on which the view is created, possibly asynchronously.
        
        For Exascale FastForward.
        """
        locid = self.id.get_location_ff(es=self.ctn.es.id)
        if isinstance(locid, h5f.FileID):
            return File(locid)
        elif isinstance(locid, h5g.GroupID):
            return Group(locid, container=self.ctn)
        elif isinstance(locid, h5d.DatasetID):
            return Dataset(locid, container=self.ctn)
        else:
            raise ValueError("%s invalid view object location" % locid)


    @property
    def query(self):
        """ Copy of the query object used to create this view """
        qid = self.id.get_query()
        return make_query(qid)


    @property
    def count(self):
        """ Count of attributes, objects, and dataset element regions in the
        view. A tuple with three elements is returned.
        """
        return self.id.get_counts()


    @property
    def attr_count(self):
        """ Count of attributes in the view. """
        return self.count[0]


    @property
    def obj_count(self):
        """ Count of objects in the view. """
        return self.count[1]


    @property
    def reg_count(self):
        """ Count of dataset element regions in the view. """
        return self.count[2]


    def __init__(self, loc, query, sel=None):
        """Create a new view object. Arguments:

        loc
            Object on which this view is created. Can be a file, group, or
            dataset.

        query
            Query object, the results of which will be available in this view.

        sel
            Optional, default is None. One of selection objects from the
            selections.py module for constraining query results presented in the
            view. Its id property must be of h5s.SpaceID type.

        For Exascale FastForward.
        """
        if not isinstance(loc, (Group, Dataset)):
            raise TypeError("A view can only be created on a file, group, or "
                            "dataset")
        if not isinstance(query.id, h5q.QueryID):
            raise TypeError("Query argument must be h5q.QueryID type")

        if isinstance(loc, File):
            self._ctn = loc
        else:
            self._ctn = loc.ctn

        if sel is None:
            self._id = h5v.create_ff(loc.id, query.id, self._ctn.rc.id,
                                     es=self._ctn.es.id)
        else:
            if not isinstance(sel.id, h5s.SpaceID):
                raise TypeError("%s is not a h5s.SpaceID" % sel)
            vcpl = h5p.create(h5p.VIEW_CREATE)
            vcpl.set_view_elmt_scope(sel.id)
            self._id = h5v.create_ff(loc.id, query.id, self._ctn.rc.id,
                                     vcpl=vcpl, es=self._ctn.es.id)


    def close(self):
        """ Close the view object """
        self.id._close()


    def attrs(self, start=0, count=1):
        """Retrieve the count (default: 1) number of attributes referenced by
        the view object beginning at offset start (default: 0), possibly
        asynchronously.

        This method is a generator of Attribute class objects.

        For Exascale FastForward.
        """
        attrs_id = self.id.get_attrs_ff(start=start, count=count,
                                        es=self.ctn.es.id)
        for aid in attrs_id:
            a = Attribute(aid, container=self.ctn)
            yield a


    def objs(self, start=0, count=1):
        """Retrieve the count (default: 1) number of HDF5 objects referenced by
        the view object beginning at offset start (default: 0), possibly
        asynchronously. HDF5 objects returned are: datasets, datatypes, groups,
        and maps.

        This method is a generator of objects of classes Dataset, Datatype,
        Map, and Group.

        For Exascale FastForward.
        """
        objs_id = self.id.get_objs_ff(start=start, count=count,
                                      es=self.ctn.es.id)
        for oid in objs_id:
            if isinstance(oid, h5d.DatasetID):
                obj = Dataset(oid, container=self.ctn)
            elif isinstance(oid, h5g.GroupID):
                obj = Group(oid, container=self.ctn)
            elif isinstance(oid, h5t.TypeID):
                obj = Datatype(oid, container=self.ctn)
            elif isinstance(oid, h5m.MapID):
                obj = Map(oid, container=self.ctn)
            else:
                raise ValueError("%s unexpected object type" % oid)

            yield obj


    def regions(self, start=0, count=1):
        """Retrieve the count (default: 1) number of region references
        from the view object beginning at offset start (default: 0),
        possibly asynchronously. Each region reference points to a dataset's
        elements included in the view.

        This method is a generator of RegionReference objects.

        For Exascale FastForward.
        """
        tl = self.id.get_elem_regions_ff(start=start, count=count,
                                         es=self.ctn.es.id)
        for dsid, spaceid in tl:
            yield h5r.create(dsid, '.', h5r.DATASET_REGION, spaceid)
