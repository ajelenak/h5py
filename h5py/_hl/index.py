"""
Python interface to Exascale FastForward HDF5 Index (H5X) API
"""

# from h5py import h5x

# _indexes = {'dummy': h5x.PLUGIN_DUMMY,
#             'fastbit': h5x.PLUGIN_FASTBIT,
#             'alacrity': h5x.PLUGIN_ALACRITY}

class Index(object):
    """
    Mixin class for adding access to the H5X indexing HDF5_FF API.

    For Exascale FastForward.
    """

    def create_index(self, plugin='dummy'):
        """Create an index of type plugin on this object.

        Arguments:

        plugin
            Plugin type. Possible values: 'dummy', 'fastbit', or 'alacrity'.
            Default 'dummy'.
        """
        plugin_id = _indexes[plugin]
        # h5x.create_ff(self.container.id, plugin_id, self.id,
        #               self.container.tr.id, es=self.container.es.id)


    def remove_index(self, plugin='dummy'):
        """Remove an index of type plugin on this object.

        Arguments:

        plugin
            Plugin type. Possible values: 'dummy', 'fastbit', or 'alacrity'.
            Default 'dummy'.
        """
        plugin_id = _indexes[plugin]
        # h5x.remove_ff(self.container.id, plugin_id, self.id,
        #               self.container.tr.id, es=self.container.es.id)


    def index_count(self):
        """Number of indexes on this object."""

        # count = h5x.get_count_ff(self.id, self.container.rc.id,
        #                          es=self.container.es.id)
        return count
