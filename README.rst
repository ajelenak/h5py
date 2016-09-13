.. image:: https://travis-ci.org/h5py/h5py.png
   :target: https://travis-ci.org/h5py/h5py

HDF5 for Python (**Special Version**)
=====================================

This is a customized version of the h5py code for a version of the HDF5 v1.8.17 library that is **not publicly available**. The new code allows retrieval of storage information about datasets, such as offsets and sizes of blocks of bytes where the datasets' data is actually located in a HDF5 file.

Websites
--------

* Main website: http://www.h5py.org
* Source code: http://github.com/h5py/h5py
* Mailing list: https://groups.google.com/d/forum/h5py

For advanced installation options, see http://docs.h5py.org.

Prerequisites
-------------

You need, at a minimum:

* Python 2.6, 2.7, 3.2, 3.3, or 3.4
* NumPy 1.6.1 or later
* The "six" package for Python 2/3 compatibility
* Customized version of the HDF5 v1.8.17 library

To build on UNIX:

* HDF5 1.8.4 or later (on Windows, HDF5 comes with h5py)
* Cython 0.19 or later
* If using Python 2.6, unittest2 is needed to run the tests

Installing on Windows
---------------------

Download an installer from http://www.h5py.org and run it.

Installing on UNIX
------------------

Via pip (recommended)::

   pip install -e git+https://github.com/ajelenak-thg/h5py@storage-info

From a release tarball or Git checkout::

   python setup.py build
   python setup.py test # optional
   [sudo] python setup.py install

Reporting bugs
--------------

Open a bug at http://github.com/h5py/h5py/issues.  For general questions, ask
on the list (https://groups.google.com/d/forum/h5py).
