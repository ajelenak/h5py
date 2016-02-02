# Test suite for Exascale FastForward H5Q API.

from .common_ff import ut, TestCaseFF
from h5py.highlevel import AQuery, CQuery

from h5py import h5
eff = h5.get_config().eff
if not eff:
    raise RuntimeError("The h5py module was not built for Exascale FastForward") 



class TestQuery(TestCaseFF):

    def test_atomic_query(self):
        """ Atomic query functionality """

        # Non-existant query type
        with self.assertRaises(ValueError):
            AQuery('blah') > 0

        # Dataset/attribute value query types
        for qt in ['data_elem', 'attr_value']:
            q = AQuery(qt) > 0.
            self.assertIsInstance(q, AQuery)
            self.assertTrue(q.is_atomic)
            self.assertFalse(q.is_compound)
            self.assertEqual(q.type, qt)
            self.assertEqual(q.op, '>')

            q = AQuery(qt) < 0.
            self.assertIsInstance(q, AQuery)
            self.assertTrue(q.is_atomic)
            self.assertFalse(q.is_compound)
            self.assertEqual(q.type, qt)
            self.assertEqual(q.op, '<')

            q = AQuery(qt) == 0.
            self.assertIsInstance(q, AQuery)
            self.assertTrue(q.is_atomic)
            self.assertFalse(q.is_compound)
            self.assertEqual(q.type, qt)
            self.assertEqual(q.op, '==')

            q = AQuery(qt) != 0.
            self.assertIsInstance(q, AQuery)
            self.assertTrue(q.is_atomic)
            self.assertFalse(q.is_compound)
            self.assertEqual(q.type, qt)
            self.assertEqual(q.op, '!=')

        # Attribute/link name query types
        for qt in ['link_name', 'attr_name']:
            with self.assertRaises(NotImplementedError):
                q = AQuery(qt) > 0.

            with self.assertRaises(NotImplementedError):
                q = AQuery(qt) < 0.

            q = AQuery(qt) == "foo"
            self.assertIsInstance(q, AQuery)
            self.assertTrue(q.is_atomic)
            self.assertFalse(q.is_compound)
            self.assertEqual(q.type, qt)
            self.assertEqual(q.op, '==')

            q = AQuery(qt) != "foo"
            self.assertIsInstance(q, AQuery)
            self.assertTrue(q.is_atomic)
            self.assertFalse(q.is_compound)
            self.assertEqual(q.type, qt)
            self.assertEqual(q.op, '!=')

        # Already atomic query cannot be used again...
        qv = AQuery("attr_value") == 5
        qn = AQuery("attr_name") == "foo"
        with self.assertRaises(RuntimeError):
            qn == "bar"
        with self.assertRaises(RuntimeError):
            qn != "bar"
        with self.assertRaises(RuntimeError):
            qv > 1
        with self.assertRaises(RuntimeError):
            qv < -5


    def test_compound_query(self):
        """Compound query functionality"""
        qv = AQuery("attr_value") == 5
        qn = AQuery("attr_name") == "foo"

        # Cannot initiate compound query object with anything but h5q.QueryID...
        with self.assertRaises(TypeError):
            CQuery("blah")

        # Cannot initiate compound query object with atomic query's id...
        with self.assertRaises(TypeError):
            CQuery(qv.id)

        # Compound query with or
        cq = qv | qn
        self.assertIsInstance(cq, CQuery)
        self.assertFalse(cq.is_atomic)
        self.assertTrue(cq.is_compound)
        self.assertEqual(cq.op, '|')

        # Compound query with and
        cq = qv & qn
        self.assertIsInstance(cq, CQuery)
        self.assertFalse(cq.is_atomic)
        self.assertTrue(cq.is_compound)
        self.assertEqual(cq.op, '&')

        # New compound query out of one compound and one atomic
        cq = cq | (AQuery('data_elem') > 47.67)
        self.assertIsInstance(cq, CQuery)
        self.assertFalse(cq.is_atomic)
        self.assertTrue(cq.is_compound)
        self.assertEqual(cq.op, '|')

        # Breaking down compound query into its components...
        q1, q2 = cq.components()

        # Check components...
        self.assertIsInstance(q1, CQuery)
        self.assertIsInstance(q2, AQuery)
        self.assertEqual(q2.type, 'data_elem')
        self.assertEqual(q2.op, '>')
