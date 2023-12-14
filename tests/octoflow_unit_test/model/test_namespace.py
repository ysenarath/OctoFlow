import unittest

from octoflow.model import Namespace


class TestNamespaceMethods(unittest.TestCase):
    def test_eq_ns_str(self):
        namespace = "octoflow.version"
        ns = Namespace(namespace)
        self.assertEqual(ns, namespace)

    def test_eq_ns_ns(self):
        namespace = "octoflow.version"
        ns_1 = Namespace(namespace)
        ns_2 = Namespace(namespace)
        self.assertEqual(ns_1, ns_2)

    def test_name(self):
        namespace = "octoflow.version"
        ns_1 = Namespace(namespace)
        self.assertEqual(ns_1.name, namespace)

    def test_attr_access(self):
        ns = Namespace()
        ns = ns.octoflow.version
        self.assertIsNotNone(ns)
        self.assertIsInstance(ns, Namespace)
        self.assertEqual(ns, "octoflow.version")


if __name__ == "__main__":
    unittest.main()
