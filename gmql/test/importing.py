import unittest
import warnings


class TestPyGMQLImporting(unittest.TestCase):
    @staticmethod
    def test_import():
        with warnings.catch_warnings():
            warnings.simplefilter('ignore', category=ImportWarning)
            import gmql as gl


if __name__ == '__main__':
    unittest.main()
