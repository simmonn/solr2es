import unittest

from solr2es import Solr2Es


class TestMigration(unittest.TestCase):
    def test_migrate_zero_docs(self):
        self.assertEqual(0, Solr2Es('solr:8983', 'elasticsearch:9200').migrate('foo'))

