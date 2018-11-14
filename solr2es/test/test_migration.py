import unittest

from elasticsearch import Elasticsearch
from pysolr import Solr

from solr2es.solr2es import Solr2Es, DEFAULT_ES_DOC_TYPE


class TestMigration(unittest.TestCase):
    es = None
    solr = None

    @classmethod
    def setUpClass(cls):
        cls.solr = Solr('http://solr:8983/solr/my_core', always_commit=True)
        cls.es = Elasticsearch(host='elasticsearch')

    @classmethod
    def tearDownClass(cls):
        cls.solr.session.close()

    def setUp(self):
        self.solr2es = Solr2Es(TestMigration.solr, TestMigration.es, refresh=True)

    def tearDown(self):
        TestMigration.solr.delete(q='*:*')
        TestMigration.es.delete_by_query(index='foo', doc_type=DEFAULT_ES_DOC_TYPE, body='{"query": {"match_all": {}}}', conflicts='proceed', refresh=True)

    def test_migrate_zero_docs(self):
        self.assertEqual(0, self.solr2es.migrate('foo'))

    def test_migrate_one_doc(self):
        TestMigration.solr.add([{"id": "doc_1", "title": "A test document"}])

        self.assertEqual(1, self.solr2es.migrate('foo'))

        doc = TestMigration.es.get_source(index="foo", doc_type=DEFAULT_ES_DOC_TYPE, id="doc_1")
        self.assertEqual(doc['id'], "doc_1")
        self.assertEqual(doc['title'], "A test document")

    def test_migrate_two_docs(self):
        TestMigration.solr.add([
            {"id": "id_1", "title": "A first document"},
            {"id": "id_2", "title": "A second document"}
        ])
        self.assertEqual(2, self.solr2es.migrate('foo'))

    def test_migrate_twelve_docs(self):
        TestMigration.solr.add([{"id": "id_%d" % i, "title": "A %d document" % i} for i in range(0, 12)])
        self.assertEqual(12, self.solr2es.migrate('foo'))
