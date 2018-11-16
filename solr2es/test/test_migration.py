import unittest

import requests
from elasticsearch import Elasticsearch
from pysolr import Solr

from solr2es.solr2es import Solr2Es, DEFAULT_ES_DOC_TYPE


class TestMigration(unittest.TestCase):
    es = None
    solr = None

    @classmethod
    def setUpClass(cls):
        cls.solr = Solr('http://solr:8983/solr/test_core', always_commit=True)
        cls.es = Elasticsearch(host='elasticsearch')

    @classmethod
    def tearDownClass(cls):
        cls.solr.session.close()

    def setUp(self):
        self.solr2es = Solr2Es(TestMigration.solr, TestMigration.es, refresh=True)

    def tearDown(self):
        TestMigration.solr.delete(q='*:*')
        TestMigration.es.indices.delete(index='foo')

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

    def test_migrate_with_es_mapping(self):
        mapping = '{"mappings": {"doc": {"properties": {"my_field": {"type": "keyword"}}}}}'
        self.solr2es.migrate('foo', mapping=mapping)
        self.assertEqual({'my_field': {'type': 'keyword'}}, self.es.indices.get_field_mapping(index=['foo'], fields=['my_field'])['foo']['mappings']['doc']['my_field']['mapping'])

    def test_migrate_with_es_mapping_with_existing_index(self):
        self.solr2es.migrate('foo', '{"mappings": {"doc": {"properties": {"my_field": {"type": "keyword"}}}}}')
        self.solr2es.migrate('foo', '{"mappings": {"doc": {"properties": {"other": {"type": "keyword"}}}}}')

        self.assertEqual({'foo': {'mappings': {}}}, self.es.indices.get_field_mapping(index=['foo'], fields=['other']))

    def test_migrate_with_mapping_on_same_name_existing_field(self):
        TestMigration.solr.add([{"my_field": "content"}])
        self.solr2es.migrate('foo', '{"mappings": {"doc": {"properties": {"my_field": {"type": "keyword"}}}}}')

        self.assertEqual(1, self.es.search('foo', 'doc', {'query': {'term': {'my_field': 'content'}}})['hits']['total'])

    def test_migrate_with_mapping_on_same_existing_field_with_different_types(self):
        requests.post('http://solr:8983/solr/test_core/schema', headers={'Content-Type': 'application/json'},
                      data='{ "add-field":{ "name":"my_int", "type":"pint", "stored":true }}')
        TestMigration.solr.add([{"id": '123', "my_int": 12}])

        self.assertEqual(0, self.solr2es.migrate('foo', '{"mappings": {"doc": {"properties": {"my_int": {"type": "date", "format": "date_time"}}}}}'))
        self.assertFalse(self.es.exists(index='foo', doc_type=DEFAULT_ES_DOC_TYPE, id="123"))

    def test_migrate_different_fields_with_translation_map(self):
        TestMigration.solr.add([{"id": "142", "my_bar": "content"}])

        self.solr2es.migrate('foo',
                             '{"mappings": {"doc": {"properties": {"my_baz": {"type": "text"}}}}}',
                             {"my_bar": "my_baz"})

        doc = self.es.get_source(index='foo', doc_type=DEFAULT_ES_DOC_TYPE, id="142")
        self.assertEqual(doc['my_baz'], "content")

    def test_migrate_nested_fields_with_translation_map(self):
        TestMigration.solr.add([{"id": "142", "nested_field": "content"}])

        self.solr2es.migrate('foo',
                             '{"mappings": {"doc": {"properties": {"nested": {"type": "object"}}}}}',
                             {"nested_field": "nested.a.b.c"})

        doc = self.es.get_source(index='foo', doc_type=DEFAULT_ES_DOC_TYPE, id="142")
        self.assertEqual({'a': {'b': {'c': 'content'}}}, doc['nested'])
