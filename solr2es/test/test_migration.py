import unittest

import requests
from elasticsearch import Elasticsearch
from pysolr import Solr

from solr2es.solr2es import Solr2Es, DEFAULT_ES_DOC_TYPE, translate_doc, tuples_to_dict


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
        requests.post('http://solr:8983/solr/test_core/schema', headers={'Content-Type': 'application/json'},
                      data='{ "add-field":{ "name":"title", "type":"string", "stored":true }}')
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
        TestMigration.solr.add([{"id": 321, "title": "content"}])
        self.solr2es.migrate('foo', '{"mappings": {"doc": {"properties": {"title": {"type": "keyword"}}}}}')

        self.assertEqual(1, self.es.search('foo', 'doc', {'query': {'term': {'title': 'content'}}})['hits']['total'])

    def test_migrate_with_mapping_on_same_existing_field_with_different_types(self):
        requests.post('http://solr:8983/solr/test_core/schema', headers={'Content-Type': 'application/json'},
                      data='{ "add-field":{ "name":"my_int", "type":"pint", "stored":true }}')
        TestMigration.solr.add([{"id": '123', "my_int": 12}])

        self.assertEqual(0, self.solr2es.migrate('foo', '{"mappings": {"doc": {"properties": {"my_int": {"type": "date", "format": "date_time"}}}}}'))
        self.assertFalse(self.es.exists(index='foo', doc_type=DEFAULT_ES_DOC_TYPE, id="123"))

    def test_migrate_different_fields_with_translation_map(self):
        requests.post('http://solr:8983/solr/test_core/schema', headers={'Content-Type': 'application/json'},
                      data='{ "add-field":{ "name":"my_bar", "type":"string", "stored":true }}')
        TestMigration.solr.add([{"id": "142", "my_bar": "content"}])

        self.solr2es.migrate('foo',
                             '{"mappings": {"doc": {"properties": {"my_baz": {"type": "text"}}}}}',
                             {"my_bar": {'name': "my_baz"}})

        doc = self.es.get_source(index='foo', doc_type=DEFAULT_ES_DOC_TYPE, id="142")
        self.assertEqual(doc['my_baz'], "content")

    def test_migrate_nested_fields_with_translation_map(self):
        requests.post('http://solr:8983/solr/test_core/schema', headers={'Content-Type': 'application/json'},
                      data='{ "add-field":{ "name":"nested_field", "type":"string", "stored":true }}')
        TestMigration.solr.add([{"id": "142", "nested_field": "content"}])

        self.solr2es.migrate('foo',
                             '{"mappings": {"doc": {"properties": {"nested": {"type": "object"}}}}}',
                             {"nested_field": {"name":"nested.a.b.c"}})

        doc = self.es.get_source(index='foo', doc_type=DEFAULT_ES_DOC_TYPE, id="142")
        self.assertEqual({'a': {'b': {'c': 'content'}}}, doc['nested'])

    def test_migrate_sibling_nested_fields_with_translation_map(self):
        requests.post('http://solr:8983/solr/test_core/schema', headers={'Content-Type': 'application/json'},
                      data='{ "add-field":{ "name":"nested_field1", "type":"string", "stored":true }}')
        requests.post('http://solr:8983/solr/test_core/schema', headers={'Content-Type': 'application/json'},
                      data='{ "add-field":{ "name":"nested_field2", "type":"string", "stored":true }}')
        TestMigration.solr.add([{"id": "142", "nested_field1": "content1", "nested_field2": "content2"}])
    
        self.solr2es.migrate('foo',
                             '{"mappings": {"doc": {"properties": {"nested": {"type": "object"}}}}}',
                             {"nested_field1": {"name": "nested.a.b"}, "nested_field2": {"name": "nested.a.c"}})
    
        doc = self.es.get_source(index='foo', doc_type=DEFAULT_ES_DOC_TYPE, id="142")
        self.assertEqual({'a': {'b': 'content1', 'c': 'content2'}}, doc['nested'])

    # def test_migrate_sibling_nested_fields_with_wildcard(self):
    #     TestMigration.solr.add([{"id": "142", "nested_field1": "content1", "nested_field2": "content2"}])
    #     self.solr2es.migrate('foo',
    #                          '{"mappings": {"doc": {"properties": {"nested": {"type": "object"}}}}}',
    #                          {"nested_(.*)": {"name": "nested.\1"}})
    #     doc = self.es.get_source(index='foo', doc_type=DEFAULT_ES_DOC_TYPE, id="142")
    #     self.assertEqual({'field1': 'content1', 'field2': 'content2'}, doc['nested'])
    #
    # def test_migrate_with_default_field_value(self):
    #     TestMigration.solr.add([{"id": "142"}])
    #     self.solr2es.migrate('foo', '{"mappings": {"doc": {"properties": {"nested": {"type": "object"}}}}}',
    #                          {"new_field": {'default_value': 'john doe'}})
    #     doc = self.es.get_source(index='foo', doc_type=DEFAULT_ES_DOC_TYPE, id="142")
    #     self.assertEqual('john doe', doc['new_field'])


class TestTranslateDoc(unittest.TestCase):
    def test_with_nested_field(self):
        self.assertEqual({'a': {'b': {'c': 'value'}}}, translate_doc({'a_b_c': 'value'}, {'a_b_c': {'name': 'a.b.c'}}))

    def test_with_simple_field(self):
        self.assertEqual({'a': 'b'}, tuples_to_dict([('a', 'b')]))

    def test_with_complex_field(self):
        self.assertEqual({'a': {'b': 'c'}}, tuples_to_dict([('a', ('b', 'c'))]))

    def test_with_very_complex_field(self):
            self.assertEqual({'a': {'b': 'content1', 'c': 'content2'}}, tuples_to_dict([('a', ('b', 'content1')), ('a', ('c', 'content2'))]))

    def test_with_more_complex_field(self):
            self.assertEqual({'nested': {'a': {'b': 'content1', 'c': 'content2'}}}, tuples_to_dict([('nested', ('a', ('b', 'content1'))), ('nested', ('a', ('c', 'content2')))]))

    def test_with_sibling_nested_fields(self):
        self.assertEqual({'a': {'b': 'value1', 'c': 'value2'}},
                         translate_doc({'a_b': 'value1', 'a_c': 'value2'}, {'a_b': {'name': 'a.b'}, 'a_c': {'name': 'a.c'}}))

    # def test_with_sibling_nested_fields_in_depth(self):
    #     self.assertEqual({'a': {'b': {'c': {'d': 'value1'}}, 'e': 'value2'}},
    #                      translate_doc({'a_b_c_d': 'value1', 'a_b_e': 'value2'}, {'a_b_c_d': {'name': 'a.b.c.d'}, 'a_b_e': {'name': 'a.b.e'}}))

