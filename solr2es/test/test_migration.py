import re
import unittest

import requests
from elasticsearch import Elasticsearch
from nose.tools import assert_raises, raises
from pysolr import Solr, SolrError

from solr2es.__main__ import Solr2Es, DEFAULT_ES_DOC_TYPE, translate_doc, _tuples_to_dict, create_es_actions, \
    IllegalStateError, TranslationMap


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

    def test_migrate_with_custom_sort_field_not_unique(self):
        TestMigration.solr.add([{"id": "id", "title": "A document"}])
        with assert_raises(SolrError) as se:
            self.solr2es.migrate('foo', sort_field='title')
        self.assertTrue('Cursor functionality requires a sort containing a uniqueKey field tie breaker'
                        in str(se.exception))

    def test_migrate_two_docs_with_id_filter(self):
        TestMigration.solr.add([
            {"id": "abc", "title": "A first document"},
            {"id": "def", "title": "A second document"}
        ])
        self.assertEqual(1, self.solr2es.migrate('foo', solr_filter_query='id:d*'))

    def test_migrate_twelve_docs(self):
        TestMigration.solr.add([{"id": "id_%d" % i, "title": "A %d document" % i} for i in range(0, 12)])
        self.assertEqual(12, self.solr2es.migrate('foo'))

    def test_migrate_with_es_mapping(self):
        mapping = '{"mappings": {"doc": {"properties": {"my_field": {"type": "keyword"}}}}}'
        self.solr2es.migrate('foo', mapping=mapping)
        self.assertEqual({'my_field': {'type': 'keyword'}},
                         self.es.indices.get_field_mapping(index=['foo'], fields=['my_field'])['foo']['mappings'][
                             'doc']['my_field']['mapping'])

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

        self.assertEqual(0, self.solr2es.migrate('foo',
                                                 '{"mappings": {"doc": {"properties": {"my_int":'
                                                 '{"type": "date", "format": "date_time"}}}}}'))
        self.assertFalse(self.es.exists(index='foo', doc_type=DEFAULT_ES_DOC_TYPE, id="123"))

    def test_migrate_different_fields_with_translation_map(self):
        requests.post('http://solr:8983/solr/test_core/schema', headers={'Content-Type': 'application/json'},
                      data='{ "add-field":{ "name":"my_bar", "type":"string", "stored":true }}')
        TestMigration.solr.add([{"id": "142", "my_bar": "content"}])

        self.solr2es.migrate('foo', '{"mappings": {"doc": {"properties": {"my_baz": {"type": "text"}}}}}',
                             TranslationMap({"my_bar": {'name': "my_baz"}}))

        doc = self.es.get_source(index='foo', doc_type=DEFAULT_ES_DOC_TYPE, id="142")
        self.assertEqual(doc['my_baz'], "content")

    def test_migrate_nested_fields_with_translation_map(self):
        requests.post('http://solr:8983/solr/test_core/schema', headers={'Content-Type': 'application/json'},
                      data='{ "add-field":{ "name":"nested_field", "type":"string", "stored":true }}')
        TestMigration.solr.add([{"id": "142", "nested_field": "content"}])

        self.solr2es.migrate('foo', '{"mappings": {"doc": {"properties": {"nested": {"type": "object"}}}}}',
                             TranslationMap({"nested_field": {"name": "nested.a.b.c"}}))

        doc = self.es.get_source(index='foo', doc_type=DEFAULT_ES_DOC_TYPE, id="142")
        self.assertEqual({'a': {'b': {'c': 'content'}}}, doc['nested'])

    def test_migrate_sibling_nested_fields_with_translation_map(self):
        requests.post('http://solr:8983/solr/test_core/schema', headers={'Content-Type': 'application/json'},
                      data='{ "add-field":{ "name":"nested_field1", "type":"string", "stored":true }}')
        requests.post('http://solr:8983/solr/test_core/schema', headers={'Content-Type': 'application/json'},
                      data='{ "add-field":{ "name":"nested_field2", "type":"string", "stored":true }}')
        TestMigration.solr.add([{"id": "142", "nested_field1": "content1", "nested_field2": "content2"}])

        self.solr2es.migrate('foo', '{"mappings": {"doc": {"properties": {"nested": {"type": "object"}}}}}',
                             TranslationMap({"nested_field1": {"name": "nested.a.b"}, "nested_field2": {"name": "nested.a.c"}}))

        doc = self.es.get_source(index='foo', doc_type=DEFAULT_ES_DOC_TYPE, id="142")
        self.assertEqual({'a': {'b': 'content1', 'c': 'content2'}}, doc['nested'])

    def test_migrate_sibling_nested_fields_with_wildcard(self):
        TestMigration.solr.add([{"id": "142", "nested_field1": "content1", "nested_field2": "content2"}])

        self.solr2es.migrate('foo',
                             '{"mappings": {"doc": {"properties": {"nested": {"type": "object"}}}}}',
                             TranslationMap({re.compile(r"nested_(.*)"): {"name": "nested.\\1"}}))

        doc = self.es.get_source(index='foo', doc_type=DEFAULT_ES_DOC_TYPE, id="142")
        self.assertEqual({'field1': 'content1', 'field2': 'content2'}, doc['nested'])

    def test_migrate_with_multiple_matching_fields_against_mapping(self):
        requests.post('http://solr:8983/solr/test_core/schema', headers={'Content-Type': 'application/json'},
                      data='{ "add-field":{ "name":"flag_field_test", "type":"string", "stored":true }}')
        TestMigration.solr.add([{"id": "142", "flag_field_test": "content1"}])

        with assert_raises(IllegalStateError) as e:
            self.solr2es.migrate('foo',
                                 '{"mappings": {"doc": {"properties": {"nested": {"type": "object"}}}}}',
                                 TranslationMap({re.compile(r"flag_field_(.*)"): {"name": "flag1_\\1"},
                                  re.compile(r"flag_(.*)"): {"name": "flag2_\\1"}}))
        self.assertTrue('Too many doc fields matching key flag_field_test in translation map' in str(e.exception))

    def test_migrate_with_multiple_matching_fields_one_exact(self):
        TestMigration.solr.add([{"id": "678", "title": "this is a title"}])

        self.solr2es.migrate('foo', None, TranslationMap({"title": {"name": "new_title"}, "ti(.*)": {"name": "regexp_title"}}))

        doc = self.es.get_source(index='foo', doc_type=DEFAULT_ES_DOC_TYPE, id="678")
        self.assertEqual('this is a title', doc['new_title'])

    def test_migrate_with_default_field_value_on_unexisting_field(self):
        TestMigration.solr.add([{"id": "142"}])

        self.solr2es.migrate('foo', '{"mappings": {"doc": {"properties": {"nested": {"type": "object"}}}}}',
                             TranslationMap({"new_field": {'default': 'john doe'}, 'field1': {'name': 'field1'}}))

        doc = self.es.get_source(index='foo', doc_type=DEFAULT_ES_DOC_TYPE, id="142")
        self.assertEqual('john doe', doc['new_field'])

    def test_migrate_with_default_field_value_on_multiple_fields(self):
        TestMigration.solr.add([{"id": "142"}])

        self.solr2es.migrate('foo', '{"mappings": {"doc": {"properties": {"nested": {"type": "object"}}}}}',
                             TranslationMap({"new_field1": {'default': 'john doe'},
                              'new_field2': {'default': 'bob smith'}}))

        doc = self.es.get_source(index='foo', doc_type=DEFAULT_ES_DOC_TYPE, id="142")
        self.assertEqual('john doe', doc['new_field1'])
        self.assertEqual('bob smith', doc['new_field2'])

    def test_migrate_with_default_field_value_on_existing_field(self):
        requests.post('http://solr:8983/solr/test_core/schema', headers={'Content-Type': 'application/json'},
                      data='{ "add-field":{ "name":"field1", "type":"string", "stored":true }}')
        TestMigration.solr.add([{"id": "142", "field1": "content1"}])

        self.solr2es.migrate('foo', '{"mappings": {"doc": {"properties": {"nested": {"type": "object"}}}}}',
                             TranslationMap({"field1": {'default': 'content2'}}))

        doc = self.es.get_source(index='foo', doc_type=DEFAULT_ES_DOC_TYPE, id="142")
        self.assertEqual('content1', doc['field1'])

    def test_migrate_with_ignored_fields(self):
        TestMigration.solr.add([{"id": "12"}])

        self.solr2es.migrate('foo', {}, TranslationMap({'_version_': {'ignore': True}}))

        doc = self.es.get_source(index='foo', doc_type=DEFAULT_ES_DOC_TYPE, id="12")
        self.assertEqual({"id": "12"}, doc)


class TestTranslateDoc(unittest.TestCase):
    def test_with_nested_field(self):
        self.assertEqual({'a': {'b': {'c': 'value'}}}, translate_doc({'a_b_c': 'value'}, TranslationMap({'a_b_c': {'name': 'a.b.c'}})))

    def test_with_empty_array_as_default_value(self):
        self.assertEqual({'array_field': []}, translate_doc({}, TranslationMap({'array_field': {'default': []}})))

    def test_with_multivalued_field(self):
        self.assertEqual({'mv_field': ['value1', 'value2']}, translate_doc({'mv_field': ['value1', 'value2']}, TranslationMap({})))

    def test_with_multivalued_field_ignored(self):
        self.assertEqual({'id': '1234', 'ignored_mv_field': 'value1'}, translate_doc({'id': '1234', 'ignored_mv_field': ['value1', 'value2']}, TranslationMap({'ignored_mv_field': {'multivalued': False}})))

    def test_with_nested_fields_and_multivalued_field(self):
        self.assertEqual({'a': {'b': ['value1']}}, translate_doc({'a_b': ['value1']}, TranslationMap({'a_b': {'name': 'a.b'}})))

    def test_with_sibling_nested_fields(self):
        self.assertEqual({'a': {'b': 'value1', 'c': 'value2'}},
                         translate_doc({'a_b': 'value1', 'a_c': 'value2'}, TranslationMap({'a_b': {'name': 'a.b'}, 'a_c': {'name': 'a.c'}})))

    def test_with_sibling_nested_fields_in_depth(self):
        self.assertEqual({'a': {'b': {'c': {'d': 'value1'}, 'e': 'value2'}}},
                         translate_doc({'a_b_c_d': 'value1', 'a_b_e': 'value2'},
                                       TranslationMap({'a_b_c_d': {'name': 'a.b.c.d'}, 'a_b_e': {'name': 'a.b.e'}})))


class TestTuplesToDict(unittest.TestCase):
    def test_with_simple_field(self):
        self.assertEqual({'a': 'b'}, _tuples_to_dict([('a', 'b')]))

    def test_with_two_simple_fields(self):
        self.assertEqual({'a': 'b', 'c': 'd'}, _tuples_to_dict((('a', 'b'), ('c', 'd'))))
        self.assertEqual({'a': 'b', 'c': 'd'}, _tuples_to_dict([('a', 'b'), ('c', 'd')]))

    def test_with_nested_one_value(self):
        self.assertEqual({'a': {'b': 'c'}}, _tuples_to_dict([('a', ('b', 'c'))]))

    def test_with_nested_two_values(self):
        self.assertEqual({'a': {'b': 'content1', 'c': 'content2'}},
                         _tuples_to_dict([('a', ('b', 'content1')), ('a', ('c', 'content2'))]))

    def test_with_two_nested_levels_homogeneous(self):
        self.assertEqual({'nested': {'a': {'b': 'content1', 'c': 'content2'}}},
                         _tuples_to_dict([('nested', ('a', ('b', 'content1'))), ('nested', ('a', ('c', 'content2')))]))

    def test_with_two_nested_levels_heterogeneous(self):
        self.assertEqual({'nested': {'a': {'b': 'content1', 'c': 'content2', 'd': 'content3'}}},
                         _tuples_to_dict([('nested', ('a', ('b', 'content1'))),
                                          ('nested', ('a', (('c', 'content2'), ('d', 'content3'))))]))


class TestCreateEsActions(unittest.TestCase):
    def test_create_es_actions(self):
        self.assertEqual('{"index": {"_index": "baz", "_type": "doc", "_id": "123"}}\n{"my_id": "123", "foo": "bar"}',
                         create_es_actions('baz', [{'my_id': '123', 'foo': 'bar'}], TranslationMap({'my_id': {'name': '_id'}})))

    def test_create_es_action_without_id_field_in_translation_map(self):
        self.assertEqual('{"index": {"_index": "baz", "_type": "doc", "_id": "123"}}\n{"id": "123", "foo": "bar"}',
                         create_es_actions('baz', [{'id': '123', 'foo': 'bar'}], TranslationMap()))

    def test_create_es_action_with_routing_field_in_translation_map(self):
        self.assertEqual('{"index": {"_index": "baz", "_type": "doc", "_id": "321", "_routing": "456"}}\n{"id": "321", "root_id": "456"}',
                         create_es_actions('baz', [{'id': '321', 'root_id': '456'}], TranslationMap({'root_id': {'routing_field': True}})))

    def test_create_es_action_with_routing_field_false(self):
        self.assertEqual('{"index": {"_index": "baz", "_type": "doc", "_id": "808"}}\n{"id": "808", "root_id": "654"}',
                         create_es_actions('baz', [{'id': '808', 'root_id': '654'}], TranslationMap({'root_id': {'routing_field': False}})))

    def test_create_es_action_with_nonexistent_routing_field(self):
        self.assertEqual('{"index": {"_index": "baz", "_type": "doc", "_id": "808"}}\n{"id": "808"}',
                         create_es_actions('baz', [{'id': '808'}], TranslationMap({'root_id': {'routing_field': True}})))

    @raises(IllegalStateError)
    def test_create_es_action_with_more_than_one_routing_field_in_translation_map(self):
        create_es_actions('baz', [{'id': '321'}], TranslationMap({'route1': {'routing_field': True},
                                                   'route2': {'routing_field': True}}))
