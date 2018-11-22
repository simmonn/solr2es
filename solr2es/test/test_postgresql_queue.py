from json import loads

import psycopg2
import unittest

from solr2es.postgresql_queue import PostgresqlQueue


class TestPostgresqlQueue(unittest.TestCase):

    def setUp(self):
        self.connection = psycopg2.connect("dbname='solr2es' user='test' host='postgresql' password='test'")

    def tearDown(self):
        self.connection.cursor().execute('truncate solr2es_queue')
        self.connection.close()

    def test_push(self):
        def producer():
            yield [{'id': 'extract_1', 'foo': 'bar'}, {'id': 'extract_2', 'toot': 'toot'}]
            yield [{'id': 'extract_3', 'baz': 'qux'}]

        PostgresqlQueue(self.connection).push_loop(producer)

        cur = self.connection.cursor()
        cur.execute('SELECT id, json, done FROM solr2es_queue ORDER BY id')
        results = list(cur.fetchall())

        self.assertEqual(3, len(results))
        self.assertEqual({'id': 'extract_1', 'foo': 'bar'}, loads(results[0][1]))
        self.assertEqual({'id': 'extract_2', 'toot': 'toot'}, loads(results[1][1]))
        self.assertEqual({'id': 'extract_3', 'baz': 'qux'}, loads(results[2][1]))
        cur.close()
