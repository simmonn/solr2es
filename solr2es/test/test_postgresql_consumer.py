from json import loads

import psycopg2
import unittest

from solr2es.postgresql_consumer import PostgresqlConsumer


class TestPostgresqlConsumer(unittest.TestCase):

    def setUp(self):
        self.connection = psycopg2.connect("dbname='solr2es' user='test' host='postgresql' password='test'")

    def tearDown(self):
        self.connection.cursor().execute('truncate solr2es_queue')
        self.connection.close()

    def test_consume(self):
        def producer():
            yield [{'extract_id': 'extract_1', 'foo': 'bar'}, {'extract_id': 'extract_2', 'toot': 'toot'}]
            yield [{'extract_id': 'extract_3', 'baz': 'qux'}]

        PostgresqlConsumer(self.connection).consume(producer)

        cur = self.connection.cursor()
        cur.execute('SELECT extract_id, json, done FROM solr2es_queue ORDER BY extract_id')
        results = list(cur.fetchall())

        self.assertEqual(3, len(results))
        self.assertEqual({'extract_id': 'extract_1', 'foo': 'bar'}, loads(results[0][1]))
        self.assertEqual({'extract_id': 'extract_2', 'toot': 'toot'}, loads(results[1][1]))
        self.assertEqual({'extract_id': 'extract_3', 'baz': 'qux'}, loads(results[2][1]))
        cur.close()
