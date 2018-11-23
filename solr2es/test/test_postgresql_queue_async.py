from json import loads

import asynctest
from aiopg import create_pool

from solr2es.postgresql_queue import PostgresqlQueueAsync


class TestPostgresqlQueueAsync(asynctest.TestCase):

    async def setUp(self):
        self.postgresql = await create_pool('dbname=solr2es user=test password=test host=postgresql')
        self.pgsql_queue = PostgresqlQueueAsync(self.postgresql)

    async def tearDown(self):
        async with self.postgresql.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute('TRUNCATE solr2es_queue')
        self.postgresql.close()

    async def test_push_loop(self):
        async def producer():
            yield [{'id': 'extract_1', 'foo': 'bar'}, {'id': 'extract_2', 'toot': 'toot'}]
            yield [{'id': 'extract_3', 'baz': 'qux'}]

        await self.pgsql_queue.push_loop(producer)

        async with self.postgresql.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute('SELECT id, json FROM solr2es_queue ORDER BY id')
                results = await cur.fetchall()

                self.assertEqual(3, len(results))
                self.assertEqual({'id': 'extract_1', 'foo': 'bar'}, loads(results[0][1]))
                self.assertEqual({'id': 'extract_2', 'toot': 'toot'}, loads(results[1][1]))
                self.assertEqual({'id': 'extract_3', 'baz': 'qux'}, loads(results[2][1]))

    async def test_push_pop_not_event_based(self):
        docs = [{'id': 'id1', 'foo': 'bar'}, {'id': 'id2', 'baz': 'qux'}]

        await self.pgsql_queue.push(docs)

        self.assertEqual(docs, await self.pgsql_queue.pop())
        self.assertEqual([], await self.pgsql_queue.pop())
