from json import loads

import asynctest
from aiopg import create_pool

from solr2es.postgresql_queue import PostgresqlQueueAsync


class TestPostgresqlQueueAsync(asynctest.TestCase):

    async def setUp(self):
        self.postgresql = await create_pool('dbname=solr2es user=test password=test host=postgresql')

    async def tearDown(self):
        async with self.postgresql.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute('TRUNCATE solr2es_queue')
        self.postgresql.close()

    async def test_push(self):
        async def producer():
            yield [{'extract_id': 'extract_1', 'foo': 'bar'}, {'extract_id': 'extract_2', 'toot': 'toot'}]
            yield [{'extract_id': 'extract_3', 'baz': 'qux'}]

        await PostgresqlQueueAsync(self.postgresql).push(producer)

        async with self.postgresql.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute('SELECT extract_id, json FROM solr2es_queue ORDER BY extract_id')
                results = list()
                async for row in cur:
                    results.append(row)

                self.assertEqual(3, len(results))
                self.assertEqual({'extract_id': 'extract_1', 'foo': 'bar'}, loads(results[0][1]))
                self.assertEqual({'extract_id': 'extract_2', 'toot': 'toot'}, loads(results[1][1]))
                self.assertEqual({'extract_id': 'extract_3', 'baz': 'qux'}, loads(results[2][1]))
