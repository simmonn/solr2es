from asyncio import ensure_future, wait_for
from json import loads

import asynctest
from aiopg.sa import create_engine

from solr2es.postgresql_queue import PostgresqlQueueAsync


class TestPostgresqlQueueAsync(asynctest.TestCase):

    async def setUp(self):
        self.engine = await create_engine(user='test', database='solr2es', host='postgresql', password='test')
        self.pgsql_queue = await PostgresqlQueueAsync.create(self.engine)

    async def tearDown(self):
        async with self.engine.acquire() as conn:
            await conn.execute('TRUNCATE solr2es_queue')
        await self.pgsql_queue.close()

    async def test_push_loop(self):
        async def producer():
            yield [{'id': 'extract_1', 'foo': 'bar'}, {'id': 'extract_2', 'toot': 'toot'}]
            yield [{'id': 'extract_3', 'baz': 'qux'}]

        await self.pgsql_queue.push_loop(producer)

        async with self.engine.acquire() as conn:
            result_proxy = await conn.execute('SELECT id, json FROM solr2es_queue ORDER BY id')
            results = await result_proxy.fetchall()

            self.assertEqual(3, len(results))
            self.assertEqual({'id': 'extract_1', 'foo': 'bar'}, loads(results[0][1]))
            self.assertEqual({'id': 'extract_2', 'toot': 'toot'}, loads(results[1][1]))
            self.assertEqual({'id': 'extract_3', 'baz': 'qux'}, loads(results[2][1]))

    async def test_push_pop_sequential(self):
        docs = [{'id': 'id1', 'foo': 'bar'}, {'id': 'id2', 'baz': 'qux'}]

        await self.pgsql_queue.push(docs)

        self.assertEqual(docs, await self.pgsql_queue.pop())

    async def test_push_single_quote(self):
        docs = [{'id': 'id1', 'foo': 'ba\'r'}]

        await self.pgsql_queue.push(docs)

        self.assertEqual(docs, await self.pgsql_queue.pop())

    async def test_queue_size(self):
        self.assertEqual(0, await self.pgsql_queue.size())
        await self.pgsql_queue.push([{'id': 'id1', 'foo': 'bar'}])
        self.assertEqual(1, await self.pgsql_queue.size())
        await self.pgsql_queue.pop()
        self.assertEqual(0, await self.pgsql_queue.size())

    async def test_push_pop_blocking(self):
        docs = [{'id': 'id1', 'foo': 'bar'}, {'id': 'id2', 'baz': 'qux'}]
        pop_future = ensure_future(self.pgsql_queue.pop())
        await self.pgsql_queue.push(docs)

        results = await wait_for(pop_future, 1)

        self.assertEqual(docs, results)

    async def test_push_pop_concurrent(self):
        docs = [{'id': 'id1', 'foo': 'bar'}, {'id': 'id2', 'baz': 'qux'}, {'id': 'id3', 'bax': 'qiz'}]
        pop_future1 = ensure_future(self.pgsql_queue.pop(timeout=0.5))
        pop_future2 = ensure_future(self.pgsql_queue.pop(timeout=0.5))
        await self.pgsql_queue.push(docs)

        results1 = await wait_for(pop_future1, 1)
        results2 = await wait_for(pop_future2, 1)

        self.assertEqual(docs, results1)
        self.assertEqual([], results2)

    async def test_pop_timeout(self):
        self.assertEqual([], await self.pgsql_queue.pop())

    async def test_push_on_conflict_do_nothing(self):
        docs = [{'id': 'id1', 'duplicate': 'true'}, {'id': 'id1', 'duplicate': 'true'}]

        await self.pgsql_queue.push(docs)

        self.assertEqual([{'id': 'id1', 'duplicate': 'true'}], await self.pgsql_queue.pop())
