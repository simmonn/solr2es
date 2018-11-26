import aiohttp
import asynctest
from aiopg import create_pool
from elasticsearch_async import AsyncElasticsearch


from solr2es.__main__ import DEFAULT_ES_DOC_TYPE, Solr2EsAsync
from solr2es.postgresql_queue import PostgresqlQueueAsync


class TestMigrationAsync(asynctest.TestCase):
    async def setUp(self):
        self.aes = AsyncElasticsearch(hosts=['elasticsearch'])
        self.solr_url = 'http://solr:8983/solr/test_core'

    async def tearDown(self):
        async with aiohttp.ClientSession() as session:
            await session.post(self.solr_url + '/update/?commit=true', data='<delete><query>*:*</query></delete>', headers={'Content-type': 'text/xml; charset=utf-8'})
        await self.aes.indices.delete(index='foo')
        await self.aes.transport.close()

    async def test_migrate_twelve_docs(self):
        async with aiohttp.ClientSession() as session:
            data = ['<doc><field name="id">id_%d</field><field name="title">A %d document</field></doc>' % (i, i)
                    for i in range(0, 12)]
            await session.post(self.solr_url + '/update/?commit=true', data='<add>%s</add>' % ''.join(data),
                               headers={'Content-type': 'text/xml; charset=utf-8'})
            solr2es_async = Solr2EsAsync(session, self.aes, self.solr_url, True)
            self.assertEqual(12, await solr2es_async.migrate('foo'))

    async def test_migrate_with_es_mapping(self):
        async with aiohttp.ClientSession() as session:
            mapping = '{"mappings": {"doc": {"properties": {"my_field": {"type": "keyword"}}}}}'
            await Solr2EsAsync(session, self.aes, self.solr_url, True).migrate('foo', mapping=mapping)
            self.assertEqual({'my_field': {'type': 'keyword'}},
                             (await self.aes.indices.get_field_mapping(index=['foo'], fields=['my_field']))
                             ['foo']['mappings']['doc']['my_field']['mapping'])


class TestResumeAsync(asynctest.TestCase):

    async def setUp(self):
        self.aes = AsyncElasticsearch(hosts=['elasticsearch'])
        self.postgresql = await create_pool('dbname=solr2es user=test password=test host=postgresql')

    async def tearDown(self):
        await self.aes.indices.delete(index='test')
        await self.aes.transport.close()
        async with self.postgresql.acquire() as conn:
            async with conn.cursor() as cur:
                await cur.execute('TRUNCATE solr2es_queue')
        self.postgresql.close()

    async def test_resume_from_postgresql_to_elasticsearch(self):
        postgresql_queue = await PostgresqlQueueAsync.create(self.postgresql)
        await postgresql_queue.push([{'id': 'id1', 'name': 'john doe'}, {'id': 'id2', 'name': 'bob smith'}])

        await Solr2EsAsync(None, self.aes, None, refresh=True).resume(postgresql_queue, 'test')

        self.assertEqual(2, (await self.aes.count('test', DEFAULT_ES_DOC_TYPE))['count'])
