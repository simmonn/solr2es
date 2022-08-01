import aiohttp
import asynctest
from elasticsearch_async import AsyncElasticsearch

from solr2es.__main__ import Solr2EsAsync


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
            index_body = '{"mappings": {"doc": {"properties": {"my_field": {"type": "keyword"}}}}}'
            await Solr2EsAsync(session, self.aes, self.solr_url, True).migrate('foo', es_index_body_str=index_body)
            self.assertEqual({'my_field': {'type': 'keyword'}},
                             (await self.aes.indices.get_field_mapping(index=['foo'], fields=['my_field']))
                             ['foo']['mappings']['doc']['my_field']['mapping'])

