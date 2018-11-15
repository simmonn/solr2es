import aiohttp
import asynctest
from elasticsearch_async import AsyncElasticsearch


from solr2es.solr2es import DEFAULT_ES_DOC_TYPE, Solr2EsAsync


class TestMigrationAsync(asynctest.TestCase):
    async def setUp(self):
        self.aes = AsyncElasticsearch(hosts=['elasticsearch'])
        self.solr_url = 'http://solr:8983/solr/test_core'

    async def tearDown(self):
        async with aiohttp.ClientSession() as session:
            await session.post(self.solr_url + '/update/?commit=true', data='<delete><query>*:*</query></delete>', headers={'Content-type': 'text/xml; charset=utf-8'})
        await self.aes.delete_by_query(index='foo', doc_type=DEFAULT_ES_DOC_TYPE, body='{"query": {"match_all": {}}}', conflicts='proceed', refresh=True)
        await self.aes.transport.close()

    async def test_migrate_twelve_docs_async(self):
        async with aiohttp.ClientSession() as session:
            data = ['<doc><field name="id">id_%d</field><field name="title">A %d document</field></doc>' % (i, i)
                    for i in range(0, 12)]
            await session.post(self.solr_url + '/update/?commit=true', data='<add>%s</add>' % ''.join(data),
                               headers={'Content-type': 'text/xml; charset=utf-8'})
            solr2es_async = Solr2EsAsync(session, self.aes, self.solr_url, True)
            self.assertEqual(12, await solr2es_async.migrate('foo'))
