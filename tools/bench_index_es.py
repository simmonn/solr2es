import asyncio
import logging
from json import dumps

from elasticsearch_async import AsyncElasticsearch, AsyncTransport

logging.basicConfig(format='%(asctime)s [%(name)s][%(process)d] %(levelname)s: %(message)s')
LOGGER = logging.getLogger('esbench')
LOGGER.setLevel(logging.INFO)

BULK_SIZE = 10
TOTAL_DOCS = 2000000
INDEX_NAME = 'bench'
DOC_TYPE = '_doc'


async def bench(es):
    nb_indexed = 0
    for i in range(1, TOTAL_DOCS, BULK_SIZE):
        bulk_content = str()
        for bulk_index in range(i, i + BULK_SIZE):
            es_action = dumps({'index': {'_index': INDEX_NAME, '_type': DOC_TYPE, '_id': i}}) + '\n' + \
                        dumps({'field': 'my content %d' % bulk_index}) + '\n'
            bulk_content += es_action
        await es.bulk(bulk_content, INDEX_NAME, DOC_TYPE, refresh=True)
        nb_indexed += BULK_SIZE
        if nb_indexed % 1000 == 0:
            LOGGER.info('indexed %s docs of %s (%.2f %% done)', nb_indexed, TOTAL_DOCS, (100 * nb_indexed) / TOTAL_DOCS)

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    elasticsearch = AsyncElasticsearch(['elasticsearch'], AsyncTransport, timeout=60)
    try:
        loop.run_until_complete(bench(elasticsearch))
    finally:
        loop.run_until_complete(elasticsearch.transport.close())
