#!/usr/bin/env python
import asyncio
import getopt
import itertools
import logging
import sys
from json import loads, dumps

import aiohttp
import asyncio_redis
import redis
from elasticsearch import Elasticsearch
from elasticsearch_async import AsyncElasticsearch
from pysolr import Solr, SolrCoreAdmin

logging.basicConfig(format='%(asctime)s [%(name)s] %(levelname)s: %(message)s')
LOGGER = logging.getLogger('solr2es')
LOGGER.setLevel(logging.INFO)

DEFAULT_ES_DOC_TYPE = 'doc'


class Solr2Es(object):
    def __init__(self, solr, es, refresh=False) -> None:
        super().__init__()
        self.solr = solr
        self.es = es
        self.refresh = refresh

    def migrate(self, index_name, mapping=None, translation_map=None) -> int:
        translation_dict = dict() if translation_map is None else translation_map
        nb_results = 0
        if not self.es.indices.exists([index_name]):
            self.es.indices.create(index_name, body=mapping)
        for results in self.produce_results():
            actions = create_es_actions(index_name, results, translation_dict)
            response = self.es.bulk(actions, index_name, DEFAULT_ES_DOC_TYPE, refresh=self.refresh)
            nb_results += len(results)
            if response['errors']:
                for err in response['items']:
                    LOGGER.warning(err)
                nb_results -= len(response['items'])
        LOGGER.info('processed %s documents', nb_results)
        return nb_results

    def produce_results(self):
        nb_results = 0
        nb_total = 0
        cursor_ended = False
        kwargs = dict(cursorMark='*', sort='id asc')
        while not cursor_ended:
            results = self.solr.search('*:*', **kwargs)
            if kwargs['cursorMark'] == '*':
                nb_total = results.hits
                LOGGER.info('found %s documents', nb_total)
            if kwargs['cursorMark'] != results.nextCursorMark:
                kwargs['cursorMark'] = results.nextCursorMark
                nb_results += len(results)
                if nb_results % 10000 == 0:
                    LOGGER.info('read %s docs of %s (%s %% done)', nb_results, nb_total, (100 * nb_results)/nb_total)
                yield results
            else:
                cursor_ended = True


class Solr2EsAsync(object):
    def __init__(self, aiohttp_session, aes, solr_url, refresh=False) -> None:
        super().__init__()
        self.solr_url = solr_url
        self.aiohttp_session = aiohttp_session
        self.aes = aes
        self.refresh = refresh

    async def migrate(self, index_name) -> int:
        nb_results = 0
        async for results in self.produce_results():
            actions = create_es_actions(index_name, results, {})
            await self.aes.bulk(actions, index_name, DEFAULT_ES_DOC_TYPE, refresh=self.refresh)
            nb_results += len(results)
        return nb_results

    async def produce_results(self):
        cursor_ended = False
        nb_results = 0
        nb_total = 0
        kwargs = dict(cursorMark='*', sort='id asc', q='*:*', wt='json')
        while not cursor_ended:
            async with self.aiohttp_session.get(self.solr_url + '/select/', params=kwargs) as resp:
                json = loads(await resp.text())
                if kwargs['cursorMark'] == '*':
                    nb_total = int(json['response']['numFound'])
                    LOGGER.info('found %s documents', json['response']['numFound'])
                if kwargs['cursorMark'] != json['nextCursorMark']:
                    kwargs['cursorMark'] = json['nextCursorMark']
                    nb_results += len(json['response']['docs'])
                    if nb_results % 10000 == 0:
                        LOGGER.info('read %s docs of %s (%s %% done)', nb_results, nb_total,
                                    (100 * nb_results) / nb_total)
                    yield json['response']['docs']
                else:
                    cursor_ended = True
        LOGGER.info('processed %s documents', nb_results)


class RedisConsumer(object):
    def __init__(self, redis) -> None:
        self.redis = redis

    def consume(self, producer):
        for results in producer():
            self.redis.lpush('solr2es:queue', *map(dumps, results))


class RedisConsumerAsync(object):
    def __init__(self, redis) -> None:
        self.redis = redis

    async def consume(self, producer):
        async for results in producer():
            await self.redis.lpush('solr2es:queue', list(map(dumps, results)))


def create_es_actions(index_name, solr_results, translation_map):
    results_ = [({'index': {'_index': index_name, '_type': DEFAULT_ES_DOC_TYPE, '_id': row['id']}}, translate_doc(row, translation_map))
                for row in solr_results]
    return '\n'.join(list(map(lambda d: dumps(d), itertools.chain(*results_))))


def translate_doc(row, translation_map):
    def translate(key, value):
        translated_key = translation_map.get(key, dict()).get('name', key)

        translated_value = value
        if type(value) is list:
            translated_value = value[0]

        if '.' in translated_key:
            translated_value = dotkey_nested_dict(translated_key.split('.')[1:], translated_value)
            translated_key = translated_key.split('.')[0]

        return translated_key, translated_value

    d = tuple(translate(k, v) for k, v in row.items())
    return tuples_to_dict(d)


def merge_dict(a, b):
    for (k, v), (k2, v2) in zip(a.items(), b.items()):
        if k == k2:
            return {k: merge_dict(v, v2)}
        else:
            return {k: v, k2: v2}


def tuples_to_dict(tuples):
    ret = dict()
    for k, v in tuples:
        if isinstance(v, tuple):
            if k in ret:
                ret[k] = merge_dict(ret[k], tuples_to_dict([v]))
            else :
                ret[k] = tuples_to_dict([v])
        else:
            ret[k] = v
    return ret


def dotkey_nested_dict(key_list, value):
    if len(key_list) == 1:
        return key_list[0], value
    last_key = key_list[-1]
    return dotkey_nested_dict(key_list[0:-1], (last_key, value))


def dump_into_redis(solrhost, redishost):
    LOGGER.info('dump from solr (%s) into redis (host=%s)', solrhost, redishost)
    RedisConsumer(redis.Redis(host=redishost)).consume(Solr2Es(Solr(solrhost, always_commit=True), None).produce_results)


def resume_from_redis(redishost, eshost, name):
    LOGGER.info('resume from redis (host=%s) to elasticsearch (%s) index %s', redishost, eshost, name)


def migrate(solrhost, eshost, name):
    LOGGER.info('migrate from solr (%s) into elasticsearch (%s) index %s', solrhost, eshost, name)
    Solr2Es(Solr(solrhost, always_commit=True), Elasticsearch(host=eshost)).migrate(name)


async def aiodump_into_redis(solrhost, redishost):
    LOGGER.info('asyncio dump from solr (%s) into redis (host=%s)', solrhost, redishost)
    async with aiohttp.ClientSession() as session:
        await RedisConsumerAsync(await asyncio_redis.Pool.create(host=redishost, port=6379, poolsize=10)).\
            consume(Solr2EsAsync(session, None, solrhost).produce_results)


async def aioresume_from_redis(redishost, eshost, name):
    LOGGER.info('asyncio resume from redis (host=%s) to elasticsearch (%s) index %s', redishost, eshost, name)


async def aiomigrate(solrhost, eshost, name):
    LOGGER.info('asyncio migrate from solr (%s) into elasticsearch (%s) index %s', solrhost, eshost, name)
    async with aiohttp.ClientSession() as session:
        await Solr2EsAsync(session, AsyncElasticsearch(hosts=[eshost]), solrhost).migrate(name)


def usage(argv):
    print('Usage: %s action' % argv[0])
    print('\t-m|--migrate: migrate solr to elasticsearch')
    print('\t-r|--resume: resume from redis')
    print('\t-d|--dump: dump into redis')
    print('\t-t|--test: test solr/elasticsearch connections')
    print('\t-a|--async: use python 3 asyncio')
    print('\t--solrhost: solr host (default solr)')
    print('\t--index: index name (default solr core name)')
    print('\t--core: core name (default solr2es)')
    print('\t--eshost: elasticsearch url (default elasticsearch)')
    print('\t--redishost: redis host (default redis)')


if __name__ == '__main__':
    options, remainder = getopt.gnu_getopt(sys.argv[1:], 'hmdtra',
            ['help', 'migrate', 'dump', 'test', 'resume', 'async', 'solrhost=', 'eshost=', 'redishost=', 'index=', 'core='])
    if len(sys.argv) == 1:
        usage(sys.argv)
        sys.exit()

    aioloop = asyncio.get_event_loop()
    with_asyncio = False
    solrhost = 'solr'
    eshost = 'elasticsearch'
    redishost = 'redis'
    core_name = 'solr2es'
    index_name = None
    action = 'migrate'
    for opt, arg in options:
        if opt in ('-h', '--help'):
            usage(sys.argv)
            sys.exit()

        if opt in ('-a', '--async'):
            with_asyncio = True

        if opt == '--solrhost':
            solrhost = arg

        if opt == '--redishost':
            redishost = arg

        if opt == '--eshost':
            eshost = arg

        if opt == '--index':
            index_name = arg

        if opt == '--core':
            core_name = arg

        if opt in ('-d', '--dump'):
            action = 'dump'
        elif opt in ('-r', '--resume'):
            action = 'resume'
        elif opt in ('-m', '--migrate'):
            action = 'migrate'
        elif opt in ('-t', '--test'):
            action = 'test'

    if index_name is None:
        index_name = core_name

    solrurl = 'http://%s:8983/solr/%s' % (solrhost, core_name)

    if action == 'migrate':
        aioloop.run_until_complete(aiomigrate(solrurl, eshost, index_name)) if with_asyncio else migrate(solrurl, eshost, index_name)
    elif action == 'dump':
        aioloop.run_until_complete(aiodump_into_redis(solrurl, redishost)) if with_asyncio else dump_into_redis(solrurl, redishost)
    elif action == 'resume':
        aioloop.run_until_complete(aioresume_from_redis(redishost, eshost, index_name)) if with_asyncio else resume_from_redis(redishost, eshost, index_name)
    elif action == 'test':
        solr_status = loads(SolrCoreAdmin('http://%s:8983/solr/admin/cores?action=STATUS&core=%s' % (solrhost, core_name)).status())
        LOGGER.info('Elasticsearch ping on %s is %s', eshost, 'OK' if Elasticsearch(host=eshost).ping() else 'KO')
        LOGGER.info('Solr status on %s is %s', solrurl, 'OK' if solr_status['status'][core_name] else 'KO')
