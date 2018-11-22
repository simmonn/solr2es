import asyncio
import getopt
import logging
import re
import sys
from collections import Mapping
from functools import partial, reduce
from itertools import chain
from json import loads, dumps

import aiohttp
import asyncio_redis
import redis
from aiopg import create_pool
from elasticsearch import Elasticsearch
from elasticsearch_async import AsyncElasticsearch
from pysolr import Solr, SolrCoreAdmin

from solr2es.postgresql_queue import PostgresqlQueueAsync, PostgresqlQueue
from solr2es.redis_queue import RedisQueueAsync, RedisQueue

logging.basicConfig(format='%(asctime)s [%(name)s][%(process)d] %(levelname)s: %(message)s')
LOGGER = logging.getLogger('solr2es')
LOGGER.setLevel(logging.INFO)

DEFAULT_ES_DOC_TYPE = 'doc'


class Solr2Es(object):
    def __init__(self, solr, es, refresh=False) -> None:
        super().__init__()
        self.solr = solr
        self.es = es
        self.refresh = refresh

    def migrate(self, index_name, mapping=None, translation_map=None, solr_filter_query='*', sort_field='id') -> int:
        translation_dict = dict() if translation_map is None else translation_map
        nb_results = 0
        if not self.es.indices.exists([index_name]):
            self.es.indices.create(index_name, body=mapping)
        for results in self.produce_results(solr_filter_query=solr_filter_query, sort_field=sort_field):
            actions = create_es_actions(index_name, results, translation_dict)
            response = self.es.bulk(actions, index_name, DEFAULT_ES_DOC_TYPE, refresh=self.refresh)
            nb_results += len(results)
            if response['errors']:
                for err in response['items']:
                    LOGGER.warning(err)
                nb_results -= len(response['items'])
        LOGGER.info('processed %s documents', nb_results)
        return nb_results

    def produce_results(self, solr_filter_query='*', sort_field='id'):
        nb_results = 0
        nb_total = 0
        cursor_ended = False
        kwargs = dict(fq=solr_filter_query, cursorMark='*', sort='%s asc' % sort_field)
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

    async def migrate(self, index_name, solr_filter_query='*', sort_field='id') -> int:
        nb_results = 0
        async for results in self.produce_results(solr_filter_query=solr_filter_query, sort_field=sort_field):
            actions = create_es_actions(index_name, results, {})
            await self.aes.bulk(actions, index_name, DEFAULT_ES_DOC_TYPE, refresh=self.refresh)
            nb_results += len(results)
        return nb_results

    async def produce_results(self, solr_filter_query='*', sort_field='id'):
        cursor_ended = False
        nb_results = 0
        nb_total = 0
        kwargs = dict(cursorMark='*', sort='%s asc' % sort_field, q='*:*', wt='json', fq=solr_filter_query)
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


def create_es_actions(index_name, solr_results, translation_map) -> str:
    default_values = {k: v['default_value'] for k, v in translation_map.items() if 'default_value' in v}
    translation_names = {k: v['name'] for k, v in translation_map.items() if 'name' in v}

    results = [({'index': {'_index': index_name, '_type': DEFAULT_ES_DOC_TYPE, '_id': row['id']}}, translate_doc(row, translation_names, default_values))
                for row in solr_results]
    return '\n'.join(list(map(lambda d: dumps(d), chain(*results))))


def translate_doc(row, translation_names, default_values) -> dict:
    def translate(key, value):
        translated_key = _translate_key(key, translation_names)
        translated_value = value[0] if type(value) is list else value

        if '.' in translated_key:
            translated_value = reduce(lambda i, acc: (acc, i), reversed(translated_key.split('.')[1:] + [value]))
            translated_key = translated_key.split('.')[0]
        return translated_key, translated_value

    defaults = default_values.copy()
    defaults.update(row)
    translated = tuple(translate(k, v) for k, v in defaults.items())
    return _tuples_to_dict(translated)


def _translate_key(key, translation_names) -> str:
    matched_fields = (((k, v) for k, v in translation_names.items() if re.search(k, key)))
    try:
        key_regexp, value_regexp = next(matched_fields)
        return re.sub(key_regexp, value_regexp, key)
    except StopIteration:
        return key


def _tuples_to_dict(tuples) -> dict:
    ret = dict()
    for k, v in tuples:
        if type(v) is tuple or type(v) is list:
            d = _tuples_to_dict(v) if type(v[0]) is tuple else _tuples_to_dict([v])
            ret[k] = deep_update(ret.get(k, {}), d)
        else:
            ret[k] = v
    return ret


def deep_update(d, u):
    """
    from https://stackoverflow.com/questions/3232943/update-value-of-a-nested-dictionary-of-varying-depth
    :param d: dict
    :param u: dict
    :return: merged dict
    """
    for k, v in u.items():
        if isinstance(v, Mapping):
            d[k] = deep_update(d.get(k, {}), v)
        else:
            d[k] = v
    return d


def dump_into_redis(solrhost, redishost, solrfq, solrid):
    LOGGER.info('dump from solr (%s) into redis (host=%s) with filter query (%s)', solrhost, redishost, solrfq)
    RedisQueue(redis.Redis(host=redishost)).push_loop(partial(Solr2Es(Solr(solrhost, always_commit=True), None).produce_results,
                                                              solr_filter_query=solrfq, sort_field=solrid))


def dump_into_pgsql(solrhost, pgsqldsn, solrfq, solrid):
    LOGGER.info('dump from solr (%s) into postgresql (dsn=%s) with filter query (%s)', solrhost, pgsqldsn, solrfq)
    PostgresqlQueue(None).push_loop(partial(Solr2Es(Solr(solrhost, always_commit=True), None).produce_results,
                                                         solr_filter_query=solrfq, sort_field=solrid))


def resume_from_redis(redishost, eshost, name):
    LOGGER.info('resume from redis (host=%s) to elasticsearch (%s) index %s', redishost, eshost, name)


def resume_from_postgresql(pgsqldsn, eshost, name):
    LOGGER.info('resume from postgresql (dsn=%s) to elasticsearch (%s) index %s', pgsqldsn, eshost, name)


def migrate(solrhost, eshost, index_name, solrfq, solrid):
    LOGGER.info('migrate from solr (%s) into elasticsearch (%s) index %s and filter query (%s)', solrhost, eshost, index_name, solrfq)
    Solr2Es(Solr(solrhost, always_commit=True), Elasticsearch(host=eshost)).migrate(index_name, solr_filter_query=solrfq, sort_field=solrid)


async def aiodump_into_redis(solrhost, redishost, solrfq, solrid):
    LOGGER.info('asyncio dump from solr (%s) into redis (host=%s) with filter query (%s)', solrhost, redishost, solrfq)
    async with aiohttp.ClientSession() as session:
        await RedisQueueAsync(await asyncio_redis.Pool.create(host=redishost, port=6379, poolsize=10)).\
            push_loop(partial(Solr2EsAsync(session, None, solrhost).produce_results, solr_filter_query=solrfq, sort_field=solrid))


async def aiodump_into_pgsql(solrhost, pgsqldsn, solrfq, solrid):
    LOGGER.info('asyncio dump from solr (%s) into postgresql (dsn=%s) with filter query (%s)', solrhost, redishost, solrfq)
    async with aiohttp.ClientSession() as session:
        await PostgresqlQueueAsync(await create_pool(pgsqldsn)).\
            push_loop(partial(Solr2EsAsync(session, None, solrhost).produce_results, solr_filter_query=solrfq, sort_field=solrid))


async def aioresume_from_redis(redishost, eshost, name):
    LOGGER.info('asyncio resume from redis (host=%s) to elasticsearch (%s) index %s', redishost, eshost, name)


async def aioresume_from_pgsql(pgsqldsn, eshost, name):
    LOGGER.info('asyncio resume from postgresql (dsn=%s) to elasticsearch (%s) index %s', pgsqldsn, eshost, name)


async def aiomigrate(solrhost, eshost, name, solrfq, solrid):
    LOGGER.info('asyncio migrate from solr (%s) into elasticsearch (%s) index %s with filter query (%s) and with id (%s)',
                solrhost, eshost, name, solrfq, solrid)
    async with aiohttp.ClientSession() as session:
        await Solr2EsAsync(session, AsyncElasticsearch(hosts=[eshost]), solrhost).migrate(
            name, solr_filter_query=solrfq, sort_field=solrid)


def usage(argv):
    print('Usage: %s action' % argv[0])
    print('\t-m|--migrate: migrate solr to elasticsearch')
    print('\t-r|--resume: resume from redis')
    print('\t-d|--dump: dump into redis (default) or posgresql (if dsn given) queue')
    print('\t-t|--test: test solr/elasticsearch connections')
    print('\t-a|--async: use python 3 asyncio')
    print('\t--solrhost: solr host (default \'solr\')')
    print('\t--solrfq: solr filter query (default \'*\')')
    print('\t--solrid: solr id field name (default \'id\')')
    print('\t--index: index name (default solr core name)')
    print('\t--core: core name (default \'solr2es\')')
    print('\t--eshost: elasticsearch url (default \'elasticsearch\')')
    print('\t--redishost: redis host (default \'redis\')')
    print('\t--postgresqldsn: postgresql Data Source Name')
    print('\t  (ex \'dbname=solr2es user=test password=test host=postgresql\' default None)')


def main():
    options, remainder = getopt.gnu_getopt(sys.argv[1:], 'hmdtra',
            ['help', 'migrate', 'dump', 'test', 'resume', 'async', 'solrhost=', 'eshost=',
             'redishost=', 'index=', 'core=', 'solrfq=', 'solrid=', 'postgresqldsn='])
    if len(sys.argv) == 1:
        usage(sys.argv)
        sys.exit()

    aioloop = asyncio.get_event_loop()
    with_asyncio = False
    solrhost = 'solr'
    solrfq = '*'
    solrid = 'id'
    eshost = 'elasticsearch'
    redishost = 'redis'
    postgresqldsn = None
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

        if opt == '--solrfq':
            solrfq = arg

        if opt == '--solrid':
            solrid = arg

        if opt == '--redishost':
            redishost = arg

        if opt == '--postgresqldsn':
            postgresqldsn = arg

        if opt == '--eshost':
            eshost = arg

        if opt == '--index':
            index_name = arg

        if opt == '--core':
            core_name = arg

        if opt in ('-d', '--dump'):
            action = 'dump' if postgresqldsn is None else 'dump_pgsql'
        elif opt in ('-r', '--resume'):
            action = 'resume' if postgresqldsn is None else 'resume_pgsql'
        elif opt in ('-m', '--migrate'):
            action = 'migrate'
        elif opt in ('-t', '--test'):
            action = 'test'

    if index_name is None:
        index_name = core_name

    solrurl = 'http://%s:8983/solr/%s' % (solrhost, core_name)

    if action == 'migrate':
        aioloop.run_until_complete(aiomigrate(solrurl, eshost, index_name, solrfq, solrid)) if with_asyncio \
            else migrate(solrurl, eshost, index_name, solrfq, solrid)
    elif action == 'dump':
        aioloop.run_until_complete(aiodump_into_redis(solrurl, redishost, solrfq, solrid)) if with_asyncio \
            else dump_into_redis(solrurl, redishost, solrfq, solrid)
    elif action == 'resume':
        aioloop.run_until_complete(aioresume_from_redis(redishost, eshost, index_name)) if with_asyncio else resume_from_redis(redishost, eshost, index_name)
    elif action == 'dump_pgsql':
        aioloop.run_until_complete(aiodump_into_pgsql(solrurl, postgresqldsn, solrfq, solrid)) if with_asyncio \
            else dump_into_pgsql(solrurl, postgresqldsn, solrfq, solrid)
    elif action == 'resume_pgsql':
        aioloop.run_until_complete(
            aioresume_from_pgsql(postgresqldsn, eshost, index_name)) if with_asyncio else resume_from_postgresql(postgresqldsn, eshost, index_name)
    elif action == 'test':
        solr_status = loads(SolrCoreAdmin('http://%s:8983/solr/admin/cores?action=STATUS&core=%s' % (solrhost, core_name)).status())
        LOGGER.info('Elasticsearch ping on %s is %s', eshost, 'OK' if Elasticsearch(host=eshost).ping() else 'KO')
        LOGGER.info('Solr status on %s is %s', solrurl, 'OK' if solr_status['status'][core_name] else 'KO')


if __name__ == "__main__":
    main()
