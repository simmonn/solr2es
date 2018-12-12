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
from aiopg.sa import create_engine
from elasticsearch import Elasticsearch
from elasticsearch_async import AsyncElasticsearch, AsyncTransport
from pysolr import Solr, SolrCoreAdmin

from solr2es.postgresql_queue import PostgresqlQueueAsync, PostgresqlQueue
from solr2es.redis_queue import RedisQueueAsync, RedisQueue

logging.basicConfig(format='%(asctime)s [%(name)s][%(process)d] %(levelname)s: %(message)s')
LOGGER = logging.getLogger('solr2es')
LOGGER.setLevel(logging.INFO)

DEFAULT_ES_DOC_TYPE = 'doc'
DEFAULT_ID_FIELD = 'id'


class IllegalStateError(RuntimeError):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class Solr2Es(object):
    def __init__(self, solr, es, refresh=False) -> None:
        super().__init__()
        self.solr = solr
        self.es = es
        self.refresh = refresh

    def migrate(self, index_name, mapping=None, translation_map=None, solr_filter_query='*',
                sort_field=DEFAULT_ID_FIELD, solr_rows_pagination=10) -> int:
        translation_dict = dict() if translation_map is None else translation_map
        nb_results = 0
        if not self.es.indices.exists([index_name]):
            self.es.indices.create(index_name, body=mapping)
        for results in self.produce_results(solr_filter_query=solr_filter_query,
                                            sort_field=sort_field, solr_rows_pagination=solr_rows_pagination):
            actions = create_es_actions(index_name, results, translation_dict)
            response = self.es.bulk(actions, index_name, DEFAULT_ES_DOC_TYPE, refresh=self.refresh)
            nb_results += len(results)
            if response['errors']:
                for err in response['items']:
                    LOGGER.warning(err)
                nb_results -= len(response['items'])
        LOGGER.info('processed %s documents', nb_results)
        return nb_results

    def produce_results(self, solr_filter_query='*', sort_field=DEFAULT_ID_FIELD, solr_rows_pagination=10):
        nb_results = 0
        nb_total = 0
        cursor_ended = False
        kwargs = dict(fq=solr_filter_query, cursorMark='*', fl='*', sort='%s asc' % sort_field, rows=solr_rows_pagination)
        while not cursor_ended:
            results = self.solr.search('*:*', **kwargs)
            if kwargs['cursorMark'] == '*':
                nb_total = results.hits
                LOGGER.info('found %s documents', nb_total)
            if kwargs['cursorMark'] != results.nextCursorMark:
                kwargs['cursorMark'] = results.nextCursorMark
                nb_results += len(results)
                if nb_results % 10000 == 0:
                    LOGGER.info('read %s docs of %s (%.2f %% done)', nb_results, nb_total, (100 * nb_results)/nb_total)
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

    async def migrate(self, index_name, es_index_body_str=None, translation_map=None, solr_filter_query='*', sort_field=DEFAULT_ID_FIELD, solr_rows_pagination=10) -> int:
        translation_dict = dict() if translation_map is None else translation_map
        if not await self.aes.indices.exists([index_name]):
            await self.aes.indices.create(index_name, body=es_index_body_str)

        nb_results = 0
        async for results in self.produce_results(solr_filter_query=solr_filter_query,
                                                  sort_field=sort_field, solr_rows_pagination=solr_rows_pagination):
            actions = create_es_actions(index_name, results, translation_dict)
            await self.aes.bulk(actions, index_name, DEFAULT_ES_DOC_TYPE, refresh=self.refresh)
            nb_results += len(results)
        return nb_results

    async def produce_results(self, solr_filter_query='*', sort_field=DEFAULT_ID_FIELD, solr_rows_pagination=10):
        cursor_ended = False
        nb_results = 0
        nb_total = 0
        kwargs = dict(cursorMark='*', sort='%s asc' % sort_field, q='*:*', wt='json',
                      fq=solr_filter_query, fl='*', rows=solr_rows_pagination)
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
                        LOGGER.info('read %s docs of %s (%.2f %% done)', nb_results, nb_total,
                                    (100 * nb_results) / nb_total)
                    yield json['response']['docs']
                else:
                    cursor_ended = True
        LOGGER.info('processed %s documents', nb_results)

    async def resume(self, queue, index_name, es_index_body_str=None, translation_map=None):
        translation_dict = dict() if translation_map is None else translation_map
        if not await self.aes.indices.exists([index_name]):
            await self.aes.indices.create(index_name, body=es_index_body_str)

        nb_results = 0
        nb_total = await queue.size()
        LOGGER.info('found %s documents', nb_total)

        results = ['']
        while results:
            try:
                results = await queue.pop()
                actions = create_es_actions(index_name, results, translation_dict)
                await self.aes.bulk(actions, index_name, DEFAULT_ES_DOC_TYPE, refresh=self.refresh)
                nb_results += len(results)
                if nb_results % 10000 == 0:
                    LOGGER.info('read %s docs of %s (%.2f %% done)', nb_results, nb_total,
                                (100 * nb_results) / nb_total)
            except Exception:
                id_key = _get_id_field_name(translation_map)
                LOGGER.exception('exception while reading results %s' % list(r.get(id_key) for r in results))
        return nb_results


def create_es_actions(index_name, solr_results, translation_map) -> str:
    default_values = {k: v['default'] for k, v in translation_map.items() if 'default' in v}
    translation_names = {k: v['name'] for k, v in translation_map.items() if 'name' in v and type(k) == str}
    translation_regexps = {k: v['name'] for k, v in translation_map.items() if 'name' in v and type(k) != str}
    translation_ignores = {k for k, v in translation_map.items() if 'ignore' in v and v['ignore']}

    id_key = _get_id_field_name(translation_map)
    routing_key = _get_routing_field_name(translation_map)

    def create_action(row):
        index_params = {'_index': index_name, '_type': DEFAULT_ES_DOC_TYPE, '_id': row[id_key]}
        if routing_key is not None and routing_key in row:
            index_params['_routing'] = row[routing_key]
        return {'index': index_params}

    results = [(create_action(row),
                translate_doc(row, translation_names, translation_regexps, default_values, translation_ignores))
                for row in solr_results]
    return '\n'.join(list(map(lambda d: dumps(d), chain(*results))))


def _get_routing_field_name(translation_map):
    if translation_map is not None:
        routing_keys = {k for k, v in translation_map.items() if 'routing_field' in v and v['routing_field']}
        if len(routing_keys) > 1:
            raise IllegalStateError('found several routing keys : %s' % routing_keys)
        return None if len(routing_keys) == 0 else routing_keys.pop()


def _get_id_field_name(translation_map):
    if translation_map is None:
        return DEFAULT_ID_FIELD
    set_id = {k for k, v in translation_map.items() if v.get('name') == '_id'}
    id_key = set_id.pop() if len(set_id) > 0 else DEFAULT_ID_FIELD
    return id_key


def translate_doc(row, translation_names, translation_regexps, default_values, translation_ignores) -> dict:
    def translate(key, value):
        translated_key = _translate_key(key, translation_names, translation_regexps)
        translated_value = value[0] if type(value) is list and len(value) > 0 else value

        if '.' in translated_key:
            translated_value = reduce(lambda i, acc: (acc, i), reversed(translated_key.split('.')[1:] + [translated_value]))
            translated_key = translated_key.split('.')[0]
        elif translated_key == '_id':
            return key, value
        return translated_key, translated_value

    defaults = default_values.copy()
    defaults.update({k: v for k, v in row.items() if k not in translation_ignores})
    translated = tuple(translate(k, v) for k, v in defaults.items())
    return _tuples_to_dict(translated)


def _translate_key(key, translation_names, translation_regexps) -> str:
    if translation_names.get(key) is not None:
        return translation_names.get(key)
    matched_fields = list(((k, v) for k, v in translation_regexps.items() if k.search(key)))
    if len(matched_fields) == 0:
        return key
    if len(matched_fields) == 1:
        old_key_regexp, new_key_regexp = matched_fields[0]
        return old_key_regexp.sub(new_key_regexp, key)
    raise IllegalStateError('Too many doc fields matching key %s in translation map : %s' % (key, matched_fields))


def _tuples_to_dict(tuples) -> dict:
    ret = dict()
    for k, v in tuples:
        if type(v) is tuple or (type(v) is list and len(v) > 0):
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


def resume_from_postgresql(pgsqldsn, eshost, name, translationmap, esmapping):
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
    LOGGER.info('asyncio dump from solr (%s) into postgresql (dsn=%s) with filter query (%s)', solrhost, pgsqldsn, solrfq)
    dsndict = dict((kvstr.split('=') for kvstr in pgsqldsn.split()))
    psql_queue = await PostgresqlQueueAsync.create(await create_engine(**dsndict), unique_id=solrid)
    async with aiohttp.ClientSession() as session:
        await psql_queue.push_loop(
            partial(Solr2EsAsync(session, None, solrhost).produce_results, solr_filter_query=solrfq, sort_field=solrid))


async def aioresume_from_redis(redishost, eshost, name):
    LOGGER.info('asyncio resume from redis (host=%s) to elasticsearch (%s) index %s', redishost, eshost, name)


async def aioresume_from_pgsql(pgsqldsn, eshost, name, translationmap, es_index_body):
    LOGGER.info('asyncio resume from postgresql (dsn=%s) to elasticsearch (%s) index %s', pgsqldsn, eshost, name)
    dsndict = dict((kvstr.split('=') for kvstr in pgsqldsn.split()))
    psql_queue = await PostgresqlQueueAsync.create(await create_engine(**dsndict))
    es_index_body_str = None if es_index_body is None else dumps(es_index_body)

    elasticsearch = AsyncElasticsearch([eshost], AsyncTransport, timeout=60)
    await Solr2EsAsync(None, elasticsearch, None).\
        resume(psql_queue, name, es_index_body_str, translationmap)
    await elasticsearch.transport.close()


async def aiomigrate(solrhost, eshost, name, solrfq, solrid):
    LOGGER.info('asyncio migrate from solr (%s) into elasticsearch (%s) index %s '
                'with filter query (%s) and with id (%s)', solrhost, eshost, name, solrfq, solrid)
    async with aiohttp.ClientSession() as session:
        await Solr2EsAsync(session, AsyncElasticsearch(hosts=[eshost]), solrhost).migrate(
            name, solr_filter_query=solrfq, sort_field=solrid)


def usage(argv):
    print('Usage: %s action' % argv[0])
    print('\t-m|--migrate: migrate solr to elasticsearch')
    print('\t-r|--resume: resume from redis (default) or postgresql (if dsn given)')
    print('\t-d|--dump: dump into redis (default) or postgresql (if dsn given) queue')
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
    print('\t--translationmap: dict string to translate fields or file path beginning with @')
    print('\t--esmapping: elasticsearch mapping string or file path beginning with @')
    print('\t--essetting: elasticsearch setting string or file path beginning with @')


def as_translation_map(dct):
    names_dict = {k: v for k, v in dct.items() if not k.startswith('[regexp]')}
    regexp_dict = {re.compile(k.replace('[regexp]', '')): v for k, v in dct.items() if k.startswith('[regexp]')}
    names_dict.update(regexp_dict)
    return names_dict


def _get_dict_from_string_or_file(input_str) -> dict:
    if input_str is None:
        return dict()
    if input_str.startswith('@'):
        with open(input_str[1:]) as translation_map:
            return loads(translation_map.read(), object_hook=as_translation_map)
    else:
        return loads(input_str, object_hook=as_translation_map)


def _get_es_mappings_and_settings(essettings_dict, esmapping_dict) -> dict:
    return_dict = dict()
    if essettings_dict is not None:
        return_dict['settings'] = essettings_dict
    if esmapping_dict is not None:
        return_dict['mappings'] = esmapping_dict
    return return_dict


def main():
    options, remainder = getopt.gnu_getopt(sys.argv[1:], 'hmdtra',
            ['help', 'migrate', 'dump', 'test', 'resume', 'async', 'solrhost=', 'eshost=',
             'redishost=', 'index=', 'core=', 'solrfq=', 'solrid=', 'postgresqldsn=', 'translationmap=',
             'esmapping=', 'essetting='])
    if len(sys.argv) == 1:
        usage(sys.argv)
        sys.exit()

    aioloop = asyncio.get_event_loop()
    with_asyncio = False
    solrhost = 'solr'
    solrfq = '*'
    solrid = DEFAULT_ID_FIELD
    eshost = 'elasticsearch'
    redishost = 'redis'
    postgresqldsn = None
    core_name = 'solr2es'
    index_name = None
    action = 'migrate'
    translationmap = None
    esmapping = None
    essetting = None
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

        if opt == '--translationmap':
            translationmap = _get_dict_from_string_or_file(arg)

        if opt == '--esmapping':
            esmapping = _get_dict_from_string_or_file(arg)

        if opt == '--essetting':
            essetting = _get_dict_from_string_or_file(arg)

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
    es_index_body = _get_es_mappings_and_settings(essetting, esmapping)

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
            aioresume_from_pgsql(postgresqldsn, eshost, index_name, translationmap, es_index_body)) if with_asyncio \
            else resume_from_postgresql(postgresqldsn, eshost, index_name, translationmap, es_index_body)
    elif action == 'test':
        solr_status = loads(SolrCoreAdmin('http://%s:8983/solr/admin/cores?action=STATUS&core=%s' % (solrhost, core_name)).status())
        LOGGER.info('Elasticsearch ping on %s is %s', eshost, 'OK' if Elasticsearch(host=eshost).ping() else 'KO')
        LOGGER.info('Solr status on %s is %s', solrurl, 'OK' if solr_status['status'][core_name] else 'KO')


if __name__ == "__main__":
    main()
