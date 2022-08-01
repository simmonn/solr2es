import asyncio
import getopt
import hashlib
import logging
import re
import sys
from collections import Mapping
from functools import reduce
from itertools import chain
from json import loads, dumps
import aiohttp
from elasticsearch import Elasticsearch
from elasticsearch_async import AsyncElasticsearch
from pysolr import Solr, SolrCoreAdmin


logging.basicConfig(format='%(asctime)s [%(name)s][%(process)d] %(levelname)s: %(message)s')
LOGGER = logging.getLogger('solr2es')
LOGGER.setLevel(logging.INFO)

DEFAULT_ES_DOC_TYPE = '_doc'
DEFAULT_ID_FIELD = 'id'


class IllegalStateError(RuntimeError):
    def __init__(self, *args: object) -> None:
        super().__init__(*args)


class TranslationMap(object):
    def __init__(self, translation_map_dict=None) -> None:
        _tm = dict() if translation_map_dict is None else translation_map_dict
        self.default_values = {k: v['default'] for k, v in _tm.items() if 'default' in v}
        self.names = {k: v['name'] for k, v in _tm.items() if 'name' in v and type(k) == str}
        self.regexps = {k: v['name'] for k, v in _tm.items() if 'name' in v and type(k) != str}
        self.ignores = {k for k, v in _tm.items() if 'ignore' in v and v['ignore']}
        self.multivalued_ignored = {k for k, v in _tm.items() if 'multivalued' in v and not v['multivalued']}
        routing_keys = {k for k, v in _tm.items() if 'routing_field' in v and v['routing_field']}
        if len(routing_keys) > 1:
            raise IllegalStateError('found several routing keys : %s' % routing_keys)
        self.routing_key_field_name = None if len(routing_keys) == 0 else routing_keys.pop()

    def get_id_field_name(self) -> str:
        set_id = {k for k, v in self.names.items() if v == '_id'}
        id_key = set_id.pop() if len(set_id) > 0 else DEFAULT_ID_FIELD
        return id_key


class Solr2Es(object):
    def __init__(self, solr, es, refresh=False) -> None:
        super().__init__()
        self.solr = solr
        self.es = es
        self.refresh = refresh

    def migrate(self, index_name, mapping=None, translation_map=TranslationMap(), solr_filter_query='*',
                sort_field=DEFAULT_ID_FIELD, solr_rows=500, solr_fields='*') -> int:
        nb_results = 0
        if not self.es.indices.exists([index_name]):
            self.es.indices.create(index_name, body=mapping)
        for results in self.produce_results(solr_filter_query=solr_filter_query,
                                            sort_field=sort_field, solr_rows_pagination=solr_rows, solr_field_list = solr_fields):
            actions_as_list = create_es_actions(index_name, results, translation_map)
            actions = '\n'.join(list(map(lambda d: dumps(d), chain(*actions_as_list))))
            response = self.es.bulk(actions, index_name, DEFAULT_ES_DOC_TYPE, refresh=self.refresh)
            nb_results += len(results)
            if response['errors']:
                for err in response['items']:
                    LOGGER.warning(err)
                nb_results -= len(response['items'])
        LOGGER.info('processed %s documents', nb_results)
        return nb_results

    def produce_results(self, solr_filter_query='*', sort_field=DEFAULT_ID_FIELD, solr_rows_pagination=10, solr_field_list = '*'):
        nb_results = 0
        nb_total = 0
        cursor_ended = False
        kwargs = dict(fq=solr_filter_query, cursorMark='*', fl=solr_field_list, sort='%s asc' % sort_field, rows=solr_rows_pagination)
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

    async def migrate(self, index_name, es_index_body_str=None, translation_map=TranslationMap(), solr_filter_query=None, sort_field=DEFAULT_ID_FIELD, solr_rows_pagination=10, solr_fields='*') -> int:
        if not await self.aes.indices.exists([index_name]):
            await self.aes.indices.create(index_name, body=es_index_body_str)

        nb_results = 0
        async for results in self.produce_results(solr_filter_query=solr_filter_query,
                                                  sort_field=sort_field,
                                                  solr_rows_pagination=solr_rows_pagination,
                                                  solr_field_list=solr_fields):
            actions_as_list = create_es_actions(index_name, results, translation_map)
            actions = '\n'.join(list(map(lambda d: dumps(d), chain(*actions_as_list))))
            response = await self.aes.bulk(actions, index_name, DEFAULT_ES_DOC_TYPE, refresh=self.refresh)
            nb_results += len(results)
            if response['errors']:
                for err in response['items']:
                    LOGGER.warning(err)
                nb_results -= len(response['items'])
        return nb_results

    async def produce_results(self, solr_filter_query='*', sort_field=DEFAULT_ID_FIELD, solr_rows_pagination=10, solr_field_list='*'):
        cursor_ended = False
        nb_results = 0
        nb_total = 0
        kwargs = dict(cursorMark='*', sort='%s asc' % sort_field, q='*:*', wt='json',
                      fq=solr_filter_query, fl=solr_field_list, rows=solr_rows_pagination)
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

def create_es_actions(index_name, solr_results, translation_map) -> list:

    def create_action(row, translation_map, id_value=None) -> dict:
        # id_value = row[translation_map.get_id_field_name()] if id_value is None else id_value
        index_params = {'_index': index_name, '_type': DEFAULT_ES_DOC_TYPE}
        routing_key = translation_map.routing_key_field_name
        if routing_key is not None and routing_key in row:
            index_params['_routing'] = row[routing_key]
        return {'index': index_params}

    def has_duplicates(results, translation_map) -> bool:
        for result in results:
            for field in translation_map.multivalued_ignored:
                if type(result[field]) is list:
                    return True
        return False

    def create_duplicate_actions(row, translation_map) -> list:
        actions = []
        for field in translation_map.multivalued_ignored:
            translated_key = _translate_key(field, translation_map.names, translation_map.regexps)
            for value in row[field][1:]:
                actions.append((
                    create_action(row, translation_map, hashlib.sha256(str(value).encode('utf-8')).hexdigest()),
                    {translated_key: value, 'documentId': row[translation_map.get_id_field_name()], 'type': 'Duplicate'}
                ))
        return actions

    results = [(create_action(row, translation_map),
                translate_doc(row, translation_map))
                for row in solr_results]
    if has_duplicates(solr_results, translation_map):
        for row in solr_results:
            results += create_duplicate_actions(row, translation_map)
    return results


def translate_doc(row, translation_map) -> dict:
    def translate(key, value):
        translated_key = _translate_key(key, translation_map.names, translation_map.regexps)
        if key in translation_map.multivalued_ignored or (type(value) is list and len(value) == 1):
            translated_value = value[0]
            if len(value) > 1:
                LOGGER.warning('multivalued field in doc id=%s key=%s size=%d', row[translation_map.get_id_field_name()], key, len(value))
        else:
            translated_value = value

        if '.' in translated_key:
            translated_value = reduce(lambda i, acc: (acc, i), reversed(translated_key.split('.')[1:] + [translated_value]))
            translated_key = translated_key.split('.')[0]
        elif translated_key == '_id':
            return key, value
        return translated_key, translated_value

    defaults = translation_map.default_values.copy()
    defaults.update({k: v for k, v in row.items() if k not in translation_map.ignores})
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
        if type(v) is tuple:
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


def migrate(solrhost, eshost, index_name, solrfq, solrid, solrfields, rows):
    LOGGER.info('migrate from solr (%s) into elasticsearch (%s) index %s and filter query (%s)', solrhost, eshost, index_name, solrfq)
    Solr2Es(Solr(solrhost, always_commit=True), Elasticsearch(hosts=eshost)).migrate(index_name, solr_filter_query=solrfq, sort_field=solrid, solr_rows=rows, solr_fields=solrfields)

async def aiomigrate(solrhost, eshost, name, solrfq, solrid, solrfields, rows):
    LOGGER.info('asyncio migrate from solr (%s) into elasticsearch (%s) index %s '
                'with filter query (%s) and with id (%s)', solrhost, eshost, name, solrfq, solrid)
    async with aiohttp.ClientSession() as session:
        await Solr2EsAsync(session, AsyncElasticsearch(hosts=[eshost]), solrhost).migrate(
            name, solr_filter_query=solrfq, sort_field=solrid, solr_fields=solrfields, solr_rows_pagination=rows)


def usage(argv):
    print('Usage: %s action' % argv[0])
    print('\t-m|--migrate: migrate solr to elasticsearch')
    print('\t-t|--test: test solr/elasticsearch connections')
    print('\t-a|--async: use python 3 asyncio')
    print('\t--solrhost: solr host (default \'solr\')')
    print('\t--solrfq: solr filter query (default \'*\')')
    print('\t--solrid: solr id field name (default \'id\')')
    print('\t--solrfields: solr fields (default \'*\')')
    print('\t--index: index name (default solr core name)')
    print('\t--core: core name (default \'solr2es\')')
    print('\t--eshost: elasticsearch url (default \'elasticsearch\')')


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


def main():
    options, remainder = getopt.gnu_getopt(sys.argv[1:], 'hmdtra',
            ['help', 'migrate', 'test', 'async', 'solrhost=', 'eshost=',
             'index=', 'core=', 'solrfq=', 'solrid=',
             'rows=', 'solrfields='])
    if len(sys.argv) == 1:
        usage(sys.argv)
        sys.exit()

    aioloop = asyncio.get_event_loop()
    with_asyncio = False
    solrhost = 'solr'
    solrfq = '*'
    solrid = DEFAULT_ID_FIELD
    eshost = 'elasticsearch'
    core_name = 'solr2es'
    index_name = None
    solr_fields= '*'
    action = 'migrate'
    rows = 500
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

        if opt == '--solrfields':
            solr_fields = arg

        if opt == '--rows':
            rows = arg

        if opt == '--eshost':
            eshost = arg

        if opt == '--index':
            index_name = arg

        if opt == '--core':
            core_name = arg

        elif opt in ('-m', '--migrate'):
            action = 'migrate'
        elif opt in ('-t', '--test'):
            action = 'test'

    if index_name is None:
        index_name = core_name

    solrurl = 'http://%s/solr/%s' % (solrhost, core_name)

    if action == 'migrate':
        aioloop.run_until_complete(aiomigrate(solrurl, eshost, index_name, solrfq, solrid, solr_fields, rows)) if with_asyncio \
            else migrate(solrurl, eshost, index_name, solrfq, solrid, solr_fields, rows)

    elif action == 'test':
        solr_status = loads(SolrCoreAdmin('http://%s:8983/solr/admin/cores?action=STATUS&core=%s' % (solrhost, core_name)).status())
        LOGGER.info('Elasticsearch ping on %s is %s', eshost, 'OK' if Elasticsearch(host=eshost).ping() else 'KO')
        LOGGER.info('Solr status on %s is %s', solrurl, 'OK' if solr_status['status'][core_name] else 'KO')


if __name__ == "__main__":
    main()
