solr2es
=======

.. image:: https://circleci.com/gh/ICIJ/solr2es.png?style=shield&circle-token=846c844f540fb3746b80b8f12656ddde665b5037
   :alt: Circle CI
   :target: https://circleci.com/gh/ICIJ/solr2es

Migration script from solr to elasticsearch

Develop
-------

To build and run tests you can make :

::

    virtualenv --python=python3.6 venv
    source venv/bin/activate
    python setup.py develop
    python setup.py test

To release :

::

    python setup.py  sdist bdist_egg upload


Use
---

**translation_map**


The purpose of a translation_map is to create a mapping between the fields coming from Solr to the ones inserted to Elasticsearch.

1. If a field from solr doesn't exist in the translation_map, it will be inserted as it is into Elasticsearch.

2. Use the property `name` to rename a field in Elasticsearch :

```
{"solr_name": {'name': "es_name"}}
```

3. Use the property `default_value` if you want to set a default value into a field in Elasticsearch.

If the field exists into Solr and has a value, it won't be changed by the translation_map.
Otherwise a field `solr_name` willl be added to Elasticsearch with value `john doe`.

```
{"solr_name": {"default_value": "john doe"}}
```

4. Use the property `name` with some `.` in it to create a nested field in Elasticsearch.

If the Solr record has a field `nested_a_b`, the Elasticsearch record will get a field `nested`, that will have a nested field `a`, that will have a nested field `b` that will get the content of `nested_a_b`.

```
{"nested_a_b": {"name": "nested.a.b"}}
```

5. Use the property `name` with some regex groups capture to rename a bulk of Solr in Elasticsearch.
This will rename all the fields prefixed by `solr_` into `elasticsearch_`.

```
{"solr_(.*)": {"name": "elasticsearch_\\1"}}
```
