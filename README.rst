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