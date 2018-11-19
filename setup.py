import os
import sys

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))
py_version = sys.version_info[:2]
if py_version < (3, 6):
    raise Exception("solr2es requires Python >= 3.6.")

with open(os.path.join(here, 'README.rst')) as readme:
    README = readme.read()
with open(os.path.join(here, 'CHANGES.rst')) as changes:
    CHANGES = changes.read()

NAME = 'solr2es'

install_requires = [
    'pysolr==3.8.1',
    'elasticsearch==6.3.1',
    'aiohttp==2.3.10',
    'elasticsearch-async==6.2.0',
    'redis==2.10.6',
    'asyncio_redis==0.15.1'
]

tests_require = [
    'nose',
    'asynctest',
    'pytz',
]
setup(
    name=NAME,
    version='0.3',
    description='Python solr/elasticsearch migration script',
    long_description=README + '\n\n' + CHANGES,
    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: 3.6",
    ],
    author='Anne L\'Hôte, Bruno Thomas',
    author_email='alhote@icij.org, bthomas@icij.org',
    license='GPL-3.0',
    url='https://github.com/ICIJ/solr2es',
    keywords='migration search engine solr elasticsearch',
    packages=find_packages(),
    include_package_data=True,
    scripts=['solr2es/solr2es.py'],
    zip_safe=False,
    test_suite="nose.collector",  
    install_requires=install_requires,
    tests_require=tests_require,
)
