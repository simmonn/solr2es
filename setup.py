import os
import sys

from setuptools import setup

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
    'idna==2.7', # to avoid conflicts between v2.8 and pysolr
    'pysolr==3.8.1',
    'elasticsearch==7.17.5',
    'elasticsearch-async==6.2.0',
    'aiohttp==3.6.2',
]

tests_require = [
    'nose==1.3.7',
    'asynctest==0.12.2',
    'pytz==2018.7',
]
setup(
    name=NAME,
    version='0.7',
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
    packages=['solr2es'],
    include_package_data=True,
    zip_safe=False,
    test_suite="nose.collector",  
    install_requires=install_requires,
    extras_require={
        'dev': tests_require,
    },
    entry_points={
          'console_scripts': [
              'solr2es = solr2es.__main__:main'
          ]
    },
)
