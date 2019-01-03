Changes
=======

v. 0.7
------

* multivalued field : flatten the array if it has one value
* multivalued field : ignore multi valuated field in translation map
* multivalued field : copy the array into elasticsearch

v. 0.6
------

* error handling : logs ids that have failed when resuming from postgresql
* adds a the possibility to specify a routing field in the translation map

v. 0.5
------

* adds postgresql resume
* elasticsearch : adds mappings and settings support
* better logs and progress marks
* doc : README
* translation map : support for empty default list
* adds postgresql blocking queue
* translation map : ignore field
* translation map : default value

v. 0.4
------

* [solr2es] wildcard support in translation_map
* [solr2es] nested fields support in translation_map
* [solr2es] adds solrid parameter to change sort field
* [solr2es] adds solrfq parameter to parallelize solr reading

v. 0.3
------

* [solr2es] adds translation map for fields
* [solr2es] adds elasticsearch mapping for index creation
* [test] compatible with 6.6.0

v. 0.2
------

* [log] adds logger and progression feedbacks
* [cli] exit if no args

v. 0.1
------

* [solr2es] initial version