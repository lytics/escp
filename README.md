ElasticSearch Copier
------------------------------

Tool to copy elasticsearch indexes.


Usage
----------------------
```sh
escp http://localhost:9200/oldindex http://localhost:9200/newindex
```

Or to only copy a subset:

```sh
escp -filter='{"term": {"somefield": "somevalue"}}' \
    http://localhost:9200/oldindex http://localhost:9200/newindex
```

Other Tools
-------------------------------
* https://github.com/taskrabbit/elasticsearch-dump
* https://github.com/mallocator/Elasticsearch-Exporter
* http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/modules-snapshots.html
