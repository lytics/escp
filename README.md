# Elasticsearch Copier

Toolkit for copying and validating Elasticsearch indexes.

* `escp` copies an index
* `esdiff` compares documents in two indexes; intended for validating copies

## Usage
```sh
# Install all utilities with go get:
go get -v github.com/lytics/escp/...
```

```sh
# Copy srcindex on host1 to dstindex on host2,host3
escp http://host1:9200/ srcindex host2:9200,host3:9200 dstindex
```

```sh
# Check document counts are equal and spot check documents
esdiff http://host1:9200/ srcindex http://host2:9200/dstindex

# Check 25% of documents
esdiff -d 4 http://host1:9200/ srcindex http://host2:9200 dstindex

# Check all documents
esdiff -d 1 http://host1:9200/ srcindex http://host2:9200 dstindex
```

## Introspection tools

* `primeshards` Correlates which indexes which have one or more primary shards on a subset of Elasticsearch nodes.
* `shardsize`   Returns a list of indexes whose shards are larger than 10GB. 

Other Tools
-------------------------------
* https://github.com/taskrabbit/elasticsearch-dump
* https://github.com/mallocator/Elasticsearch-Exporter
* http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/modules-snapshots.html
