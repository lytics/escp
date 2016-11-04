primeshards
===========

Correlates indexes which have primary shards on a subset of nodes. Useful for narrowing down hot indexes and their subsequent shards by host.

If a single primary shard is allocated on one of the given nodes, the index is added to the returned index list. 

```
Usage of ./primeshards:
  -host string
    	Elasticsearch query address (default "http://localhost:9200/")
  -nodes string
    	Nodes to find common primary shards eg: "es1,es2,es3"
```
