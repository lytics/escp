Shardsize
=========

Returns a list of indexes whose shards are larger than 10GB.

Large shard sizes can be problematic in elasticsearch. There is no clear optimal shard size; it is mostly arbitrated by your data structures.

`cmd/shardsize` calculates which indicies have excessively large shards and prints them to stdout.

#### eg
`./shardsize --host=http://localhost:9200`

