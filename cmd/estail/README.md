# estail

estail is tailing tool for Elasticsearch

## Install

To install `estail`, run below command

```
go get -u github.com/lytics/estail
```

## Usage

```
$ estail -h

Usage of estail:
  -exclude string
        comma separated list of field:value pairs to exclude
  -host string
        host and port of elasticsearch (default "localhost:9200")
  -id
        show _id field
  -include string
        comma separated list of field:value pairs to include
  -message value
        message fields to display
  -poll int
        time in seconds to poll for new data from ES (default 1)
  -prefix string
        prefix of log indexes (default "logstash-")
  -size int
        number of docs to return per polling interval (default 1000)
  -source
        use _source field to output result
  -ssl
        use https for URI scheme
  -timestamp string
        timestap field to sort by (default "@timestamp")
```

