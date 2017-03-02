# estail

**estail** is tool for outputing the latest data in a Elasticsearch timeseries index.  Where data is stored in mutiple indexes (one per bucket of time), for example kibana logstash indexes.  

Moved from https://github.com/lytics/estail (this old version only supported older versions of ES).  We worked to utilize the packages in the `escp` repo.

Work in Progress!  It currently works well for tailing data but doesn't suppore a `tail -f` style streaming of realtime changes.

## Install

To install `estail`, run below command

```
go get -u github.com/lytics/estail
```

## Usage

```
$ estail -h

Usage of estail:
  -dur duration
    	now() - dur are how many logs are pulled (default 30s)
  -exclude string
    	DOESNT WORK: comma separated list of field:value pairs to exclude
  -host string
    	host and port of elasticsearch (default "localhost:9200")
  -include string
    	DOESNT WORK: comma separated list of field:value pairs to include
  -prefix string
    	prefix of log indexes (default "logstash-2017.02.23")
  -size int
    	number of docs to return per polling interval (default 1000)
  -ssl
    	use https for URI scheme
  -timestamp string
    	timestap field to sort by (default "@timestamp")

```

