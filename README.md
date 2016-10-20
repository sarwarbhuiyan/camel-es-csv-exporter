# camel-es-csv-exporter

# Introduction

This is an example camel application which can use the camel-elasticsearch-http component to read data from one index (or index pattern), run an optional query and write the results to a CSV file based on the columns selected.

# Usage

Download the release zip (that includes the uber jar with all dependencies) and run es-csv-exporter like follows:

To see usage instructions, run:

```
> ./es-csv-exporter
usage: es-csv-exporter
    --columns <columns>             Header column keys (e.g.
                                    department,name.first,name.last
    --outputFile <outputFile>       Header column keys (e.g.
                                    department,name.first,name.last
    --query <query>                 Query (default: { "query":
                                    "{"match_all": {}}}
    --scrollPeriod <scrollPeriod>   Scroll Period (default: 1m)
    --sourceHost <sourceHost>       Source Elasticsearch Host
    --sourceIndex <sourceIndex>     Source Index
    --sourcePort <sourcePort>       Source Elasticsearch Port

Example usage:

```
>./es-csv-exporter --columns request,verb --sourceHost localhost --sourcePort 9200 --sourceIndex logstash-* --outputFile report.csv --query '{"query":{ "match": {"verb": "POST"}}}'

```
