# aws-elasticsearch-php-handler

### PHP Handler to connect to AWS ElasticSearch service.

#### Installation

Use composer to install. Simply add `"sportarchive/aws-elasticsearch-php-handler": "dev-master"` to your composer requirements and update.

#### Instructions

##### Use basic lucene query syntax to query for results. You can also supply a max count and sort order in the query.

```
$client = new ElasticsearchHandler(["https://yourawselasticsearchendpoint.aws-region.es.amazonaws.com:443"]);

$ESindex = "index_name";
$EStype = "object_type";
$query = "key:value AND foo:bar";
$count = 12;
$sort = "anotherkey:asc";

$results = $client->raw($ESindex, $query, $count, $sort, $EStype);
```

The count, sort, and type are not required. If they are not supplied, these values will default to:

```
$count = 10;
$sort = "";
$EStype = $ESindex;
```

The `$results` variable will be the raw data returned from the elasticsearch official php library, and includes a variety of metadata.

##### If you only need the source objects that were stored, use the convenience function `query` instead.

```
$results = $client->query($ESindex, $query, $count, $sort, $EStype);
```

The `$results` variable will now be an array containing the source objects for your query. Query simply parses the data before returning to make it easier to use.

##### If you wish to simply get the count for the total number of results, there is also a count function.

```
$client = new ElasticsearchHandler(["https://yourawselasticsearchendpoint.aws-region.es.amazonaws.com:443"]);

$ESindex = "index_name";
$EStype = "object_type";
$query = "key:value AND foo:bar";

$results = $client->raw($ESindex, $query, $EStype);
```
This function acts the same way as `raw` and `query`, but instead uses Elasticsearch's `search_type:count` so it does not actually retrieve or return the data.