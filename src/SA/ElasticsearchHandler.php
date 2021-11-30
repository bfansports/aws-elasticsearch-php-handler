<?php

namespace SA;

use Aws\Credentials\CredentialProvider;
use Aws\Signature\SignatureV4;
use Elasticsearch\ClientBuilder;
use GuzzleHttp\Psr7\Request;
use GuzzleHttp\Psr7\Uri;
use GuzzleHttp\Ring\Future\CompletedFutureArray;
use Psr\Http\Message\ResponseInterface;

class ElasticsearchHandler {
    private $timeout = 10;
    private $client;

    public function __construct($endpoints) {

        error_log('ici 1');

        $psr7Handler = \Aws\default_http_handler();
        $signer = new SignatureV4("es", $_SERVER['AWS_DEFAULT_REGION']);

        error_log('ici 2');

        if ( !empty($_SERVER['AWS_PROFILE']) ) {
            $credentialProvider = CredentialProvider::sso('profile ' . $_SERVER['AWS_PROFILE']);
        }
        else {
            $credentialProvider = CredentialProvider::defaultProvider([
                'timeout' => $this->timeout,
            ]);
        }

        error_log('ici 3');

        $handler = function (array $request) use (
            $psr7Handler,
            $signer,
            $credentialProvider,
            $endpoints
        ) {


            error_log('ici 6');

            // Amazon ES listens on standard ports (443 for HTTPS, 80 for HTTP).
            $request['headers']['Host'][0] = parse_url(
                $request['headers']['Host'][0],
                PHP_URL_HOST
            );

            error_log('ici 7');

            // Create a PSR-7 request from the array passed to the handler
            $psr7Request = new Request(
                $request['http_method'],
                (new Uri($request['uri']))
                    ->withScheme($request['scheme'])
                    ->withHost($request['headers']['Host'][0]),
                $request['headers'],
                $request['body']
            );

            error_log('ici 8');

            // Sign the PSR-7 request with credentials from the environment
            $credentials = $credentialProvider()->wait();

            error_log('ici 9');
            $signedRequest = $signer->signRequest($psr7Request, $credentials);
            error_log('ici 10');



            // Send the signed request to Amazon ES
            /** @var ResponseInterface $response */
            $response = $psr7Handler($signedRequest)
                ->then(
                    function (\Psr\Http\Message\ResponseInterface $r) {

                        error_log('ici 12');
                        return $r;
                    },
                    function ($error) {
                        return $error['response'];
                    }
                )
                ->wait();

            error_log('ici 11');
            // Convert the PSR-7 response to a RingPHP response
            return new CompletedFutureArray([
                "status" => $response->getStatusCode(),
                "headers" => $response->getHeaders(),
                "body" => $response->getBody()->detach(),
                "transfer_stats" => ["total_time" => 0],
                "effective_url" => (string) $psr7Request->getUri(),
            ]);
        };


        error_log('ici 3');

        $this->client = ClientBuilder::create()
            ->setHandler($handler)
            ->setHosts($endpoints)
            ->build();

        error_log('ici 4');
    }

    public function aggregate($index, $query, $data, $type = null) {
        $params = [
            "index" => $index,
            "type" => $index,
            "body" => [
                "query" => [
                    "query_string" => [
                        "query" => $query,
                    ],
                ],
            ],
        ];

        if ($type != null) {
            $params['type'] = $type;
        }

        $body = [];
        foreach ($data as $k => $v) {
            $name = $k;
            $type = $v['type'];
            $field = $v['field'];
            $body[$name] = [
                $type => [
                    "field" => $field,
                ],
            ];
        }

        $params['body']['aggs'] = $body;

        return $this->client->search($params)['aggregations'];
    }

    public function count($index, $query, $type = null) {
        $params = [
            "index" => $index,
            "type" => $index,
            "body" => [
                "query" => [
                    "query_string" => [
                        "query" => $query,
                    ],
                ],
            ],
        ];

        if ($type != null) {
            $params['type'] = $type;
        }

        return $this->client->count($params)['count'];
    }

    public function createDocument($index, $data, $id = null, $type = null) {
        $params = [
            "index" => $index,
            "type" => $index,
            "body" => $data,
        ];

        if ($type != null) {
            $params['type'] = $type;
        }

        if ($id != null) {
            $params['id'] = $id;
        }

        return $this->client->index($params);
    }

    public function createIndex($index) {
        $params = [
            "index" => $index,
        ];

        return $this->client->indices()->create($params);
    }

    public function deleteDocument($index, $id) {
        $params = [
            "index" => $index,
            "type" => $index,
            "id" => $id,
        ];

        return $this->client->delete($params);
    }

    public function deleteIndex($index) {
        $params = [
            "index" => $index,
        ];

        return $this->client->indices()->delete($params);
    }

    public function indexExists($index) {
        $params = [
            "index" => $index,
        ];

        return $this->client->indices()->exists($params);
    }

    public function indices() {
        return $this->client->indices()->stats();
    }

    public function query(
        $index,
        $query,
        $count = 1,
        $sort = null,
        $offset = 0,
        $type = null
    ) {
        $results = $this->raw($index, $query, $count, $sort, $offset, $type);

        $results = array_map(function ($item) {
            return $item['_source'];
        }, $results['hits']['hits']);

        return $results;
    }

    public function raw(
        $index,
        $query,
        $count = 1,
        $sort = null,
        $offset = 0,
        $type = null
    ) {
        $params = [
            "index" => $index,
            "type" => $index,
            "from" => $offset,
            "body" => [
                "query" => [
                    "query_string" => [
                        "query" => $query,
                    ],
                ],
            ],
            "size" => $count,
        ];

        if ($type != null) {
            $params['type'] = $type;
        }

        if ($sort != null && $sort != "") {
            $params['sort'] = $sort;
        }

        $results = $this->client->search($params);

        return $results;
    }

    public function scan($index, $query, $type = null) {
        $params = [
            "body" => [
                "sort" => [["_uid" => "asc"]],
                "query" => [
                    "query_string" => [
                        "query" => $query,
                    ],
                ],
            ],
            "index" => $index,
            "size" => 10000,
            "type" => $index,
        ];

        if ($type != null) {
            $params['type'] = $type;
        }

        $results = [];
        $total = 0;
        do {
            $temp = $this->client->search($params);
            $total = $temp['hits']['total'];
            $hits = $temp['hits']['hits'];
            $last = end($hits);

            $results = array_merge($results, $hits);

            if ( $last ) {
                $params['body']['search_after'] = $last['sort'];
            }
            else {
                $params['body']['search_after'] = null;
            }
        } while (count($results) < $total);

        return $results;
    }

    public function search($params = []) {
        return $this->client->search($params);
    }

    public function getCacheKey() {
        return $this->cacheKey;
    }

    public function setCacheKey($cacheKey) {
        $this->cacheKey = $cacheKey;
    }

    public function getIndexParameters($params) {
        return $this->client->indices()->getSettings($params);
    }

    public function putIndexParameters($params) {
        return $this->client->indices()->putSettings($params);
    }

    public function getIndexMapping($params) {
        return $this->client->indices()->getMapping($params);
    }

    public function putIndexMapping($params) {
        return $this->client->indices()->putMapping($params);
    }

    public function updateIndexAliases($params) {
        return $this->client->indices()->updateAliases($params);
    }

    public function getIndexAliases() {
        return $this->client->indices()->getAliases();
    }

    public function reindex($params) {
        return $this->client->reindex($params);
    }

    public function getRetriesCount() {
        //deprecated - we dont remove the function for the moment to not break existing script
    }

    public function setRetriesCount($retriesCount) {
        //deprecated - we dont remove the function for the moment to not break existing script
    }

    public function getTimeout() {
        return $this->timeout;
    }

    public function setTimeout($timeout) {
        $this->timeout = $timeout;
    }

    public function putIndexSettings($params) {
        return $this->client->indices()->putSettings($params);
    }

    public function getIndexSettings($params) {
        return $this->client->indices()->getSettings($params);
    }
}
