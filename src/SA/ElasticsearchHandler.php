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
    private $client;

    public function __construct($endpoints) {
        $psr7Handler = \Aws\default_http_handler();
        $signer = new SignatureV4("es", $_SERVER['AWS_DEFAULT_REGION']);
        $credentialProvider = CredentialProvider::defaultProvider();

        $handler = function(array $request) use($psr7Handler, $signer, $credentialProvider, $endpoints) {
            // Amazon ES listens on standard ports (443 for HTTPS, 80 for HTTP).
            $request['headers']['host'][0] = parse_url($request['headers']['host'][0], PHP_URL_HOST);

            // Create a PSR-7 request from the array passed to the handler
            $psr7Request = new Request(
                $request['http_method'],
                (new Uri($request['uri']))
                    ->withScheme($request['scheme'])
                    ->withHost($request['headers']['host'][0]),
                $request['headers'],
                $request['body']
            );

            // Sign the PSR-7 request with credentials from the environment
            $signedRequest = $signer->signRequest(
                $psr7Request,
                call_user_func($credentialProvider)->wait()
            );

            // Send the signed request to Amazon ES
            /** @var ResponseInterface $response */
            $response = $psr7Handler($signedRequest)->wait();

            // Convert the PSR-7 response to a RingPHP response
            return new CompletedFutureArray([
                "status" => $response->getStatusCode(),
                "headers" => $response->getHeaders(),
                "body" => $response->getBody()->detach(),
                "transfer_stats" => ["total_time" => 0],
                "effective_url" => (string) $psr7Request->getUri(),
            ]);
        };

        $this->client = ClientBuilder::create()
            ->setHandler($handler)
            ->setHosts($endpoints)
            ->build();
    }

    public function aggregate($index, $query, $data, $type = null) {
        $params = [
            "index" => $index,
            "type" => $index,
            "q" => $query,
            "size" => 0,
            "search_type" => "count",
        ];

        if($type != null)
            $params['type'] = $type;

        $body = [];
        foreach($data as $k => $v) {
            $name = $k;
            $type = $v['type'];
            $field = $v['field'];
            $body[$name] = [
                $type => [
                    "field" => $field,
                ],
            ];
        }

        $params['body'] = ["aggs" => $body];

        return $this->client->search($params)['aggregations'];
    }

    public function count($index, $query, $type = null) {
        $params = [
            "index" => $index,
            "type" => $index,
            "q" => $query,
            "size" => 0,
            "search_type" => "count",
        ];

        if($type != null)
            $params['type'] = $type;

        return $this->client->search($params)['hits']['total'];
    }

    public function query($index, $query, $count = 1, $sort = "", $type = null) {
        $sortarr = [];
        $temp = explode(",", $sort);

        foreach($temp as $t) {
            $e = explode(":", $t, 2);
            $sortarr[$e[0]] = [
                "order" => $e[1],
            ];
        }

        $params = [
            "index" => $index,
            "type" => $index,
            "q" => $query,
            "size" => $count,
            "sort" => $sortarr,
        ];

        if($type != null)
            $params['type'] = $type;

        $results = $this->client->search($params);

        $results = array_map(function($item) {
            return $item['_source'];
        }, $results['hits']['hits']);

        return $results;
    }

    public function raw($index, $query, $count = 1, $sort = "", $type = null) {
        $sortarr = [];
        $temp = explode(",", $sort);

        foreach($temp as $t) {
            $e = explode(":", $t, 2);
            $sortarr[$e[0]] = [
                "order" => $e[1],
            ];
        }

        $params = [
            "index" => $index,
            "type" => $index,
            "q" => $query,
            "size" => $count,
            "sort" => $sortarr,
        ];

        if($type != null)
            $params['type'] = $type;

        $results = $this->client->search($params);

        return $results;
    }
}
