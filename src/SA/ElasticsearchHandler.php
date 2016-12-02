<?php

/**
 * Copyright (C) 2015, Sport Archive Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License (version 3) as published by
 * the Free Software Foundation;
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * For a complete version of the license, head to:
 * http://www.gnu.org/licenses/gpl-3.0.en.html
 *
 * Cloud Processing Engine, Copyright (C) 2015, Sport Archive Inc
 * Cloud Processing Engine comes with ABSOLUTELY NO WARRANTY;
 * This is free software, and you are welcome to redistribute it
 * under certain conditions;
 *
 * June 29th 2015
 * Sport Archive Inc.
 * info@sportarchive.tv
 *
 */

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

    public function __construct() {
        $psr7Handler = Aws\default_http_handler();
        $signer = new SignatureV4("es", $_SERVER['AWS_DEFAULT_REGION']);
        $credentialProvider = CredentialProvider::defaultProvider();

        $handler = function(array $request) use($psr7Handler, $signer, $credentialProvider) {
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
            ->setHosts([
                "https://search-esclust-esclus-k1g650pjv9tp-brzmmafmbdwd7mhnp7gbkr7lou.eu-west-1.es.amazonaws.com:443",
            ])
            ->build();
    }

    public function query($index, $query, $count = 1, $sort = "", $type = null) {
        $params = [
            "index" => $index,
            "type" => $index,
            "q" => $query,
            "size" => $count,
            "sort" => $sort,
        ];

        if($type != null)
            $params['type'] = $type;

        $results = $this->client->search($params);

        $results = array_map(function($item) {
            return $item['_source'];
        }, $results['hits']['hits']);

        return $results;
    }

    public function count($index, $query, $type = null) {
        $params = [
            "index" => $index,
            "type" => $index,
            "q" => $query,
            "search_type" => "count",
        ];

        if($type != null)
            $params['type'] = $type;

        return $this->client->search($params)['hits']['total'];
    }
}
