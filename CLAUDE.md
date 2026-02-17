# aws-elasticsearch-php-handler

## What This Is

**DEPRECATED**: Legacy PHP library for connecting to AWS Elasticsearch Service. This library has been superseded by `aws-opensearch-php-handler`, which uses the OpenSearch client instead of the deprecated Elasticsearch client.

A PHP library for querying AWS Elasticsearch (pre-OpenSearch migration). Provides Lucene query syntax interface for searching, counting, and retrieving documents.

## Tech Stack

- **Language**: PHP 7.0+
- **AWS SDK**: `aws/aws-sdk-php` 3.x — AWS authentication and signing
- **Elasticsearch Client**: `elasticsearch/elasticsearch` (version not pinned in composer.json)
- **Autoloading**: PSR-0 (namespace `SA`)

## Quick Start

```bash
# Installation (deprecated — use aws-opensearch-php-handler instead)
composer require bfansports/aws-elasticsearch-php-handler

# Usage (same API as OpenSearch version)
use SA\ElasticsearchHandler;

$client = new ElasticsearchHandler(["https://search-domain.region.es.amazonaws.com:443"]);

$index = "organizations";
$query = "name:\"Lakers\" AND active:true";
$count = 20;
$sort = "createdAt:desc";
$type = "organization";  // Types are deprecated in ES 7+

// Get full ES response with metadata
$results = $client->raw($index, $query, $count, $sort, $type);

// Get just the source documents (convenience method)
$docs = $client->query($index, $query, $count, $sort, $type);

// Get count only (no document retrieval)
$totalCount = $client->count($index, $query, $type);
```

## Project Structure

- `src/SA/ElasticsearchHandler.php` — Main handler class
- `composer.json` — Dependencies and autoload config
- `LICENSE` — MIT-style license
- `README.md` — Usage instructions
- `.github/` — GitHub Actions workflow (if present)

## Dependencies

**External:**
- AWS Elasticsearch Service (pre-OpenSearch migration)
- AWS IAM credentials — for signing requests

**Consumed by:**
- Legacy bFAN PHP services (if any still use this)
- `sa_site_v2` (migrated to `aws-opensearch-php-handler`)

<!-- Ask: Are there any components still using this library, or has everything migrated to aws-opensearch-php-handler? -->
<!-- Ask: Should this repo be archived or marked as deprecated in GitHub? -->

## API / Interface

**Main class**: `SA\ElasticsearchHandler`

**Constructor:**
```php
new ElasticsearchHandler(array $hosts)
```
- `$hosts` — Array of Elasticsearch endpoint URLs (HTTPS)

**Methods:**
- `raw($index, $query, $count = 10, $sort = "", $type = null)` — Returns full ES response with metadata
- `query($index, $query, $count = 10, $sort = "", $type = null)` — Returns array of source documents only
- `count($index, $query, $type = null)` — Returns total count of matching documents

**Query syntax**: Lucene query string syntax (e.g., `field:value AND other:foo`)

## Key Patterns

- **AWS Signature V4 authentication**: Uses AWS SDK to sign requests to Elasticsearch
- **IAM role-based access**: Relies on IAM role permissions
- **Lucene query strings**: Simple query format (not full Elasticsearch DSL)
- **Type parameter**: Supports Elasticsearch types (deprecated in ES 7+, removed in OpenSearch 2+)

## Environment

**Required IAM permissions:**
- `es:ESHttpGet` on the Elasticsearch domain
- `es:ESHttpPost` if using write operations (not exposed by current API)

**AWS credentials**: Must be available via IAM role or environment variables.

## Deployment

**Deprecated**: Do not deploy new versions. Migrate consumers to `aws-opensearch-php-handler`.

**Migration path:**
1. Replace `bfansports/aws-elasticsearch-php-handler` with `bfansports/aws-opensearch-php-handler` in `composer.json`
2. Change `use SA\ElasticsearchHandler;` to `use SA\OpensearchHandler;`
3. Update class instantiation: `new ElasticsearchHandler()` → `new OpensearchHandler()`
4. Remove `$type` parameter from method calls (OpenSearch 2+ does not support types)
5. Test queries and verify results

## Testing

<!-- Ask: Does this repo have unit tests? -->

**Manual testing**: Not recommended. Use `aws-opensearch-php-handler` instead.

## Gotchas

- **DEPRECATED**: AWS Elasticsearch Service was rebranded to OpenSearch Service in 2021. This library uses the deprecated `elasticsearch-php` client.
- **Elasticsearch types removed**: Elasticsearch 7.x deprecated types, and OpenSearch 2.x removed them entirely. The `$type` parameter in this library is obsolete.
- **No longer maintained**: This library should not receive updates. All development effort goes to `aws-opensearch-php-handler`.
- **Composer version conflicts**: The `elasticsearch/elasticsearch` dependency may conflict with other packages that require newer versions.
- **Security updates**: The Elasticsearch PHP client may have unpatched vulnerabilities. Migrate to OpenSearch ASAP.

<!-- Ask: What's the timeline for fully deprecating this library? -->
<!-- Ask: Are there any blocking issues preventing migration to aws-opensearch-php-handler? -->
<!-- Ask: Should we add a deprecation notice to the README and composer.json? -->