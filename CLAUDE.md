# aws-elasticsearch-php-handler

## What This Is

> **DEPRECATED — DO NOT USE FOR NEW PROJECTS**
>
> This library has been fully superseded by [`aws-opensearch-php-handler`](https://github.com/bfansports/aws-opensearch-php-handler).
> AWS renamed Elasticsearch Service to OpenSearch Service in 2021. This library uses the abandoned `elasticsearch/elasticsearch` v6.x PHP client.
> **No consumers in the bFAN codebase actively use this library** — migration is complete.
> This repo should be archived on GitHub.

Legacy PHP library for connecting to AWS Elasticsearch Service. Provides a Lucene query syntax interface for searching, counting, aggregating, and managing documents via AWS Signature V4-authenticated requests.

## Deprecation & Migration Status

| Item | Status |
|------|--------|
| Successor repo | `bfansports/aws-opensearch-php-handler` (active, v1.0.10+) |
| sa_site_v2 (admin panel) | Migrated to OpenSearch handler in `composer.json` |
| sa_site_daemons | Migrated to OpenSearch handler |
| PHP code imports | **No PHP files** import `ElasticsearchHandler` anywhere in the codebase |
| composer.lock residue | `sa_site_v2/scripts/composer.lock` still resolves to this repo's git URL (stale lock — see Gotchas) |
| GitHub repo status | **Not archived** (should be) |
| Packagist | May have namespace overlap with opensearch handler — needs verification |

### Migration Checklist (for any remaining consumers)

1. Replace `bfansports/aws-elasticsearch-php-handler` with `bfansports/aws-opensearch-php-handler` in `composer.json`
2. Change `use SA\ElasticsearchHandler;` to `use SA\OpensearchHandler;`
3. Change `new ElasticsearchHandler()` to `new OpensearchHandler()`
4. Remove `$type` parameter from all method calls (OpenSearch 2+ does not support types)
5. Run `composer update` to regenerate lock file
6. Test queries — API is otherwise identical

## Tech Stack

- **Language**: PHP 7.0+ (EOL — OpenSearch handler requires PHP 7.4+)
- **AWS SDK**: `aws/aws-sdk-php` 3.x — AWS authentication and signing
- **Elasticsearch Client**: `elasticsearch/elasticsearch` ~6.7 (abandoned, EOL)
- **HTTP Transport**: Guzzle via `GuzzleHttp\Ring` (abandoned library)
- **Autoloading**: PSR-0 (deprecated; OpenSearch handler also uses PSR-0)

## Project Structure

```
src/SA/ElasticsearchHandler.php   # Main handler class (346 lines)
composer.json                      # Dependencies and autoload config
LICENSE                            # MIT license
README.md                          # Usage instructions (outdated parameter order)
.github/workflows/                 # S3 backup workflow (targets wrong branch)
```

## Dependencies

**External:**
- AWS Elasticsearch Service (now OpenSearch Service)
- AWS IAM credentials — for signing requests

**Consumed by:**
- **No active consumers** — migration to `aws-opensearch-php-handler` is complete
- Stale reference in `sa_site_v2/scripts/composer.lock` (lock file only, not composer.json)

## API / Interface

**Main class**: `SA\ElasticsearchHandler`

**Constructor:**
```php
new ElasticsearchHandler(array $endpoints)
```
- `$endpoints` — Array of Elasticsearch endpoint URLs (HTTPS)
- Reads `$_SERVER['AWS_DEFAULT_REGION']` directly (no parameter, no fallback)
- Reads `$_SERVER['AWS_PROFILE']` for SSO credential provider

**Query methods (all accept raw Lucene query strings):**
- `raw($index, $query, $count = 1, $sort = null, $offset = 0, $type = null)` — Full ES response
- `query(...)` — Same params as `raw()`, returns `_source` documents only
- `count($index, $query, $type = null)` — Document count
- `aggregate($index, $query, $data, $type = null)` — Aggregation results
- `scan($index, $query, $type = null)` — Paginated full-index scan (unbounded memory)
- `search($params = [])` — Raw pass-through to ES client

**Index management:**
- `createIndex($index)`, `deleteIndex($index)`, `indexExists($index)`
- `getIndexSettings($params)`, `putIndexSettings($params)`
- `getIndexMapping($params)`, `putIndexMapping($params)`
- `updateIndexAliases($params)`, `getIndexAliases()`
- `reindex($params)`, `indices()`

**Document operations:**
- `createDocument($index, $data, $id = null, $type = null)`
- `deleteDocument($index, $id)`

## Key Patterns

- **AWS Signature V4**: All requests signed via `Aws\Signature\SignatureV4`
- **RingPHP bridge**: Converts PSR-7 responses to RingPHP format for the ES client
- **Lucene query strings**: Uses `query_string` query type (injection risk if user input is unsanitized)
- **search_after pagination**: `scan()` method uses `search_after` for deep pagination

## Security Findings (from AI Audit 2026-02-17)

- **Query injection**: All `$query` parameters pass raw Lucene syntax to `query_string` — no sanitization. Callers must escape special characters if handling user input.
- **Silent error swallowing**: HTTP error callback returns `$error['response']` without logging. Can mask failures and produce confusing TypeErrors downstream.
- **No request timeout**: Only credential provider has a timeout. HTTP requests can hang indefinitely.
- **Undeclared `$cacheKey` property**: `getCacheKey()`/`setCacheKey()` use a dynamic property — triggers `E_DEPRECATED` on PHP 8.2+.

## Environment

**Required:**
- `$_SERVER['AWS_DEFAULT_REGION']` — AWS region (no fallback; errors if unset)
- IAM credentials via role, environment vars, or `~/.aws/credentials`

**Optional:**
- `$_SERVER['AWS_PROFILE']` — Uses SSO credential provider when set

## Deployment

**DO NOT DEPLOY.** This library is deprecated. Migrate consumers to `aws-opensearch-php-handler`.

## Testing

No unit tests exist. No test infrastructure. Manual testing only against a live ES cluster.

## Gotchas

- **DEPRECATED**: AWS Elasticsearch Service was rebranded to OpenSearch Service in 2021. This library uses the abandoned `elasticsearch-php` v6.x client.
- **Stale composer.lock**: `sa_site_v2/scripts/composer.lock` maps the OpenSearch package name to this repo's git URL. This is a Packagist/Composer resolution artifact. Running `composer update` in that directory should fix it, but verify the resolved URL changes to the OpenSearch repo.
- **README parameter order is wrong**: README shows `raw($index, $query, $count, $sort, $type)` but actual signature has `$offset` before `$type`.
- **`scan()` loads everything into memory**: No streaming, no memory limit. Can OOM on large indices.
- **Backup workflow targets `develop` branch**: But default branch is `master`. Workflow likely never runs.
- **`createDocument()` derives type from index name**: Uses `explode("_", $index)[0]` as the default `$type` — fragile naming convention.
- **`getRetriesCount()` / `setRetriesCount()` are dead code**: Empty bodies, return null.
- **Elasticsearch types are removed**: The `$type` parameter throughout this library is obsolete (removed in ES 7+, OpenSearch 2+).
- **`actions/checkout@v2`**: GitHub Actions workflow uses deprecated action version (Node.js 12 EOL).
