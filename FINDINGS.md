# AI Audit Findings — aws-elasticsearch-php-handler

**Date:** 2026-02-17  
**Auditor:** Backend Developer (AI Agent — Claude Opus 4.6)  
**Repo:** bfansports/aws-elasticsearch-php-handler  
**Branch audited:** master (tag 2.2)  
**Scope:** Deprecation status, security, code quality, migration completeness

---

## Critical

### C1. Stale composer.lock in sa_site_v2/scripts resolves Elasticsearch handler instead of OpenSearch

**File:** `repos/web/sa_site_v2/scripts/composer.lock` (lines 160-202)  
**Impact:** The lock file maps `bfansports/aws-opensearch-php-handler` (the correct package name) to the **old** `aws-elasticsearch-php-handler` git repo (`0e8f4c6`). This means `composer install` in that directory installs the deprecated Elasticsearch client (`elasticsearch/elasticsearch` v6.8.3) instead of the OpenSearch client.

**Evidence:**
```json
{
  "name": "bfansports/aws-opensearch-php-handler",
  "source": {
    "url": "https://github.com/bfansports/aws-elasticsearch-php-handler.git",
    "reference": "0e8f4c6659f7cc0a755e888bd24e5bae53805426"
  }
}
```

The `composer.json` correctly requires `aws-opensearch-php-handler`, but the lock file was never regenerated after the package migration. This is likely a Packagist naming conflict or the opensearch handler's Packagist entry was registered over the elasticsearch handler's old name.

**Fix:** In `sa_site_v2/scripts/`, run `composer update bfansports/aws-opensearch-php-handler` (or `composer update` entirely) to regenerate the lock file pointing to the correct OpenSearch repo. Verify the resolved URL is `github.com/bfansports/aws-opensearch-php-handler.git`.

---

## High

### H1. Lucene Query String Injection — user input passed directly to `query_string`

**File:** `src/SA/ElasticsearchHandler.php` (lines 85-116, 118-135, 193-242, 244-281)  
**Impact:** Every public method that accepts a `$query` parameter (`aggregate`, `count`, `query`, `raw`, `scan`) passes it directly into Elasticsearch's `query_string` query without any sanitization or escaping.

Lucene query string syntax supports:
- Field traversal: `_exists_:fieldname`, `fieldname:*`
- Wildcard and regex: `field:/regex/`, `field:*`
- Boosting and fuzzy: `term~2`, `term^5`
- Boolean operators that can alter query semantics

If any calling code passes user-controlled input into `$query`, an attacker could:
- Enumerate index fields and data
- Craft expensive regex/wildcard queries (ReDoS against Elasticsearch)
- Bypass intended query restrictions

**Severity context:** This library is deprecated and may not receive user-facing input directly. However, the same pattern exists in `aws-opensearch-php-handler` (identical code). The OpenSearch successor should be the fix target.

**Fix (for OpenSearch handler):** Sanitize or escape special Lucene characters (`+ - && || ! ( ) { } [ ] ^ " ~ * ? : \ /`) before embedding in `query_string`, or migrate to `match` / `multi_match` queries where possible.

### H2. Repository not archived on GitHub despite being fully deprecated

**Impact:** The repo is not archived (`"archived": false`), not marked deprecated in topics, and has no deprecation notice in the GitHub description. New consumers could accidentally depend on it. The `develop` branch and stale feature branches (`feature/debug`, `revert-19-feature/remove-timeout`) remain open.

**Fix:**
1. Archive the repo on GitHub (Settings > Danger Zone > Archive)
2. Add `deprecated` to the repo topics
3. Update the GitHub description to: `[DEPRECATED] Use aws-opensearch-php-handler instead`
4. Delete stale branches: `feature/debug`, `revert-19-feature/remove-timeout`

### H3. Deprecated `elasticsearch/elasticsearch` v6.8.3 dependency — end of life

**File:** `composer.json` (line 22)  
**Impact:** Requires `elasticsearch/elasticsearch: ~6.7`. Elasticsearch PHP client 6.x is long end-of-life (EOL since ~2022). No security patches are released. Known issues include:
- Dependency on `guzzlehttp/ringphp` which is abandoned
- Potential HTTP client vulnerabilities in older Guzzle transports

**Fix:** Do not update this library. Migrate consumers to `aws-opensearch-php-handler`.

---

## Medium

### M1. GitHub Actions workflow uses deprecated `actions/checkout@v2`

**File:** `.github/workflows/github-backup.yml` (line 8)  
**Impact:** `actions/checkout@v2` uses Node.js 12 which is EOL. GitHub shows deprecation warnings. The workflow also triggers on `push` to `develop` branch, but the default branch is `master` — so it may never trigger.

**Fix:** If the repo is archived, disable the workflow. If kept active, update to `actions/checkout@v4` and fix the branch trigger to `master`.

### M2. GitHub Actions backup uses long-lived AWS access keys

**File:** `.github/workflows/github-backup.yml` (lines 13-14)  
**Impact:** Uses `secrets.AWS_ACCESS_KEY` and `secrets.AWS_SECRET_KEY` — long-lived IAM credentials stored as GitHub secrets. Best practice is to use OIDC with `aws-actions/configure-aws-credentials` and assume a role.

**Fix:** Low priority given the repo should be archived. Document for other repos that still use this pattern.

### M3. `$_SERVER['AWS_DEFAULT_REGION']` — hardcoded superglobal access

**File:** `src/SA/ElasticsearchHandler.php` (line 19)  
**Impact:** Uses `$_SERVER` directly in the constructor. No fallback, no validation. If the variable is unset, PHP emits an `E_NOTICE` and `SignatureV4` receives `null` as the region, which may produce confusing errors.

The same pattern exists in `aws-opensearch-php-handler` (line 19).

**Fix (for OpenSearch handler):** Accept region as a constructor parameter with `$_SERVER['AWS_DEFAULT_REGION']` as fallback. Throw a clear exception if neither is available.

### M4. Undeclared property `$cacheKey` used in getter/setter

**File:** `src/SA/ElasticsearchHandler.php` (lines 287-293)  
**Impact:** `getCacheKey()` and `setCacheKey()` reference `$this->cacheKey` but it is never declared as a class property. In PHP 8.2+, dynamic properties are deprecated and will be removed in PHP 9.0. This triggers `E_DEPRECATED` warnings.

**Fix:** Not applicable (deprecated library). Note: same issue exists in `aws-opensearch-php-handler` (lines 275-281).

### M5. Error handler in async response swallows errors silently

**File:** `src/SA/ElasticsearchHandler.php` (lines 63-65)  
**Impact:**
```php
function ($error) {
    return $error['response'];
}
```
The error callback in the HTTP handler returns `$error['response']` without logging or re-throwing. If the response is null or malformed, the subsequent `getStatusCode()` call on line 71 will throw an uncatchable `TypeError`. There is no error logging or structured error handling.

### M6. `scan()` unbounded memory — loads all results into memory

**File:** `src/SA/ElasticsearchHandler.php` (lines 244-281)  
**Impact:** The `scan()` method uses `search_after` pagination with `size: 10000` and accumulates all results in `$results` via `array_merge`. For large indices, this can exhaust PHP memory. There is no memory guard or streaming mechanism.

**Fix:** Not applicable (deprecated). The OpenSearch version has the same issue but with better safety checks (lines 222-268).

### M7. Backup workflow targets wrong branch

**File:** `.github/workflows/github-backup.yml` (line 5)  
**Impact:** Triggers on pushes to `develop`, but the default branch is `master`. The `develop` branch exists but may be stale. The backup workflow likely never runs on current changes.

---

## Low

### L1. PSR-0 autoloading is deprecated

**File:** `composer.json` (line 9)  
**Impact:** Uses PSR-0 autoloading. PSR-0 has been deprecated in favor of PSR-4 since 2014. Not a functional issue, but indicates the library has not been modernized.

### L2. PHP 7.0 minimum version — end of life since 2019

**File:** `composer.json` (line 23)  
**Impact:** Requires `php: >=7.0`. PHP 7.0 has been EOL since December 2019. The OpenSearch successor requires PHP 7.4+.

### L3. `getRetriesCount()` / `setRetriesCount()` are dead code

**File:** `src/SA/ElasticsearchHandler.php` (lines 323-329)  
**Impact:** Empty method bodies with a comment saying they are deprecated. These should have been removed. They return `null` implicitly, which could confuse callers.

### L4. README usage example has incorrect parameter order

**File:** `README.md` (line 22)  
**Impact:** The README shows `$client->raw($ESindex, $query, $count, $sort, $EStype)` but the actual method signature is `raw($index, $query, $count, $sort, $offset, $type)` — missing the `$offset` parameter. Callers following the README would pass `$type` as `$offset`.

### L5. `createDocument()` derives `$type` from index name

**File:** `src/SA/ElasticsearchHandler.php` (lines 137-154)  
**Impact:** `$type` defaults to `explode("_", $index)[0]`, which is a fragile convention. If the index name doesn't follow the expected naming pattern, the wrong type is assigned. Elasticsearch types are deprecated anyway.

### L6. `$timeout` property only used in credential provider, not HTTP requests

**File:** `src/SA/ElasticsearchHandler.php` (lines 14, 26, 331-337)  
**Impact:** `$timeout` is only passed to `CredentialProvider::defaultProvider()`. There is no HTTP request timeout configured on the Guzzle handler or Elasticsearch client. Long-running queries could hang indefinitely.

---

## Agent Skill Improvements

### S1. Update OpenSearch handler CLAUDE.md with shared findings

Findings H1, M3, M4, M5, and M6 apply identically to `aws-opensearch-php-handler`. Its CLAUDE.md Gotchas section should be updated with:
- Query injection warning for `query_string` queries
- `$_SERVER` region fallback issue
- Undeclared `$cacheKey` property (PHP 8.2 deprecation)
- Silent error swallowing in HTTP handler
- Unbounded memory in `scan()`

### S2. Create GitHub Issue in sa_site_v2 for stale composer.lock

The stale lock file (C1) is in a different repo. A GitHub Issue should be filed in `bfansports/sa_site_v2` to track the fix.

### S3. Packagist audit

Verify Packagist package ownership. The lock file suggests possible namespace confusion between the elasticsearch and opensearch packages. Check if `bfansports/aws-opensearch-php-handler` is correctly registered on Packagist as a distinct package pointing to the correct git repo.

---

## Positive Observations

1. **AWS Signature V4 authentication** — Properly uses AWS SDK's `SignatureV4` for request signing. No hardcoded credentials in the library code.
2. **Credential provider chain** — Supports both SSO profiles and the default credential provider chain, which is the recommended approach.
3. **Migration completed** — No PHP files in the codebase actively import `ElasticsearchHandler`. The migration to `OpensearchHandler` appears functionally complete. Only the stale lock file remains.
4. **API compatibility** — The OpenSearch successor maintains the same method signatures (minus `$type`), making migration straightforward for consumers.
5. **MIT license** — Clean, permissive license with no compliance concerns.
6. **Single responsibility** — The library does one thing (AWS-authenticated Elasticsearch access) and does it simply. No unnecessary abstractions.
