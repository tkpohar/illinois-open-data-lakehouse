# Rate limits & retries

Socrata portals throttle unauthenticated clients and, under load, authenticated ones too.
`SocrataClient` treats **rate limits** and **other failures** as two separate problems
with separate retry budgets.

## Rate limits

A response is a rate limit when it is **HTTP 429**, or **HTTP 503 with a `Retry-After`**
header (servers in maintenance or overload use 503 + `Retry-After` the same way).

| Situation | What the client does |
|---|---|
| `Retry-After: 7` (seconds) | waits exactly 7 s, retries the same page |
| `Retry-After: Wed, 11 Nov 2026 03:45:00 GMT` (HTTP date) | waits until that time (never negative) |
| no / unparseable `Retry-After` | exponential backoff: `backoff_seconds × 2^(n-1)` |
| asked to wait > `max_retry_after` (default 300 s) | raises `RateLimitError` immediately, no sleep |
| more than `max_rate_limit_retries` (default 5) in a row on one request | raises `RateLimitError` |

Backoff waits are also capped at `max_retry_after`. Rate-limit retries **don't** consume
the error retry budget, so a throttled run doesn't fail just because the server asked it
to slow down a few times. The retry resumes at the same `$offset`, so no page is skipped
or duplicated.

`RateLimitError` subclasses `requests.HTTPError`, carries `.retry_after` (seconds or None)
and `.response`, and — like any ingestion error — makes `lakehouse ingest` log the
traceback and exit with status 1.

## Other failures

Connection errors, timeouts and other 4xx/5xx responses (including 503 *without*
`Retry-After`) use exponential backoff, up to `max_retries` attempts (default 3), then
re-raise the original exception.

## Observability

Every wait is logged at WARNING with structured fields (see [logging.md](logging.md)):

```json
{"level": "WARNING", "message": "Rate limited (HTTP 429); waiting 7.0s before retry 1",
 "run_id": "...", "status": 429, "attempt": 1, "wait_s": 7.0, "retry_after_header": true}
```

and counted on the client:

```python
client.stats   # RequestStats(requests=12, retries=2, rate_limited=1, waited_s=9.0)
```

## Avoiding rate limits

Set `SOCRATA_APP_TOKEN` (free from the portal) — it's sent as `X-App-Token` and gets a
much higher request budget than anonymous access.

## Configuration

```python
SocrataClient(
    max_retries=3,              # non-rate-limit attempts
    backoff_seconds=2.0,        # base for exponential backoff
    max_rate_limit_retries=5,   # consecutive rate-limit retries per request
    max_retry_after=300.0,      # longest wait the client will sleep through
    sleep=time.sleep,           # injectable for tests
)
```
