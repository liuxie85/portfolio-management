# Service API

`src/service/http.py` is the service-first HTTP boundary. It uses FastAPI and
delegates to `src/service/application.py`, which currently wraps the existing
`skill_api.py` facade during migration.

## Run

```bash
python scripts/service.py start
python scripts/service.py status
python scripts/service.py stop
```

Default URL: `http://127.0.0.1:8765`.

The service is unauthenticated and binds to loopback hosts only by default.
Binding to `0.0.0.0` or any other non-loopback address requires the explicit
`--allow-remote` flag and should only be used behind an authenticated network
boundary.

Overrides:

- `PORTFOLIO_SERVICE_HOST`
- `PORTFOLIO_SERVICE_PORT`
- `PORTFOLIO_SERVICE_URL`

## Read Endpoints

- `GET /health`
- `GET /accounts?include_default=true`
- `GET /accounts/overview?accounts=alice,bob&price_timeout=30`
- `GET /holdings?account=alice&include_cash=true&include_price=false`
- `GET /cash?account=alice`
- `GET /nav?account=alice&days=30`
- `GET /report/full?account=alice&price_timeout=30`
- `GET /report/{daily|monthly|yearly}?account=alice&price_timeout=30`

Legacy `/accounts/{account}/...` routes remain available for compatibility, but
new clients should pass account as a query parameter so account names do not
need to be embedded in the URL path.

## Migration Rule

New product behavior should enter through `src/service/application.py` and the
HTTP route layer first. `skill_api.py`, `mcp_server.py`, and `scripts/pm.py`
remain compatibility adapters while heavy business logic is moved out of the
Skill facade over time.

`scripts/pm.py` read-only commands and MCP read tools prefer the local HTTP
service and silently fall back to `skill_api.py` when the service is unavailable.
Use `--no-service` in the CLI for explicit direct mode, or `--require-service`
to fail instead of falling back when the service cannot be reached.
