# Market Pulse

Metrics for equities and crypto, computed from two pipelines that work differently
because the underlying markets do.

Spanish and US stocks trade during fixed exchange hours, so their data arrives once
a day in a lump. Crypto trades continuously, so its data arrives as a stream of
individual trades with no natural boundary. The first case wants a scheduler with
backfill and idempotent reruns. The second wants a message broker and a batch job
that closes the day. Both land in the same Parquet layout and feed one set of
metrics, which is the part worth looking at: a stock and a trading pair end up as
rows of the same shape.

## Architecture

```
yfinance ────► Airflow: ingest_stocks ────► Parquet (raw)
  daily bars     fetch / validate / write        date=/market=
                                                      │
                                                      ├──► Airflow: build_marts ──► snapshot.json ──► FastAPI ──► React
                                                      │      unify + metrics
Binance WS ──► producer ──► Kafka ──► consumer ──► Parquet (raw ticks)
  trade stream              crypto_ticks           date=/pair=
                                                      │
                                            Airflow: process_crypto
                                              ticks → OHLC bars (1m, 1h)
```

The two producers on the left never meet until `build_marts`, which reduces daily
stock bars and hourly crypto bars to one close per instrument per day and computes
returns, rolling volatility, drawdown and comparison against a benchmark. Stocks
are measured against their index, crypto against BTC.

## Layout

```
airflow/dags/     ingest_stocks, process_crypto, build_marts
ingestion/        fetching, validation, Parquet writes, Kafka producer and consumer
analytics/        unified series, metrics, snapshot export
api/              FastAPI over the snapshot
web/              Vite + React dashboard
docker/           compose stack and images
deploy/nginx/     server block for a subdomain
tests/            pytest suite, no Airflow process required
```

DAG files contain scheduling, ordering and retry policy and nothing else. The work
they call lives in `ingestion/` and `analytics/`, which is why the test suite runs
in a couple of seconds without a scheduler, a broker or a database.

## Running it

```bash
cp docker/.env.example docker/.env
echo "AIRFLOW_UID=$(id -u)" >> docker/.env
python -c "import secrets; print('FERNET_KEY=' + secrets.token_urlsafe(32))" >> docker/.env
python -c "import secrets; print('AIRFLOW_JWT_SECRET=' + secrets.token_urlsafe(32))" >> docker/.env
python -c "import secrets; print('AIRFLOW_ADMIN_PASSWORD=' + secrets.token_urlsafe(24))" >> docker/.env

docker compose -f docker/docker-compose.yml build
docker compose -f docker/docker-compose.yml up -d
```

The Airflow UI is at `127.0.0.1:8080` and the API at `127.0.0.1:8000`. Neither is
published on a public interface. The crypto producer and consumer start with the
stack and keep running; the DAGs start paused.

For the dashboard in development:

```bash
cd web && npm install && npm run dev
```

Tests and linters:

```bash
uv run --extra dev pytest -q -m "not network"
uv run --extra dev ruff check ingestion analytics api tests airflow/dags
cd web && npm run lint && npm run build
```

Tests marked `network` call yfinance and are excluded by default, so a green run
never depends on an external service being up.

## Decisions

**Idempotency comes from the file layout, not from a merge.** A stock partition
path is a pure function of date and market. Rerunning a day deletes the directory
and writes it again. There is no read-modify-write step to get wrong, which is what
makes retries and `catchup` safe.

**Kafka messages are keyed by trading pair.** The key decides the partition, so all
trades for a pair go to one partition and keep their order. An unkeyed produce would
round robin across partitions and lose that.

**The consumer commits offsets after the batch is on disk.** A crash in between
replays the batch, which duplicates ticks. Committing first would drop them instead.
Duplicates are removable later and missing trades are not.

**Deduplication happens during aggregation.** The consumer only sees its own batch
and cannot tell a replay from a new trade. The daily aggregation sees the whole day
and drops repeats by pair and trade id, which closes the gap the commit ordering
deliberately leaves open.

**`build_marts` waits on sensors rather than a safe-looking hour.** It watches the
two upstream DAGs and reschedules between checks instead of holding a worker slot.

**The API reads a file.** `build_marts` writes `snapshot.json` once a day, so a
request is a dictionary lookup. The public service holds no connection to storage
and builds no query from user input.

**yfinance truncates history without erroring.** A batch request sometimes returns
a handful of bars for some tickers instead of the full range, with no gaps inside
it and no exception. Those tickers are refetched individually, and validation warns
if short coverage survives. This was found by checking the source before writing
the DAG, and it is the reason `validate_raw` exists as a separate step.

## Airflow 3

The stack runs Airflow 3, where the DAG processor is a separate service rather than
part of the scheduler. If `airflow-dag-processor` fails to start, the scheduler
still reports healthy and the UI still loads, but no DAG ever appears. The test
suite parses every DAG file and asserts the DAG object is reachable from module
globals, because losing that reference also parses cleanly and registers nothing.

## Deployment

`deploy/nginx/market-pulse.conf` is a server block for a host that already
terminates TLS. It serves the built assets and proxies `/api/` to the API on
loopback. Methods other than GET and HEAD are refused at the proxy, and requests
are rate limited per address using the zone in `rate-limit.conf`.

Airflow is not proxied. It runs arbitrary code by design, so it stays on loopback
and is reached through an SSH tunnel:

```bash
ssh -L 8080:127.0.0.1:8080 user@host
```

CI parses the compose file and fails if any port is published beyond loopback, so
opening a service to the internet requires editing a check rather than mistyping a
port mapping.

## Not in this project

There is no cloud warehouse, no dbt and no Spark. Aggregation is pandas inside an
Airflow task, which is enough at this volume. The scope was cut to keep the two
orchestration pieces working and defensible instead of spreading thin across more
tools.
