# Wiring up the ingest pipeline

## 1. config.json
`db_clients` is currently inside the big `/* ... */` comment at the top of config.json.
Add this as a live key, e.g. right after that comment closes (just before `"app": {`):

```json
"db_clients": [
  {
    "name": "default",
    "rdbms": "postgresql",
    "host": "127.0.0.1",
    "port": 6432,
    "dbname": "test",
    "user": "bench",
    "passwd": "bench",
    "is_fast": false,
    "number_of_connections": 8,
    "timeout": 10.0,
    "auto_batch": false
  }
],
```
Port 6432 = PgBouncer, not Postgres. Drogon must be built with PostgreSQL (libpq) support.

## 2. Database + PgBouncer
```bash
psql -h 127.0.0.1 -p 5432 -U bench -d test -f schema.sql     # direct to Postgres
sudo cp pgbouncer/pgbouncer.ini pgbouncer/userlist.txt /etc/pgbouncer/
sudo systemctl restart pgbouncer                              # needs PgBouncer >= 1.21
```

## 3. Build / run
Files to copy into the repo: `main.cc` (replaces yours), `include/ingest/*.hpp`,
`schema.sql`, `pgbouncer/`, `bench/post.lua`, `test/pipeline_test.cc`.
CMakeLists.txt needs no change (`include/` is already on the include path).

Tunables (env vars): INGEST_BATCH_MAX=2000, INGEST_FLUSH_US=5000, INGEST_WRITERS=2,
INGEST_MAX_INFLIGHT=8, INGEST_QUEUE_CAP=262144 (power of two).

## 4. Verify
```bash
wrk -t8 -c256 -d30s -s bench/post.lua http://127.0.0.1:5555/ingest
curl -s localhost:5555/stats                 # written should reach accepted, failed should be 0
psql -h 127.0.0.1 -p 5432 -U bench -d test -c "select count(*) from events"
```
Count your RPS from 202 responses AND check `written == accepted` / the row count,
otherwise you are measuring the queue, not the pipeline.

## Standalone test (no Drogon/Postgres needed)
g++ -std=c++17 -O2 -pthread -I include test/pipeline_test.cc -o pipeline_test && ./pipeline_test