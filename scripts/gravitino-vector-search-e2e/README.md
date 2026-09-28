# Gravitino distributed vector-search E2E

This package reproduces distributed `VECTOR_SEARCH` against a live Gravitino 1.3.0
Lance REST server backed by MinIO. Spark receives no object-store endpoint, access key,
secret key, or session token. The only Spark catalog connection is the Lance REST URI, so
dataset location and storage credentials must come from the server.

## Scenarios

The runner creates three isolated tables for one run:

| Scenario | Assertion |
| --- | --- |
| `spark.sql.lance.search.distributed.enabled=false` | Gravitino 1.3.0 `/query` returns the expected 404 |
| Four bare fragments | Four fallback tasks and exact top-5 |
| `local[2]` versus `local-cluster[2,1,1024]` | Identical fallback, indexed, mixed and option results |
| Four `IVF_FLAT` segments | Four indexed tasks and exact global top-k |
| Three indexed fragments plus one bare fragment | The nearest row in the bare fragment wins the global merge |
| `fast_search=true` | Only indexed fragments are planned; the bare-fragment row is excluded |
| Filter, offset and `k > row count` | Results match a brute-force Spark calculation |
| Executor credential refresh enabled | Four executor REST clients describe the four fallback fragments |
| Executor credential refresh disabled | Only the driver describes the table; executors use driver-vended options |

Credential traffic is measured by a loopback-only reverse proxy. It records request metadata
and client source ports as JSONL, but never records request/response bodies or credential headers.
Each Spark process has one driver REST client; additional distinct clients in the credential phases
are executor namespace clients.

## Prerequisites

- A running Gravitino 1.3.0 Lance REST service and its configured MinIO backend.
- An existing Gravitino Lance catalog and schema. Defaults are `lance_spark_vs.vs`.
- Spark 3.5.x with Scala 2.12.
- A built `lance-spark-bundle-3.5_2.12` jar from the current checkout.
- `bash`, `python3`, `curl`, and `rg`.

Build the bundle if needed:

```bash
./mvnw -q -DskipTests package -pl lance-spark-bundle-3.5_2.12 -am
```

## Run

```bash
export SPARK_HOME=/path/to/spark-3.5.3-bin-hadoop3
export GRAVITINO_LANCE_REST_URI=http://127.0.0.1:9101/lance
export GRAVITINO_CATALOG=lance_spark_vs
export GRAVITINO_NAMESPACE=vs

./scripts/gravitino-vector-search-e2e/run.sh
```

The runner explicitly removes the standard AWS credential environment variables from every Spark
process and rejects any `spark.sql.catalog.lance.storage.*` configuration. Do not add Hadoop S3A or
Spark catalog storage credentials when using this test; doing so invalidates the credential-vending
assertion.

By default, successful runs purge their three uniquely named tables. To retain them for inspection:

```bash
E2E_KEEP_TABLES=1 ./scripts/gravitino-vector-search-e2e/run.sh
```

Useful overrides:

| Variable | Default | Purpose |
| --- | --- | --- |
| `E2E_RUN_ID` | UTC timestamp | Reuse a stable identifier in logs and table names |
| `E2E_OUTPUT_DIR` | `/tmp/lance-spark-gravitino-e2e-$E2E_RUN_ID` | Spark and proxy logs |
| `LANCE_SPARK_BUNDLE_JAR` | Auto-detected target jar | Test a specific connector artifact |
| `E2E_PROXY_PORT` | Free loopback port | Pin the request-recording proxy port |
| `E2E_STRICT_CREDENTIAL_PORTS` | `1` | Set to `0` to report rather than assert client counts |
| `E2E_KEEP_TABLES` | `0` | Retain test tables when set to `1` |

The high-signal output is prefixed with `E2E|`, `RESULT|`, `COMPARE|`, or `CREDENTIALS|`.
Full Spark logs and `rest-requests.jsonl` remain under the output directory.
The runner enables Spark's local-cluster test mode and places its worker and shuffle directories
under the output directory, so Spark does not need write access to `$SPARK_HOME/work`.

## Failure handling

- A failed run does not automatically delete tables, preserving evidence for inspection.
- To remove retained tables safely, use the same `E2E_RUN_ID` (and the same catalog/namespace)
  with `./scripts/gravitino-vector-search-e2e/run.sh cleanup`. This drops only that run's three
  uniquely named tables with `PURGE`.
- Do not point `E2E_TABLE_PREFIX` at an existing non-test table. Setup intentionally refuses to
  replace existing tables.
