#!/usr/bin/env bash
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

ACTION="${1:-run}"
if [[ "$ACTION" != "run" && "$ACTION" != "cleanup" ]]; then
  echo "Usage: $0 [run|cleanup]" >&2
  exit 2
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SCENARIO="$SCRIPT_DIR/DistributedVectorSearchE2E.scala"
PROXY_SCRIPT="$SCRIPT_DIR/rest_logging_proxy.py"
SUMMARY_SCRIPT="$SCRIPT_DIR/summarize_proxy_log.py"

RUN_ID="${E2E_RUN_ID:-$(date -u +%Y%m%d_%H%M%S)}"
TABLE_PREFIX="${E2E_TABLE_PREFIX:-dvs_e2e_$RUN_ID}"
NAMESPACE="${GRAVITINO_NAMESPACE:-vs}"
CATALOG_PARENT="${GRAVITINO_CATALOG:-lance_spark_vs}"
REST_URI="${GRAVITINO_LANCE_REST_URI:-http://127.0.0.1:9101/lance}"
OUTPUT_DIR="${E2E_OUTPUT_DIR:-/tmp/lance-spark-gravitino-e2e-$RUN_ID}"
KEEP_TABLES="${E2E_KEEP_TABLES:-0}"
STRICT_PORTS="${E2E_STRICT_CREDENTIAL_PORTS:-1}"
BUNDLE_MODULE="${BUNDLE_MODULE:-lance-spark-bundle-3.5_2.12}"

if [[ -n "${SPARK_HOME:-}" ]]; then
  SPARK_SHELL="$SPARK_HOME/bin/spark-shell"
else
  SPARK_SHELL="$(command -v spark-shell || true)"
fi
if [[ ! -x "$SPARK_SHELL" ]]; then
  echo "Set SPARK_HOME to a Spark 3.5.x distribution containing bin/spark-shell" >&2
  exit 1
fi

if [[ -n "${LANCE_SPARK_BUNDLE_JAR:-}" ]]; then
  BUNDLE_JAR="$LANCE_SPARK_BUNDLE_JAR"
else
  BUNDLE_JAR="$(find "$ROOT/$BUNDLE_MODULE/target" -maxdepth 1 -type f \
    -name "$BUNDLE_MODULE-*.jar" ! -name 'original-*' ! -name '*-sources.jar' \
    ! -name '*-javadoc.jar' -print 2>/dev/null | sort | head -n 1)"
fi
if [[ ! -s "$BUNDLE_JAR" ]]; then
  echo "Bundle jar not found. Build it first or set LANCE_SPARK_BUNDLE_JAR." >&2
  exit 1
fi

mkdir -p "$OUTPUT_DIR"
PROXY_LOG="$OUTPUT_DIR/rest-requests.jsonl"
PROXY_STDOUT="$OUTPUT_DIR/rest-proxy.log"
: >"$PROXY_LOG"

if [[ -n "${E2E_PROXY_PORT:-}" ]]; then
  PROXY_PORT="$E2E_PROXY_PORT"
else
  PROXY_PORT="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')"
fi
REST_PATH="$(python3 -c 'import sys; from urllib.parse import urlsplit; print(urlsplit(sys.argv[1]).path.rstrip("/"))' "$REST_URI")"
PROXY_URI="http://127.0.0.1:$PROXY_PORT$REST_PATH"

python3 "$PROXY_SCRIPT" \
  --listen-port "$PROXY_PORT" \
  --upstream "$REST_URI" \
  --log "$PROXY_LOG" >"$PROXY_STDOUT" 2>&1 &
PROXY_PID=$!

stop_proxy() {
  if kill -0 "$PROXY_PID" 2>/dev/null; then
    kill "$PROXY_PID" 2>/dev/null || true
    wait "$PROXY_PID" 2>/dev/null || true
  fi
}
trap stop_proxy EXIT

for _attempt in $(seq 1 50); do
  if curl -fsS "http://127.0.0.1:$PROXY_PORT/__e2e_health" >/dev/null 2>&1; then
    break
  fi
  sleep 0.1
done
if ! curl -fsS "http://127.0.0.1:$PROXY_PORT/__e2e_health" >/dev/null; then
  echo "REST logging proxy failed to start; see $PROXY_STDOUT" >&2
  exit 1
fi

run_spark() {
  local phase="$1"
  local mode="$2"
  local master="$3"
  local refresh="$4"
  local log="$OUTPUT_DIR/$phase.log"
  local worker_dir="$OUTPUT_DIR/spark-work-$phase"
  local local_dir="$OUTPUT_DIR/spark-local-$phase"

  mkdir -p "$worker_dir" "$local_dir"

  echo "Running phase=$phase mode=$mode master=$master credential_refresh=$refresh"
  env \
    -u AWS_ACCESS_KEY_ID \
    -u AWS_SECRET_ACCESS_KEY \
    -u AWS_SESSION_TOKEN \
    -u AWS_PROFILE \
    -u AWS_DEFAULT_PROFILE \
    SPARK_TESTING=1 \
    SPARK_LOCAL_IP=127.0.0.1 \
    SPARK_WORKER_DIR="$worker_dir" \
    SPARK_LOCAL_DIRS="$local_dir" \
    TMPDIR="$worker_dir" \
    E2E_MODE="$mode" \
    E2E_RUN_ID="$RUN_ID" \
    E2E_TABLE_PREFIX="$TABLE_PREFIX" \
    E2E_NAMESPACE="$NAMESPACE" \
    "$SPARK_SHELL" \
      --master "$master" \
      --jars "$BUNDLE_JAR" \
      --conf spark.ui.enabled=false \
      --conf spark.driver.host=127.0.0.1 \
      --conf spark.executor.memory=768m \
      --conf spark.worker.dir="$worker_dir" \
      --conf spark.local.dir="$local_dir" \
      --conf spark.sql.shuffle.partitions=4 \
      --conf spark.sql.extensions=org.lance.spark.extensions.LanceSparkSessionExtensions \
      --conf spark.sql.catalog.lance=org.lance.spark.LanceNamespaceSparkCatalog \
      --conf spark.sql.catalog.lance.impl=rest \
      --conf spark.sql.catalog.lance.uri="$PROXY_URI" \
      --conf spark.sql.catalog.lance.parent="$CATALOG_PARENT" \
      --conf 'spark.sql.catalog.lance.parent_delimiter=$' \
      --conf spark.sql.catalog.lance.executor_credential_refresh="$refresh" \
      --conf spark.sql.catalog.lance.headers.x-e2e-phase="$phase" \
      -i "$SCENARIO" >"$log" 2>&1
  rg '^(E2E|RESULT)\|' "$log"
}

if [[ "$ACTION" == "cleanup" ]]; then
  run_spark cleanup cleanup 'local[2]' false
  echo "Cleanup complete for lance.$NAMESPACE.$TABLE_PREFIX"
  exit 0
fi

run_spark setup_local setup 'local[2]' true
run_spark verify_local verify 'local[2]' true
run_spark verify_cluster verify 'local-cluster[2,1,1024]' true
run_spark credentials_on credentials 'local-cluster[2,1,1024]' true
run_spark credentials_off credentials 'local-cluster[2,1,1024]' false

local_topk="$(sed -n 's/^RESULT|.*fallback_topk=\([^|]*\).*$/\1/p' "$OUTPUT_DIR/verify_local.log" | tail -n 1)"
cluster_topk="$(sed -n 's/^RESULT|.*fallback_topk=\([^|]*\).*$/\1/p' "$OUTPUT_DIR/verify_cluster.log" | tail -n 1)"
if [[ -z "$local_topk" || "$local_topk" != "$cluster_topk" ]]; then
  echo "local/local-cluster fallback mismatch: local=$local_topk cluster=$cluster_topk" >&2
  exit 1
fi
echo "COMPARE|fallback_topk=$local_topk|status=PASS"

strict_arg=()
if [[ "$STRICT_PORTS" == "1" ]]; then
  strict_arg=(--strict)
fi
python3 "$SUMMARY_SCRIPT" \
  --log "$PROXY_LOG" \
  --phase credentials_on \
  --table-suffix "${TABLE_PREFIX}_fallback" \
  --expected-executor-clients 4 \
  "${strict_arg[@]}"
python3 "$SUMMARY_SCRIPT" \
  --log "$PROXY_LOG" \
  --phase credentials_off \
  --table-suffix "${TABLE_PREFIX}_fallback" \
  --expected-executor-clients 0 \
  "${strict_arg[@]}"

if [[ "$KEEP_TABLES" == "0" ]]; then
  run_spark cleanup cleanup 'local[2]' false
else
  echo "Keeping tables with prefix lance.$NAMESPACE.$TABLE_PREFIX"
fi

echo "E2E complete"
echo "run_id=$RUN_ID"
echo "table_prefix=lance.$NAMESPACE.$TABLE_PREFIX"
echo "output_dir=$OUTPUT_DIR"
echo "proxy_log=$PROXY_LOG"
