#!/usr/bin/env bash
# Load artifacts into KafkaSQL Registry, take snapshots, measure dump size,
# restart Registry, and record time-to-ready for before/after comparisons.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"

REGISTRY_URL="${REGISTRY_URL:-http://localhost:18080}"
# Health lives on Quarkus management port (9000), not the app port (8080).
MANAGEMENT_URL="${MANAGEMENT_URL:-http://localhost:19000}"
ARTIFACTS="${ARTIFACTS:-200}"
CONTENT_BYTES="${CONTENT_BYTES:-8192}"
SNAPSHOTS="${SNAPSHOTS:-3}"
GROUP_ID="${GROUP_ID:-snapshot-repro}"
CLEAR_SNAPSHOTS="${CLEAR_SNAPSHOTS:-0}"
SAVE_BASELINE="${SAVE_BASELINE:-0}"
READY_TIMEOUT_SEC="${READY_TIMEOUT_SEC:-600}"
# When 1, force-recreate registry so compose env (e.g. kafka-store) is applied.
RECREATE="${RECREATE:-0}"

log() { printf '[repro] %s\n' "$*" >&2; }
die() { printf '[repro] ERROR: %s\n' "$*" >&2; exit 1; }

need() { command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"; }
need curl
need python3

if [[ -z "${COMPOSE:-}" ]]; then
  if docker compose version >/dev/null 2>&1; then
    COMPOSE="docker compose"
  elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE="docker-compose"
  else
    die "neither 'docker compose' nor 'docker-compose' is available"
  fi
fi

SNAPSHOT_DIR="$ROOT/data/snapshots"
RESULTS_DIR="$ROOT/results"
mkdir -p "$SNAPSHOT_DIR" "$RESULTS_DIR"

wait_ready() {
  local label="$1"
  # Optional second arg: epoch seconds already started (e.g. include force-recreate time)
  local start="${2:-$(date +%s)}"
  local end
  log "waiting for ready ($label) ..."
  while true; do
    if curl -sf "$MANAGEMENT_URL/health/ready" >/dev/null 2>&1; then
      end="$(date +%s)"
      echo $((end - start))
      return 0
    fi
    if (( $(date +%s) - start > READY_TIMEOUT_SEC )); then
      die "timed out waiting for ready after ${READY_TIMEOUT_SEC}s ($label) via $MANAGEMENT_URL/health/ready"
    fi
    sleep 2
  done
}

snapshot_dir_stats() {
  SNAPSHOT_DIR="$SNAPSHOT_DIR" python3 - <<'PY'
import os, json
root = os.environ["SNAPSHOT_DIR"]
files = []
total = 0
if os.path.isdir(root):
    for name in os.listdir(root):
        path = os.path.join(root, name)
        if os.path.isfile(path):
            size = os.path.getsize(path)
            files.append({"name": name, "bytes": size})
            total += size
files.sort(key=lambda f: f["name"])
print(json.dumps({"file_count": len(files), "total_bytes": total, "files": files}))
PY
}

pad_content() {
  local target="$1"
  python3 - "$target" <<'PY'
import json, sys
n = int(sys.argv[1])
base = {
    "type": "object",
    "title": "ReproSchema",
    "description": "",
    "properties": {"id": {"type": "string"}},
}
overhead = len(json.dumps(base))
pad = max(0, n - overhead)
base["description"] = "x" * pad
print(json.dumps(base))
PY
}

create_artifacts() {
  local i content body
  content="$(pad_content "$CONTENT_BYTES")"
  log "creating ${ARTIFACTS} artifacts (~${CONTENT_BYTES} bytes content each) in group '${GROUP_ID}'"
  for ((i = 1; i <= ARTIFACTS; i++)); do
    body="$(python3 - "$i" "$content" <<'PY'
import json, sys
i = sys.argv[1]
schema = json.loads(sys.argv[2])
print(json.dumps({
    "artifactId": f"schema-{i}",
    "artifactType": "JSON",
    "name": f"schema-{i}",
    "firstVersion": {
        "version": "1.0.0",
        "content": {
            "content": json.dumps(schema),
            "contentType": "application/json"
        }
    }
}))
PY
)"
    curl -sf -X POST \
      "$REGISTRY_URL/apis/registry/v3/groups/${GROUP_ID}/artifacts" \
      -H 'Content-Type: application/json' \
      -d "$body" >/dev/null \
      || die "failed creating artifact schema-${i}"
    if (( i % 50 == 0 )) || (( i == ARTIFACTS )); then
      log "  created ${i}/${ARTIFACTS}"
    fi
  done
}

trigger_snapshots() {
  local i
  log "triggering ${SNAPSHOTS} snapshot(s) via POST /apis/registry/v3/admin/snapshots"
  for ((i = 1; i <= SNAPSHOTS; i++)); do
    curl -sf -X POST "$REGISTRY_URL/apis/registry/v3/admin/snapshots" \
      -H 'Content-Type: application/json' >/dev/null \
      || die "snapshot ${i} failed"
    log "  snapshot ${i}/${SNAPSHOTS} ok"
    sleep 1
  done
}

log "bringing up stack ..."
if [[ "$RECREATE" == "1" ]]; then
  $COMPOSE up -d --force-recreate
else
  $COMPOSE up -d
fi

INITIAL_READY_SEC="$(wait_ready initial)"
log "registry ready in ${INITIAL_READY_SEC}s"

create_artifacts
trigger_snapshots

STATS_JSON="$(snapshot_dir_stats)"
log "snapshot dir stats: $STATS_JSON"

if [[ "$CLEAR_SNAPSHOTS" == "1" ]]; then
  log "CLEAR_SNAPSHOTS=1 — wiping $SNAPSHOT_DIR before restart (simulates ephemeral /tmp)"
  find "$SNAPSHOT_DIR" -type f -delete
  WIPED=true
else
  WIPED=false
fi

# Cold restart: force-recreate both containers so page cache / container layers are not reused.
# Kafka named volume keeps journal + snapshots topics; local Registry dumps may still be wiped above.
log "force-recreating kafka + registry (kafka volume retains topics) ..."
RESTART_START="$(date +%s)"
$COMPOSE up -d --force-recreate kafka registry
RESTART_READY_SEC="$(wait_ready after-force-recreate "$RESTART_START")"
log "registry ready after force-recreate in ${RESTART_READY_SEC}s"

STATS_AFTER="$(snapshot_dir_stats)"

RESULT_FILE="$RESULTS_DIR/latest.json"
export STATS_JSON STATS_AFTER RESULT_FILE
export REGISTRY_URL ARTIFACTS CONTENT_BYTES SNAPSHOTS
export INITIAL_READY_SEC RESTART_READY_SEC WIPED
export REGISTRY_IMAGE="${REGISTRY_IMAGE:-apicurio/apicurio-registry:latest-snapshot}"
export KAFKA_STORE="${APICURIO_KAFKASQL_SNAPSHOT_KAFKA_STORE_ENABLED:-false}"

python3 - <<'PY'
import json, os, datetime
stats = json.loads(os.environ["STATS_JSON"])
stats_after = json.loads(os.environ["STATS_AFTER"])
out = {
    "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
    "registry_url": os.environ["REGISTRY_URL"],
    "registry_image": os.environ["REGISTRY_IMAGE"],
    "kafka_store_enabled": os.environ.get("KAFKA_STORE", "false").lower() in ("1", "true", "yes"),
    "artifacts": int(os.environ["ARTIFACTS"]),
    "content_bytes": int(os.environ["CONTENT_BYTES"]),
    "snapshots_triggered": int(os.environ["SNAPSHOTS"]),
    "clear_snapshots_before_restart": os.environ["WIPED"] == "true",
    "restart_mode": "force-recreate-kafka-and-registry",
    "initial_ready_sec": int(os.environ["INITIAL_READY_SEC"]),
    "restart_ready_sec": int(os.environ["RESTART_READY_SEC"]),
    "snapshot_dir_before_restart": stats,
    "snapshot_dir_after_restart": stats_after,
}
path = os.environ["RESULT_FILE"]
with open(path, "w") as f:
    json.dump(out, f, indent=2)
    f.write("\n")
print(json.dumps(out, indent=2))
PY

if [[ "$SAVE_BASELINE" == "1" ]]; then
  cp "$RESULT_FILE" "$RESULTS_DIR/baseline.json"
  log "saved baseline to $RESULTS_DIR/baseline.json"
fi

TOTAL_BYTES="$(python3 -c "import json; print(json.load(open('$RESULT_FILE'))['snapshot_dir_before_restart']['total_bytes'])")"
log "done. results: $RESULT_FILE"
log "summary: snapshot_bytes=${TOTAL_BYTES} restart_ready_sec=${RESTART_READY_SEC}s clear_snapshots=${WIPED}"
