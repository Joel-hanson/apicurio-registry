#!/usr/bin/env bash
# Large KafkaSQL snapshot startup bench using real schemas/ fixtures.
#
# Speeds up load via seed_parallel.py (concurrent POSTs).
# Measures Registry-only force-recreate (Kafka stays up) so bootstrap savings
# are not hidden by broker restart. Also records "bootstrapped in X ms" from logs.
#
# Usage:
#   REGISTRY_IMAGE=apicurio/apicurio-registry:snapshot-fix ./bench-large-startup.sh
#   ARTIFACTS=10000 PARALLEL=48 PAD_BYTES=2048 ./bench-large-startup.sh
#
# Env knobs:
#   ARTIFACTS     default 5000
#   PARALLEL      default 32
#   PAD_BYTES     default 1024  (extra bytes per artifact content)
#   SNAPSHOTS     default 1
#   BUILD         1 = mvn package + docker build local image first
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"
REPO_ROOT="$(cd "$ROOT/../.." && pwd)"

ARTIFACTS="${ARTIFACTS:-5000}"
PARALLEL="${PARALLEL:-32}"
PAD_BYTES="${PAD_BYTES:-1024}"
SNAPSHOTS="${SNAPSHOTS:-1}"
GROUP_ID="${GROUP_ID:-snapshot-bench}"
REGISTRY_URL="${REGISTRY_URL:-http://localhost:18080}"
MANAGEMENT_URL="${MANAGEMENT_URL:-http://localhost:19000}"
READY_TIMEOUT_SEC="${READY_TIMEOUT_SEC:-1200}"
BUILD="${BUILD:-0}"
REGISTRY_IMAGE="${REGISTRY_IMAGE:-apicurio/apicurio-registry:snapshot-fix}"

log() { printf '[bench] %s\n' "$*" >&2; }
die() { printf '[bench] ERROR: %s\n' "$*" >&2; exit 1; }

need() { command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"; }
need curl
need python3
need docker

if [[ -z "${COMPOSE:-}" ]]; then
  if docker compose version >/dev/null 2>&1; then
    COMPOSE="docker compose"
  elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE="docker-compose"
  else
    die "neither 'docker compose' nor 'docker-compose' is available"
  fi
fi

[[ -d "$ROOT/schemas" ]] || die "missing schemas/ — expected fixtures under $ROOT/schemas"
[[ -f "$ROOT/seed_parallel.py" ]] || die "missing seed_parallel.py"

if [[ "$BUILD" == "1" ]]; then
  log "BUILD=1 — packaging + local image"
  (cd "$REPO_ROOT" && ./mvnw -pl app -am package -DskipTests)
  mkdir -p "$ROOT/.docker-context"
  rm -rf "$ROOT/.docker-context/quarkus-app"
  cp -a "$REPO_ROOT/app/target/quarkus-app" "$ROOT/.docker-context/"
  docker build -f "$ROOT/Dockerfile.local" -t "$REGISTRY_IMAGE" "$ROOT/.docker-context"
fi
docker image inspect "$REGISTRY_IMAGE" >/dev/null 2>&1 \
  || die "image not found: $REGISTRY_IMAGE (set REGISTRY_IMAGE or BUILD=1)"

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="$ROOT/results/large-${STAMP}"
mkdir -p "$OUT_DIR" "$ROOT/data/snapshots"
log "output: $OUT_DIR"
log "image=$REGISTRY_IMAGE artifacts=$ARTIFACTS parallel=$PARALLEL pad_bytes=$PAD_BYTES"

wait_ready() {
  local label="$1"
  local start="${2:-$(date +%s)}"
  log "waiting ready ($label) ..."
  while true; do
    if curl -sf "$MANAGEMENT_URL/health/ready" >/dev/null 2>&1; then
      echo $(( $(date +%s) - start ))
      return 0
    fi
    if (( $(date +%s) - start > READY_TIMEOUT_SEC )); then
      die "timeout waiting ready ($label)"
    fi
    sleep 2
  done
}

wait_not_ready() {
  local start
  start="$(date +%s)"
  while curl -sf "$MANAGEMENT_URL/health/ready" >/dev/null 2>&1; do
    if (( $(date +%s) - start > 60 )); then
      return 0
    fi
    sleep 1
  done
}

snapshot_dir_stats() {
  SNAPSHOT_DIR="$ROOT/data/snapshots" python3 - <<'PY'
import os, json
root = os.environ["SNAPSHOT_DIR"]
files, total = [], 0
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

trigger_snapshots() {
  local i
  local label="${1:-snapshot}"
  local t0 t1 elapsed
  for ((i = 1; i <= SNAPSHOTS; i++)); do
    t0="$(python3 -c 'import time; print(int(time.time()*1000))')"
    curl -sf -X POST "$REGISTRY_URL/apis/registry/v3/admin/snapshots" \
      -H 'Content-Type: application/json' >/dev/null \
      || die "snapshot $i failed"
    t1="$(python3 -c 'import time; print(int(time.time()*1000))')"
    elapsed=$((t1 - t0))
    log "${label} $i/$SNAPSHOTS ok (${elapsed} ms wall)"
    echo "$elapsed"
    sleep 1
  done
}

# Average of SNAPSHOTS timed creates; prints one integer ms to stdout, logs details.
timed_snapshot_avg_ms() {
  local label="$1"
  local times total=0 n=0 t
  # shellcheck disable=SC2207
  times=( $(trigger_snapshots "$label") )
  for t in "${times[@]}"; do
    total=$((total + t))
    n=$((n + 1))
  done
  if (( n == 0 )); then
    echo 0
  else
    echo $((total / n))
  fi
}

extract_bootstrap_ms() {
  # Last bootstrap line after a recreate
  docker logs kafkasql-repro-registry 2>&1 \
    | grep 'KafkaSQL storage bootstrapped in' \
    | tail -1 \
    | sed -n 's/.*bootstrapped in \([0-9][0-9]*\) ms.*/\1/p'
}

extract_restore_hints() {
  local out="$1"
  docker logs kafkasql-repro-registry 2>&1 \
    | grep -E 'No usable snapshot|Materialized|Restoring snapshot|Seeked journal|bootstrapped in|Publishing Kafka-resident' \
    | tail -40 >"$out" || true
}

recreate_registry() {
  # Registry only — Kafka volume + broker stay warm so we measure bootstrap, not Kafka boot.
  local kafka_store="$1"
  export APICURIO_KAFKASQL_SNAPSHOT_KAFKA_STORE_ENABLED="$kafka_store"
  export REGISTRY_IMAGE
  wait_not_ready || true
  local t0
  t0="$(date +%s)"
  $COMPOSE up -d --force-recreate registry >/dev/null
  local ready
  ready="$(wait_ready "registry-only kafka_store=${kafka_store}" "$t0")"
  echo "$ready"
}

record_run() {
  local name="$1" clear="$2" kafka_store="$3" ready_sec="$4" bootstrap_ms="$5" stats_before="$6"
  python3 - "$OUT_DIR/${name}.json" "$name" "$clear" "$kafka_store" "$ready_sec" "$bootstrap_ms" "$stats_before" <<'PY'
import json, sys, datetime
path, name, clear, store, ready, boot, stats = sys.argv[1:8]
out = {
    "timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
    "name": name,
    "clear_snapshots_before_restart": clear == "1",
    "kafka_store_enabled": store.lower() in ("1", "true", "yes"),
    "restart_ready_sec": int(ready),
    "bootstrap_ms": int(boot) if str(boot).isdigit() else None,
    "snapshot_dir_before_restart": json.loads(stats),
    "restart_mode": "force-recreate-registry-only",
}
open(path, "w").write(json.dumps(out, indent=2) + "\n")
PY
}

# --- bring up empty stack with kafka-store off for baseline seed ---
log "resetting stack ..."
$COMPOSE down -v >/dev/null 2>&1 || true
rm -rf "$ROOT/data/snapshots"
mkdir -p "$ROOT/data/snapshots"

export REGISTRY_IMAGE
export APICURIO_KAFKASQL_SNAPSHOT_KAFKA_STORE_ENABLED=false
$COMPOSE up -d --force-recreate
INITIAL_READY="$(wait_ready initial)"
log "initial ready in ${INITIAL_READY}s"

log "seeding ${ARTIFACTS} artifacts (parallel=${PARALLEL}) ..."
SEED_START="$(date +%s)"
REGISTRY_URL="$REGISTRY_URL" GROUP_ID="$GROUP_ID" \
  ARTIFACTS="$ARTIFACTS" PARALLEL="$PARALLEL" PAD_BYTES="$PAD_BYTES" \
  SCHEMAS_DIR="$ROOT/schemas" \
  python3 "$ROOT/seed_parallel.py"
SEED_SEC=$(( $(date +%s) - SEED_START ))
log "seed completed in ${SEED_SEC}s"

log "taking filesystem-only snapshot(s) ..."
SNAP_FS_MS="$(timed_snapshot_avg_ms filesystem-only)"
STATS_FS="$(snapshot_dir_stats)"
log "snapshot stats (filesystem-only): $STATS_FS (avg create ${SNAP_FS_MS} ms)"
echo "$STATS_FS" >"$OUT_DIR/snapshot-stats-filesystem.json"
echo "{\"avg_create_wall_ms\": ${SNAP_FS_MS}}" >"$OUT_DIR/perf-snapshot-create-filesystem.json"
"$ROOT/measure-storage.sh" >"$OUT_DIR/storage-after-filesystem-snapshot.json"
log "storage after filesystem snapshot written"

# --- A: keep dumps (filesystem happy path) ---
log "=== A: current-keep-dumps (registry-only recreate) ==="
READY_A="$(recreate_registry false)"
BOOT_A="$(extract_bootstrap_ms)"
extract_restore_hints "$OUT_DIR/current-keep-dumps.logs.txt"
record_run "current-keep-dumps" 0 false "$READY_A" "${BOOT_A:-}" "$STATS_FS"
log "A ready=${READY_A}s bootstrap_ms=${BOOT_A:-unknown}"

# --- B: wipe dumps, no kafka chunks (today ephemeral failure) ---
log "=== B: current-clear-dumps ==="
find "$ROOT/data/snapshots" -type f -delete
STATS_CLEARED="$(snapshot_dir_stats)"
READY_B="$(recreate_registry false)"
BOOT_B="$(extract_bootstrap_ms)"
extract_restore_hints "$OUT_DIR/current-clear-dumps.logs.txt"
record_run "current-clear-dumps" 1 false "$READY_B" "${BOOT_B:-}" "$STATS_FS"
log "B ready=${READY_B}s bootstrap_ms=${BOOT_B:-unknown}"

# --- publish kafka-resident snapshot on top of existing journal ---
log "enabling kafka-store and publishing a Kafka-resident snapshot ..."
# Registry is up without dumps; full replay just happened. Turn store on and snapshot.
export APICURIO_KAFKASQL_SNAPSHOT_KAFKA_STORE_ENABLED=true
$COMPOSE up -d --force-recreate registry >/dev/null
wait_ready "kafka-store-enabled" >/dev/null
SNAP_KS_MS="$(timed_snapshot_avg_ms kafka-store)"
STATS_KAFKA="$(snapshot_dir_stats)"
log "snapshot stats (with kafka-store): $STATS_KAFKA (avg create ${SNAP_KS_MS} ms)"
echo "$STATS_KAFKA" >"$OUT_DIR/snapshot-stats-kafka-store.json"
echo "{\"avg_create_wall_ms\": ${SNAP_KS_MS}}" >"$OUT_DIR/perf-snapshot-create-kafka-store.json"
"$ROOT/measure-storage.sh" >"$OUT_DIR/storage-after-kafka-store-snapshot.json"
log "storage after kafka-store snapshot written"
# Capture publish lines
docker logs kafkasql-repro-registry 2>&1 \
  | grep -E 'Publishing Kafka-resident|bootstrapped in' \
  | tail -20 >"$OUT_DIR/kafka-store-publish.logs.txt" || true

# --- C: wipe dumps, restore from Kafka chunks ---
log "=== C: proposed-kafka-store-clear ==="
find "$ROOT/data/snapshots" -type f -delete
READY_C="$(recreate_registry true)"
BOOT_C="$(extract_bootstrap_ms)"
extract_restore_hints "$OUT_DIR/proposed-kafka-store-clear.logs.txt"
record_run "proposed-kafka-store-clear" 1 true "$READY_C" "${BOOT_C:-}" "$STATS_KAFKA"
log "C ready=${READY_C}s bootstrap_ms=${BOOT_C:-unknown}"

# --- summary ---
OUT_DIR="$OUT_DIR" ARTIFACTS="$ARTIFACTS" PARALLEL="$PARALLEL" PAD_BYTES="$PAD_BYTES" \
  SEED_SEC="$SEED_SEC" REGISTRY_IMAGE="$REGISTRY_IMAGE" INITIAL_READY="$INITIAL_READY" \
  SNAP_FS_MS="$SNAP_FS_MS" SNAP_KS_MS="$SNAP_KS_MS" \
  python3 - <<'PY'
import json, os
from pathlib import Path
out = Path(os.environ["OUT_DIR"])
runs = []
for name in ("current-keep-dumps", "current-clear-dumps", "proposed-kafka-store-clear"):
    p = out / f"{name}.json"
    runs.append(json.loads(p.read_text()))

def load_storage(name):
    p = out / name
    if not p.exists():
        return None
    return json.loads(p.read_text())

storage_fs = load_storage("storage-after-filesystem-snapshot.json")
storage_ks = load_storage("storage-after-kafka-store-snapshot.json")

def topic_bytes(storage, topic):
    if not storage:
        return None
    for t in storage.get("kafka_topics") or []:
        if t.get("topic") == topic:
            return t.get("disk_bytes")
    return None

def topic_offsets(storage, topic):
    if not storage:
        return None
    for t in storage.get("kafka_topics") or []:
        if t.get("topic") == topic:
            return t.get("end_offset_sum")
    return None

storage = {
    "after_filesystem_snapshot": {
        "local_snapshot_bytes": (storage_fs or {}).get("local_snapshot_dir", {}).get("total_bytes"),
        "local_snapshot_files": (storage_fs or {}).get("local_snapshot_dir", {}).get("file_count"),
        "kafkasql_snapshots_disk_bytes": topic_bytes(storage_fs, "kafkasql-snapshots"),
        "kafkasql_snapshots_end_offset_sum": topic_offsets(storage_fs, "kafkasql-snapshots"),
        "kafkasql_journal_disk_bytes": topic_bytes(storage_fs, "kafkasql-journal"),
        "kafkasql_journal_end_offset_sum": topic_offsets(storage_fs, "kafkasql-journal"),
    },
    "after_kafka_store_snapshot": {
        "local_snapshot_bytes": (storage_ks or {}).get("local_snapshot_dir", {}).get("total_bytes"),
        "local_snapshot_files": (storage_ks or {}).get("local_snapshot_dir", {}).get("file_count"),
        "kafkasql_snapshots_disk_bytes": topic_bytes(storage_ks, "kafkasql-snapshots"),
        "kafkasql_snapshots_end_offset_sum": topic_offsets(storage_ks, "kafkasql-snapshots"),
        "kafkasql_journal_disk_bytes": topic_bytes(storage_ks, "kafkasql-journal"),
        "kafkasql_journal_end_offset_sum": topic_offsets(storage_ks, "kafkasql-journal"),
    },
}
fs_snap = storage["after_filesystem_snapshot"]["kafkasql_snapshots_disk_bytes"]
ks_snap = storage["after_kafka_store_snapshot"]["kafkasql_snapshots_disk_bytes"]
if isinstance(fs_snap, int) and isinstance(ks_snap, int):
    storage["kafka_store_snapshots_topic_delta_bytes"] = ks_snap - fs_snap

perf = {
    "snapshot_create_avg_wall_ms": {
        "filesystem_only": int(os.environ.get("SNAP_FS_MS") or 0),
        "kafka_store": int(os.environ.get("SNAP_KS_MS") or 0),
    },
    "bootstrap_ms": {r["name"]: r.get("bootstrap_ms") for r in runs},
    "restart_ready_sec": {r["name"]: r.get("restart_ready_sec") for r in runs},
    "notes": (
        "Snapshot create with kafka-store includes H2 SCRIPT + Base64 chunk publish. "
        "Steady-state artifact APIs are unaffected except while a snapshot runs on the "
        "journal consumer thread. bootstrap_ms is the main restore/replay signal."
    ),
}

summary = {
    "registry_image": os.environ["REGISTRY_IMAGE"],
    "artifacts": int(os.environ["ARTIFACTS"]),
    "parallel": int(os.environ["PARALLEL"]),
    "pad_bytes": int(os.environ["PAD_BYTES"]),
    "seed_sec": int(os.environ["SEED_SEC"]),
    "initial_ready_sec": int(os.environ["INITIAL_READY"]),
    "restart_mode": "force-recreate-registry-only",
    "runs": [
        {
            "name": r["name"],
            "kafka_store_enabled": r["kafka_store_enabled"],
            "clear_snapshots_before_restart": r["clear_snapshots_before_restart"],
            "restart_ready_sec": r["restart_ready_sec"],
            "bootstrap_ms": r.get("bootstrap_ms"),
        }
        for r in runs
    ],
    "storage": storage,
    "performance": perf,
}
(out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")

print()
print("=== large startup comparison (Registry-only recreate) ===")
print(f"artifacts={summary['artifacts']} parallel={summary['parallel']} pad_bytes={summary['pad_bytes']} seed_sec={summary['seed_sec']}")
print()
hdr = f"{'run':<32} {'store':<6} {'clear':<5} {'ready_s':>8} {'bootstrap_ms':>12}"
print(hdr)
print("-" * len(hdr))
for r in summary["runs"]:
    print(
        f"{r['name']:<32} "
        f"{'on' if r['kafka_store_enabled'] else 'off':<6} "
        f"{'yes' if r['clear_snapshots_before_restart'] else 'no':<5} "
        f"{r['restart_ready_sec']:>8} "
        f"{str(r.get('bootstrap_ms') or '?'):>12}"
    )
print()
print("=== performance ===")
fs_ms = perf["snapshot_create_avg_wall_ms"]["filesystem_only"]
ks_ms = perf["snapshot_create_avg_wall_ms"]["kafka_store"]
print(f"snapshot create avg (filesystem-only): {fs_ms} ms")
print(f"snapshot create avg (kafka-store):     {ks_ms} ms")
print(f"create overhead (store - fs):          {ks_ms - fs_ms} ms")
print()
print("=== storage ===")
afs = storage["after_filesystem_snapshot"]
aks = storage["after_kafka_store_snapshot"]
print(f"local dump after FS snapshot:     {afs['local_snapshot_bytes']} bytes ({afs['local_snapshot_files']} files)")
print(f"local dump after kafka-store snap:{aks['local_snapshot_bytes']} bytes ({aks['local_snapshot_files']} files)")
print(f"kafkasql-snapshots disk (FS only):{afs['kafkasql_snapshots_disk_bytes']} bytes, end_offset={afs['kafkasql_snapshots_end_offset_sum']}")
print(f"kafkasql-snapshots disk (store):  {aks['kafkasql_snapshots_disk_bytes']} bytes, end_offset={aks['kafkasql_snapshots_end_offset_sum']}")
if storage.get("kafka_store_snapshots_topic_delta_bytes") is not None:
    print(f"snapshots topic delta (store):    {storage['kafka_store_snapshots_topic_delta_bytes']} bytes")
print(f"kafkasql-journal disk:            {aks['kafkasql_journal_disk_bytes']} bytes, end_offset={aks['kafkasql_journal_end_offset_sum']}")
print()
print("Expect bootstrap_ms: clear(off) >> keep ≈ kafka-store-clear")
print("Expect create ms: kafka-store >= filesystem-only (chunk publish overhead)")
print("Expect storage: kafka-store grows kafkasql-snapshots; local folder can be wiped afterward")
print(f"Details: {out}")
PY

log "done. leaving stack up for log inspection; tear down with: docker compose down -v"
