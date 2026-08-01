#!/usr/bin/env bash
# Run filesystem-vs-kafka-store snapshot restart comparisons end-to-end.
#
# Defaults: ARTIFACTS=1000 CONTENT_BYTES=16384 (override via env).
#
# Usage (from this directory):
#   ./compare-approaches.sh
#   BUILD=1 ./compare-approaches.sh          # also package + build local image
#   REGISTRY_IMAGE=apicurio/apicurio-registry:snapshot-fix ./compare-approaches.sh
#
# Writes per-run JSON + log excerpts under results/compare-<timestamp>/
# and a summary.json + summary table at the end.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$ROOT"
REPO_ROOT="$(cd "$ROOT/../.." && pwd)"

ARTIFACTS="${ARTIFACTS:-1000}"
CONTENT_BYTES="${CONTENT_BYTES:-16384}"
SNAPSHOTS="${SNAPSHOTS:-3}"
RECREATE="${RECREATE:-1}"
READY_TIMEOUT_SEC="${READY_TIMEOUT_SEC:-900}"
BUILD="${BUILD:-0}"
REGISTRY_IMAGE="${REGISTRY_IMAGE:-apicurio/apicurio-registry:snapshot-fix}"

log() { printf '[compare] %s\n' "$*" >&2; }
die() { printf '[compare] ERROR: %s\n' "$*" >&2; exit 1; }

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

build_local_image() {
  log "BUILD=1 — packaging app and building ${REGISTRY_IMAGE}"
  (cd "$REPO_ROOT" && ./mvnw -pl app -am package -DskipTests)
  mkdir -p "$ROOT/.docker-context"
  rm -rf "$ROOT/.docker-context/quarkus-app"
  [[ -d "$REPO_ROOT/app/target/quarkus-app" ]] \
    || die "missing $REPO_ROOT/app/target/quarkus-app after Maven package"
  cp -a "$REPO_ROOT/app/target/quarkus-app" "$ROOT/.docker-context/"
  docker build -f "$ROOT/Dockerfile.local" -t "$REGISTRY_IMAGE" "$ROOT/.docker-context"
}

if [[ "$BUILD" == "1" ]]; then
  build_local_image
fi

if ! docker image inspect "$REGISTRY_IMAGE" >/dev/null 2>&1; then
  die "image not found: ${REGISTRY_IMAGE} (set REGISTRY_IMAGE or run with BUILD=1)"
fi

STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="$ROOT/results/compare-${STAMP}"
mkdir -p "$OUT_DIR"
log "output directory: $OUT_DIR"
log "image=${REGISTRY_IMAGE} artifacts=${ARTIFACTS} content_bytes=${CONTENT_BYTES} snapshots=${SNAPSHOTS}"

# name|CLEAR_SNAPSHOTS|KAFKA_STORE|description
COMBOS=(
  "current-clear-dumps|1|false|Filesystem only; wipe local dumps before cold recreate (ephemeral /tmp)"
  "current-keep-dumps|0|false|Filesystem only; keep local dumps (durable PVC / disk happy path)"
  "proposed-kafka-store-clear|1|true|Kafka-resident dumps; wipe local dumps (proposed no-PVC path)"
  "proposed-kafka-store-keep|0|true|Kafka-resident dumps; keep local dumps (control)"
)

run_one() {
  local name="$1" clear="$2" kafka_store="$3" desc="$4"
  local result_json="$OUT_DIR/${name}.json"
  local logs_txt="$OUT_DIR/${name}.logs.txt"

  log "================================================================"
  log "RUN ${name}"
  log "  ${desc}"
  log "  CLEAR_SNAPSHOTS=${clear} KAFKA_STORE=${kafka_store}"
  log "================================================================"

  $COMPOSE down -v >/dev/null 2>&1 || true
  rm -rf "$ROOT/data/snapshots"
  mkdir -p "$ROOT/data/snapshots"

  REGISTRY_IMAGE="$REGISTRY_IMAGE" \
    ARTIFACTS="$ARTIFACTS" \
    CONTENT_BYTES="$CONTENT_BYTES" \
    SNAPSHOTS="$SNAPSHOTS" \
    RECREATE="$RECREATE" \
    READY_TIMEOUT_SEC="$READY_TIMEOUT_SEC" \
    CLEAR_SNAPSHOTS="$clear" \
    APICURIO_KAFKASQL_SNAPSHOT_KAFKA_STORE_ENABLED="$kafka_store" \
    ./run-repro.sh

  cp "$ROOT/results/latest.json" "$result_json"

  docker logs kafkasql-repro-registry 2>&1 \
    | grep -E 'No usable snapshot|Materialized|Restoring snapshot|Seeked journal|bootstrapped in|Publishing Kafka-resident' \
    | tail -40 >"$logs_txt" || true

  log "saved ${result_json}"
  log "saved ${logs_txt}"
}

for combo in "${COMBOS[@]}"; do
  IFS='|' read -r name clear kafka_store desc <<<"$combo"
  run_one "$name" "$clear" "$kafka_store" "$desc"
done

log "writing summary ..."
OUT_DIR="$OUT_DIR" ARTIFACTS="$ARTIFACTS" CONTENT_BYTES="$CONTENT_BYTES" \
  REGISTRY_IMAGE="$REGISTRY_IMAGE" SNAPSHOTS="$SNAPSHOTS" python3 - <<'PY'
import json, os
from pathlib import Path

out = Path(os.environ["OUT_DIR"])
rows = []
for path in sorted(out.glob("*.json")):
    if path.name == "summary.json":
        continue
    d = json.loads(path.read_text())
    rows.append({
        "name": path.stem,
        "kafka_store_enabled": d.get("kafka_store_enabled"),
        "clear_snapshots_before_restart": d.get("clear_snapshots_before_restart"),
        "initial_ready_sec": d.get("initial_ready_sec"),
        "restart_ready_sec": d.get("restart_ready_sec"),
        "snapshot_files_before": d.get("snapshot_dir_before_restart", {}).get("file_count"),
        "snapshot_bytes_before": d.get("snapshot_dir_before_restart", {}).get("total_bytes"),
        "snapshot_files_after": d.get("snapshot_dir_after_restart", {}).get("file_count"),
    })

summary = {
    "registry_image": os.environ["REGISTRY_IMAGE"],
    "artifacts": int(os.environ["ARTIFACTS"]),
    "content_bytes": int(os.environ["CONTENT_BYTES"]),
    "snapshots_triggered": int(os.environ["SNAPSHOTS"]),
    "runs": rows,
}
(out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")

print()
print("=== snapshot approach comparison ===")
print(f"image={summary['registry_image']} artifacts={summary['artifacts']} content_bytes={summary['content_bytes']}")
print()
hdr = f"{'run':<32} {'store':<6} {'clear':<5} {'restart_s':>9} {'bytes_before':>12} {'files_b':>7} {'files_a':>7}"
print(hdr)
print("-" * len(hdr))
for r in rows:
    print(
        f"{r['name']:<32} "
        f"{'on' if r['kafka_store_enabled'] else 'off':<6} "
        f"{'yes' if r['clear_snapshots_before_restart'] else 'no':<5} "
        f"{r['restart_ready_sec']:>9} "
        f"{r['snapshot_bytes_before']:>12} "
        f"{r['snapshot_files_before']:>7} "
        f"{r['snapshot_files_after']:>7}"
    )
print()
print("Interpretation:")
print("  current-clear-dumps          → today’s ephemeral /tmp without kafka-store (expect slow)")
print("  current-keep-dumps           → durable local dumps (expect fast; filesystem baseline)")
print("  proposed-kafka-store-clear   → proposed path: no local dumps, restore from Kafka (expect ≈ keep)")
print("  proposed-kafka-store-keep    → control with both local + kafka chunks")
print()
print(f"Details: {out}")
print(f"Summary: {out / 'summary.json'}")
PY

log "tearing down stack (keeping results) ..."
$COMPOSE down -v >/dev/null 2>&1 || true

log "done."
