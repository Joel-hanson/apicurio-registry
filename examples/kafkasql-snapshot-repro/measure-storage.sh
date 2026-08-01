#!/usr/bin/env bash
# Measure local snapshot-folder size and Kafka topic on-disk size / end offsets.
#
# Usage (stack must be up):
#   ./measure-storage.sh
#   ./measure-storage.sh | tee results/storage-now.json
#
# Topics: kafkasql-journal, kafkasql-snapshots, kafkasql-events
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SNAPSHOT_DIR="${SNAPSHOT_DIR:-$ROOT/data/snapshots}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-kafkasql-repro-kafka}"
BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
LOG_DIRS="${LOG_DIRS:-/var/lib/kafka/data}"

die() { printf '[storage] ERROR: %s\n' "$*" >&2; exit 1; }

need() { command -v "$1" >/dev/null 2>&1 || die "missing $1"; }
need python3
need docker

docker inspect "$KAFKA_CONTAINER" >/dev/null 2>&1 \
  || die "kafka container not running: $KAFKA_CONTAINER"

# On-disk bytes per topic directory under KAFKA_LOG_DIRS
TOPIC_DU_RAW="$(docker exec "$KAFKA_CONTAINER" sh -c "
  for d in ${LOG_DIRS}/*/; do
    [ -d \"\$d\" ] || continue
    base=\$(basename \"\$d\")
    topic=\${base%-*}
    part=\${base##*-}
    case \$part in
      ''|*[!0-9]*) continue ;;
    esac
    bytes=\$(du -sb \"\$d\" 2>/dev/null | awk '{print \$1}')
    echo \"\$topic \$part \$bytes\"
  done
")"

# End offsets (next offset = approx message count when starting from 0)
OFFSETS_RAW="$(
  for t in kafkasql-journal kafkasql-snapshots kafkasql-events; do
    docker exec "$KAFKA_CONTAINER" \
      /opt/kafka/bin/kafka-get-offsets.sh \
      --bootstrap-server "$BOOTSTRAP" \
      --topic "$t" \
      --time -1 2>/dev/null || true
  done
)"

export SNAPSHOT_DIR TOPIC_DU_RAW OFFSETS_RAW
python3 - <<'PY'
import json, os, re
from collections import defaultdict

root = os.environ["SNAPSHOT_DIR"]
files, total = [], 0
if os.path.isdir(root):
    for name in sorted(os.listdir(root)):
        path = os.path.join(root, name)
        if os.path.isfile(path):
            size = os.path.getsize(path)
            files.append({"name": name, "bytes": size})
            total += size

disk = defaultdict(lambda: {"bytes": 0, "partitions": {}})
for line in os.environ.get("TOPIC_DU_RAW", "").splitlines():
    parts = line.split()
    if len(parts) != 3:
        continue
    topic, part, nbytes = parts
    disk[topic]["bytes"] += int(nbytes)
    disk[topic]["partitions"][part] = int(nbytes)

offsets = defaultdict(int)
# lines like: kafkasql-snapshots:0:12
for line in os.environ.get("OFFSETS_RAW", "").splitlines():
    m = re.match(r"^([^:]+):(\d+):(\d+)\s*$", line.strip())
    if not m:
        continue
    offsets[m.group(1)] += int(m.group(3))

interesting = ["kafkasql-journal", "kafkasql-snapshots", "kafkasql-events"]
topic_rows = []
for name in interesting:
    topic_rows.append({
        "topic": name,
        "disk_bytes": disk.get(name, {}).get("bytes"),
        "end_offset_sum": offsets.get(name),
        "partitions": disk.get(name, {}).get("partitions") or None,
    })

dump = total
out = {
    "local_snapshot_dir": {
        "path": root,
        "file_count": len(files),
        "total_bytes": total,
        "files": files,
    },
    "kafka_topics": topic_rows,
    "notes": {
        "local_dump_bytes": dump,
        "approx_base64_chunk_payload_bytes": int(dump * 4 / 3) if dump else None,
        "interpretation": (
            "Filesystem-only: pay local_dump_bytes on each pod/PVC. "
            "Kafka-store: pay kafkasql-snapshots disk (~4/3 dump + metadata records); "
            "local folder can be empty after wipe. "
            "Journal topic size is independent (grows with every write)."
        ),
    },
}
print(json.dumps(out, indent=2))
PY
