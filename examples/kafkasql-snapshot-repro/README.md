# KafkaSQL snapshot repro (real Kafka + Registry)

Reproduce KafkaSQL snapshot disk growth under a `/tmp`-like path and measure Registry restart time. Use the same run after a fix to compare numbers.

## Stack

- **Kafka**: official `apache/kafka:3.9.0` in KRaft mode (no ZooKeeper), data on named volume `kafka-data` (`KAFKA_LOG_DIRS=/var/lib/kafka/data`)
- **Registry**: KafkaSQL storage, snapshots written to `/tmp/apicurio-snapshots` (bind-mounted to `./data/snapshots`)
  - App API: host port **18080** → container `8080` (`REGISTRY_HOST_PORT`)
  - Health/metrics: host port **19000** → Quarkus management `9000` (`REGISTRY_MANAGEMENT_HOST_PORT`)
- Kafka host port is **19092** (internal compose traffic uses `kafka:9093`)
- Measured “restart” is **`compose up --force-recreate` of both kafka and registry** (cold containers; Kafka topics survive via `kafka-data`). Use `docker compose down -v` only when you want a fully empty Kafka.

## Quick start

```bash
cd examples/kafkasql-snapshot-repro
chmod +x run-repro.sh
./run-repro.sh
```

That will:

1. `docker compose up -d`
2. Wait until Registry is ready
3. Create artifacts (default 200 × ~8KB)
4. Trigger 3 snapshots
5. Report snapshot file count / total bytes under `./data/snapshots`
6. Force-recreate Kafka + Registry (Kafka volume keeps topics) and time until ready again
7. Write `results/latest.json`

Save a baseline for later comparison:

```bash
SAVE_BASELINE=1 ./run-repro.sh
```

### Large startup-time bench (recommended for kafka-store benefit)

Uses real schemas under `schemas/` (copied from `integration-tests/.../artifactTypes` plus a few larger fixtures). Seeds **in parallel** (`seed_parallel.py`) and measures **Registry-only** force-recreate so Kafka boot does not hide bootstrap savings.

```bash
cd examples/kafkasql-snapshot-repro
chmod +x bench-large-startup.sh seed_parallel.py

# defaults: ARTIFACTS=5000 PARALLEL=32 PAD_BYTES=1024
REGISTRY_IMAGE=apicurio/apicurio-registry:snapshot-fix ./bench-large-startup.sh

# heavier journal / dump
ARTIFACTS=10000 PARALLEL=48 PAD_BYTES=4096 \
  REGISTRY_IMAGE=apicurio/apicurio-registry:snapshot-fix ./bench-large-startup.sh
```

Compares:

1. **current-keep-dumps** — local dump present  
2. **current-clear-dumps** — dumps wiped, no Kafka chunks → full journal replay  
3. **proposed-kafka-store-clear** — dumps wiped, restore from Kafka chunks  

Look at `bootstrap_ms` and `performance` in `results/large-*/summary.json`. The same summary includes **`storage`** and **`performance.snapshot_create_avg_wall_ms`** (filesystem-only vs kafka-store create overhead).

Performance dimensions covered:

| Signal | What it tells you |
|--------|-------------------|
| `bootstrap_ms` | Restore/replay cost on Registry restart |
| `snapshot_create_avg_wall_ms` | Extra cost to publish Base64 chunks when kafka-store is on |
| Steady-state APIs | Not hit except while a snapshot runs (H2 `SCRIPT` blocks the journal consumer briefly) |

Measure storage anytime the stack is up:

```bash
./measure-storage.sh | tee results/storage-now.json
```

Compare especially:

- `local_snapshot_dir.total_bytes` — PVC / `/tmp` pressure  
- `kafkasql-snapshots.disk_bytes` — grows with kafka-store (Base64 chunks ≈ 4/3 of dump); Registry tombstones older kafka-store snapshots beyond `apicurio.kafkasql.snapshot.kafka-store.retain-count` (default **2**) so compaction can reclaim space (`cleanup.policy=compact,delete` when kafka-store is enabled)  
- Test scripts call `lib-kafka-store-prep.sh` **before** starting Registry with kafka-store, so an existing `cleanup.policy=delete` topic is altered first (otherwise startup verification fails)  
- `kafkasql-journal.disk_bytes` — grows with writes regardless of snapshot mode  

Seed alone (Registry already up):

```bash
ARTIFACTS=5000 PARALLEL=32 PAD_BYTES=1024 python3 ./seed_parallel.py
```

### Full A/B comparison script

Runs four combinations with defaults `ARTIFACTS=1000` `CONTENT_BYTES=16384` and writes a timestamped folder under `results/compare-*/` plus a summary table:

1. **current-clear-dumps** — kafka-store off, wipe local dumps  
2. **current-keep-dumps** — kafka-store off, keep local dumps  
3. **proposed-kafka-store-clear** — kafka-store on, wipe local dumps  
4. **proposed-kafka-store-keep** — kafka-store on, keep local dumps  

```bash
cd examples/kafkasql-snapshot-repro
chmod +x compare-approaches.sh run-repro.sh

# image already built as apicurio/apicurio-registry:snapshot-fix
./compare-approaches.sh

# or package + build the local image first
BUILD=1 ./compare-approaches.sh

# overrides
ARTIFACTS=2000 CONTENT_BYTES=8192 REGISTRY_IMAGE=apicurio/apicurio-registry:snapshot-fix \
  ./compare-approaches.sh
```

### Simulate ephemeral `/tmp` (snapshot files wiped on restart)

```bash
CLEAR_SNAPSHOTS=1 ./run-repro.sh
```

With dumps gone **and** Kafka-resident store off, Registry cannot restore and must replay the journal — startup stays slow.

### Kafka-resident snapshot store (no PVC)

Publishes dump bytes as chunked records on `kafkasql-snapshots`. After wiping local files, restore still works:

```bash
# A: ephemeral /tmp, filesystem-only snapshots → expect slow restart
docker-compose down -v && rm -rf data/snapshots/*
REGISTRY_IMAGE=apicurio/apicurio-registry:snapshot-fix \
  APICURIO_KAFKASQL_SNAPSHOT_KAFKA_STORE_ENABLED=false \
  CLEAR_SNAPSHOTS=1 RECREATE=1 ARTIFACTS=1000 CONTENT_BYTES=16384 \
  ./run-repro.sh
cp results/latest.json results/clear-fs-only.json

# B: ephemeral /tmp, Kafka-resident dumps → expect fast restart
docker-compose down -v && rm -rf data/snapshots/*
REGISTRY_IMAGE=apicurio/apicurio-registry:snapshot-fix \
  APICURIO_KAFKASQL_SNAPSHOT_KAFKA_STORE_ENABLED=true \
  CLEAR_SNAPSHOTS=1 RECREATE=1 ARTIFACTS=1000 CONTENT_BYTES=16384 \
  ./run-repro.sh
cp results/latest.json results/clear-kafka-store.json
```

Compare `restart_ready_sec` and look for `Materialized Kafka-resident snapshot` / `storage=kafka` in Registry logs.

| Env var | Default | Meaning |
|---------|---------|---------|
| `ARTIFACTS` | `200` | Number of artifacts to create |
| `CONTENT_BYTES` | `8192` | Approx schema body size per artifact |
| `SNAPSHOTS` | `3` | How many times to call admin snapshots |
| `CLEAR_SNAPSHOTS` | `0` | `1` = wipe snapshot dir before restart |
| `SAVE_BASELINE` | `0` | `1` = also write `results/baseline.json` |
| `RECREATE` | `0` | `1` = `compose up --force-recreate` (pick up env changes) |
| `APICURIO_KAFKASQL_SNAPSHOT_KAFKA_STORE_ENABLED` | `false` | Publish dump chunks to Kafka |
| `REGISTRY_IMAGE` | `apicurio/apicurio-registry:latest-snapshot` | Image used by compose |
| `REGISTRY_URL` | `http://localhost:18080` | Registry API base URL |
| `MANAGEMENT_URL` | `http://localhost:19000` | Quarkus management URL (health) |
| `REGISTRY_HOST_PORT` | `18080` | Host port → Registry app `8080` |
| `REGISTRY_MANAGEMENT_HOST_PORT` | `19000` | Host port → management `9000` |
| `READY_TIMEOUT_SEC` | `600` | Max wait for `/health/ready` on management port |

## After you change Registry

Build a local image from your Maven output (repo `.dockerignore` excludes `target/`, so stage the quarkus app first):

```bash
# from repo root
./mvnw -pl app -am package -DskipTests
mkdir -p examples/kafkasql-snapshot-repro/.docker-context
rm -rf examples/kafkasql-snapshot-repro/.docker-context/quarkus-app
cp -a app/target/quarkus-app examples/kafkasql-snapshot-repro/.docker-context/
docker build -f examples/kafkasql-snapshot-repro/Dockerfile.local \
  -t apicurio/apicurio-registry:snapshot-fix \
  examples/kafkasql-snapshot-repro/.docker-context

cd examples/kafkasql-snapshot-repro
docker-compose down -v
rm -rf data/snapshots/*
REGISTRY_IMAGE=apicurio/apicurio-registry:snapshot-fix ./run-repro.sh
diff -u results/benchmark.json results/latest.json || true
```

Compare especially:

- `snapshot_dir_before_restart.total_bytes` — should drop with gzip `SCRIPT TO`
- `snapshot_dir_before_restart.file_count` — should stay at 1 if old dumps are cleaned up
- `restart_ready_sec` with snapshots kept — improves mainly with seek-to-offset on large journals (not much change on small demos)

## Tear down

```bash
# -v removes kafka-data (journal + snapshot topic records)
docker compose down -v
rm -rf data/snapshots/*
```

## Notes

- Auth is off so `POST /apis/registry/v3/admin/snapshots` works without credentials.
- Snapshot path is set explicitly to `/tmp/apicurio-snapshots` so the bind mount matches what Registry writes.
- Ready checks use **`http://localhost:19000/health/ready`** (Quarkus management interface). Hitting `/health/ready` on the app port (`8080`/`18080`) returns 404.
- `restart_ready_sec` includes Kafka + Registry container recreate time, not a soft `restart` of Registry alone.
- For gzip-only format experiments without Kafka, see `../kafkasql-snapshot-benchmark`.
