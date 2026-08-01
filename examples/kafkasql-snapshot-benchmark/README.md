# KafkaSQL snapshot format benchmark

Local micro-benchmark comparing H2 snapshot strategies relevant to Apicurio Registry KafkaSQL:

| Strategy | What it models |
|----------|----------------|
| `current` | Today's `SCRIPT TO` / `RUNSCRIPT FROM` (uncompressed SQL) |
| `gzip-script` | Option 1: same SQL dump with H2 `COMPRESSION GZIP` |
| `h2-backup` | Option 2: file-based H2 + `BACKUP TO` / Restore |

This **does not** start Kafka or Registry on purpose. Full KafkaSQL startup time is dominated by journal replay when no snapshot is used; this demo isolates **dump + restore** cost and **on-disk size** so you can see how much format changes buy you.

## Why Docker Compose (and not full Registry)

A full `Registry + Kafka` compose would mostly measure journal consume time, not snapshot format. This compose is enough to:

- reproduce results quickly
- tune `ROWS` / `CONTENT_BYTES`
- avoid building the whole Apicurio reactor

If you later want an end-to-end demo, add Registry KafkaSQL on top — but use this first for format decisions.

## Quick start (Docker)

```bash
cd examples/kafkasql-snapshot-benchmark
docker compose up --build
```

Larger dataset (more like a big schema collection):

```bash
ROWS=10000 CONTENT_BYTES=16384 docker compose up --build
```

## Quick start (local Maven)

Requires JDK 17+:

```bash
cd examples/kafkasql-snapshot-benchmark
mvn -q -DskipTests package
java -jar target/kafkasql-snapshot-benchmark-1.0.0-SNAPSHOT.jar

# or
ROWS=5000 CONTENT_BYTES=8192 mvn -q exec:java
```

## Configuration

| Env var | Default | Meaning |
|---------|---------|---------|
| `ROWS` | `2000` | Number of content/artifact rows |
| `CONTENT_BYTES` | `8192` | Bytes of schema-like JSON per content row |
| `WARMUP` | `1` | Untimed warmup iterations |
| `ITERATIONS` | `3` | Timed iterations (reported averages) |
| `WORK_DIR` | `bench-work` / `/tmp/bench-work` | Scratch directory for DB files and dumps |

Logical content size ≈ `ROWS * CONTENT_BYTES` (plus SQL/metadata overhead on disk).

## How to read the output

The summary table prints average **create**, **restore**, **total**, **file size**, and **vs_cur** (baseline total / strategy total).

- **File size** → `/tmp` / PVC pressure  
- **restore_ms** → closest proxy to “pod loads snapshot into H2”  
- **vs_cur > 1** → faster than current uncompressed SCRIPT  

Expect roughly:

- `gzip-script`: much smaller files; create/restore often similar or a bit slower/faster depending on CPU vs I/O  
- `h2-backup`: often faster restore and smaller than plain SCRIPT, but needs **file-based** H2 (not today’s `jdbc:h2:mem` KafkaSQL URL)

## Mapping back to Apicurio

Today in `H2SqlStatements`:

```java
SCRIPT TO ?
RUNSCRIPT FROM ?
```

Option 1 would become something like:

```java
SCRIPT TO ? COMPRESSION GZIP
RUNSCRIPT FROM ? COMPRESSION GZIP
```

Option 2 needs a KafkaSQL design change to use (or flush to) a file-backed H2 before `BACKUP TO` is viable.
