# Schema fixtures for kafkasql-snapshot-repro

Copied from:

- `integration-tests/src/test/resources/artifactTypes/` (OpenAPI, AsyncAPI, Avro, JSON Schema, …)
- `app/src/test/resources/.../maven/stock/` and `openapi-yaml/petstore-api.yaml` under `extra/`

Used by `seed_parallel.py` / `bench-large-startup.sh`. By default protobuf/graphql/thrift are skipped (`INCLUDE_ALL=1` to include).
