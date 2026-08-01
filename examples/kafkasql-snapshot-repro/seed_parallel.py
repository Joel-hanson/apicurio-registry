#!/usr/bin/env python3
"""
Fast parallel artifact seeder for kafkasql-snapshot-repro.

Cycles through schemas/ fixtures (copied from integration-tests artifactTypes),
creates TARGET unique artifacts concurrently, and optionally pads content so the
H2 dump / journal payloads are heavier.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

TYPE_MAP = {
    "openapi": ("OPENAPI", "application/json"),
    "asyncapi": ("ASYNCAPI", "application/json"),
    "avro": ("AVRO", "application/json"),
    "jsonschema": ("JSON", "application/json"),
    "jsonSchema": ("JSON", "application/json"),
    "protobuf": ("PROTOBUF", "application/x-protobuf"),
    "graphql": ("GRAPHQL", "application/graphql"),
    "kafkaconnect": ("KCONNECT", "application/json"),
    "kafkaConnect": ("KCONNECT", "application/json"),
    "openrpc": ("OPENRPC", "application/json"),
    "agentcard": ("AGENTCARD", "application/json"),
    "thrift": ("THRIFT", "application/x-thrift"),
    "extra": ("JSON", "application/json"),
}

EXT_OVERRIDE = {
    ".yaml": "application/yaml",
    ".yml": "application/yaml",
    ".proto": "application/x-protobuf",
    ".graphql": "application/graphql",
    ".thrift": "application/x-thrift",
    ".avsc": "application/json",
}


def detect_type(path: Path, schemas_root: Path) -> tuple[str, str]:
    rel = path.relative_to(schemas_root)
    folder = rel.parts[0] if len(rel.parts) > 1 else "extra"
    artifact_type, content_type = TYPE_MAP.get(folder, ("JSON", "application/json"))
    content_type = EXT_OVERRIDE.get(path.suffix.lower(), content_type)
    # YAML openapi under extra/
    if path.suffix.lower() in (".yaml", ".yml") and folder == "extra":
        artifact_type = "OPENAPI"
    if path.name.startswith("FLI") or "Stock" in path.name or "ItemBase" in path.name:
        artifact_type = "JSON"
        content_type = "application/json"
    return artifact_type, content_type


# Types that register reliably without special content-type / rule setup.
SAFE_FOLDERS = {
    "openapi",
    "asyncapi",
    "avro",
    "jsonSchema",
    "jsonschema",
    "kafkaConnect",
    "kafkaconnect",
    "openrpc",
    "agentcard",
    "extra",
}


def load_templates(schemas_root: Path, include_all: bool) -> list[dict]:
    files = sorted(
        p
        for p in schemas_root.rglob("*")
        if p.is_file() and p.suffix.lower()
        in {".json", ".yaml", ".yml", ".avsc", ".proto", ".graphql", ".thrift"}
    )
    templates = []
    for path in files:
        rel = path.relative_to(schemas_root)
        folder = rel.parts[0] if len(rel.parts) > 1 else "extra"
        if not include_all and folder not in SAFE_FOLDERS:
            continue
        artifact_type, content_type = detect_type(path, schemas_root)
        raw = path.read_text(encoding="utf-8")
        templates.append(
            {
                "path": str(path.relative_to(schemas_root)),
                "artifact_type": artifact_type,
                "content_type": content_type,
                "raw": raw,
            }
        )
    if not templates:
        raise SystemExit(f"no schema templates found under {schemas_root}")
    return templates


def uniquify(raw: str, content_type: str, artifact_id: str, pad_bytes: int) -> str:
    marker = f"bench-{artifact_id}"
    pad = ("x" * pad_bytes) if pad_bytes > 0 else ""
    if "json" in content_type and not content_type.endswith("yaml"):
        try:
            obj = json.loads(raw)
            if isinstance(obj, dict):
                obj["x-apicurio-bench-id"] = marker
                if pad:
                    obj["x-apicurio-bench-pad"] = pad
                return json.dumps(obj, separators=(",", ":"))
        except json.JSONDecodeError:
            pass
    # Non-JSON / YAML: append a trailing comment-ish marker (still unique bytes)
    suffix = f"\n# {marker}\n"
    if pad:
        suffix += f"# pad:{pad}\n"
    return raw + suffix


def create_one(
    registry_url: str,
    group_id: str,
    index: int,
    template: dict,
    pad_bytes: int,
    timeout_sec: float,
) -> None:
    artifact_id = f"bench-{index:06d}"
    content = uniquify(template["raw"], template["content_type"], artifact_id, pad_bytes)
    body = {
        "artifactId": artifact_id,
        "artifactType": template["artifact_type"],
        "name": artifact_id,
        "firstVersion": {
            "version": "1.0.0",
            "content": {
                "content": content,
                "contentType": template["content_type"],
            },
        },
    }
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        f"{registry_url}/apis/registry/v3/groups/{group_id}/artifacts",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
        if resp.status not in (200, 201):
            raise RuntimeError(f"{artifact_id}: HTTP {resp.status}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--registry-url", default=os.environ.get("REGISTRY_URL", "http://localhost:18080"))
    parser.add_argument("--group-id", default=os.environ.get("GROUP_ID", "snapshot-bench"))
    parser.add_argument("--schemas-dir", default=os.environ.get("SCHEMAS_DIR", "schemas"))
    parser.add_argument("--artifacts", type=int, default=int(os.environ.get("ARTIFACTS", "5000")))
    parser.add_argument("--parallel", type=int, default=int(os.environ.get("PARALLEL", "32")))
    parser.add_argument("--pad-bytes", type=int, default=int(os.environ.get("PAD_BYTES", "0")))
    parser.add_argument("--timeout-sec", type=float, default=float(os.environ.get("REQUEST_TIMEOUT_SEC", "60")))
    parser.add_argument(
        "--include-all",
        action="store_true",
        default=os.environ.get("INCLUDE_ALL", "0") in ("1", "true", "yes"),
        help="Also seed protobuf/graphql/thrift templates (may fail validation)",
    )
    args = parser.parse_args()

    schemas_root = Path(args.schemas_dir).resolve()
    templates = load_templates(schemas_root, include_all=args.include_all)
    print(
        f"[seed] templates={len(templates)} artifacts={args.artifacts} "
        f"parallel={args.parallel} pad_bytes={args.pad_bytes} group={args.group_id}",
        flush=True,
    )

    start = time.time()
    done = 0
    errors: list[str] = []

    def job(i: int) -> None:
        tmpl = templates[(i - 1) % len(templates)]
        create_one(args.registry_url, args.group_id, i, tmpl, args.pad_bytes, args.timeout_sec)

    with ThreadPoolExecutor(max_workers=args.parallel) as pool:
        futures = {pool.submit(job, i): i for i in range(1, args.artifacts + 1)}
        for fut in as_completed(futures):
            i = futures[fut]
            try:
                fut.result()
            except Exception as exc:  # noqa: BLE001 - collect and fail at end
                err = f"artifact {i}: {exc}"
                if isinstance(exc, urllib.error.HTTPError):
                    try:
                        err += f" body={exc.read().decode('utf-8', errors='replace')[:300]}"
                    except Exception:
                        pass
                errors.append(err)
            done += 1
            if done % 100 == 0 or done == args.artifacts:
                elapsed = time.time() - start
                rate = done / elapsed if elapsed > 0 else 0
                print(f"[seed] {done}/{args.artifacts} ({rate:.1f}/s)", flush=True)

    elapsed = time.time() - start
    print(f"[seed] finished {args.artifacts - len(errors)}/{args.artifacts} in {elapsed:.1f}s", flush=True)
    if errors:
        print(f"[seed] ERRORS ({len(errors)}):", file=sys.stderr)
        for line in errors[:20]:
            print(f"  {line}", file=sys.stderr)
        if len(errors) > 20:
            print(f"  ... {len(errors) - 20} more", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
