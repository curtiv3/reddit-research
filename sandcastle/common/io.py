from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable

import logging

logger = logging.getLogger(__name__)


def read_jsonl(path: str | Path) -> Iterable[dict[str, Any]]:
    file_path = Path(path)
    if not file_path.exists():
        return []

    def _iter():
        with file_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    logger.warning("Skipping malformed JSONL line")
                    continue

    return _iter()


def append_jsonl(path: str | Path, rows: Iterable[dict[str, Any]]) -> None:
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_json(path: str | Path, payload: Any) -> None:
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("w", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")


def read_json(path: str | Path) -> Any:
    file_path = Path(path)
    if not file_path.exists():
        return None
    return json.loads(file_path.read_text(encoding="utf-8"))


def count_file(path: str | Path) -> None:
    file_path = Path(path)
    summary: dict[str, Any] = {"file": str(file_path)}
    if not file_path.exists():
        summary.update({"error": "file_not_found"})
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return

    if file_path.suffix == ".jsonl":
        count = 0
        ids = set()
        per_engine: dict[str, int] = {}
        per_query: dict[str, int] = {}
        with file_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                count += 1
                if isinstance(row, dict):
                    if row.get("id"):
                        ids.add(row.get("id"))
                    engine = row.get("engine")
                    if engine:
                        per_engine[engine] = per_engine.get(engine, 0) + 1
                    query = row.get("query")
                    if query:
                        per_query[query] = per_query.get(query, 0) + 1
        summary.update(
            {
                "valid_objects": count,
                "unique_ids": len(ids),
                "per_engine": per_engine,
                "per_query": per_query,
            }
        )
    elif file_path.suffix == ".json":
        payload = read_json(file_path)
        if isinstance(payload, list):
            ids = {item.get("id") for item in payload if isinstance(item, dict) and item.get("id")}
            summary.update({"items": len(payload), "unique_ids": len(ids)})
        elif isinstance(payload, dict) and "clusters" in payload:
            per_cluster = {
                cluster.get("cluster_id"): len(cluster.get("items", []))
                for cluster in payload.get("clusters", [])
            }
            summary.update({"clusters": len(payload.get("clusters", [])), "per_cluster": per_cluster})
        else:
            summary.update({"items": 1 if payload is not None else 0})
    else:
        summary.update({"error": "Unsupported file type"})

    print(json.dumps(summary, ensure_ascii=False, indent=2))
