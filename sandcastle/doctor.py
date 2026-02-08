from __future__ import annotations

import json
import os
from pathlib import Path

import requests

from sandcastle.config import Config, resolve_path


def check_writable(path: Path) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    return os.access(path.parent, os.W_OK)


def run_doctor(config: Config) -> None:
    checks = []

    outputs = config.outputs
    for key in ["raw_results", "deduped", "clusters", "terms", "quality", "reddit_posts", "reddit_intents"]:
        path = resolve_path(config.path.parent, outputs.get(key, f"data/{key}"))
        checks.append({"check": f"output_writable:{key}", "ok": check_writable(path)})

    for engine in config.engines:
        if engine.get("name") == "searxng":
            endpoint = engine.get("endpoint")
            ok = False
            if endpoint:
                try:
                    resp = requests.get(endpoint, params={"q": "test", "format": "json"}, timeout=5)
                    ok = resp.status_code < 400
                except requests.RequestException:
                    ok = False
            checks.append({"check": "searxng_reachable", "ok": ok, "endpoint": endpoint})

    reddit_query = (config.reddit.get("queries") or [None])[0]
    if reddit_query:
        try:
            resp = requests.get(
                "https://www.reddit.com/search.json",
                params={"q": reddit_query, "limit": 1, "type": "link"},
                headers={"User-Agent": "sandcastle/0.1"},
                timeout=5,
            )
            ok = resp.status_code < 400
        except requests.RequestException:
            ok = False
        checks.append({"check": "reddit_reachable", "ok": ok})

    print(json.dumps({"checks": checks}, ensure_ascii=False, indent=2))
