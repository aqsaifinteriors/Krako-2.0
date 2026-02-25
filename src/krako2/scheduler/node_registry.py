from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


class Node(BaseModel):
    node_id: str
    enabled: bool = True
    health_status: str = "healthy"
    supported_kinds: list[str] = Field(default_factory=list)
    available_concurrency: int = 0
    active_queue_depth: int = 0
    utilization: float = 0.0
    trust_score: float = 0.5
    region: str | None = None
    version: str = "0.1.0"
    last_heartbeat_ts: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class NodeRegistry:
    def __init__(self, registry_path: str | Path = "data/node_registry.json") -> None:
        self.registry_path = Path(registry_path)
        self.registry_path.parent.mkdir(parents=True, exist_ok=True)
        if not self.registry_path.exists():
            self._atomic_write({"nodes": []})

    def _read(self) -> dict[str, Any]:
        with self.registry_path.open("r", encoding="utf-8") as f:
            return json.load(f)

    def _atomic_write(self, data: dict[str, Any]) -> None:
        tmp_path = self.registry_path.with_suffix(self.registry_path.suffix + ".tmp")
        with tmp_path.open("w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, sort_keys=True, indent=2)
            f.write("\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, self.registry_path)

    def list_nodes(self) -> list[Node]:
        data = self._read()
        nodes = data.get("nodes", [])
        return [Node.model_validate(n) for n in nodes]

    def update_node(self, node_id: str, partial_fields: dict[str, Any]) -> None:
        data = self._read()
        nodes = data.get("nodes", [])

        updated = False
        for i, raw_node in enumerate(nodes):
            if raw_node.get("node_id") == node_id:
                merged = {**raw_node, **partial_fields}
                nodes[i] = Node.model_validate(merged).model_dump(mode="json")
                updated = True
                break

        if not updated:
            raise KeyError(f"node not found: {node_id}")

        data["nodes"] = nodes
        self._atomic_write(data)


def list_nodes() -> list[Node]:
    return NodeRegistry().list_nodes()


def update_node(node_id: str, partial_fields: dict[str, Any]) -> None:
    NodeRegistry().update_node(node_id, partial_fields)
