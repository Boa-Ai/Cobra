import json
import math
import threading
import time
import uuid
from pathlib import Path
from typing import Any

DEFAULT_NODE_TYPE = "Note"
DEFAULT_STATUS = "new"
DEFAULT_SEVERITY = "info"
MAX_TEXT_LEN = 1800

TYPE_RING_ORDER = [
    "Objective",
    "Scope",
    "Asset",
    "Surface",
    "Hypothesis",
    "Action",
    "Evidence",
    "Finding",
    "Risk",
    "Recommendation",
    "Artifact",
    "Agent",
    "Note",
]


class GraphStore:
    def __init__(self, file_path: Path):
        self.file_path = file_path
        self._lock = threading.Lock()

    def _default_state(self) -> dict[str, Any]:
        return {"version": 1, "sessions": {}}

    def _default_session_graph(self) -> dict[str, Any]:
        return {
            "nodes": [],
            "edges": [],
            "updated_at": time.time(),
        }

    def _load(self) -> dict[str, Any]:
        if not self.file_path.exists():
            return self._default_state()
        try:
            data = json.loads(self.file_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return self._default_state()
        if not isinstance(data, dict):
            return self._default_state()
        sessions = data.get("sessions")
        if not isinstance(sessions, dict):
            data["sessions"] = {}
        if "version" not in data:
            data["version"] = 1
        return data

    def _save(self, state: dict[str, Any]) -> None:
        self.file_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.file_path.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp_path.replace(self.file_path)

    def _ensure_session_graph(self, state: dict[str, Any], session_id: str) -> dict[str, Any]:
        sessions = state.setdefault("sessions", {})
        raw = sessions.get(session_id)
        graph = self._normalize_graph(raw) if raw is not None else None
        if graph is None:
            graph = self._default_session_graph()
        sessions[session_id] = graph
        return graph

    def _normalize_graph(self, graph: Any) -> dict[str, Any] | None:
        if not isinstance(graph, dict):
            return None
        nodes_raw = graph.get("nodes")
        edges_raw = graph.get("edges")
        nodes: list[dict[str, Any]] = []
        edges: list[dict[str, Any]] = []
        if isinstance(nodes_raw, list):
            for node in nodes_raw:
                normalized = self._normalize_node(node)
                if normalized:
                    nodes.append(normalized)
        if isinstance(edges_raw, list):
            for edge in edges_raw:
                normalized = self._normalize_edge(edge)
                if normalized:
                    edges.append(normalized)
        updated_at = graph.get("updated_at")
        if not isinstance(updated_at, (int, float)):
            updated_at = time.time()
        return {
            "nodes": nodes,
            "edges": edges,
            "updated_at": float(updated_at),
        }

    def _trim_text(self, value: Any, max_len: int = MAX_TEXT_LEN) -> str:
        text = str(value or "").replace("\r\n", "\n").replace("\r", "\n").strip()
        if len(text) <= max_len:
            return text
        return text[: max_len - 16].rstrip() + "\n...(truncated)"

    def _coerce_confidence(self, value: Any) -> float | None:
        if value is None or value == "":
            return None
        try:
            numeric = float(value)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(numeric):
            return None
        return max(0.0, min(1.0, numeric))

    def _normalize_node(self, raw: Any) -> dict[str, Any] | None:
        if not isinstance(raw, dict):
            return None
        node_id = str(raw.get("id") or "").strip()
        if not node_id:
            return None

        node_type = str(raw.get("type") or DEFAULT_NODE_TYPE).strip() or DEFAULT_NODE_TYPE
        label = self._trim_text(raw.get("label") or node_type, max_len=200) or node_type
        description = self._trim_text(raw.get("description") or "")
        status = str(raw.get("status") or DEFAULT_STATUS).strip() or DEFAULT_STATUS
        severity = str(raw.get("severity") or DEFAULT_SEVERITY).strip() or DEFAULT_SEVERITY
        created_by = str(raw.get("created_by") or "system").strip() or "system"

        x = raw.get("x")
        y = raw.get("y")
        x = float(x) if isinstance(x, (int, float)) and math.isfinite(x) else None
        y = float(y) if isinstance(y, (int, float)) and math.isfinite(y) else None

        created_at = raw.get("created_at")
        updated_at = raw.get("updated_at")
        now = time.time()
        if not isinstance(created_at, (int, float)):
            created_at = now
        if not isinstance(updated_at, (int, float)):
            updated_at = created_at

        refs_raw = raw.get("refs")
        refs: list[dict[str, Any]] = []
        if isinstance(refs_raw, list):
            for item in refs_raw:
                if isinstance(item, dict):
                    refs.append({k: v for k, v in item.items() if isinstance(k, str)})

        data_raw = raw.get("data")
        data = data_raw if isinstance(data_raw, dict) else {}

        return {
            "id": node_id,
            "type": node_type,
            "label": label,
            "description": description,
            "status": status,
            "confidence": self._coerce_confidence(raw.get("confidence")),
            "severity": severity,
            "created_by": created_by,
            "source": raw.get("source") if isinstance(raw.get("source"), dict) else {},
            "refs": refs,
            "data": data,
            "x": x,
            "y": y,
            "created_at": float(created_at),
            "updated_at": float(updated_at),
        }

    def _normalize_edge(self, raw: Any) -> dict[str, Any] | None:
        if not isinstance(raw, dict):
            return None
        from_id = str(raw.get("from") or "").strip()
        to_id = str(raw.get("to") or "").strip()
        if not from_id or not to_id:
            return None
        edge_type = str(raw.get("type") or "related").strip() or "related"
        edge_id = str(raw.get("id") or "").strip() or f"edge:{from_id}:{edge_type}:{to_id}"
        label = self._trim_text(raw.get("label") or "", max_len=180)
        created_by = str(raw.get("created_by") or "system").strip() or "system"
        created_at = raw.get("created_at")
        updated_at = raw.get("updated_at")
        now = time.time()
        if not isinstance(created_at, (int, float)):
            created_at = now
        if not isinstance(updated_at, (int, float)):
            updated_at = created_at
        return {
            "id": edge_id,
            "from": from_id,
            "to": to_id,
            "type": edge_type,
            "label": label,
            "created_by": created_by,
            "data": raw.get("data") if isinstance(raw.get("data"), dict) else {},
            "created_at": float(created_at),
            "updated_at": float(updated_at),
        }

    def _is_curated_node(self, node: dict[str, Any]) -> bool:
        if not isinstance(node, dict):
            return False
        data = node.get("data") if isinstance(node.get("data"), dict) else {}
        if data.get("manual") is True:
            return True
        node_id = str(node.get("id") or "").strip().lower()
        created_by = str(node.get("created_by") or "").strip().lower()
        if created_by == "user" and node_id.startswith("node:"):
            return True
        if created_by == "agent" and data.get("agent_suggested") is True:
            return True
        return False

    def _curated_graph_view(self, graph: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
        all_nodes = list(graph.get("nodes", []))
        curated_nodes = [node for node in all_nodes if self._is_curated_node(node)]
        keep_ids = {str(node.get("id") or "") for node in curated_nodes}
        curated_edges = [
            edge
            for edge in graph.get("edges", [])
            if str(edge.get("from") or "") in keep_ids and str(edge.get("to") or "") in keep_ids
        ]
        hidden_nodes = max(0, len(all_nodes) - len(curated_nodes))
        return curated_nodes, curated_edges, hidden_nodes

    def _next_position(self, graph: dict[str, Any], node_type: str) -> tuple[float, float]:
        def _position_is_clear(x_pos: float, y_pos: float, min_distance: float = 220.0) -> bool:
            min_distance_sq = min_distance * min_distance
            for existing in graph.get("nodes", []):
                ex = existing.get("x")
                ey = existing.get("y")
                if not isinstance(ex, (int, float)) or not isinstance(ey, (int, float)):
                    continue
                if not math.isfinite(ex) or not math.isfinite(ey):
                    continue
                dx = float(ex) - x_pos
                dy = float(ey) - y_pos
                if (dx * dx + dy * dy) < min_distance_sq:
                    return False
            return True

        if node_type == "Run":
            run_count = sum(1 for node in graph.get("nodes", []) if str(node.get("type")) == "Run")
            base_x = float(run_count * 170)
            base_y = float(-run_count * 52)
            if _position_is_clear(base_x, base_y, min_distance=240.0):
                return base_x, base_y
            for step in range(1, 40):
                angle = (step * 0.57) + (run_count * 0.11)
                radius = 120 + (step * 28)
                x_try = base_x + math.cos(angle) * radius
                y_try = base_y + math.sin(angle) * radius * 0.72
                if _position_is_clear(float(x_try), float(y_try), min_distance=240.0):
                    return float(x_try), float(y_try)
            return base_x, base_y

        type_index = TYPE_RING_ORDER.index(node_type) if node_type in TYPE_RING_ORDER else len(TYPE_RING_ORDER)
        type_count = sum(1 for node in graph.get("nodes", []) if str(node.get("type")) == node_type)

        lanes = max(1, min(12, type_count // 8 + 1))
        lane_index = type_count % lanes
        orbit_step = type_count // lanes
        angle = (orbit_step * 0.72) + (type_index * 0.51) + (lane_index * 0.2)
        radius = 230 + (type_index * 82) + (lane_index * 96)
        x = math.cos(angle) * radius
        y = math.sin(angle) * radius * 0.78
        if _position_is_clear(float(x), float(y)):
            return float(x), float(y)

        for step in range(1, 50):
            alt_angle = angle + (step * 0.62)
            alt_radius = radius + (step * 42)
            x_try = math.cos(alt_angle) * alt_radius
            y_try = math.sin(alt_angle) * alt_radius * 0.78
            if _position_is_clear(float(x_try), float(y_try)):
                return float(x_try), float(y_try)
        return float(x), float(y)

    def _find_node_index(self, graph: dict[str, Any], node_id: str) -> int:
        for index, node in enumerate(graph.get("nodes", [])):
            if str(node.get("id")) == node_id:
                return index
        return -1

    def _find_edge_index(self, graph: dict[str, Any], edge_id: str) -> int:
        for index, edge in enumerate(graph.get("edges", [])):
            if str(edge.get("id")) == edge_id:
                return index
        return -1

    def _touch_graph(self, graph: dict[str, Any]) -> None:
        graph["updated_at"] = float(time.time())

    def get_graph(self, session_id: str) -> dict[str, Any]:
        with self._lock:
            state = self._load()
            graph = self._ensure_session_graph(state, session_id)
            curated_nodes, curated_edges, hidden_nodes = self._curated_graph_view(graph)
            self._save(state)
            return {
                "session_id": session_id,
                "nodes": curated_nodes,
                "edges": curated_edges,
                "manual_only": True,
                "nodes_hidden": hidden_nodes,
                "updated_at": float(graph.get("updated_at") or time.time()),
            }

    def delete_session(self, session_id: str) -> bool:
        with self._lock:
            state = self._load()
            sessions = state.get("sessions")
            if not isinstance(sessions, dict) or session_id not in sessions:
                return False
            del sessions[session_id]
            self._save(state)
            return True

    def upsert_node(self, session_id: str, raw_node: dict[str, Any]) -> dict[str, Any]:
        node = self._normalize_node(raw_node) if isinstance(raw_node, dict) else None
        if not node:
            raise ValueError("Invalid node payload.")

        with self._lock:
            state = self._load()
            graph = self._ensure_session_graph(state, session_id)
            now = time.time()
            idx = self._find_node_index(graph, node["id"])
            if idx >= 0:
                existing = graph["nodes"][idx]
                merged = {**existing, **node}
                merged["updated_at"] = float(now)
                graph["nodes"][idx] = self._normalize_node(merged) or existing
            else:
                if node.get("x") is None or node.get("y") is None:
                    x, y = self._next_position(graph, str(node.get("type") or DEFAULT_NODE_TYPE))
                    node["x"] = x if node.get("x") is None else node["x"]
                    node["y"] = y if node.get("y") is None else node["y"]
                node["created_at"] = float(now)
                node["updated_at"] = float(now)
                graph["nodes"].append(node)
            self._touch_graph(graph)
            self._save(state)
            return graph["nodes"][self._find_node_index(graph, node["id"])]

    def create_node(self, session_id: str, payload: dict[str, Any], created_by: str = "user") -> dict[str, Any]:
        raw = payload if isinstance(payload, dict) else {}
        node_id = str(raw.get("id") or "").strip() or f"node:{uuid.uuid4().hex[:12]}"
        data = raw.get("data") if isinstance(raw.get("data"), dict) else {}
        if "manual" not in data:
            data = {**data, "manual": True}
        node = {
            "id": node_id,
            "type": str(raw.get("type") or DEFAULT_NODE_TYPE).strip() or DEFAULT_NODE_TYPE,
            "label": self._trim_text(raw.get("label") or "Untitled node", max_len=200) or "Untitled node",
            "description": self._trim_text(raw.get("description") or ""),
            "status": str(raw.get("status") or DEFAULT_STATUS).strip() or DEFAULT_STATUS,
            "confidence": self._coerce_confidence(raw.get("confidence")),
            "severity": str(raw.get("severity") or DEFAULT_SEVERITY).strip() or DEFAULT_SEVERITY,
            "created_by": str(created_by or "user").strip() or "user",
            "source": raw.get("source") if isinstance(raw.get("source"), dict) else {},
            "refs": raw.get("refs") if isinstance(raw.get("refs"), list) else [],
            "data": data,
            "x": raw.get("x"),
            "y": raw.get("y"),
        }
        return self.upsert_node(session_id, node)

    def patch_node(self, session_id: str, node_id: str, patch: dict[str, Any]) -> dict[str, Any] | None:
        if not node_id:
            return None
        with self._lock:
            state = self._load()
            graph = self._ensure_session_graph(state, session_id)
            idx = self._find_node_index(graph, node_id)
            if idx < 0:
                return None
            existing = graph["nodes"][idx]
            next_node = dict(existing)

            allowed = {"label", "description", "type", "status", "severity", "confidence", "x", "y", "data", "refs"}
            for key in allowed:
                if key in patch:
                    next_node[key] = patch.get(key)
            next_node["updated_at"] = float(time.time())
            normalized = self._normalize_node(next_node)
            if not normalized:
                return None
            graph["nodes"][idx] = normalized
            self._touch_graph(graph)
            self._save(state)
            return normalized

    def create_edge(self, session_id: str, payload: dict[str, Any], created_by: str = "user") -> dict[str, Any]:
        raw = payload if isinstance(payload, dict) else {}
        from_id = str(raw.get("from") or "").strip()
        to_id = str(raw.get("to") or "").strip()
        edge_type = str(raw.get("type") or "related").strip() or "related"
        if not from_id or not to_id:
            raise ValueError("Edge requires 'from' and 'to'.")
        edge_id = str(raw.get("id") or "").strip() or f"edge:{from_id}:{edge_type}:{to_id}"
        edge = {
            "id": edge_id,
            "from": from_id,
            "to": to_id,
            "type": edge_type,
            "label": self._trim_text(raw.get("label") or "", max_len=180),
            "created_by": str(created_by or "user").strip() or "user",
            "data": raw.get("data") if isinstance(raw.get("data"), dict) else {},
        }
        normalized = self._normalize_edge(edge)
        if not normalized:
            raise ValueError("Invalid edge payload.")

        with self._lock:
            state = self._load()
            graph = self._ensure_session_graph(state, session_id)
            idx = self._find_edge_index(graph, normalized["id"])
            now = float(time.time())
            if idx >= 0:
                existing = graph["edges"][idx]
                merged = {**existing, **normalized}
                merged["updated_at"] = now
                graph["edges"][idx] = self._normalize_edge(merged) or existing
            else:
                normalized["created_at"] = now
                normalized["updated_at"] = now
                graph["edges"].append(normalized)
            self._touch_graph(graph)
            self._save(state)
            return graph["edges"][self._find_edge_index(graph, normalized["id"])]

    # Legacy compatibility hooks kept intentionally minimal.
    def start_run(self, session_id: str, run_id: str, prompt: str) -> dict[str, str]:
        return {
            "run_node_id": f"run:{run_id}",
            "prompt_node_id": f"objective:{run_id}",
            "mission_node_id": "mission:root",
        }

    def ingest_event(self, session_id: str, run_id: str, event_type: str, data: dict[str, Any]) -> None:
        return

    def finalize_run(self, session_id: str, run_id: str, final_text: str, ok: bool = True) -> None:
        return

    def build_context(self, session_id: str, max_nodes: int = 36) -> str:
        with self._lock:
            state = self._load()
            graph = self._ensure_session_graph(state, session_id)
            nodes, edges, _hidden_nodes = self._curated_graph_view(graph)
            self._save(state)

        if not nodes:
            return "No manually curated mission graph context is available yet."

        type_counts: dict[str, int] = {}
        for node in nodes:
            node_type = str(node.get("type") or "Note")
            type_counts[node_type] = type_counts.get(node_type, 0) + 1
        counts_line = ", ".join(f"{k}:{v}" for k, v in sorted(type_counts.items(), key=lambda item: item[0].lower()))

        sorted_nodes = sorted(nodes, key=lambda item: float(item.get("updated_at") or 0), reverse=True)
        highlighted = sorted_nodes[:max(6, min(max_nodes, 48))]
        open_nodes = [
            item
            for item in highlighted
            if str(item.get("status") or "").lower() in {"new", "open", "running", "in_progress", "active"}
        ][:16]
        if not open_nodes:
            open_nodes = highlighted[:12]

        lines = [
            "Mission graph snapshot:",
            f"- Node counts: {counts_line}",
            f"- Total edges: {len(edges)}",
            "- Open/high-signal nodes:",
        ]
        for node in open_nodes:
            confidence = node.get("confidence")
            conf_text = f"{float(confidence):.2f}" if isinstance(confidence, (int, float)) else "n/a"
            desc = self._trim_text(node.get("description") or "", max_len=160).replace("\n", " ")
            lines.append(
                f"  - [{node.get('id')}] {node.get('type')} :: {node.get('label')} "
                f"(status={node.get('status')}, severity={node.get('severity')}, confidence={conf_text}) "
                f"{desc}"
            )
        lines.append("Use this memory to avoid duplicate work and to target uncovered gaps.")
        return "\n".join(lines)

    def apply_agent_update(self, session_id: str, run_id: str, update_payload: dict[str, Any]) -> dict[str, int]:
        return {"created_nodes": 0, "created_edges": 0}
