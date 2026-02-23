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

    def _next_position(self, graph: dict[str, Any], node_type: str) -> tuple[float, float]:
        if node_type == "Run":
            run_count = sum(1 for node in graph.get("nodes", []) if str(node.get("type")) == "Run")
            return float(run_count * 110), float(-run_count * 36)

        type_index = TYPE_RING_ORDER.index(node_type) if node_type in TYPE_RING_ORDER else len(TYPE_RING_ORDER)
        type_count = sum(1 for node in graph.get("nodes", []) if str(node.get("type")) == node_type)
        angle = (type_count * 0.92) + (type_index * 0.43)
        radius = 160 + (type_index * 58)
        return float(math.cos(angle) * radius), float(math.sin(angle) * radius)

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
            self._save(state)
            return {
                "session_id": session_id,
                "nodes": list(graph.get("nodes", [])),
                "edges": list(graph.get("edges", [])),
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
            "data": raw.get("data") if isinstance(raw.get("data"), dict) else {},
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

    def _ensure_root_nodes(self, graph: dict[str, Any], session_id: str) -> str:
        mission_id = "mission:root"
        if self._find_node_index(graph, mission_id) >= 0:
            return mission_id
        x, y = 0.0, 0.0
        node = {
            "id": mission_id,
            "type": "Objective",
            "label": "Session Mission",
            "description": f"Primary mission model for session {session_id}.",
            "status": "active",
            "confidence": 0.6,
            "severity": "info",
            "created_by": "system",
            "source": {},
            "refs": [],
            "data": {"session_id": session_id},
            "x": x,
            "y": y,
            "created_at": float(time.time()),
            "updated_at": float(time.time()),
        }
        graph["nodes"].append(node)
        return mission_id

    def start_run(self, session_id: str, run_id: str, prompt: str) -> dict[str, str]:
        run_node_id = f"run:{run_id}"
        prompt_node_id = f"objective:{run_id}"

        with self._lock:
            state = self._load()
            graph = self._ensure_session_graph(state, session_id)
            mission_id = self._ensure_root_nodes(graph, session_id)

            if self._find_node_index(graph, run_node_id) < 0:
                x, y = self._next_position(graph, "Run")
                graph["nodes"].append(
                    {
                        "id": run_node_id,
                        "type": "Run",
                        "label": f"Run {run_id[:8]}",
                        "description": "Active execution run.",
                        "status": "running",
                        "confidence": 0.7,
                        "severity": "info",
                        "created_by": "system",
                        "source": {"run_id": run_id},
                        "refs": [],
                        "data": {"run_id": run_id},
                        "x": x,
                        "y": y,
                        "created_at": float(time.time()),
                        "updated_at": float(time.time()),
                    }
                )

            if self._find_node_index(graph, prompt_node_id) < 0:
                x, y = self._next_position(graph, "Objective")
                graph["nodes"].append(
                    {
                        "id": prompt_node_id,
                        "type": "Objective",
                        "label": self._trim_text(prompt.splitlines()[0] if prompt else "Objective", max_len=160),
                        "description": self._trim_text(prompt, max_len=900),
                        "status": "in_progress",
                        "confidence": 0.5,
                        "severity": "info",
                        "created_by": "user",
                        "source": {"run_id": run_id},
                        "refs": [],
                        "data": {"prompt": self._trim_text(prompt, max_len=900)},
                        "x": x,
                        "y": y,
                        "created_at": float(time.time()),
                        "updated_at": float(time.time()),
                    }
                )

            edge_payloads = [
                {"from": mission_id, "to": run_node_id, "type": "tracks", "created_by": "system"},
                {"from": run_node_id, "to": prompt_node_id, "type": "targets", "created_by": "system"},
            ]
            for payload in edge_payloads:
                edge = self._normalize_edge(payload)
                if not edge:
                    continue
                if self._find_edge_index(graph, edge["id"]) < 0:
                    edge["created_at"] = float(time.time())
                    edge["updated_at"] = float(time.time())
                    graph["edges"].append(edge)

            self._touch_graph(graph)
            self._save(state)
            return {"run_node_id": run_node_id, "prompt_node_id": prompt_node_id, "mission_node_id": mission_id}

    def _upsert_runtime_node(self, graph: dict[str, Any], node: dict[str, Any]) -> dict[str, Any]:
        normalized = self._normalize_node(node)
        if not normalized:
            return node
        idx = self._find_node_index(graph, normalized["id"])
        now = float(time.time())
        if idx >= 0:
            existing = graph["nodes"][idx]
            merged = {**existing, **normalized}
            merged["updated_at"] = now
            normalized = self._normalize_node(merged) or existing
            graph["nodes"][idx] = normalized
        else:
            if normalized.get("x") is None or normalized.get("y") is None:
                x, y = self._next_position(graph, str(normalized.get("type") or DEFAULT_NODE_TYPE))
                normalized["x"] = x if normalized.get("x") is None else normalized.get("x")
                normalized["y"] = y if normalized.get("y") is None else normalized.get("y")
            normalized["created_at"] = now
            normalized["updated_at"] = now
            graph["nodes"].append(normalized)
        return normalized

    def _upsert_runtime_edge(self, graph: dict[str, Any], edge_payload: dict[str, Any]) -> None:
        edge = self._normalize_edge(edge_payload)
        if not edge:
            return
        idx = self._find_edge_index(graph, edge["id"])
        now = float(time.time())
        if idx >= 0:
            existing = graph["edges"][idx]
            merged = {**existing, **edge}
            merged["updated_at"] = now
            graph["edges"][idx] = self._normalize_edge(merged) or existing
        else:
            edge["created_at"] = now
            edge["updated_at"] = now
            graph["edges"].append(edge)

    def ingest_event(self, session_id: str, run_id: str, event_type: str, data: dict[str, Any]) -> None:
        if not session_id or not run_id:
            return
        event_name = str(event_type or "").strip().lower()
        payload = data if isinstance(data, dict) else {}
        now = float(time.time())

        with self._lock:
            state = self._load()
            graph = self._ensure_session_graph(state, session_id)
            self._ensure_root_nodes(graph, session_id)
            run_node_id = f"run:{run_id}"

            if self._find_node_index(graph, run_node_id) < 0:
                self._upsert_runtime_node(
                    graph,
                    {
                        "id": run_node_id,
                        "type": "Run",
                        "label": f"Run {run_id[:8]}",
                        "description": "Execution run.",
                        "status": "running",
                        "confidence": 0.7,
                        "severity": "info",
                        "created_by": "system",
                        "source": {"run_id": run_id},
                        "refs": [],
                        "data": {"run_id": run_id},
                    },
                )

            if event_name == "run_status":
                phase = str(payload.get("phase") or "").strip().lower()
                status = "running"
                if phase in {"end", "completed", "complete"}:
                    status = "completed"
                elif phase in {"error", "failed"}:
                    status = "failed"
                self._upsert_runtime_node(
                    graph,
                    {
                        "id": run_node_id,
                        "type": "Run",
                        "label": f"Run {run_id[:8]}",
                        "status": status,
                        "severity": "high" if status == "failed" else "info",
                        "created_by": "system",
                        "source": {"run_id": run_id},
                        "data": {"run_id": run_id, "phase": phase or status},
                        "updated_at": now,
                    },
                )

            elif event_name in {"tool_start", "tool_update", "tool_execution"}:
                execution_id = str(payload.get("execution_id") or "").strip()
                step_index = payload.get("action_index_1based") or payload.get("step_index_1based") or "?"
                action_key = execution_id or f"step-{step_index}"
                action_node_id = f"action:{run_id}:{action_key}"
                tool_name = str(payload.get("tool_name") or "tool").strip() or "tool"
                command = self._trim_text(payload.get("command") or tool_name, max_len=280) or tool_name
                output = self._trim_text(payload.get("tool_output") or "", max_len=1500)
                rationale = self._trim_text(payload.get("rationale") or "", max_len=500)

                status = "running"
                if event_name == "tool_execution":
                    status = "failed" if bool(payload.get("is_error")) else "completed"

                action_data = {
                    "run_id": run_id,
                    "execution_id": execution_id,
                    "tool_name": tool_name,
                    "command": command,
                    "last_output": output,
                    "rationale": rationale,
                    "step_index_1based": step_index,
                    "event_type": event_name,
                }
                self._upsert_runtime_node(
                    graph,
                    {
                        "id": action_node_id,
                        "type": "Action",
                        "label": command,
                        "description": rationale or f"{tool_name} execution",
                        "status": status,
                        "confidence": 0.8 if status == "completed" else 0.6,
                        "severity": "high" if status == "failed" else "info",
                        "created_by": "agent",
                        "source": {"run_id": run_id, "execution_id": execution_id, "event_type": event_name},
                        "refs": [
                            {
                                "kind": "event",
                                "event_type": event_name,
                                "execution_id": execution_id,
                                "step_index_1based": step_index,
                            }
                        ],
                        "data": action_data,
                    },
                )
                self._upsert_runtime_edge(
                    graph,
                    {
                        "from": run_node_id,
                        "to": action_node_id,
                        "type": "tests",
                        "created_by": "system",
                    },
                )

                if event_name == "tool_execution":
                    evidence_node_id = f"evidence:{run_id}:{action_key}"
                    evidence_desc = output or "(no output)"
                    self._upsert_runtime_node(
                        graph,
                        {
                            "id": evidence_node_id,
                            "type": "Evidence",
                            "label": f"{tool_name} output",
                            "description": evidence_desc,
                            "status": "captured",
                            "confidence": 0.9,
                            "severity": "high" if bool(payload.get("is_error")) else "info",
                            "created_by": "agent",
                            "source": {"run_id": run_id, "execution_id": execution_id, "event_type": event_name},
                            "refs": [
                                {
                                    "kind": "event",
                                    "event_type": event_name,
                                    "execution_id": execution_id,
                                    "step_index_1based": step_index,
                                }
                            ],
                            "data": {
                                "run_id": run_id,
                                "execution_id": execution_id,
                                "tool_name": tool_name,
                                "command": command,
                                "output": evidence_desc,
                            },
                        },
                    )
                    self._upsert_runtime_edge(
                        graph,
                        {
                            "from": action_node_id,
                            "to": evidence_node_id,
                            "type": "produced",
                            "created_by": "system",
                        },
                    )

            elif event_name == "reasoning":
                text = self._trim_text(payload.get("text") or "", max_len=800)
                if text:
                    reasoning_node_id = f"reasoning:{run_id}"
                    self._upsert_runtime_node(
                        graph,
                        {
                            "id": reasoning_node_id,
                            "type": "Agent",
                            "label": "Agent notes",
                            "description": text,
                            "status": "active",
                            "confidence": 0.4,
                            "severity": "info",
                            "created_by": "agent",
                            "source": {"run_id": run_id, "event_type": "reasoning"},
                            "refs": [{"kind": "event", "event_type": "reasoning"}],
                            "data": {"run_id": run_id, "latest_note": text},
                        },
                    )
                    self._upsert_runtime_edge(
                        graph,
                        {
                            "from": run_node_id,
                            "to": reasoning_node_id,
                            "type": "derived_from",
                            "created_by": "system",
                        },
                    )

            elif event_name == "error":
                text = self._trim_text(payload.get("message") or "Run error.", max_len=900)
                node_id = f"finding:error:{run_id}"
                self._upsert_runtime_node(
                    graph,
                    {
                        "id": node_id,
                        "type": "Finding",
                        "label": "Run error",
                        "description": text,
                        "status": "open",
                        "confidence": 0.95,
                        "severity": "high",
                        "created_by": "system",
                        "source": {"run_id": run_id, "event_type": "error"},
                        "refs": [{"kind": "event", "event_type": "error"}],
                        "data": {"run_id": run_id, "message": text},
                    },
                )
                self._upsert_runtime_edge(
                    graph,
                    {
                        "from": run_node_id,
                        "to": node_id,
                        "type": "produced",
                        "created_by": "system",
                    },
                )

            self._touch_graph(graph)
            self._save(state)

    def finalize_run(self, session_id: str, run_id: str, final_text: str, ok: bool = True) -> None:
        run_node_id = f"run:{run_id}"
        summary_node_id = f"summary:{run_id}"
        clean_text = self._trim_text(final_text, max_len=2400)
        with self._lock:
            state = self._load()
            graph = self._ensure_session_graph(state, session_id)
            self._upsert_runtime_node(
                graph,
                {
                    "id": run_node_id,
                    "type": "Run",
                    "label": f"Run {run_id[:8]}",
                    "description": "Execution run.",
                    "status": "completed" if ok else "failed",
                    "confidence": 0.8 if ok else 0.95,
                    "severity": "info" if ok else "high",
                    "created_by": "system",
                    "source": {"run_id": run_id},
                    "data": {"run_id": run_id, "finalized": True},
                },
            )
            self._upsert_runtime_node(
                graph,
                {
                    "id": summary_node_id,
                    "type": "Recommendation",
                    "label": "Run summary",
                    "description": clean_text or "(no summary)",
                    "status": "final",
                    "confidence": 0.7 if ok else 0.5,
                    "severity": "info" if ok else "high",
                    "created_by": "agent",
                    "source": {"run_id": run_id, "event_type": "final_result"},
                    "refs": [{"kind": "summary", "run_id": run_id}],
                    "data": {"run_id": run_id},
                },
            )
            self._upsert_runtime_edge(
                graph,
                {
                    "from": run_node_id,
                    "to": summary_node_id,
                    "type": "derived_from",
                    "created_by": "system",
                },
            )
            self._touch_graph(graph)
            self._save(state)

    def build_context(self, session_id: str, max_nodes: int = 36) -> str:
        with self._lock:
            state = self._load()
            graph = self._ensure_session_graph(state, session_id)
            nodes = list(graph.get("nodes", []))
            edges = list(graph.get("edges", []))
            self._save(state)

        if not nodes:
            return "No mission graph context is available yet."

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
        payload = update_payload if isinstance(update_payload, dict) else {}
        nodes_payload = payload.get("nodes")
        edges_payload = payload.get("edges")
        created_nodes = 0
        created_edges = 0

        with self._lock:
            state = self._load()
            graph = self._ensure_session_graph(state, session_id)

            if isinstance(nodes_payload, list):
                for raw in nodes_payload:
                    if not isinstance(raw, dict):
                        continue
                    node_id = str(raw.get("id") or "").strip() or f"agent:{run_id}:{uuid.uuid4().hex[:10]}"
                    node = {
                        "id": node_id,
                        "type": str(raw.get("type") or DEFAULT_NODE_TYPE).strip() or DEFAULT_NODE_TYPE,
                        "label": self._trim_text(raw.get("label") or "Agent node", max_len=200),
                        "description": self._trim_text(raw.get("description") or "", max_len=1200),
                        "status": str(raw.get("status") or DEFAULT_STATUS).strip() or DEFAULT_STATUS,
                        "confidence": self._coerce_confidence(raw.get("confidence")),
                        "severity": str(raw.get("severity") or DEFAULT_SEVERITY).strip() or DEFAULT_SEVERITY,
                        "created_by": "agent",
                        "source": {"run_id": run_id, "event_type": "agent_update"},
                        "refs": raw.get("refs") if isinstance(raw.get("refs"), list) else [],
                        "data": raw.get("data") if isinstance(raw.get("data"), dict) else {},
                        "x": raw.get("x"),
                        "y": raw.get("y"),
                    }
                    normalized = self._normalize_node(node)
                    if not normalized:
                        continue
                    existing_idx = self._find_node_index(graph, normalized["id"])
                    if existing_idx < 0:
                        created_nodes += 1
                    self._upsert_runtime_node(graph, normalized)

            if isinstance(edges_payload, list):
                for raw in edges_payload:
                    if not isinstance(raw, dict):
                        continue
                    from_id = str(raw.get("from") or "").strip()
                    to_id = str(raw.get("to") or "").strip()
                    if not from_id or not to_id:
                        continue
                    edge = {
                        "id": str(raw.get("id") or "").strip()
                        or f"edge:{from_id}:{str(raw.get('type') or 'related').strip() or 'related'}:{to_id}",
                        "from": from_id,
                        "to": to_id,
                        "type": str(raw.get("type") or "related").strip() or "related",
                        "label": self._trim_text(raw.get("label") or "", max_len=180),
                        "created_by": "agent",
                        "data": raw.get("data") if isinstance(raw.get("data"), dict) else {},
                    }
                    normalized = self._normalize_edge(edge)
                    if not normalized:
                        continue
                    existing_idx = self._find_edge_index(graph, normalized["id"])
                    if existing_idx < 0:
                        created_edges += 1
                    self._upsert_runtime_edge(graph, normalized)

            self._touch_graph(graph)
            self._save(state)
            return {"nodes": created_nodes, "edges": created_edges}
