import json
import mimetypes
import os
import queue
import re
import socket
import threading
import uuid
from pathlib import Path
from typing import Any

from flask import Flask, Response, jsonify, render_template, request, stream_with_context

from cobra_lite.config import (
    BASE_DIR,
    GRAPH_FILE,
    OPENCLAW_GATEWAY_URL,
    OPENCLAW_STATE_DIR,
    SESSIONS_FILE,
    STATE_FILE,
)
from cobra_lite.services.gateway_client import (
    RunCancelledError,
    effective_gateway_url,
    extract_missing_provider,
    send_to_openclaw,
    verify_openclaw_connection,
)
from cobra_lite.services.graph_store import GraphStore
from cobra_lite.services.session_store import SessionStore
from cobra_lite.services.state_store import JsonStateStore


def _find_available_port(host: str, preferred_port: int, max_attempts: int = 50) -> int:
    for offset in range(max_attempts):
        candidate_port = preferred_port + offset
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            try:
                sock.bind((host, candidate_port))
            except OSError:
                continue
            return candidate_port

    raise RuntimeError(
        f"No available port found in range {preferred_port}-{preferred_port + max_attempts - 1}."
    )


def _sse_pack(event_type: str, payload: Any) -> str:
    if not isinstance(payload, dict):
        payload = {"value": payload}
    return f"event: {event_type}\n" f"data: {json.dumps(payload, ensure_ascii=False, default=str)}\n\n"


def _enqueue_progress(event_queue: queue.Queue, payload: dict[str, Any]) -> None:
    if not isinstance(payload, dict):
        event_queue.put({"type": "message", "data": payload})
        return
    event_queue.put(payload)


GRAPH_UPDATE_BLOCK_RE = re.compile(r"<graph_update>\s*(.*?)\s*</graph_update>", re.IGNORECASE | re.DOTALL)
MISSION_OVERVIEW_BLOCK_RE = re.compile(r"<mission_overview>\s*(.*?)\s*</mission_overview>", re.IGNORECASE | re.DOTALL)
GRAPH_SUGGESTIONS_HEADER_RE = re.compile(r"^#{0,3}\s*graph suggestions\s*:?\s*$", re.IGNORECASE)
ANTHROPIC_AUTH_INVALID_RE = re.compile(
    r"(invalid|incorrect|unauthorized|forbidden).*(x-api-key|api key)|authentication[_\s-]?error",
    re.IGNORECASE,
)
OVERVIEW_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?])\s+")
OVERVIEW_GENERIC_LINE_RE = re.compile(
    r"^(?:perfect!?|great!?|excellent!?|let me|here(?:'s| is)|i(?:'ve| have) completed|task completed|run complete)\b",
    re.IGNORECASE,
)
OVERVIEW_SEVERITY_HEADING_RE = re.compile(r"^(?:critical|high|medium|low|info)(?:\s+severity)?(?:\s*\(\d+\))?:?$", re.IGNORECASE)


def _extract_graph_update_block(text: str) -> tuple[str, dict[str, Any] | None]:
    raw = str(text or "")
    match = GRAPH_UPDATE_BLOCK_RE.search(raw)
    if not match:
        return raw.strip(), None

    payload_raw = (match.group(1) or "").strip()
    parsed: dict[str, Any] | None = None
    try:
        loaded = json.loads(payload_raw)
        if isinstance(loaded, dict):
            parsed = loaded
    except json.JSONDecodeError:
        parsed = None

    cleaned = GRAPH_UPDATE_BLOCK_RE.sub("", raw, count=1).strip()
    return cleaned, parsed


def _extract_graph_suggestions(text: str) -> tuple[str, list[dict[str, str]]]:
    source = str(text or "")
    lines = source.splitlines()
    start_idx = -1
    for idx, line in enumerate(lines):
        if GRAPH_SUGGESTIONS_HEADER_RE.match(line.strip()):
            start_idx = idx
            break
    if start_idx < 0:
        return source.strip(), []

    suggestions: list[dict[str, str]] = []
    end_idx = len(lines)
    for idx in range(start_idx + 1, len(lines)):
        raw_line = lines[idx]
        line = raw_line.strip()
        if not line:
            if suggestions:
                end_idx = idx + 1
                break
            continue
        if re.match(r"^#{1,6}\s+", line):
            end_idx = idx
            break
        if re.match(r"^[A-Z][A-Za-z0-9 _/-]{1,40}:$", line):
            end_idx = idx
            break

        content = re.sub(r"^[-*+]\s+", "", line)
        content = re.sub(r"^\d+\.\s+", "", content).strip()
        if not content:
            continue

        parsed: dict[str, str] | None = None
        match = re.match(r"^\[(.+?)\]\s+(.+?)(?:\s*[—\-|:]\s+(.+))?$", content)
        if match:
            parsed = {
                "type": str(match.group(1) or "Note").strip() or "Note",
                "label": str(match.group(2) or "Suggested node").strip() or "Suggested node",
                "why": str(match.group(3) or "").strip(),
            }
        else:
            parts = [str(item).strip() for item in content.split("|")]
            if len(parts) >= 2 and parts[0] and parts[1]:
                parsed = {
                    "type": parts[0],
                    "label": parts[1],
                    "why": " | ".join(part for part in parts[2:] if part),
                }

        if not parsed:
            continue
        if len(parsed["label"]) < 6:
            continue
        suggestions.append(parsed)
        if len(suggestions) >= 8:
            break

    deduped: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in suggestions:
        key = f"{item['type'].lower()}|{item['label'].lower()}"
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)

    cleaned_lines = lines[:start_idx] + lines[end_idx:]
    cleaned = "\n".join(cleaned_lines).strip()
    return cleaned, deduped


def _extract_mission_overview_block(text: str) -> tuple[str, str | None]:
    raw = str(text or "")
    match = MISSION_OVERVIEW_BLOCK_RE.search(raw)
    if not match:
        return raw.strip(), None
    overview = str(match.group(1) or "").strip()
    cleaned = MISSION_OVERVIEW_BLOCK_RE.sub("", raw, count=1).strip()
    return cleaned, overview or None


def _sanitize_overview_line(value: str) -> str:
    line = str(value or "").strip()
    if not line:
        return ""
    line = re.sub(r"^[-*+]\s+", "", line)
    line = re.sub(r"^\d+\.\s+", "", line)
    line = re.sub(r"^#{1,6}\s*", "", line)
    line = re.sub(r"\*\*([^*]+)\*\*", r"\1", line)
    line = re.sub(r"__([^_]+)__", r"\1", line)
    line = re.sub(r"(?<!\*)\*([^*]+)\*(?!\*)", r"\1", line)
    line = re.sub(r"`([^`]*)`", r"\1", line)
    line = re.sub(r"\s+", " ", line).strip(" -\t")
    return line


def _normalize_overview_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", _sanitize_overview_line(value).lower()).strip()


def _overview_section_aliases() -> dict[str, tuple[str, ...]]:
    return {
        "summary": ("high level summary",),
        "scope": ("scope", "objective", "focus"),
        "current_state": ("current state", "status", "findings"),
        "recommended_next_step": (
            "recommended next step",
            "recommended next steps",
            "next step",
            "next steps",
            "immediate",
            "short term",
            "short-term",
            "long term",
            "long-term",
        ),
        "assurance": ("positive findings", "no critical issues found", "assurance"),
    }


def _is_standard_mission_overview(text: str) -> bool:
    raw = str(text or "")
    patterns = (
        r"^##\s*High-Level Summary\s*$",
        r"^##\s*Scope\s*$",
        r"^##\s*Current State\s*$",
        r"^##\s*Recommended Next Step\s*$",
    )
    return all(re.search(pattern, raw, re.IGNORECASE | re.MULTILINE) for pattern in patterns)


def _looks_like_overview_heading(raw_line: str, cleaned_line: str) -> bool:
    raw = str(raw_line or "").strip()
    cleaned = _sanitize_overview_line(cleaned_line)
    if not raw or not cleaned:
        return False
    normalized = _normalize_overview_key(cleaned)
    if raw.lstrip().startswith("#"):
        return True
    if normalized in {alias for aliases in _overview_section_aliases().values() for alias in aliases}:
        return True
    if OVERVIEW_SEVERITY_HEADING_RE.match(cleaned):
        return True
    if cleaned.endswith(":") and len(cleaned) <= 44:
        return True
    return False


def _should_skip_overview_line(value: str) -> bool:
    line = _sanitize_overview_line(value)
    if not line:
        return True
    key = _normalize_overview_key(line)
    if not key:
        return True
    if key in {"task completed", "run complete", "actions taken", "tools used"}:
        return True
    if key in {alias for aliases in _overview_section_aliases().values() for alias in aliases}:
        return True
    if OVERVIEW_GENERIC_LINE_RE.match(line):
        return True
    if OVERVIEW_SEVERITY_HEADING_RE.match(line):
        return True
    if line.endswith(":") and len(line) <= 44:
        return True
    return False


def _overview_candidate_lines(text: str, *, max_items: int = 6) -> list[str]:
    candidates: list[str] = []
    for raw_line in str(text or "").splitlines():
        line = _sanitize_overview_line(raw_line)
        if not line:
            continue
        if _should_skip_overview_line(line):
            continue
        if len(line) < 18:
            continue
        candidates.append(line)
        if len(candidates) >= max_items:
            return candidates

    collapsed = re.sub(r"\s+", " ", str(text or "")).strip()
    if not collapsed:
        return candidates
    for sentence in OVERVIEW_SENTENCE_SPLIT_RE.split(collapsed):
        line = _sanitize_overview_line(sentence)
        if not line or _should_skip_overview_line(line) or len(line) < 18:
            continue
        candidates.append(line.rstrip(" :;"))
        if len(candidates) >= max_items:
            break
    return candidates


def _dedupe_lines(lines: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for raw in lines:
        line = _sanitize_overview_line(raw)
        if not line:
            continue
        key = re.sub(r"[^a-z0-9]+", " ", line.lower()).strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(line)
    return out


def _extract_overview_section_lines(
    text: str,
    section_name: str,
    *,
    max_items: int = 4,
    stop_sections: tuple[str, ...] = (),
    allow_nested_headings: bool = False,
) -> list[str]:
    aliases = _overview_section_aliases()
    alias_keys = {_normalize_overview_key(value) for value in aliases.get(section_name, ())}
    stop_keys = {_normalize_overview_key(value) for stop_name in stop_sections for value in aliases.get(stop_name, ())}
    if not alias_keys:
        return []

    collected: list[str] = []
    collecting = False
    for raw_line in str(text or "").splitlines():
        line = _sanitize_overview_line(raw_line)
        if not line:
            continue
        key = _normalize_overview_key(line)
        if key in alias_keys:
            collecting = True
            continue
        if not collecting:
            continue
        if key in stop_keys:
            break
        if _looks_like_overview_heading(raw_line, line) and key not in alias_keys:
            if allow_nested_headings:
                continue
            break
        if _should_skip_overview_line(line):
            continue
        collected.append(line)
        if len(collected) >= max_items:
            break
    return _dedupe_lines(collected)


def _ensure_sentence(value: str) -> str:
    text = str(value or "").strip().rstrip(";")
    if not text:
        return ""
    if text[-1] not in ".!?":
        text += "."
    return text


def _build_structured_mission_overview(existing_overview: str, prompt: str, final_text: str) -> str:
    prompt_line = _sanitize_overview_line(prompt)
    scope_lines = _dedupe_lines(
        ([f"Mission: {prompt_line}"] if prompt_line else [])
        + _extract_overview_section_lines(existing_overview, "scope", max_items=2)
        + _extract_overview_section_lines(final_text, "scope", max_items=2, stop_sections=("current_state", "recommended_next_step"))
    )
    current_state_lines = _dedupe_lines(
        _extract_overview_section_lines(
            final_text,
            "current_state",
            max_items=4,
            stop_sections=("recommended_next_step",),
            allow_nested_headings=True,
        )
        + _extract_overview_section_lines(existing_overview, "current_state", max_items=3)
    )
    if not current_state_lines:
        current_state_lines = _dedupe_lines(
            _overview_candidate_lines(final_text, max_items=4) + _overview_candidate_lines(existing_overview, max_items=2)
        )

    next_step_lines = _dedupe_lines(
        _extract_overview_section_lines(
            final_text,
            "recommended_next_step",
            max_items=3,
            stop_sections=("current_state", "scope", "assurance"),
            allow_nested_headings=True,
        )
        + _extract_overview_section_lines(existing_overview, "recommended_next_step", max_items=3)
    )
    assurance_lines = _dedupe_lines(
        _extract_overview_section_lines(
            final_text,
            "assurance",
            max_items=2,
            stop_sections=("recommended_next_step",),
            allow_nested_headings=True,
        )
        + _extract_overview_section_lines(existing_overview, "assurance", max_items=2)
    )

    if not scope_lines and prompt_line:
        scope_lines = [f"Mission: {prompt_line}"]
    if not current_state_lines and prompt_line:
        current_state_lines = [f"Mission is active for: {prompt_line}"]
    if not next_step_lines:
        next_step_lines = (
            [f"Validate and prioritize the highest-signal item currently tracked: {current_state_lines[0]}"]
            if current_state_lines
            else ["Continue the mission and refresh this overview after the next completed run."]
        )

    summary_parts: list[str] = []
    if prompt_line:
        summary_parts.append(_ensure_sentence(f"Mission focus: {prompt_line}"))
    if current_state_lines:
        summary_parts.append(_ensure_sentence(f"Current state: {current_state_lines[0]}"))
    if assurance_lines:
        summary_parts.append(_ensure_sentence(f"Assurance: {assurance_lines[0]}"))

    summary = " ".join(part for part in summary_parts if part).strip()
    if not summary:
        fallback_summary = _overview_candidate_lines(final_text, max_items=1) or _overview_candidate_lines(existing_overview, max_items=1)
        summary = _ensure_sentence(fallback_summary[0] if fallback_summary else "Mission is active and awaiting more signal")

    lines = [
        "## High-Level Summary",
        summary,
        "",
        "## Scope",
        *[f"- {line}" for line in scope_lines[:3]],
        "",
        "## Current State",
        *[f"- {line}" for line in current_state_lines[:4]],
        "",
        "## Recommended Next Step",
        *[f"- {line}" for line in next_step_lines[:3]],
    ]
    return "\n".join(lines).strip()


def _normalize_mission_overview(existing_overview: str, prompt: str, overview_text: str) -> str:
    raw = str(overview_text or "").strip()
    if not raw:
        return ""
    if _is_standard_mission_overview(raw):
        return raw
    return _build_structured_mission_overview(existing_overview, prompt, raw)


def _latest_session_message(session: dict[str, Any], role: str) -> str:
    messages = session.get("messages")
    if not isinstance(messages, list):
        return ""
    normalized_role = str(role or "").strip().lower()
    for raw in reversed(messages):
        if not isinstance(raw, dict):
            continue
        if str(raw.get("role") or "").strip().lower() != normalized_role:
            continue
        text = str(raw.get("content") or "").strip()
        if text:
            return text
    return ""


def _backfill_session_overview(session: dict[str, Any]) -> str:
    existing_overview = str(session.get("overview") or "").strip()
    if existing_overview and _is_standard_mission_overview(existing_overview):
        return existing_overview
    latest_user = _latest_session_message(session, "user")
    latest_assistant = _latest_session_message(session, "assistant")
    source_text = latest_assistant or existing_overview
    if not source_text:
        return ""
    return _build_structured_mission_overview(existing_overview, latest_user, source_text)


def create_app() -> Flask:
    app = Flask(
        __name__,
        template_folder=str(BASE_DIR / "templates"),
        static_folder=str(BASE_DIR / "static"),
        static_url_path="/static",
    )
    app.secret_key = os.getenv("FLASK_SECRET_KEY", "cobra-lite-dev-secret")

    state_store = JsonStateStore(STATE_FILE)
    session_store = SessionStore(SESSIONS_FILE)
    graph_store = GraphStore(GRAPH_FILE)
    workspace_dir_override = str(os.getenv("OPENCLAW_WORKSPACE_DIR") or "").strip()
    file_read_limit = 256_000
    list_limit = 1000
    run_control_lock = threading.Lock()
    run_cancel_events: dict[str, threading.Event] = {}
    session_active_runs: dict[str, str] = {}

    def _register_run_control(session_id: str, run_id: str) -> threading.Event:
        event = threading.Event()
        with run_control_lock:
            run_cancel_events[run_id] = event
            session_active_runs[session_id] = run_id
        return event

    def _cleanup_run_control(session_id: str, run_id: str) -> None:
        with run_control_lock:
            run_cancel_events.pop(run_id, None)
            if session_active_runs.get(session_id) == run_id:
                session_active_runs.pop(session_id, None)

    def _cancel_run(session_id: str | None, run_id: str | None) -> tuple[bool, str]:
        target_event: threading.Event | None = None
        resolved_run_id = ""
        resolved_session_id = str(session_id or "").strip()
        requested_run_id = str(run_id or "").strip()

        with run_control_lock:
            if requested_run_id:
                target_event = run_cancel_events.get(requested_run_id)
                resolved_run_id = requested_run_id
            elif resolved_session_id:
                active_run_id = session_active_runs.get(resolved_session_id, "")
                if active_run_id:
                    target_event = run_cancel_events.get(active_run_id)
                    resolved_run_id = active_run_id
            else:
                return False, "Missing session_id or run_id."

        if not target_event:
            return False, "No active run found to stop."
        target_event.set()
        if resolved_session_id:
            return True, f"Stop requested for {resolved_run_id} in session {resolved_session_id}."
        return True, f"Stop requested for {resolved_run_id}."

    def _resolve_workspace_root() -> Path:
        candidates: list[Path] = []
        if workspace_dir_override:
            candidates.append(Path(workspace_dir_override).expanduser())
        candidates.append(Path(OPENCLAW_STATE_DIR).expanduser() / "workspace")
        candidates.append(Path.home() / ".openclaw" / "workspace")

        for candidate in candidates:
            try:
                if candidate.exists() and candidate.is_dir():
                    return candidate.resolve()
            except OSError:
                continue
        primary = candidates[0] if candidates else (Path.home() / ".openclaw" / "workspace")
        return primary.resolve()

    def _ensure_workspace_root_exists() -> Path:
        root = _resolve_workspace_root()
        root.mkdir(parents=True, exist_ok=True)
        return root

    def _workspace_path_from_relative(relative_path: str | None) -> Path:
        root = _ensure_workspace_root_exists()
        rel = str(relative_path or "").strip().replace("\\", "/")
        rel = rel[1:] if rel.startswith("/") else rel
        target = (root / rel).resolve()
        if target != root and root not in target.parents:
            raise ValueError("Invalid path.")
        return target

    def _relative_workspace_path(path_obj: Path) -> str:
        root = _resolve_workspace_root()
        if path_obj == root:
            return ""
        return str(path_obj.relative_to(root)).replace("\\", "/")

    def _is_binary(path_obj: Path) -> bool:
        try:
            sample = path_obj.read_bytes()[:8192]
        except OSError:
            return True
        if not sample:
            return False
        return b"\x00" in sample

    @app.get("/api/files/root")
    def files_root():
        root = _ensure_workspace_root_exists()
        return jsonify(
            {
                "ok": True,
                "workspace_root": str(root),
                "exists": True,
            }
        )

    @app.get("/api/files/list")
    def files_list():
        rel_path = request.args.get("path")
        try:
            target = _workspace_path_from_relative(rel_path)
        except ValueError:
            return jsonify({"ok": False, "message": "Invalid path."}), 400

        if not target.exists():
            return jsonify({"ok": False, "message": "Path not found."}), 404
        if not target.is_dir():
            return jsonify({"ok": False, "message": "Path is not a directory."}), 400

        entries: list[dict[str, Any]] = []
        try:
            children = sorted(
                list(target.iterdir()),
                key=lambda item: (not item.is_dir(), item.name.lower()),
            )[:list_limit]
        except OSError as exc:
            return jsonify({"ok": False, "message": str(exc)}), 500

        for child in children:
            try:
                stat = child.stat()
            except OSError:
                continue
            entries.append(
                {
                    "name": child.name,
                    "path": _relative_workspace_path(child),
                    "type": "dir" if child.is_dir() else "file",
                    "size": stat.st_size if child.is_file() else None,
                    "modified_at": int(stat.st_mtime),
                }
            )

        return jsonify(
            {
                "ok": True,
                "workspace_root": str(_resolve_workspace_root()),
                "path": _relative_workspace_path(target),
                "parent_path": _relative_workspace_path(target.parent) if target != _resolve_workspace_root() else None,
                "entries": entries,
            }
        )

    @app.get("/api/files/read")
    def files_read():
        rel_path = request.args.get("path")
        try:
            target = _workspace_path_from_relative(rel_path)
        except ValueError:
            return jsonify({"ok": False, "message": "Invalid path."}), 400

        if not target.exists():
            return jsonify({"ok": False, "message": "File not found."}), 404
        if not target.is_file():
            return jsonify({"ok": False, "message": "Path is not a file."}), 400

        try:
            stat = target.stat()
        except OSError as exc:
            return jsonify({"ok": False, "message": str(exc)}), 500

        mime_type, _ = mimetypes.guess_type(str(target))
        is_binary = _is_binary(target)
        if is_binary:
            return jsonify(
                {
                    "ok": True,
                    "path": _relative_workspace_path(target),
                    "name": target.name,
                    "size": stat.st_size,
                    "mime_type": mime_type or "application/octet-stream",
                    "is_binary": True,
                    "content": "",
                    "truncated": False,
                }
            )

        try:
            raw_bytes = target.read_bytes()
        except OSError as exc:
            return jsonify({"ok": False, "message": str(exc)}), 500

        truncated = len(raw_bytes) > file_read_limit
        preview_bytes = raw_bytes[:file_read_limit]
        try:
            content = preview_bytes.decode("utf-8")
        except UnicodeDecodeError:
            content = preview_bytes.decode("utf-8", errors="replace")

        return jsonify(
            {
                "ok": True,
                "path": _relative_workspace_path(target),
                "name": target.name,
                "size": stat.st_size,
                "mime_type": mime_type or "text/plain",
                "is_binary": False,
                "content": content,
                "truncated": truncated,
            }
        )

    def _resolve_anthropic_key() -> str | None:
        env_key = str(os.getenv("ANTHROPIC_API_KEY") or "").strip()
        if env_key:
            return env_key
        return state_store.get_provider_key("anthropic")

    def _missing_provider_payload(provider: str = "anthropic") -> dict[str, Any]:
        normalized_provider = (provider or "anthropic").strip().lower() or "anthropic"
        if normalized_provider == "anthropic":
            message = "Anthropic API key is required. Add your key in settings to continue."
        else:
            message = f'{normalized_provider.title()} API key is required. Add your key in settings to continue.'
        return {
            "ok": False,
            "code": "missing_provider_key",
            "provider": normalized_provider,
            "message": message,
        }

    def _classify_runtime_error(message: str) -> dict[str, Any]:
        raw = str(message or "").strip()
        text = raw.lower()
        missing_provider = extract_missing_provider(raw)
        if missing_provider:
            return _missing_provider_payload(missing_provider)
        if ANTHROPIC_AUTH_INVALID_RE.search(raw):
            return {
                "ok": False,
                "code": "provider_auth_invalid",
                "provider": "anthropic",
                "message": "Anthropic authentication failed. Update your API key in settings and retry.",
            }
        if "emitted no terminal actions" in text or "no terminal actions were emitted by the gateway run" in text:
            return {
                "ok": False,
                "code": "terminal_telemetry_missing",
                "message": (
                    "Gateway run completed without terminal telemetry. "
                    "Check gateway run configuration, provider auth, and tool permissions."
                ),
            }
        if (
            "websocket transport is unavailable" in text
            or "gateway connect failed" in text
            or "cannot reach gateway" in text
            or "connection refused" in text
            or "timed out" in text
            or "name or service not known" in text
        ):
            return {
                "ok": False,
                "code": "gateway_connectivity",
                "message": "Gateway connectivity issue detected. Verify gateway URL/auth and retry.",
            }
        return {
            "ok": False,
            "code": "runtime_error",
            "message": raw or "Unknown runtime error.",
        }

    def _apply_graph_updates(
        session_id: str,
        graph_update_payload: dict[str, Any] | None,
        graph_suggestions: list[dict[str, str]],
    ) -> int:
        existing = graph_store.get_graph(session_id)
        existing_nodes = existing.get("nodes") if isinstance(existing, dict) else []
        existing_keys = {
            f"{str(node.get('type') or '').strip().lower()}|{str(node.get('label') or '').strip().lower()}"
            for node in (existing_nodes if isinstance(existing_nodes, list) else [])
            if isinstance(node, dict)
        }

        created = 0

        if isinstance(graph_update_payload, dict):
            raw_nodes = graph_update_payload.get("nodes")
            if isinstance(raw_nodes, list):
                for raw in raw_nodes:
                    if not isinstance(raw, dict):
                        continue
                    node_type = str(raw.get("type") or "Note").strip() or "Note"
                    label = str(raw.get("label") or "").strip()
                    if len(label) < 6:
                        continue
                    key = f"{node_type.lower()}|{label.lower()}"
                    if key in existing_keys:
                        continue
                    description = str(raw.get("description") or raw.get("why") or "").strip()
                    status = str(raw.get("status") or "new").strip() or "new"
                    severity = str(raw.get("severity") or "info").strip() or "info"
                    confidence = raw.get("confidence")
                    payload = {
                        "type": node_type,
                        "label": label,
                        "description": description,
                        "status": status,
                        "severity": severity,
                        "confidence": confidence,
                        "data": {"manual": True, "agent_auto": True},
                    }
                    try:
                        graph_store.create_node(session_id, payload, created_by="agent")
                        existing_keys.add(key)
                        created += 1
                    except Exception:
                        continue

        for item in graph_suggestions:
            node_type = str(item.get("type") or "Note").strip() or "Note"
            label = str(item.get("label") or "").strip()
            if len(label) < 6:
                continue
            key = f"{node_type.lower()}|{label.lower()}"
            if key in existing_keys:
                continue
            payload = {
                "type": node_type,
                "label": label,
                "description": str(item.get("why") or "").strip(),
                "status": "new",
                "severity": "info",
                "confidence": 0.6,
                "data": {"manual": True, "agent_auto": True},
            }
            try:
                graph_store.create_node(session_id, payload, created_by="agent")
                existing_keys.add(key)
                created += 1
            except Exception:
                continue

        return created

    def _finalize_agent_result(
        session_id: str,
        result: dict[str, Any],
        *,
        prompt_text: str,
        existing_overview: str,
    ) -> tuple[dict[str, Any], int]:
        final_text_raw = str(result.get("final_observation") or "Task completed.").strip()
        text_without_graph_block, graph_update = _extract_graph_update_block(final_text_raw)
        text_without_overview_block, mission_overview = _extract_mission_overview_block(text_without_graph_block)
        final_text, graph_suggestions = _extract_graph_suggestions(text_without_overview_block)
        created_graph_nodes = _apply_graph_updates(session_id, graph_update, graph_suggestions)
        result["final_observation"] = final_text
        if not mission_overview:
            mission_overview = _build_structured_mission_overview(existing_overview, prompt_text, final_text)
        else:
            mission_overview = _normalize_mission_overview(existing_overview, prompt_text, mission_overview)

        overview_session = (
            session_store.update_overview(session_id, mission_overview)
            if mission_overview
            else session_store.get_session(session_id)
        ) or {}
        result["mission_overview"] = str(overview_session.get("overview") or "")
        result["mission_overview_updated_at"] = overview_session.get("overview_updated_at")
        return result, created_graph_nodes

    def _ensure_session_overview(session_id: str, session: dict[str, Any] | None = None) -> dict[str, Any] | None:
        current_session = session or session_store.get_session(session_id)
        if not current_session:
            return None
        existing_overview = str(current_session.get("overview") or "").strip()
        if existing_overview and _is_standard_mission_overview(existing_overview):
            return current_session
        synthesized = _backfill_session_overview(current_session)
        if not synthesized:
            return current_session
        return session_store.update_overview(session_id, synthesized) or current_session

    def _resolve_session_id(payload: dict[str, Any]) -> str:
        requested = str(payload.get("session_id") or "").strip()
        if requested and session_store.get_session(requested):
            session_store.set_last_session_id(requested)
            return requested

        last_id = session_store.get_last_session_id()
        if last_id and session_store.get_session(last_id):
            return last_id

        created = session_store.create_session()
        return str(created.get("id"))

    @app.get("/")
    def index() -> str:
        saved_gateway = state_store.get_gateway_url()
        gateway = saved_gateway or OPENCLAW_GATEWAY_URL
        return render_template(
            "index.html",
            has_gateway=bool(saved_gateway),
            has_anthropic_key=bool(_resolve_anthropic_key()),
            default_gateway_url=OPENCLAW_GATEWAY_URL,
            saved_gateway_url=gateway,
        )

    @app.get("/api/sessions")
    def list_sessions():
        sessions = session_store.list_sessions()
        return jsonify(
            {
                "ok": True,
                "sessions": sessions,
                "last_session_id": session_store.get_last_session_id(),
            }
        )

    @app.post("/api/sessions")
    def create_session():
        payload = request.get_json(silent=True) or {}
        title = str(payload.get("title") or "").strip() or None
        session = session_store.create_session(title=title)
        graph_store.get_graph(str(session.get("id")))
        return jsonify({"ok": True, "session": session})

    @app.get("/api/sessions/<session_id>")
    def get_session(session_id: str):
        session = session_store.get_session(session_id)
        if not session:
            return jsonify({"ok": False, "message": "Session not found."}), 404
        session = _ensure_session_overview(session_id, session) or session
        session_store.set_last_session_id(session_id)
        return jsonify({"ok": True, "session": session})

    @app.delete("/api/sessions/<session_id>")
    def delete_session(session_id: str):
        deleted = session_store.delete_session(session_id)
        if not deleted:
            return jsonify({"ok": False, "message": "Session not found."}), 404
        graph_store.delete_session(session_id)
        return jsonify({"ok": True, "message": "Session deleted."})

    @app.patch("/api/sessions/<session_id>/execution-html")
    @app.post("/api/sessions/<session_id>/execution-html")
    def patch_session_execution_html(session_id: str):
        payload = request.get_json(silent=True) or {}
        html = payload.get("html")
        if not isinstance(html, str):
            return jsonify({"ok": False, "message": "Invalid payload: 'html' must be a string."}), 400
        session = session_store.update_execution_html(session_id, html)
        if not session:
            return jsonify({"ok": False, "message": "Session not found."}), 404
        return jsonify({"ok": True, "session_id": session_id, "updated_at": session.get("updated_at")})

    @app.get("/api/graph/<session_id>")
    def get_graph(session_id: str):
        if not session_store.get_session(session_id):
            return jsonify({"ok": False, "message": "Session not found."}), 404
        graph = graph_store.get_graph(session_id)
        return jsonify({"ok": True, **graph})

    @app.get("/api/graph/context/<session_id>")
    def get_graph_context(session_id: str):
        if not session_store.get_session(session_id):
            return jsonify({"ok": False, "message": "Session not found."}), 404
        context_text = graph_store.build_context(session_id)
        return jsonify({"ok": True, "session_id": session_id, "context": context_text})

    @app.post("/api/graph/<session_id>/nodes")
    def create_graph_node(session_id: str):
        if not session_store.get_session(session_id):
            return jsonify({"ok": False, "message": "Session not found."}), 404
        payload = request.get_json(silent=True) or {}
        try:
            node = graph_store.create_node(session_id, payload, created_by="user")
        except ValueError as exc:
            return jsonify({"ok": False, "message": str(exc)}), 400
        return jsonify({"ok": True, "node": node})

    @app.patch("/api/graph/<session_id>/nodes/<node_id>")
    def patch_graph_node(session_id: str, node_id: str):
        if not session_store.get_session(session_id):
            return jsonify({"ok": False, "message": "Session not found."}), 404
        payload = request.get_json(silent=True) or {}
        node = graph_store.patch_node(session_id, node_id, payload)
        if not node:
            return jsonify({"ok": False, "message": "Node not found or invalid payload."}), 404
        return jsonify({"ok": True, "node": node})

    @app.post("/api/graph/<session_id>/edges")
    def create_graph_edge(session_id: str):
        if not session_store.get_session(session_id):
            return jsonify({"ok": False, "message": "Session not found."}), 404
        payload = request.get_json(silent=True) or {}
        try:
            edge = graph_store.create_edge(session_id, payload, created_by="user")
        except ValueError as exc:
            return jsonify({"ok": False, "message": str(exc)}), 400
        return jsonify({"ok": True, "edge": edge})

    @app.post("/api/verify-gateway")
    def verify_gateway():
        payload = request.get_json(silent=True) or {}
        gateway_url = (payload.get("gateway_url") or "").strip()

        if not gateway_url:
            gateway_url = OPENCLAW_GATEWAY_URL

        is_valid, message = verify_openclaw_connection(gateway_url)
        if not is_valid:
            return jsonify({"ok": False, "message": message}), 400

        state_store.set_gateway_url(gateway_url)
        return jsonify({"ok": True, "message": message})

    @app.get("/api/auth-status")
    def auth_status():
        return jsonify(
            {
                "ok": True,
                "providers": {
                    "anthropic": {
                        "configured": bool(_resolve_anthropic_key()),
                    }
                },
            }
        )

    @app.post("/api/auth/anthropic")
    def save_anthropic_key():
        payload = request.get_json(silent=True) or {}
        api_key = str(payload.get("api_key") or "").strip()
        if not api_key:
            return jsonify({"ok": False, "message": "API key cannot be empty."}), 400
        state_store.set_provider_key("anthropic", api_key)
        return jsonify({"ok": True, "message": "Anthropic key saved."})

    @app.post("/api/prompt/stop")
    def stop_prompt_run():
        payload = request.get_json(silent=True) or {}
        session_id = str(payload.get("session_id") or "").strip()
        run_id = str(payload.get("run_id") or "").strip()
        ok, message = _cancel_run(session_id if session_id else None, run_id if run_id else None)
        status_code = 200 if ok else 404
        return jsonify({"ok": ok, "message": message, "session_id": session_id or None, "run_id": run_id or None}), status_code

    @app.post("/api/prompt")
    def submit_prompt():
        payload = request.get_json(silent=True) or {}
        prompt = (payload.get("prompt") or "").strip()
        if not prompt:
            return jsonify({"ok": False, "message": "Prompt cannot be empty."}), 400

        session_id = _resolve_session_id(payload)
        session_store.append_message(session_id, "user", prompt)
        current_session = _ensure_session_overview(session_id, session_store.get_session(session_id) or {}) or {}
        gateway_url = effective_gateway_url(state_store.get_gateway_url())
        anthropic_api_key = _resolve_anthropic_key()
        if not anthropic_api_key:
            missing_payload = _missing_provider_payload("anthropic")
            session_store.append_message(session_id, "assistant", f"Error: {missing_payload['message']}")
            return jsonify({**missing_payload, "session_id": session_id}), 400

        run_id = f"run-{uuid.uuid4().hex[:12]}"
        cancel_event = _register_run_control(session_id, run_id)
        graph_context = graph_store.build_context(session_id)

        try:
            result = send_to_openclaw(
                prompt=prompt,
                gateway_url=gateway_url,
                session_id=session_id,
                anthropic_api_key=anthropic_api_key,
                graph_context=graph_context,
                mission_overview=str(current_session.get("overview") or ""),
                cancel_event=cancel_event,
            )
            result, created_graph_nodes = _finalize_agent_result(
                session_id,
                result,
                prompt_text=prompt,
                existing_overview=str(current_session.get("overview") or ""),
            )
            session_store.append_message(session_id, "assistant", str(result.get("final_observation") or "Task completed."))
            return jsonify(
                {
                    "ok": True,
                    "message": "Prompt accepted.",
                    "session_id": session_id,
                    "run_id": run_id,
                    "result": result,
                    "graph_nodes_created": created_graph_nodes,
                }
            )
        except RunCancelledError:
            message = "Run stopped by user."
            session_store.append_message(session_id, "assistant", message)
            return jsonify({"ok": False, "session_id": session_id, "run_id": run_id, "message": message}), 499
        except Exception as e:
            classified = _classify_runtime_error(str(e))
            session_store.append_message(session_id, "assistant", f"Error: {classified['message']}")
            status_code = 400 if classified.get("code") in {"missing_provider_key", "provider_auth_invalid"} else 500
            return jsonify({**classified, "session_id": session_id}), status_code
        finally:
            _cleanup_run_control(session_id, run_id)

    @app.post("/api/prompt/stream")
    def submit_prompt_stream():
        payload = request.get_json(silent=True) or {}
        prompt = (payload.get("prompt") or "").strip()
        if not prompt:
            return jsonify({"ok": False, "message": "Prompt cannot be empty."}), 400

        session_id = _resolve_session_id(payload)
        session_store.append_message(session_id, "user", prompt)
        current_session = _ensure_session_overview(session_id, session_store.get_session(session_id) or {}) or {}
        gateway_url = effective_gateway_url(state_store.get_gateway_url())
        anthropic_api_key = _resolve_anthropic_key()
        if not anthropic_api_key:
            missing_payload = _missing_provider_payload("anthropic")
            session_store.append_message(session_id, "assistant", f"Error: {missing_payload['message']}")
            return jsonify({**missing_payload, "session_id": session_id}), 400

        run_id = f"run-{uuid.uuid4().hex[:12]}"
        cancel_event = _register_run_control(session_id, run_id)
        graph_context = graph_store.build_context(session_id)

        events: queue.Queue = queue.Queue()

        def emit(event: dict[str, Any]) -> None:
            payload = event if isinstance(event, dict) else {"type": "message", "data": event}
            event_type = str(payload.get("type") or "message").strip() or "message"
            data = payload.get("data") if isinstance(payload.get("data"), dict) else {}
            tagged_data = {**data, "session_id": session_id, "run_id": run_id}
            _enqueue_progress(events, {"type": event_type, "data": tagged_data})

        def execute() -> None:
            try:
                result = send_to_openclaw(
                    prompt=prompt,
                    gateway_url=gateway_url,
                    session_id=session_id,
                    anthropic_api_key=anthropic_api_key,
                    graph_context=graph_context,
                    mission_overview=str(current_session.get("overview") or ""),
                    progress_callback=emit,
                    cancel_event=cancel_event,
                )
                result, created_graph_nodes = _finalize_agent_result(
                    session_id,
                    result,
                    prompt_text=prompt,
                    existing_overview=str(current_session.get("overview") or ""),
                )
                session_store.append_message(session_id, "assistant", str(result.get("final_observation") or "Task completed."))
                if created_graph_nodes > 0:
                    emit({"type": "graph_updated", "data": {"created_nodes": created_graph_nodes}})
                emit({"type": "final_result", "data": {"result": result, "session_id": session_id, "run_id": run_id}})
                emit({"type": "done", "data": {"ok": True, "session_id": session_id, "run_id": run_id}})
                events.put(None)
            except RunCancelledError:
                message = "Run stopped by user."
                emit({"type": "cancelled", "data": {"message": message, "session_id": session_id, "run_id": run_id}})
                emit({"type": "done", "data": {"ok": False, "message": message, "session_id": session_id, "run_id": run_id}})
                events.put(None)
            except Exception as exc:
                classified = _classify_runtime_error(str(exc))
                session_store.append_message(session_id, "assistant", f"Error: {classified['message']}")
                emit(
                    {
                        "type": "error",
                        "data": {
                            "message": classified["message"],
                            "code": classified.get("code", "runtime_error"),
                            "provider": classified.get("provider", ""),
                            "session_id": session_id,
                            "run_id": run_id,
                        },
                    }
                )
                emit({"type": "done", "data": {"ok": False, "session_id": session_id, "run_id": run_id}})
                events.put(None)
            finally:
                _cleanup_run_control(session_id, run_id)

        thread = threading.Thread(target=execute, daemon=True)
        thread.start()

        def event_stream():
            while True:
                event = events.get()
                if event is None:
                    break
                event_type = str(event.get("type", "message")).strip() or "message"
                yield _sse_pack(event_type, event.get("data"))

        response = Response(stream_with_context(event_stream()), mimetype="text/event-stream")
        response.headers["Cache-Control"] = "no-cache"
        response.headers["X-Accel-Buffering"] = "no"
        return response

    return app


def run_server(app: Flask) -> None:
    debug_mode = os.getenv("FLASK_DEBUG", "0") == "1"
    host = os.getenv("HOST", "127.0.0.1")
    requested_port = int(os.getenv("PORT", "5001"))
    port = _find_available_port(host=host, preferred_port=requested_port)

    print("\n" + "=" * 70)
    print("🦅 Cobra Lite - Security Testing Interface")
    print("=" * 70)
    print(f"🌐 Web UI: http://{host}:{port}")
    print(f"🔧 Gateway: {OPENCLAW_GATEWAY_URL}")
    print("=" * 70 + "\n")

    if port != requested_port:
        print(f"⚠️  Port {requested_port} is busy. Using port {port} instead.\n")

    app.run(host=host, port=port, debug=debug_mode)
