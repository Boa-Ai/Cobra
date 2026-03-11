import json
import os
import re
from typing import Any

from cobra_lite.config import (
    FINAL_RESPONSE_AUTH_TOKEN,
    GRAPH_FILE,
    OPENCLAW_GATEWAY_URL,
    SESSIONS_FILE,
    STATE_FILE,
)
from cobra_lite.services.gateway_client import (
    RunCancelledError,
    effective_gateway_url,
    extract_missing_provider,
    send_to_openclaw,
)
from cobra_lite.services.graph_store import GraphStore
from cobra_lite.services.session_store import SessionStore
from cobra_lite.services.state_store import JsonStateStore

GRAPH_UPDATE_BLOCK_RE = re.compile(r"<graph_update>\s*(.*?)\s*</graph_update>", re.IGNORECASE | re.DOTALL)
MISSION_OVERVIEW_BLOCK_RE = re.compile(r"<mission_overview>\s*(.*?)\s*</mission_overview>", re.IGNORECASE | re.DOTALL)
FINAL_RESPONSE_BLOCK_RE = re.compile(r"<final_response_json>\s*(.*?)\s*</final_response_json>", re.IGNORECASE | re.DOTALL)
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


class MissionRunError(RuntimeError):
    def __init__(self, payload: dict[str, Any]):
        self.payload = payload
        super().__init__(str(payload.get("message") or "Mission run failed."))


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
    except Exception:
        parsed = None

    cleaned = GRAPH_UPDATE_BLOCK_RE.sub("", raw, count=1).strip()
    return cleaned, parsed


def _extract_final_response_block(text: str) -> tuple[str, dict[str, Any] | None]:
    raw = str(text or "")
    match = FINAL_RESPONSE_BLOCK_RE.search(raw)
    if not match:
        return raw.strip(), None

    payload_raw = (match.group(1) or "").strip()
    parsed: dict[str, Any] | None = None
    try:
        loaded = json.loads(payload_raw)
        if isinstance(loaded, dict):
            parsed = loaded
    except Exception:
        parsed = None

    cleaned = FINAL_RESPONSE_BLOCK_RE.sub("", raw, count=1).strip()
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


def _normalize_vulnerability_entry(item: Any) -> dict[str, str] | None:
    if not isinstance(item, dict):
        return None
    severity = str(item.get("Severity") or item.get("severity") or "").strip()
    title = str(item.get("Title") or item.get("title") or "").strip()
    description = str(item.get("Description") or item.get("description") or "").strip()
    reproduce_steps = str(
        item.get("Reproduce steps")
        or item.get("reproduce_steps")
        or item.get("reproduceSteps")
        or item.get("steps")
        or ""
    ).strip()
    if not title:
        return None
    return {
        "Severity": severity or "Unknown",
        "Title": title,
        "Description": description,
        "Reproduce steps": reproduce_steps,
    }


def _build_final_response_payload(final_text: str, payload: dict[str, Any] | None, auth_token: str) -> dict[str, Any]:
    report_content = str((payload or {}).get("report_content") or "").strip() or str(final_text or "").strip()
    raw_vulnerabilities = (payload or {}).get("found_vulnerabilities")
    normalized_vulnerabilities: list[dict[str, str]] = []
    if isinstance(raw_vulnerabilities, list):
        for item in raw_vulnerabilities:
            normalized = _normalize_vulnerability_entry(item)
            if normalized is not None:
                normalized_vulnerabilities.append(normalized)

    return {
        "report_content": report_content,
        "found_vulnerabilities": normalized_vulnerabilities,
        "auth_token": auth_token,
    }


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


class MissionRunner:
    def __init__(self) -> None:
        self.state_store = JsonStateStore(STATE_FILE)
        self.session_store = SessionStore(SESSIONS_FILE)
        self.graph_store = GraphStore(GRAPH_FILE)

    def resolve_gateway_url(self) -> str:
        return effective_gateway_url(self.state_store.get_gateway_url())

    def _resolve_anthropic_key(self) -> str | None:
        env_key = str(os.getenv("ANTHROPIC_API_KEY") or "").strip()
        if env_key.lower() in {"your-anthropic-api-key", "changeme", "replace-me"}:
            return None
        if env_key:
            return env_key
        stored = self.state_store.get_provider_key("anthropic")
        if str(stored or "").strip().lower() in {"your-anthropic-api-key", "changeme", "replace-me"}:
            return None
        return stored

    def _missing_provider_payload(self, provider: str = "anthropic") -> dict[str, Any]:
        normalized_provider = (provider or "anthropic").strip().lower() or "anthropic"
        if normalized_provider == "anthropic":
            message = "Anthropic API key is required. Set ANTHROPIC_API_KEY in .env to continue."
        else:
            message = f"{normalized_provider.title()} API key is required. Configure it and retry."
        return {
            "ok": False,
            "code": "missing_provider_key",
            "provider": normalized_provider,
            "message": message,
        }

    def _classify_runtime_error(self, message: str) -> dict[str, Any]:
        raw = str(message or "").strip()
        text = raw.lower()
        missing_provider = extract_missing_provider(raw)
        if missing_provider:
            return self._missing_provider_payload(missing_provider)
        if ANTHROPIC_AUTH_INVALID_RE.search(raw):
            return {
                "ok": False,
                "code": "provider_auth_invalid",
                "provider": "anthropic",
                "message": "Anthropic authentication failed. Update ANTHROPIC_API_KEY and retry.",
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
                "message": "Gateway connectivity issue detected. Verify OPENCLAW_GATEWAY_URL/auth and retry.",
            }
        return {
            "ok": False,
            "code": "runtime_error",
            "message": raw or "Unknown runtime error.",
        }

    def _apply_graph_updates(
        self,
        session_id: str,
        graph_update_payload: dict[str, Any] | None,
        graph_suggestions: list[dict[str, str]],
    ) -> int:
        existing = self.graph_store.get_graph(session_id)
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
                    payload = {
                        "type": node_type,
                        "label": label,
                        "description": str(raw.get("description") or raw.get("why") or "").strip(),
                        "status": str(raw.get("status") or "new").strip() or "new",
                        "severity": str(raw.get("severity") or "info").strip() or "info",
                        "confidence": raw.get("confidence"),
                        "data": {"manual": True, "agent_auto": True},
                    }
                    try:
                        self.graph_store.create_node(session_id, payload, created_by="agent")
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
                self.graph_store.create_node(session_id, payload, created_by="agent")
                existing_keys.add(key)
                created += 1
            except Exception:
                continue

        return created

    def _finalize_agent_result(
        self,
        session_id: str,
        result: dict[str, Any],
        *,
        prompt_text: str,
        existing_overview: str,
    ) -> tuple[dict[str, Any], int]:
        final_text_raw = str(result.get("final_observation") or "Task completed.").strip()
        text_without_graph_block, graph_update = _extract_graph_update_block(final_text_raw)
        text_without_overview_block, mission_overview = _extract_mission_overview_block(text_without_graph_block)
        text_without_response_block, final_response_payload = _extract_final_response_block(text_without_overview_block)
        final_text, graph_suggestions = _extract_graph_suggestions(text_without_response_block)
        created_graph_nodes = self._apply_graph_updates(session_id, graph_update, graph_suggestions)
        result["final_observation"] = final_text
        result["final_response"] = _build_final_response_payload(
            final_text,
            final_response_payload,
            FINAL_RESPONSE_AUTH_TOKEN,
        )
        if not mission_overview:
            mission_overview = _build_structured_mission_overview(existing_overview, prompt_text, final_text)
        else:
            mission_overview = _normalize_mission_overview(existing_overview, prompt_text, mission_overview)

        overview_session = (
            self.session_store.update_overview(session_id, mission_overview)
            if mission_overview
            else self.session_store.get_session(session_id)
        ) or {}
        result["mission_overview"] = str(overview_session.get("overview") or "")
        result["mission_overview_updated_at"] = overview_session.get("overview_updated_at")
        return result, created_graph_nodes

    def _ensure_session_overview(self, session_id: str, session: dict[str, Any] | None = None) -> dict[str, Any] | None:
        current_session = session or self.session_store.get_session(session_id)
        if not current_session:
            return None
        existing_overview = str(current_session.get("overview") or "").strip()
        if existing_overview and _is_standard_mission_overview(existing_overview):
            return current_session
        synthesized = _backfill_session_overview(current_session)
        if not synthesized:
            return current_session
        return self.session_store.update_overview(session_id, synthesized) or current_session

    def run_prompt(
        self,
        prompt: str,
        *,
        session_id: str,
        progress_callback: Any | None = None,
    ) -> dict[str, Any]:
        prompt_text = str(prompt or "").strip()
        if not prompt_text:
            raise MissionRunError({"ok": False, "code": "invalid_prompt", "message": "Prompt cannot be empty."})

        active_session_id = str(session_id or "").strip() or "cobra-lite"
        self.session_store.append_message(active_session_id, "user", prompt_text)
        current_session = self._ensure_session_overview(
            active_session_id,
            self.session_store.get_session(active_session_id) or {},
        ) or {}

        anthropic_api_key = self._resolve_anthropic_key()
        if not anthropic_api_key:
            payload = self._missing_provider_payload("anthropic")
            self.session_store.append_message(active_session_id, "assistant", f"Error: {payload['message']}")
            raise MissionRunError(payload)

        try:
            result = send_to_openclaw(
                prompt=prompt_text,
                gateway_url=self.resolve_gateway_url(),
                session_id=active_session_id,
                anthropic_api_key=anthropic_api_key,
                graph_context=self.graph_store.build_context(active_session_id),
                mission_overview=str(current_session.get("overview") or ""),
                progress_callback=progress_callback,
            )
            result, created_graph_nodes = self._finalize_agent_result(
                active_session_id,
                result,
                prompt_text=prompt_text,
                existing_overview=str(current_session.get("overview") or ""),
            )
            final_observation = str(result.get("final_observation") or "Task completed.").strip() or "Task completed."
            self.session_store.append_message(active_session_id, "assistant", final_observation)
            return {
                "ok": True,
                "session_id": active_session_id,
                "gateway_url": self.resolve_gateway_url() or OPENCLAW_GATEWAY_URL,
                "result": result,
                "graph_nodes_created": created_graph_nodes,
            }
        except RunCancelledError:
            message = "Run stopped by user."
            self.session_store.append_message(active_session_id, "assistant", message)
            raise MissionRunError({"ok": False, "code": "run_cancelled", "message": message, "session_id": active_session_id})
        except MissionRunError:
            raise
        except Exception as exc:
            payload = self._classify_runtime_error(str(exc))
            payload["session_id"] = active_session_id
            self.session_store.append_message(active_session_id, "assistant", f"Error: {payload['message']}")
            raise MissionRunError(payload) from exc
